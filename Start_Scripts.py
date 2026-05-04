#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Start_Scripts.py

Workflow-Launcher:
- Lädt zentrale config.yaml über config_loader
- Liest run_scripts-Liste und startet die jeweiligen Teilskripte
- Systemunabhängig: keine festen Laufwerksbuchstaben

Workflow-Schritte:
  01. Prompts & Tagesordner erstellen
  02. Marketing CSV + Plattform-Listings generieren
  03. Marketing Ordner erstellen
  04. Bilder bei Leonardo generieren + URLs/Pfade in pending.json speichern
  05. Bilder umbenennen + Pfade in pending.json aktualisieren
  06. Workflow pausieren für manuelle Sichtkontrolle (Sentinel-Lock-Datei)
  07a. MusicGen → Musik für Videos generieren
  07. FFmpeg → Video für Metricool erstellen (mit Musik eingemischt)
  08. YouTube Upload
  09. Gelöschte Bilder filtern + Upscaling der verbliebenen Bilder
  10. Etsy Listing
  11. Meta Video Post (FB + IG Reels)
"""

import os
import sys
import csv
import subprocess
import shutil
import atexit
import tempfile
import re
from pathlib import Path
from datetime import datetime, timedelta
import json

# zoneinfo für Berlin-Lokalzeit (Pre-Flight-Logging, Konsistenz mit
# publisher/reel_lifecycle_scheduler.py seit 2026-04-19).
from zoneinfo import ZoneInfo
BERLIN_TZ = ZoneInfo("Europe/Berlin")

from config_loader import (
    load_config,
    get_day_folder,
    load_master_listings,
    save_master_listings,
    find_master_item,
    atomic_write_json,
)

# =============================================================================
# CLI-Flag-Parsing (ab 2026-04-26: Pipeline-Split Vorabend / Morgen)
# =============================================================================
# Flags MÜSSEN vor load_config() ausgewertet werden, weil sie PIPELINE_CONFIG
# und PIPELINE_TARGET_DATE als Env-Variablen setzen, die der config_loader liest.
#
# Unterstützte Flags:
#   --staging                    → config.staging.yaml (bestehend)
#   --evening                    → config.evening.yaml (neu, Vorabend-Block)
#   --morning                    → config.morning.yaml (neu, Morgen-Block)
#   --resume                     → Vorabend-Wiederaufnahme (nur Step 06)
#                                   nutzt config.evening.yaml + run_scripts=[review]
#   --target-date=YYYY-MM-DD     → Override des Tagesordner-Datums (neu)
#
# Backward-Compat: ohne Flags → config.yaml (alle Steps wie heute).
# Konflikt-Regel: --evening, --morning, --staging sind paarweise exklusiv.
# --resume erzwingt --evening (impliziert config.evening.yaml).
# =============================================================================

def _parse_cli_flags():
    """
    Liest die DPS-Pipeline-CLI-Flags aus sys.argv, entfernt sie dort und setzt
    Env-Variablen für config_loader. Konflikte führen zu sys.exit(2) mit
    klarer Fehlermeldung.

    Wirkung auf sys.argv: alle bekannten Flags werden entfernt, damit
    subprocess-Calls und Step-Skripte sie nicht erneut sehen.

    Returns: dict mit den gesetzten Modi (für main() zur Verzweigung).
    """
    modes = {
        "staging": False,
        "evening": False,
        "morning": False,
        "resume":  False,
        "target_date": None,
    }

    # --target-date=YYYY-MM-DD
    target_date_pattern = re.compile(r"^--target-date=(.+)$")
    for arg in list(sys.argv):
        m = target_date_pattern.match(arg)
        if m:
            date_str = m.group(1).strip()
            # Format-Validierung strikt YYYY-MM-DD
            try:
                datetime.strptime(date_str, "%Y-%m-%d")
            except ValueError:
                print(f"❌ --target-date Format ungültig: '{date_str}' (erwartet: YYYY-MM-DD)")
                sys.exit(2)
            modes["target_date"] = date_str
            sys.argv.remove(arg)

    # Boolean-Flags
    for flag, key in [("--staging", "staging"),
                      ("--evening", "evening"),
                      ("--morning", "morning"),
                      ("--resume",  "resume")]:
        if flag in sys.argv:
            modes[key] = True
            sys.argv.remove(flag)

    # --resume impliziert --evening
    if modes["resume"]:
        modes["evening"] = True

    # Konflikt-Check: höchstens einer von {staging, evening, morning} darf gesetzt sein.
    exclusive = sum(1 for k in ("staging", "evening", "morning") if modes[k])
    if exclusive > 1:
        print("❌ CLI-Konflikt: --staging / --evening / --morning sind paarweise "
              "exklusiv. Bitte nur eines verwenden.")
        sys.exit(2)

    # PIPELINE_CONFIG setzen
    if modes["staging"]:
        os.environ["PIPELINE_CONFIG"] = "config.staging.yaml"
        print("🔧 STAGING-Modus aktiv (config.staging.yaml)")
    elif modes["evening"]:
        os.environ["PIPELINE_CONFIG"] = "config.evening.yaml"
        if modes["resume"]:
            print("🌙 EVENING-RESUME aktiv (config.evening.yaml, nur Step 06)")
        else:
            print("🌙 EVENING-Modus aktiv (config.evening.yaml, Vorabend-Block Step 01–06)")
    elif modes["morning"]:
        os.environ["PIPELINE_CONFIG"] = "config.morning.yaml"
        print("🌅 MORNING-Modus aktiv (config.morning.yaml, Morgen-Block Step 07a–11)")
    # else: ohne Flag → config.yaml (Backward-Compat, kein Print, kein Env-Set)

    # PIPELINE_TARGET_DATE setzen
    if modes["target_date"]:
        os.environ["PIPELINE_TARGET_DATE"] = modes["target_date"]
        print(f"📅 --target-date={modes['target_date']} → PIPELINE_TARGET_DATE gesetzt")

    return modes


# Flags parsen bevor load_config() läuft
_CLI_MODES = _parse_cli_flags()


def _is_evening_only_mode() -> bool:
    """
    True wenn der Lauf im --evening- (oder --resume-, das impliziert --evening-)
    Modus aktiv ist.

    Im Vorabend-Modus dürfen drei Code-Blöcke in main() NICHT laufen:
      - listings_gate()                          (Z.797 – schreibt etsy-listing.csv,
                                                  ENTER-Pause; gehört in den Morgen)
      - End-of-Pipeline-Hook (Z.825-844)         (öffnet payhip + stockportal CSVs;
                                                  beide existieren noch nicht)
      - archive_and_clear_pending_if_enabled()   (Z.849 – würde unfertige
                                                  pending.json-Items mit Status
                                                  "Renamed" nach done.json
                                                  verschieben und pending leeren)

    Im --morning- und im default-Lauf (kein Flag, config.yaml) liefert dieser
    Predicate False → die drei Blöcke laufen wie bisher.

    Hintergrund: Patch 2026-04-28 nach Vorfall vom 27.04 ~22:34, bei dem ein
    Vorabend-Lauf zwei IDs DPS-WP-20260428-2136-001/-002 mit Status "Renamed"
    aus pending.json nach done.json archivierte → Morgen-Lauf 03:40 brach mit
    Pre-Flight-Veto (d) ab.
    """
    return bool(_CLI_MODES.get("evening", False))


# Config laden
cfg = load_config()
config = cfg["config"]
SCRIPT_PATH = cfg["SCRIPT_PATH"]

# Im --resume-Modus: run_scripts auf [review] reduzieren.
# Begründung: config.evening.yaml hat run_scripts=[prompts,knorko,csv,...,review].
# Bei Resume soll NUR Step 06 nochmal laufen (Step 01–05 sind bereits abgeschlossen).
# Wir patchen das config-Dict nach load_config(), nicht die YAML-Datei.
if _CLI_MODES["resume"]:
    config["run_scripts"] = ["review"]
    print("🔁 --resume: run_scripts auf [review] reduziert (Step 06 only)")

# === Fixture-Reset für Staging-Isolation ===
def reset_fixture_for_staging():
    """
    Wenn STAGING_ISOLATION aktiv ist und eine Fixture verwendet wird:
    - Erstelle eine Arbeitskopie der Original-Fixture im Staging-Temp-Ordner
    - Setze PIPELINE_STAGING_PENDING_FILE Env-Variable
    - So wird die Original-Fixture nicht überschrieben
    """
    staging_isolation = cfg.get("STAGING_ISOLATION", False)
    staging_temp_dir = cfg.get("STAGING_TEMP_DIR", None)
    pending_file_config = config.get("pending_file", "prompts_pending.json")

    if not staging_isolation or "_fixture" not in pending_file_config:
        return

    try:
        fixture_src = SCRIPT_PATH / "fixtures" / pending_file_config

        if not fixture_src.exists():
            print(f"⚠️  Fixture-Datei nicht gefunden: {fixture_src}")
            return

        # Erstelle Arbeitskopie im Staging-Temp-Ordner
        staging_temp_path = Path(staging_temp_dir) if staging_temp_dir else Path(tempfile.gettempdir())
        fixture_work = staging_temp_path / pending_file_config

        # Kopiere Fixture
        shutil.copy2(fixture_src, fixture_work)

        # Setze Env-Variable für config_loader
        os.environ["PIPELINE_STAGING_PENDING_FILE"] = str(fixture_work)

        print(f"🔄 Fixture-Arbeitskopie erstellt: {fixture_work}")
    except Exception as e:
        print(f"⚠️  Fehler beim Erstellen der Fixture-Arbeitskopie: {e}")

def run_script(name: str, command: str, use_shell: bool = False, required: bool = True):
    """Startet ein Teilskript.
    required=True  → Fehler bricht den gesamten Workflow ab (Standard).
    required=False → Fehler wird geloggt, der Workflow läuft weiter.
    """
    print(f"\n[{name}] wird gestartet...")
    try:
        if use_shell:
            subprocess.check_call(command, shell=True)
        else:
            subprocess.check_call([sys.executable, os.path.join(SCRIPT_PATH, command)])
        print(f"✅ {name} erfolgreich abgeschlossen.")
    except subprocess.CalledProcessError as e:
        if required:
            print(f"❌ {name} fehlgeschlagen. Abbruch.")
            sys.exit(e.returncode)
        else:
            print(f"⚠️  {name} fehlgeschlagen (nicht kritisch) – Workflow wird fortgesetzt.")


def _trim_done_file(done_file: Path, max_age_days: int = 60) -> None:
    """Entfernt Einträge aus done.json, die älter als max_age_days sind."""
    if not done_file.exists():
        return
    try:
        with done_file.open("r", encoding="utf-8") as f:
            entries = json.load(f)
        if not isinstance(entries, list):
            return
    except Exception:
        return

    cutoff = datetime.now() - timedelta(days=max_age_days)
    kept, removed = [], 0
    for e in entries:
        ts = e.get("timestamp", "")
        try:
            if datetime.fromisoformat(ts) >= cutoff:
                kept.append(e)
            else:
                removed += 1
        except (ValueError, TypeError):
            kept.append(e)  # Eintrag ohne lesbares Datum behalten

    if removed:
        atomic_write_json(done_file, kept)
        print(f"🗑️  done.json bereinigt: {removed} Einträge älter als {max_age_days} Tage entfernt ({len(kept)} behalten).")


def cleanup_staging_isolation():
    """Bereinigt Staging-Temp-Ordner nach erfolgreichem Lauf und löscht Env-Variable."""
    staging_isolation = cfg.get("STAGING_ISOLATION", False)
    staging_temp_dir = cfg.get("STAGING_TEMP_DIR", None)
    STAGING_TEMP_DIR_ENV = "PIPELINE_STAGING_TEMP_DIR"

    if staging_isolation and staging_temp_dir:
        try:
            staging_temp_path = Path(staging_temp_dir)
            if staging_temp_path.exists():
                print(f"\n🧹 STAGING-CLEANUP: Lösche Temp-Ordner: {staging_temp_path}")
                shutil.rmtree(staging_temp_path)
                print(f"✅ Staging-Temp-Ordner erfolgreich gelöscht.")
        except Exception as e:
            print(f"⚠️  STAGING-CLEANUP: Fehler beim Löschen von {staging_temp_dir}: {e}")
            print(f"   → Bitte manuell löschen oder später bereinigen.")
        finally:
            # Lösche Env-Variable, damit ein neuer Lauf einen frischen Ordner bekommt
            if STAGING_TEMP_DIR_ENV in os.environ:
                del os.environ[STAGING_TEMP_DIR_ENV]


# === Registriere atexit-Handler für Staging-Cleanup (NACH Funktionsdefinition) ===
# Dieser Handler wird bei jedem Programmende aufgerufen (normal oder via sys.exit())
atexit.register(cleanup_staging_isolation)


def archive_and_clear_pending_if_enabled():
    """Archiviert pending.json nach done.json und leert optional pending.json
    Im Staging-Modus (STAGING_ISOLATION) wird die Fixture-Datei NOT geleert."""

    staging_isolation = cfg.get("STAGING_ISOLATION", False)

    # Im Staging-Modus: Nur Log, keine Änderung der Fixture
    if staging_isolation:
        print("ℹ️ STAGING-Modus: Archivierung und Leerung übersprungen (Fixture unverändert).")
        return

    pending_file = Path(cfg["PENDING_FILE"])
    done_file = Path(cfg.get("DONE_FILE", pending_file.parent / "prompts_done.json"))

    if not pending_file.exists():
        print("ℹ️ Keine pending.json gefunden – nichts zu archivieren.")
        return

    try:
        with pending_file.open("r", encoding="utf-8") as f:
            pending_entries = json.load(f)
            if not isinstance(pending_entries, list):
                pending_entries = []
    except Exception:
        print("⚠️ pending.json beschädigt – Archivierung übersprungen.")
        pending_entries = []

    if pending_entries:
        if done_file.exists():
            try:
                with done_file.open("r", encoding="utf-8") as f:
                    done_entries = json.load(f)
                    if not isinstance(done_entries, list):
                        done_entries = []
            except Exception:
                done_entries = []
        else:
            done_entries = []

        done_entries.extend(pending_entries)

        try:
            atomic_write_json(done_file, done_entries)
            print(f"✅ Archivierung: {len(pending_entries)} Einträge nach {done_file.name} kopiert.")
        except Exception as e:
            print(f"❌ Fehler beim Schreiben von {done_file}: {e}")

    _trim_done_file(done_file, max_age_days=config.get("done_max_age_days", 60))

    if config.get("clear_pending", False):
        try:
            atomic_write_json(pending_file, [])
            print(f"🧹 Pending geleert: {pending_file.name}")
        except Exception as e:
            print(f"❌ Fehler beim Leeren von {pending_file}: {e}")
    else:
        print("ℹ️ Pending nicht geleert (clear_pending = false).")


def _open_csv_in_excel(csv_path: Path) -> bool:
    """
    Oeffnet eine CSV im Standardprogramm (bevorzugt Excel) im Hintergrund.
    Gibt True bei Erfolg zurueck, False wenn kein Oeffnen moeglich war.
    Non-blocking: wir warten nicht auf Excel-Exit.
    """
    try:
        # Windows: Excel explizit via Registry suchen, sonst os.startfile
        if os.name == "nt":
            try:
                import winreg
                with winreg.OpenKey(
                    winreg.HKEY_LOCAL_MACHINE,
                    r"SOFTWARE\Microsoft\Windows\CurrentVersion\App Paths\excel.exe",
                ) as k:
                    excel_exe, _ = winreg.QueryValueEx(k, "")
                subprocess.Popen([excel_exe, str(csv_path)])
                return True
            except OSError:
                pass
            os.startfile(str(csv_path))  # type: ignore[attr-defined]
            return True
        # Non-Windows (Cowork-VM etc.): xdg-open Best-Effort
        subprocess.Popen(["xdg-open", str(csv_path)],
                         stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        return True
    except Exception as e:
        print(f"⚠️  Konnte {csv_path.name} nicht oeffnen: {e}")
        return False


def _csv_is_unlocked(csv_path: Path) -> bool:
    """
    Prueft ob die CSV exklusiv schreibbar ist (= Excel hat die Arbeitsmappe
    geschlossen). Gibt True zurueck wenn der Lock weg ist.
    """
    try:
        # Append-mode testet Schreib-Lock, ohne den Inhalt zu beruehren
        with csv_path.open("a", encoding="utf-8-sig"):
            return True
    except PermissionError:
        return False
    except Exception:
        # Andere Fehler nicht als Lock werten
        return True


def listings_gate() -> None:
    """
    Listings-Gate (Phase 3, vor Step 10/11).

    Schreibt etsy-listing.csv aus master-listings.json im Tagesordner,
    oeffnet diese Datei, wartet bis der User product_link und promo_code
    eingetragen und Excel geschlossen hat, und synct beide Felder per id
    zurueck in master-listings.json.

    Skip-Bedingungen:
      - STAGING_ISOLATION ist aktiv (keine Excel-Interaktion in der VM)
      - keine master-Items vorhanden
      - alle master-Items haben bereits product_link gesetzt
    """
    staging_isolation = bool(cfg.get("STAGING_ISOLATION", False))
    if staging_isolation:
        print("\nℹ️  Listings-Gate: STAGING_ISOLATION aktiv – uebersprungen.")
        return

    day_folder = get_day_folder(
        Path(cfg["IMAGES_PATH"]),
        date_format=cfg["DATE_FORMAT"],
        target_date=cfg["TARGET_DATE"],
    )

    master = load_master_listings(day_folder)
    items = master.get("items", [])
    if not items:
        print("\nℹ️  Listings-Gate: keine master-Items gefunden – uebersprungen.")
        return

    # Filter: nolist-Items ausschließen (Produkte mit < 5 Bildern)
    items = [it for it in items if it.get("status") != "nolist"]
    if not items:
        print("\nℹ️  Listings-Gate: alle master-Items haben Status 'nolist' – uebersprungen.")
        return

    if all((it.get("product_link") or "") for it in items):
        print("\nℹ️  Listings-Gate: alle master-Items haben bereits "
              "product_link – uebersprungen (Re-Run-Idempotenz).")
        return

    # Etsy-listing.csv schreiben aus master-listings.json
    csv_path = day_folder / "etsy-listing.csv"
    etsy_csv_fieldnames = [
        "id", "etsy_tags_en", "short_line_en", "etsy_description_en", "etsy_title_en",
        "etsy_tags_de", "short_line_de", "etsy_description_de", "etsy_title_de",
        "product_link", "promo_code"
    ]

    # Promo-Code-Default (ab 2026-04-26-j, Indi-Patch via Hiwi/Thomas):
    # Wenn das Master-Item noch keinen promo_code hat, wird "NEWCUST50" als Pre-Fill
    # in der CSV gesetzt - Operator kann den Wert in Excel ueberschreiben. Bestehende
    # promo_code-Werte aus master-listings.json haben Vorrang (NIE ueberschreiben).
    DEFAULT_PROMO_CODE = "NEWCUST50"

    rows = []
    for it in items:
        existing_promo = (it.get("promo_code") or "").strip()
        promo_value = existing_promo if existing_promo else DEFAULT_PROMO_CODE
        rows.append({
            "id":                  it.get("id", ""),
            "etsy_tags_en":        it.get("etsy_tags_en", ""),
            "short_line_en":       it.get("short_line_en", ""),
            "etsy_description_en": it.get("etsy_description_en", ""),
            "etsy_title_en":       it.get("etsy_title_en", ""),
            "etsy_tags_de":        it.get("etsy_tags_de", ""),
            "short_line_de":       it.get("short_line_de", ""),
            "etsy_description_de": it.get("etsy_description_de", ""),
            "etsy_title_de":       it.get("etsy_title_de", ""),
            "product_link":        "",            # User traegt das in Excel ein
            "promo_code":          promo_value,   # Pre-Fill NEWCUST50 (oder bestehender Wert)
        })

    tmp = csv_path.with_suffix(csv_path.suffix + ".tmp")
    try:
        with tmp.open("w", encoding="utf-8-sig", newline="") as f:
            writer = csv.DictWriter(
                f,
                fieldnames=etsy_csv_fieldnames,
                delimiter=";",
                quotechar='"',
                quoting=csv.QUOTE_ALL,
            )
            writer.writeheader()
            writer.writerows(rows)
        tmp.replace(csv_path)
        print(f"✅ etsy-listing.csv geschrieben ({len(rows)} Items)")
    except Exception as e:
        print(f"❌ etsy-listing.csv konnte nicht geschrieben werden: {e}")
        sys.exit(1)

    print()
    print("=" * 60)
    print("🛒 LISTINGS-GATE (Etsy als führender Shop)")
    print("=" * 60)
    print(f"Datei: {csv_path}")
    print()
    print("Bitte erledige folgende Schritte:")
    print("  1. Erstelle die Etsy-Listings (Produkt-Link + Promo-Code)")
    print("  2. Trage product_link und promo_code je Zeile in der CSV ein")
    print("  3. Speichere und SCHLIESSE die Arbeitsmappe in Excel")
    print("  4. Druecke dann ENTER, um den Workflow fortzusetzen")
    print("=" * 60)

    open_excel = bool(config.get("open_etsy_csv_at_gate", True))
    if open_excel:
        if not _open_csv_in_excel(csv_path):
            print("⚠️  Excel konnte nicht automatisch geoeffnet werden – "
                  "bitte oeffne die Datei manuell.")
    else:
        print("ℹ️  open_etsy_csv_at_gate=false – bitte oeffne die Datei selbst.")

    # Loop: ENTER abwarten, dann Lock pruefen
    while True:
        try:
            input("\n⏳ ENTER druecken wenn Eintraege gespeichert und Excel geschlossen ist "
                  "(oder 'q' zum Abbrechen): ").strip().lower()
        except KeyboardInterrupt:
            print("\n⚠️  Listings-Gate abgebrochen.")
            sys.exit(1)
        if not _csv_is_unlocked(csv_path):
            print(f"⚠️  {csv_path.name} ist noch gesperrt – "
                  f"bitte Arbeitsmappe in Excel schliessen und erneut ENTER.")
            continue
        break

    # CSV zurueckgelesen, master-listings.json updaten
    updated, missing = 0, 0
    try:
        with csv_path.open("r", encoding="utf-8-sig", newline="") as f:
            reader = csv.DictReader(f, delimiter=";")
            for row in reader:
                entry_id = (row.get("id") or "").strip()
                if not entry_id:
                    continue
                item = find_master_item(master, entry_id)
                if item is None:
                    missing += 1
                    print(f"   ⚠️  master-Item mit id '{entry_id}' nicht gefunden – uebersprungen.")
                    continue
                # NUR diese beiden Felder werden zurueckgeschrieben
                link = (row.get("product_link") or "").strip()
                code = (row.get("promo_code") or "").strip()
                item["product_link"] = link or None
                item["promo_code"]   = code or None
                updated += 1
    except Exception as e:
        print(f"❌ {csv_path.name} konnte nicht gelesen werden: {e}")
        sys.exit(1)

    try:
        save_master_listings(day_folder, master)
        print(f"\n🗂️  master-listings.json aktualisiert: "
              f"{updated} Item(s) mit product_link / promo_code.")
        if missing:
            print(f"   ⚠️  {missing} CSV-Zeile(n) ohne passende master-id uebersprungen.")
    except Exception as e:
        print(f"❌ master-listings.json konnte nicht geschrieben werden: {e}")
        sys.exit(1)

    print("✅ Listings-Gate abgeschlossen.")
    print()


# =============================================================================
# Pre-Flight-Check für --morning (ab 2026-04-26)
# =============================================================================
# Verifiziert, dass der Vorabend-Block sauber durch ist, bevor der Morgen-Lauf
# automatisch losläuft. Bei Veto: sys.exit(2) (Windows Task Scheduler "Last
# Result" = 2 → eindeutig unterscheidbar von regulären Pipeline-Fehlern).
#
# Pflicht-Pruefpunkte (a)–(e):
#   (a) <day_folder> für target_date existiert
#   (b) REVIEW_PENDING.lock ist ABWESEND (Step 06 sauber durch)
#   (c) master-listings.json enthält ≥1 Item mit Status ≠ "nolist"
#   (d) alle nicht-nolist-Listings haben in prompts_pending.json Status
#       "Renamed" oder höher (akzeptierte Status-Strings: aus cfg STATUSES,
#       in Pipeline-Reihenfolge ab "renamed")
#   (e) für jedes nicht-nolist-Listing existiert <listing>/Mockups/ mit ≥1
#       Bild (Cowork-Task canva-mockup-pipeline-creation ist gelaufen)
# =============================================================================

# Pipeline-Status-Reihenfolge (aufsteigend). Ein Status "X" oder "höher"
# bedeutet: Index in dieser Liste ≥ Index von "X". Status, die hier nicht
# gelistet sind, gelten als "vor renamed" → Pre-Flight failed.
_STATUS_ORDER = [
    "Prompt Generated",
    "CSV generated",
    "Marketing Done",
    "All Done",
    "Renamed",
    "Video Done",
    "Music Done",
    "YouTube Done",
    "Upscaled",
    "Etsy Listed",
    "Meta Posted",
]


def _status_at_least_renamed(status: str) -> bool:
    """True wenn status >= 'Renamed' in der Pipeline-Reihenfolge."""
    if not status:
        return False
    if status not in _STATUS_ORDER:
        return False
    return _STATUS_ORDER.index(status) >= _STATUS_ORDER.index("Renamed")


def pre_flight_morning(cfg) -> tuple[bool, str | None]:
    """
    Verifiziert Voraussetzungen für den Morgen-Block.

    Returns: (ok, fail_reason).
      ok=True  → fail_reason=None, Pipeline darf weiterlaufen.
      ok=False → fail_reason ist eine konkrete, menschenlesbare Begründung
                  mit Listing-Name und Soll/Ist (für Logging und Task
                  Scheduler-Diagnose).
    """
    # Diagnostik-Header (Konsistenz mit reel_lifecycle_scheduler-Stil)
    now_local = datetime.now(BERLIN_TZ)
    print()
    print("=" * 60)
    print(f"🛂 PRE-FLIGHT-CHECK (--morning)  now_local={now_local.isoformat()}")
    print("=" * 60)

    target_date = cfg.get("TARGET_DATE")
    images_path = Path(cfg["IMAGES_PATH"])
    date_format = cfg.get("DATE_FORMAT", "%Y-%m-%d")

    day_folder = get_day_folder(images_path, date_format=date_format,
                                target_date=target_date)

    # (a) day_folder existiert?
    if not day_folder.exists():
        return False, (f"(a) Tagesordner existiert nicht: {day_folder}. "
                       f"Wurde der Vorabend-Lauf für {target_date.strftime(date_format)} "
                       f"nicht ausgeführt?")
    print(f"   ✓ (a) Tagesordner existiert: {day_folder}")

    # (b) REVIEW_PENDING.lock ist ABWESEND?
    lock_file = day_folder / "REVIEW_PENDING.lock"
    if lock_file.exists():
        return False, (f"(b) REVIEW_PENDING.lock existiert noch: {lock_file}. "
                       f"Step 06 (Sichtkontrolle) wurde nicht abgeschlossen — "
                       f"Lock-Datei muss vom Nutzer gelöscht werden.")
    print(f"   ✓ (b) REVIEW_PENDING.lock ist abwesend")

    # (c) master-listings.json enthält ≥1 Item mit Status ≠ "nolist"?
    master = load_master_listings(day_folder)
    items = master.get("items", [])
    non_nolist_items = [it for it in items if it.get("status") != "nolist"]
    if not items:
        return False, (f"(c) master-listings.json enthält keine Items "
                       f"({day_folder / 'master-listings.json'}). "
                       f"Step 02 (Marketing CSV) wurde nicht ausgeführt oder "
                       f"hat nichts geschrieben.")
    if not non_nolist_items:
        return False, (f"(c) master-listings.json hat {len(items)} Item(s), "
                       f"alle mit Status 'nolist'. Kein Listing zu verarbeiten.")
    print(f"   ✓ (c) master-listings.json: {len(non_nolist_items)} Item(s) mit "
          f"Status ≠ 'nolist' (von {len(items)} insgesamt)")

    # (d) Status-Prüfung in prompts_pending.json
    pending_file = Path(cfg["PENDING_FILE"])
    if not pending_file.exists():
        return False, (f"(d) prompts_pending.json existiert nicht: {pending_file}. "
                       f"Vorabend-Block wurde nicht ausgeführt.")

    try:
        with pending_file.open("r", encoding="utf-8") as f:
            pending = json.load(f)
        if not isinstance(pending, list):
            return False, (f"(d) prompts_pending.json ist kein Listen-Format: "
                           f"{pending_file}")
    except Exception as e:
        return False, f"(d) prompts_pending.json konnte nicht gelesen werden: {e}"

    # Ein non-nolist Item muss mindestens einen Pending-Eintrag mit
    # Status "Renamed" oder höher haben.
    pending_by_id = {e.get("id"): e for e in pending if isinstance(e, dict)}
    for item in non_nolist_items:
        item_id = item.get("id", "")
        if not item_id:
            continue
        entry = pending_by_id.get(item_id)
        if entry is None:
            return False, (f"(d) Listing '{item.get('marketing_title', item_id)}' "
                           f"(id={item_id}) hat kein Eintrag in prompts_pending.json. "
                           f"Step 05 (Rename) wurde nicht ausgeführt?")
        status = entry.get("status", "")
        if not _status_at_least_renamed(status):
            return False, (f"(d) Listing '{item.get('marketing_title', item_id)}' "
                           f"(id={item_id}) hat Status '{status}' in "
                           f"prompts_pending.json (erwartet: 'Renamed' oder höher).")
    print(f"   ✓ (d) prompts_pending.json: alle {len(non_nolist_items)} non-nolist-"
          f"Listings haben Status >= 'Renamed'")

    # (e) Für jedes non-nolist-Listing: <listing>/Mockups/ mit ≥1 Bild?
    image_suffixes = {".jpg", ".jpeg", ".png", ".webp"}
    for item in non_nolist_items:
        folder_name = item.get("folder") or item.get("marketing_title", "")
        if not folder_name:
            return False, (f"(e) Item id={item.get('id')} hat weder 'folder' noch "
                           f"'marketing_title' — kann Mockups-Pfad nicht ableiten.")
        mockups_dir = day_folder / folder_name / "Mockups"
        if not mockups_dir.exists():
            return False, (f"(e) Mockups-Ordner fehlt: {mockups_dir}. "
                           f"Cowork-Task 'canva-mockup-pipeline-creation' wurde "
                           f"nicht ausgeführt für '{folder_name}'.")
        # Mindestens 1 Bild im Mockups-Ordner (flach, nicht rekursiv).
        mockup_images = [f for f in mockups_dir.iterdir()
                         if f.is_file() and f.suffix.lower() in image_suffixes]
        if not mockup_images:
            return False, (f"(e) Mockups-Ordner ist leer: {mockups_dir}. "
                           f"Cowork-Task hat keine Mockups erzeugt für '{folder_name}'.")
    print(f"   ✓ (e) alle {len(non_nolist_items)} non-nolist-Listings haben "
          f"≥1 Bild in <listing>/Mockups/")

    print("=" * 60)
    print("✅ Pre-Flight-Check PASS — Pipeline darf weiterlaufen.")
    print("=" * 60)
    return True, None


def main():
    print("=" * 52)
    print("🚀 Starte Workflow mit zentralem Config-File")
    print("=" * 52)

    # === Pre-Flight-Check für --morning (vor allen Steps) ===
    if _CLI_MODES.get("morning"):
        ok, fail_reason = pre_flight_morning(cfg)
        if not ok:
            print()
            print("=" * 60)
            print(f"❌ PRE-FLIGHT VETO (--morning): {fail_reason}")
            print("=" * 60)
            print("→ Morgen-Lauf abgebrochen mit Exit-Code 2.")
            sys.exit(2)

    # === Default-Lauf Guard: verhindert Überschreiben einer aktiven Pipeline ===
    # Greift NUR wenn kein Flag gesetzt ist (kein --morning, --evening, --staging).
    # Hintergrund: Vorfall 2026-04-28 — versehentlicher Default-Lauf überschrieb
    # laufende Vorabend-Session (IDs mit Status "Renamed" in pending.json).
    # "Prompt Generated" und "Simulation" gelten als OK (normaler Step_01-Output).
    # Alles darüber ("CSV generated", "Renamed", ...) bedeutet: Pipeline läuft.
    if not any(_CLI_MODES.get(m) for m in ("morning", "evening", "staging")):
        _guard_pending_file = Path(cfg["PENDING_FILE"])
        if _guard_pending_file.exists():
            try:
                with _guard_pending_file.open("r", encoding="utf-8") as _f:
                    _guard_pending = json.load(_f)
                if isinstance(_guard_pending, list):
                    _ok_statuses = {"Prompt Generated", "Simulation"}
                    _active_items = [
                        e for e in _guard_pending
                        if isinstance(e, dict)
                        and e.get("status") not in _ok_statuses
                    ]
                    if _active_items:
                        print()
                        print("=" * 60)
                        print("⛔ DEFAULT-LAUF ABGEBROCHEN — Pipeline bereits aktiv")
                        print("=" * 60)
                        print(f"  prompts_pending.json enthält {len(_active_items)} "
                              f"Item(s) mit Status > 'Prompt Generated'.")
                        print()
                        for _item in _active_items[:5]:
                            print(f"    • {_item.get('id', '?')}  Status: {_item.get('status', '?')}")
                        if len(_active_items) > 5:
                            print(f"    … und {len(_active_items) - 5} weitere.")
                        print()
                        print("  Mögliche Aktionen:")
                        print("    → Vorabend-Lauf läuft noch: bitte mit  --morning  fortfahren")
                        print("    → Oder mit  --resume  (nur Step 06 Sichtkontrolle)")
                        print("    → Oder pending.json manuell prüfen")
                        print("=" * 60)
                        sys.exit(2)
            except (json.JSONDecodeError, OSError):
                pass  # Lesbarkeitsfehler → Guard überspringen, Step_01 wird es melden

    # === Fixture-Reset für Staging durchführen ===
    reset_fixture_for_staging()

    run_scripts = config.get("run_scripts", [])

    print("Aktive Schritte:")
    for script in run_scripts:
        print(f"  ✓ {script}")
    print()

    # === PHASE 1: Generierung ===
    print("─" * 52)
    print("📋 PHASE 1: Generierung")
    print("─" * 52)

    # Step 01: Prompts & Tagesordner
    if "prompts" in run_scripts:
        run_script("Step 01 – Prompts & Tagesordner", "Step_01_Generate_prompts_und_Dayfolders.py")

    # Step 01b: Notion Theme Generierung (Knorko)
    product_types = config.get("product_types", {})
    if "knorko" in run_scripts and product_types.get("notion_theme", 0) > 0:
        run_script("Step 01b – Notion Theme (Knorko)", "Step_01b_Knorko_Theme.py")

    # Step 02: Marketing CSV
    if "csv" in run_scripts:
        run_script("Step 02 – Marketing CSV", "Step_02_Generate_Marketing_CSV.py")

    # Step 03: Marketing Ordner
    if "marketing" in run_scripts:
        run_script("Step 03 – Marketing Ordner", "Step_03_Create_Marketing_Folders.py")

    # Step 04: Bilder generieren (inkl. URL-Speicherung)
    if "images" in run_scripts:
        run_script("Step 04 – Bilder generieren (Leonardo)", "Step_04_generate_images_leonardo.py")

    # Step 05: Bilder umbenennen (inkl. Pfad-Aktualisierung)
    if "rename" in run_scripts:
        run_script("Step 05 – Bilder umbenennen", "Step_05_rename_images.py")

    # Step 05b: Hintergrundentfernung (nur bei clip_art > 0)
    if "remove_bg" in run_scripts:
        product_types_local = config.get("product_types", {})
        if product_types_local.get("clip_art", 0) > 0:
            run_script("Step 05b – Hintergrundentfernung (rembg)", "Step_05b_Remove_Background.py")
        else:
            print("ℹ️  Step 05b: clip_art = 0 — Hintergrundentfernung übersprungen.")

    # === PHASE 2: Sichtkontrolle & Media-Erstellung ===
    print()
    print("─" * 52)
    print("👁️  PHASE 2: Sichtkontrolle & Media-Erstellung")
    print("─" * 52)

    # Step 06: Pause für Sichtkontrolle
    if "review" in run_scripts:
        run_script("Step 06 – Sichtkontrolle (Pause)", "Step_06_Review_Pause.py")

    # Step 07a: Musik generieren (MusicGen)
    if "music" in run_scripts:
        run_script("Step 07a – Musik generieren (MusicGen)", "Step_07a_Generate_Music.py")

    # Step 07: Video erstellen
    if "video" in run_scripts:
        run_script("Step 07 – Video erstellen (FFmpeg)", "Step_07_Create_Video.py")

    # Step 08: YouTube Upload
    if "youtube" in run_scripts:
        run_script("Step 08 – YouTube Upload", "Step_08_Upload_YouTube.py")

    # === PHASE 3: Verarbeitung & Export ===
    print()
    print("─" * 52)
    print("⚙️  PHASE 3: Verarbeitung & Export")
    print("─" * 52)

    # Step 09: Filter + Upscaling
    if "upscale" in run_scripts:
        run_script("Step 09 – Filter & Upscaling", "Step_09_Upscale_Pics.py")

    # === Listings-Gate (Inline, vor Step 10/11) ===
    # Nur im Morgen-Block oder default-Lauf. Vorabend hat noch keine Mockups
    # und keinen Bedarf für etsy-listing.csv (wird im Morgen-Lauf erstellt).
    if not _is_evening_only_mode():
        listings_gate()
    else:
        print("ℹ️  Vorabend-Modus: Listings-Gate uebersprungen "
              "(wird im Morgen-Lauf erstellt).")

    # === Approval Gate (nur im Staging-Modus) ===
    if os.environ.get("PIPELINE_CONFIG") == "config.staging.yaml":
        approval_file = SCRIPT_PATH / ".approval"
        if not approval_file.exists():
            print("\n" + "=" * 52)
            print("🔐 APPROVAL GATE: Staging-Testlauf vor Produktionscode")
            print("=" * 52)
            print("Bilder wurden hochgeladen und gefiltert (Step 09).")
            print("Um fortzufahren (Step 10–11), benötige ich Freigabe:")
            print()
            print("  Windows:  approve_for_prod.bat")
            print("  Linux:    ./approve_for_prod.sh")
            print()
            print("Nach Freigabe: Diesen Workflow erneut starten.")
            print("=" * 52)
            sys.exit(0)

    # Step 10: Etsy Listing
    if "etsy" in run_scripts:
        run_script("Step 10 – Etsy Listing", "Step_10_List_On_Etsy.py", required=False)

    # Step 11: Meta Video Post (FB + IG Reels)
    if "meta" in run_scripts:
        run_script("Step 11 – Meta Video Post (FB + IG)", "Step_11_Post_Video_Meta.py", required=False)

    # === End-of-Pipeline: payhip-listing.csv + stockportal-listing.csv oeffnen ===
    # Nur im Morgen-Block oder default-Lauf. Vorabend hat weder payhip- noch
    # stockportal-CSV (beide werden von Step_08 bzw. Step_11 im Morgen-Lauf
    # erzeugt) — Hook würde nur "nicht gefunden"-Logs produzieren.
    open_end = bool(config.get("open_payhip_and_stockportal_at_end", True))
    if open_end and not _is_evening_only_mode():
        try:
            day_folder_end = get_day_folder(
                Path(cfg["IMAGES_PATH"]),
                date_format=cfg["DATE_FORMAT"],
                target_date=cfg["TARGET_DATE"],
            )
            for fname in ("payhip-listing.csv", "stockportal-listing.csv"):
                fpath = day_folder_end / fname
                if fpath.exists():
                    if not _open_csv_in_excel(fpath):
                        print(f"⚠️  {fname} konnte nicht automatisch geoeffnet werden – "
                              f"bitte oeffne die Datei manuell.")
                else:
                    print(f"ℹ️  {fname} nicht gefunden im Tagesordner – uebersprungen.")
        except Exception as e:
            print(f"⚠️  End-of-Pipeline-Oeffner fehlgeschlagen: {e}")
    elif _is_evening_only_mode():
        print("ℹ️  Vorabend-Modus: End-of-Pipeline-Hook uebersprungen "
              "(payhip-/stockportal-CSVs entstehen erst im Morgen-Lauf).")
    else:
        print("ℹ️  open_payhip_and_stockportal_at_end=false – bitte selbst oeffnen.")

    # === Abschluss ===
    print()
    print("=" * 52)
    # Im Vorabend-Modus dürfen die Items NICHT archiviert oder pending geleert
    # werden — Status ist erst "Renamed" (Step 05), Morgen-Lauf braucht die
    # Items noch in pending.json (Pre-Flight-Check (d) prüft Status).
    if not _is_evening_only_mode():
        archive_and_clear_pending_if_enabled()
    else:
        print("ℹ️  Vorabend-Modus: pending.json wird im Morgen-Lauf archiviert "
              "(NICHT jetzt — Items sind noch nicht durch die Pipeline).")
    # cleanup_staging_isolation() wird via atexit automatisch aufgerufen
    print("=" * 52)
    print("🎯 Workflow erfolgreich abgeschlossen!")
    print("=" * 52)


if __name__ == "__main__":
    main()