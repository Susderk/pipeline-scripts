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
import subprocess
import shutil
import atexit
from pathlib import Path
from datetime import datetime, timedelta
import json

from config_loader import load_config

# === Staging-Modus prüfen ===
if "--staging" in sys.argv:
    os.environ["PIPELINE_CONFIG"] = "config.staging.yaml"
    print("🔧 STAGING-Modus aktiv (config.staging.yaml)")
    sys.argv.remove("--staging")  # Aus argv entfernen, damit subprocess nicht verwirrt wird

# Config laden
cfg = load_config()
config = cfg["config"]
SCRIPT_PATH = cfg["SCRIPT_PATH"]

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
        with done_file.open("w", encoding="utf-8") as f:
            json.dump(kept, f, ensure_ascii=False, indent=2)
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
            with done_file.open("w", encoding="utf-8") as f:
                json.dump(done_entries, f, ensure_ascii=False, indent=2)
            print(f"✅ Archivierung: {len(pending_entries)} Einträge nach {done_file.name} kopiert.")
        except Exception as e:
            print(f"❌ Fehler beim Schreiben von {done_file}: {e}")

    _trim_done_file(done_file, max_age_days=config.get("done_max_age_days", 60))

    if config.get("clear_pending", False):
        try:
            with pending_file.open("w", encoding="utf-8") as f:
                json.dump([], f, ensure_ascii=False, indent=2)
            print(f"🧹 Pending geleert: {pending_file.name}")
        except Exception as e:
            print(f"❌ Fehler beim Leeren von {pending_file}: {e}")
    else:
        print("ℹ️ Pending nicht geleert (clear_pending = false).")


def main():
    print("=" * 52)
    print("🚀 Starte Workflow mit zentralem Config-File")
    print("=" * 52)

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

    # === Abschluss ===
    print()
    print("=" * 52)
    archive_and_clear_pending_if_enabled()
    # cleanup_staging_isolation() wird via atexit automatisch aufgerufen
    print("=" * 52)
    print("🎯 Workflow erfolgreich abgeschlossen!")
    print("=" * 52)


if __name__ == "__main__":
    main()