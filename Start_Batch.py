#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Start_Batch.py

Urlaubs-Batch-Wrapper: Generiert mehrere Tagesordner sequenziell ohne Sichtkontrolle.
Verwendet config.batch.yaml (run_scripts ohne 'review', image_count: 7).

Verwendung:
  python Start_Batch.py
  python Start_Batch.py --start=2026-05-21 --end=2026-06-02

Ablauf:
  1. Zeigt interaktiv Start-/Enddatum (Defaults: heute und heute+10)
  2. CLI-Args --start / --end überschreiben interaktive Prompts
  3. Iteriert über jedes Datum in der Range (inkl. Start und Ende):
     a) Idempotenz-Check: wenn Tagesordner bereits vorbereitet → SKIP
     b) Setzt PIPELINE_TARGET_DATE und PIPELINE_CONFIG=config.batch.yaml
     c) subprocess.run(["python", "Start_Scripts.py", "--evening"]) — blockierend
     d) Bei Exit-Code != 0: Fehler loggen, nächstes Datum fortsetzen
     e) 2 Sekunden Pause zwischen Läufen
  4. Sammelreport am Ende (N erzeugt / M übersprungen / K Fehler)
  5. KeyboardInterrupt → sauberes Beenden mit Hinweis

Neue Datei: pipeline/Start_Batch.py (Urlaubs-Batch 2026-04-02)
"""

import os
import sys
import json
import subprocess
import time
import argparse
from pathlib import Path
from datetime import datetime, timedelta

# Pipeline-Verzeichnis ist immer das Verzeichnis dieser Datei
PIPELINE_DIR = Path(__file__).resolve().parent
SCRIPT_DIR = PIPELINE_DIR
JSON_DIR = PIPELINE_DIR.parent / "JSON Dateien"

# Config-Datei für den Batch-Modus
BATCH_CONFIG = "config.batch.yaml"


# ============================================================================
# HELPER: Tagesordner-Pfad berechnen
# ============================================================================

def _build_day_folder_path(date_obj: datetime, images_path: str) -> Path:
    """
    Berechnet den Tagesordner-Pfad nach Pipeline-Konvention:
    <images_path>/<YYYY>/<YYYY MMMM>/<YYYY-MM-DD>
    Monatsname englisch via strftime("%B") (Python-Default).
    """
    year = date_obj.strftime("%Y")
    month_name = date_obj.strftime("%B")  # englisch ohne setlocale
    day_str = date_obj.strftime("%Y-%m-%d")
    return Path(images_path) / year / f"{year} {month_name}" / day_str


def _load_images_path() -> str:
    """
    Liest images_path aus config.yaml (nicht config.batch.yaml!),
    damit der echte Produktionspfad verwendet wird.
    Fallback: leerer String (Idempotenz-Check überspringt dann Pfad-Prüfung).
    """
    config_path = PIPELINE_DIR / "config.yaml"
    if not config_path.exists():
        print(f"⚠️  config.yaml nicht gefunden: {config_path}")
        return ""
    try:
        import yaml
        with open(config_path, "r", encoding="utf-8") as f:
            cfg = yaml.safe_load(f)
        return cfg.get("images_path", "") or ""
    except Exception as e:
        print(f"⚠️  config.yaml nicht lesbar: {e}")
        return ""


# ============================================================================
# IDEMPOTENZ-CHECK
# ============================================================================

def _day_already_prepared(date_str: str, pipeline_dir: Path) -> bool:
    """
    Prüft, ob ein Tag bereits vorbereitet wurde.

    Ein Tag gilt als "bereits vorbereitet" wenn:
    1. Der Tagesordner im Dateisystem existiert UND
    2. prompts_pending.json ODER prompts_done.json mindestens einen Eintrag
       mit ID enthält, die mit 'DPS-WP-<YYYYMMDD>' beginnt
       (YYYYMMDD = Datum ohne Bindestriche).

    images_path wird aus config.yaml gelesen (nicht config.batch.yaml).
    """
    # YYYYMMDD aus date_str berechnen (z.B. "2026-05-21" → "20260521")
    date_compact = date_str.replace("-", "")
    id_prefix = f"DPS-WP-{date_compact}"

    # 1) Tagesordner-Existenz prüfen
    images_path = _load_images_path()
    if images_path:
        try:
            date_obj = datetime.strptime(date_str, "%Y-%m-%d")
            day_folder = _build_day_folder_path(date_obj, images_path)
            if not day_folder.exists():
                return False  # Ordner existiert nicht → noch nicht vorbereitet
        except ValueError:
            return False

    # 2) JSON-Dateien auf passende IDs prüfen
    json_dir = pipeline_dir.parent / "JSON Dateien"

    for json_filename in ("prompts_pending.json", "prompts_done.json"):
        json_path = json_dir / json_filename
        if not json_path.exists():
            continue
        try:
            with open(json_path, "r", encoding="utf-8") as f:
                data = json.load(f)
            if not isinstance(data, list):
                continue
            for entry in data:
                entry_id = entry.get("id", "") or ""
                if entry_id.startswith(id_prefix):
                    return True  # Gefunden → bereits vorbereitet
        except Exception:
            continue

    return False


# ============================================================================
# DATUM-RANGE GENERIEREN
# ============================================================================

def _date_range(start_date: datetime, end_date: datetime):
    """Generator: liefert alle Datumsobjekte von start_date bis end_date (inkl.)."""
    current = start_date
    while current <= end_date:
        yield current
        current += timedelta(days=1)


# ============================================================================
# CLI-ARGUMENT-PARSING
# ============================================================================

def _parse_args():
    """Parst --start und --end CLI-Arguments."""
    parser = argparse.ArgumentParser(
        description="Urlaubs-Batch: mehrere Tagesordner sequenziell ohne Sichtkontrolle generieren.",
        add_help=True,
    )
    parser.add_argument(
        "--start",
        metavar="YYYY-MM-DD",
        help="Startdatum (inkl.). Default: heute.",
    )
    parser.add_argument(
        "--end",
        metavar="YYYY-MM-DD",
        help="Enddatum (inkl.). Default: heute+10.",
    )
    return parser.parse_args()


def _parse_date_interactive(prompt_text: str, default: datetime) -> datetime:
    """
    Fragt interaktiv nach einem Datum.
    Default wird bei leerem Input verwendet.
    """
    default_str = default.strftime("%Y-%m-%d")
    while True:
        raw = input(f"{prompt_text} [Default: {default_str}]: ").strip()
        if not raw:
            return default
        try:
            return datetime.strptime(raw, "%Y-%m-%d")
        except ValueError:
            print(f"  ❌ Ungültiges Format '{raw}' — erwartet: YYYY-MM-DD. Bitte erneut eingeben.")


# ============================================================================
# HAUPTLOGIK
# ============================================================================

def main():
    args = _parse_args()

    today = datetime.today().replace(hour=0, minute=0, second=0, microsecond=0)
    default_start = today
    default_end = today + timedelta(days=10)

    # Datum aus CLI-Args oder interaktiver Eingabe
    if args.start:
        try:
            start_date = datetime.strptime(args.start, "%Y-%m-%d")
        except ValueError:
            print(f"❌ Ungültiges --start-Datum: '{args.start}' (erwartet: YYYY-MM-DD)")
            sys.exit(1)
    else:
        start_date = _parse_date_interactive("Startdatum (inkl.)", default_start)

    if args.end:
        try:
            end_date = datetime.strptime(args.end, "%Y-%m-%d")
        except ValueError:
            print(f"❌ Ungültiges --end-Datum: '{args.end}' (erwartet: YYYY-MM-DD)")
            sys.exit(1)
    else:
        end_date = _parse_date_interactive("Enddatum (inkl.)", default_end)

    if end_date < start_date:
        print(f"❌ Enddatum {end_date.strftime('%Y-%m-%d')} liegt vor Startdatum {start_date.strftime('%Y-%m-%d')}.")
        sys.exit(1)

    # Datum-Liste aufbauen
    dates = list(_date_range(start_date, end_date))
    total_days = len(dates)

    print()
    print("=" * 60)
    print(f"  Urlaubs-Batch — {total_days} Tag(e)")
    print(f"  Start: {start_date.strftime('%Y-%m-%d')}  Ende: {end_date.strftime('%Y-%m-%d')}")
    print(f"  Config: {BATCH_CONFIG}")
    print(f"  Script: Start_Scripts.py --evening")
    print("=" * 60)
    print()

    # Bestätigung einholen
    try:
        confirm = input(f"Batch für {total_days} Tag(e) starten? [j/N] ").strip().lower()
    except (KeyboardInterrupt, EOFError):
        print("\n⏹  Abgebrochen.")
        sys.exit(0)

    if confirm not in ("j", "ja", "y", "yes"):
        print("⏹  Batch nicht gestartet.")
        sys.exit(0)

    print()

    # Statistik
    count_generated = 0
    count_skipped = 0
    count_errors = 0
    aborted_date = None  # für KeyboardInterrupt

    for i, date_obj in enumerate(dates, 1):
        date_str = date_obj.strftime("%Y-%m-%d")
        print(f"[{i}/{total_days}] {date_str} —", end=" ", flush=True)

        # Idempotenz-Check
        try:
            already_done = _day_already_prepared(date_str, PIPELINE_DIR)
        except Exception as e:
            print(f"⚠️  Idempotenz-Check Fehler: {e} — fortsetzen als NEU")
            already_done = False

        if already_done:
            print(f"⏭  Bereits vorbereitet — übersprungen.")
            count_skipped += 1
            continue

        print(f"🔄 Starte Pipeline ...", flush=True)

        # Env-Vars setzen
        env = os.environ.copy()
        env["PIPELINE_TARGET_DATE"] = date_str
        env["PIPELINE_CONFIG"] = BATCH_CONFIG

        try:
            result = subprocess.run(
                [sys.executable, "Start_Scripts.py", "--evening"],
                cwd=str(PIPELINE_DIR),
                env=env,
            )
            if result.returncode == 0:
                print(f"  ✅ {date_str} erfolgreich abgeschlossen.")
                count_generated += 1
            else:
                print(f"  ❌ {date_str} Fehler (Exit-Code {result.returncode}) — weiter mit nächstem Tag.")
                count_errors += 1

        except KeyboardInterrupt:
            aborted_date = date_str
            print(f"\n⏹  Abgebrochen durch Benutzer bei Datum {date_str}.")
            break

        except Exception as e:
            print(f"  ❌ {date_str} Unerwarteter Fehler: {e} — weiter mit nächstem Tag.")
            count_errors += 1

        # Pause zwischen Läufen (außer beim letzten)
        if i < total_days and aborted_date is None:
            time.sleep(2)

    # Sammelreport
    print()
    print("=" * 60)
    print("  BATCH-REPORT")
    print("=" * 60)
    print(f"  Gesamt:      {total_days} Tag(e)")
    print(f"  ✅ Erzeugt:  {count_generated}")
    print(f"  ⏭  Skipped:  {count_skipped}")
    print(f"  ❌ Fehler:   {count_errors}")
    if aborted_date:
        print(f"  ⏹  Abgebrochen bei: {aborted_date}")
    print("=" * 60)

    if count_errors > 0:
        sys.exit(1)
    else:
        sys.exit(0)


if __name__ == "__main__":
    main()
