#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Step_03_Create_Marketing_Folders.py

- Liest alle Einträge mit Status "CSV generated" (normaler Lauf) oder
  "Simulation" (Dry-Run) aus prompts_pending.json.
- Legt für jeden Eintrag einen Marketing-Ordner im Tagesordner an.
- Erstellt zusätzlich einen Unterordner '4k'.
- Setzt Status auf "Marketing Done" (nur im echten Lauf).
"""

import sys
import json
from pathlib import Path

from config_loader import load_config, atomic_write_json

cfg    = load_config()
config = cfg["config"]

PENDING_FILE = cfg["PENDING_FILE"]
STATUSES     = cfg["STATUSES"]
STAGING_ISOLATION = cfg["STAGING_ISOLATION"]
STAGING_IMAGES_PATH = cfg["IMAGES_PATH"]
remap_pending_entries_to_staging = cfg["remap_pending_entries_to_staging"]

flags       = cfg["get_script_flags"]("marketing")
RUN_ENABLED = bool(flags["run"])
DRYRUN      = bool(flags["dry_run"])

# === HELPERS ===
# Hinweis: `atomic_write_json` wird aus `config_loader` importiert (oben).
# Gehärtete Variante mit Retry/Backoff gegen Windows-Dateilocks — keine lokale
# Kopie mehr. Migration 2026-04-20 (session-log-2026-04-20-d.md).

# === MAIN ===
def main():
    if not RUN_ENABLED:
        print("ℹ️ [marketing] ist in run_scripts deaktiviert – nichts zu tun.")
        sys.exit(0)

    if not PENDING_FILE.exists():
        print("❌ prompts_pending.json fehlt.")
        sys.exit(1)

    try:
        with PENDING_FILE.open("r", encoding="utf-8") as f:
            pending = json.load(f)
            if not isinstance(pending, list):
                print("❌ prompts_pending.json hat kein Listenformat.")
                sys.exit(1)
    except Exception:
        print("❌ prompts_pending.json beschädigt.")
        sys.exit(1)

    # === STAGING-ISOLATION: Remap day_folder zu Staging-Temp-Ordner ===
    if STAGING_ISOLATION:
        remap_pending_entries_to_staging(pending, STAGING_IMAGES_PATH)
        print(f"🎭 Pending-Einträge zu Staging-Ordner remapped.")

    sim_status            = STATUSES.get("simulation",    "Simulation")
    csv_generated_status  = STATUSES.get("csv_generated", "CSV generated")
    marketing_done_status = STATUSES.get("marketing_done","Marketing Done")

    processed = 0

    for entry in pending:
        if DRYRUN:
            if entry.get("status") != sim_status:
                continue
            print(f"🧪 DRY-RUN: Würde Marketing-Ordner (4k + Mockups) für {entry.get('id')} simulieren.")
            processed += 1
            continue

        # Im echten Lauf: Nur Einträge mit Status "CSV generated" verarbeiten
        # (Step_02 hat diesen Status gesetzt + optional remapped, falls Staging aktiv)
        if entry.get("status") != csv_generated_status:
            continue

        day_folder = Path(entry.get("day_folder", ""))
        if not day_folder.exists():
            print(f"📁 Tagesordner wird erstellt: {day_folder}")
            day_folder.mkdir(parents=True, exist_ok=True)

        folder_title  = entry.get("marketing_title", "Untitled")
        target_folder = day_folder / folder_title
        (target_folder / "4k").mkdir(parents=True, exist_ok=True)
        (target_folder / "Mockups").mkdir(parents=True, exist_ok=True)

        entry["status"] = marketing_done_status
        entry["folder"] = str(target_folder)
        processed += 1

    if DRYRUN:
        print(f"🧪 DRY-RUN: {processed} Marketing-Ordner simuliert. Keine Dateien geschrieben.")
    else:
        try:
            atomic_write_json(PENDING_FILE, pending)
        except Exception as e:
            print(f"❌ Schreiben von {PENDING_FILE} fehlgeschlagen: {e}")
            sys.exit(1)
        print(f"✅ {processed} Marketing-Ordner erstellt und Pending auf '{marketing_done_status}' gesetzt.")

if __name__ == "__main__":
    main()