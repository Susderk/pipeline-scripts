#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Step_05_rename_images.py

- Benennt Bilder im Marketing-Ordner nach dem Schema: <Ordnername> wallpaper NNN.ext
- Aktualisiert die lokalen Pfade und Dateinamen in prompts_pending.json
  (entry["images"] Liste wird mit neuen Pfaden und Dateinamen aktualisiert)
- Setzt Status auf "Renamed"
- Schreibt neue Dateinamen und Pfade zurück in pending.json
"""

import os
import sys
import json
from pathlib import Path
from config_loader import load_config


def atomic_write_json(path: Path, data) -> None:
    tmp = path.with_suffix(path.suffix + ".tmp")
    path.parent.mkdir(parents=True, exist_ok=True)
    with tmp.open("w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)
    tmp.replace(path)


def rename_images_in_folder(folder_path: str, dryrun: bool = False) -> list:
    """
    Benennt Bilder um und gibt eine Liste von Dicts zurück:
    [{"old_path": ..., "new_path": ..., "new_filename": ...}, ...]
    """
    print(f"📂 Starte Bearbeitung des Ordners: {folder_path}")
    if not os.path.isdir(folder_path):
        print(f"⚠️ Ordner existiert nicht: {folder_path}")
        return []

    extensions = ('.jpg', '.jpeg', '.png', '.gif', '.bmp', '.tiff', '.webp')
    all_files = os.listdir(folder_path)
    files = [f for f in all_files if f.lower().endswith(extensions)]

    if not files:
        print(f"🔎 Keine Bilder im Ordner: {folder_path}. Gefundene Dateien: {all_files}")
        return []

    renamed_list = []
    counter = 1
    folder_basename = os.path.basename(folder_path)

    for filename in sorted(files):
        old_path = os.path.join(folder_path, filename)
        ext = os.path.splitext(filename)[1].lower()
        new_filename = f"{folder_basename} wallpaper {counter:03d}{ext}"
        new_path = os.path.join(folder_path, new_filename)

        if filename != new_filename:
            if dryrun:
                print(f"🧪 DRY-RUN: Würde umbenennen: {filename} → {new_filename}")
            else:
                try:
                    os.rename(old_path, new_path)
                except Exception as e:
                    print(f"❌ Konnte {filename} nicht umbenennen: {e}")
                    counter += 1
                    continue

        renamed_list.append({
            "old_path": old_path,
            "new_path": new_path,
            "new_filename": new_filename
        })
        counter += 1

    print(f"✅ Bearbeitet: {folder_path}. {'Simuliert' if dryrun else 'Umbenannt'}: {len(renamed_list)}")
    return renamed_list


def update_images_in_entry(entry: dict, renamed_list: list) -> None:
    """
    Aktualisiert die 'images' Liste im pending-Eintrag mit den neuen Pfaden.
    Matcht alte Pfade mit neuen Pfaden anhand der Sortierreihenfolge.
    """
    images = entry.get("images", [])
    if not images:
        return

    # Sortiere beide Listen nach altem Pfad / Dateiname für konsistentes Matching
    images_sorted = sorted(images, key=lambda x: x.get("local_path", ""))
    renamed_sorted = sorted(renamed_list, key=lambda x: x.get("old_path", ""))

    # Gleiche Anzahl? Dann 1:1 matchen
    if len(images_sorted) == len(renamed_sorted):
        for img, renamed in zip(images_sorted, renamed_sorted):
            img["local_path"] = renamed["new_path"]
            img["filename"] = renamed["new_filename"]
    else:
        # Fallback: anhand old_path matchen
        old_to_new = {r["old_path"]: r for r in renamed_list}
        for img in images:
            old = img.get("local_path", "")
            if old in old_to_new:
                img["local_path"] = old_to_new[old]["new_path"]
                img["filename"] = old_to_new[old]["new_filename"]


def main():
    print("[Step 5 - Rename] wird gestartet...")
    cfg = load_config()

    flags_rename = cfg["get_script_flags"]("rename")
    RUN_ENABLED  = bool(flags_rename["run"])
    DRYRUN       = bool(flags_rename["dry_run"])

    if not RUN_ENABLED:
        print("ℹ️ [rename] ist in run_scripts deaktiviert – nichts zu tun.")
        sys.exit(0)

    prompts_pending_path = cfg['PENDING_FILE']
    all_done_status   = cfg["STATUSES"].get("all_done", "All Done")
    renamed_status    = cfg["STATUSES"].get("renamed", "Renamed")
    simulation_status = cfg["STATUSES"].get("simulation", "Simulation")

    if not os.path.exists(prompts_pending_path):
        print(f"❌ Datei nicht gefunden: {prompts_pending_path}")
        sys.exit(1)

    try:
        with open(prompts_pending_path, 'r', encoding='utf-8') as f:
            entries = json.load(f)
        if not isinstance(entries, list):
            print("❌ prompts_pending.json hat kein Listenformat.")
            sys.exit(1)
    except Exception:
        print("❌ prompts_pending.json beschädigt.")
        sys.exit(1)

    total_renamed = 0
    updated = False

    for entry in entries:
        status = entry.get("status")

        if DRYRUN:
            if status != simulation_status:
                print(f"🚫 Eintrag wird übersprungen (Status ist '{status}')")
                continue
            folder_path = entry.get("folder")
            if folder_path:
                renamed = rename_images_in_folder(folder_path, dryrun=True)
                total_renamed += len(renamed)
            continue

        if status != all_done_status:
            print(f"🚫 Eintrag wird übersprungen (Status ist '{status}')")
            continue

        folder_path = entry.get("folder")
        if not folder_path:
            print(f"⚠️ Kein Ordnerpfad im Eintrag vorhanden: {entry}")
            continue

        renamed_list = rename_images_in_folder(folder_path, dryrun=False)
        if renamed_list:
            update_images_in_entry(entry, renamed_list)
            entry["status"] = renamed_status
            updated = True
            total_renamed += len(renamed_list)
        else:
            print(f"⚠️ Keine Bilder umbenannt für: {folder_path}")

    if DRYRUN:
        print(f"🧪 DRY-RUN: {total_renamed} Umbenennungen simuliert. Keine Änderungen gespeichert.")
    elif updated:
        try:
            atomic_write_json(Path(prompts_pending_path), entries)
            print(f"✅ {prompts_pending_path} aktualisiert (Status auf '{renamed_status}', Pfade aktualisiert).")
        except Exception as e:
            print(f"❌ Fehler beim Speichern von {prompts_pending_path}: {e}")

    print(f"=== Insgesamt umbenannte Bilder: {total_renamed} ===")
    print("✅ Step 5 abgeschlossen.")

if __name__ == "__main__":
    main()