#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Step_06_Review_Pause.py

Pausiert den Workflow für manuelle Sichtkontrolle der Bilder.

Mechanismus:
- Erstellt eine REVIEW_PENDING.lock Datei im Tagesordner
- Zeigt dem Nutzer den Bildordner an
- Wartet in einer Schleife bis:
  1. Die Lock-Datei manuell gelöscht wurde UND
  2. ENTER gedrückt wurde
- Erst wenn beides erfüllt ist, läuft der Workflow weiter

Sichtkontrolle durch den Nutzer:
- Unerwünschte Bilder einfach im Ordner löschen
- Lock-Datei löschen wenn Prüfung abgeschlossen
- ENTER drücken → Workflow läuft weiter
"""

import json
import sys
import subprocess
from pathlib import Path
from config_loader import load_config, get_day_folder, load_master_listings, save_master_listings, atomic_write_json


def main():
    print("[Step 6 - Sichtkontrolle] wird gestartet...")

    cfg = load_config()

    flags = cfg["get_script_flags"]("review")
    RUN_ENABLED = bool(flags.get("run", True))

    if not RUN_ENABLED:
        print("ℹ️ [review] ist in run_scripts deaktiviert – Pause übersprungen.")
        sys.exit(0)

    IMAGES_PATH = cfg["IMAGES_PATH"]
    DATE_FORMAT = cfg["DATE_FORMAT"]
    TARGET_DATE = cfg["TARGET_DATE"]
    day_folder  = get_day_folder(IMAGES_PATH, DATE_FORMAT, TARGET_DATE)

    # === STAGING-MODUS: Vereinfachte Sichtkontrolle ===
    STAGING_ISOLATION = cfg.get("STAGING_ISOLATION", False)
    if STAGING_ISOLATION:
        print("\n" + "=" * 60)
        print("🎭 STAGING-MODUS: Sichtkontrolle vereinfacht")
        print("=" * 60)
        print("   (keine Lock-Datei, Clipboard & ZIP-Import übersprungen)")
        try:
            input("\n⏳ Drücke ENTER um fortzufahren...")
        except KeyboardInterrupt:
            print("\n⚠️  Abgebrochen. Workflow gestoppt.")
            sys.exit(1)
        print("\n✅ Staging-Review bestätigt.")
        print()
        return

    lock_file = day_folder / "REVIEW_PENDING.lock"

    # Lock-Datei erstellen
    try:
        lock_file.parent.mkdir(parents=True, exist_ok=True)
        lock_file.write_text(
            "Sichtkontrolle ausstehend.\n"
            "Bitte prüfe die Bilder im Tagesordner und lösche unerwünschte Bilder.\n"
            "Lösche diese Datei wenn abgeschlossen ist.\n"
            "Drücke dann ENTER im Terminal um den Workflow fortzusetzen.",
            encoding="utf-8"
        )
        print(f"🔒 Lock-Datei erstellt: {lock_file}")
    except Exception as e:
        print(f"❌ Konnte Lock-Datei nicht erstellen: {e}")
        sys.exit(1)

    print()
    print("=" * 60)
    print("⏸️  WORKFLOW PAUSIERT – SICHTKONTROLLE & MOCKUPS")
    print("=" * 60)
    print(f"📂 Tagesordner: {day_folder}")
    print()
    print("Bitte erledige BEIDES bevor du fortsetzt:")
    print()
    print("  🖼️  Bildkontrolle:")
    print("      1. Bilder im Ordner prüfen")
    print("      2. Unerwünschte Bilder löschen")
    print()
    print("  🎨  Canva-Mockups:")
    print("      3. Mockups in Canva erstellen")
    print("      4. PNGs in die Mockups-Unterordner ablegen")
    print(f"         (<FolderName>/Mockups/ je Marketing-Ordner)")
    print()
    print(f"  🔓  Danach: Lock-Datei löschen ({lock_file.name})")
    print("      und ENTER drücken")
    print("=" * 60)

    # === BILDKONTROLLE-TOOL STARTEN (VOR der Pause!) ===
    # Startet image_review_tool.py als Subprocess (non-blocking mit .wait())
    SCRIPT_PATH = Path(__file__).resolve().parent
    image_review_tool = SCRIPT_PATH / "image_review_tool.py"

    if image_review_tool.exists():
        print()
        print("=" * 60)
        print("🖼️  Starte Bildkontrolle-Tool...")
        print("=" * 60)
        try:
            proc = subprocess.Popen([sys.executable, str(image_review_tool), str(day_folder)])
            proc.wait()  # Blockieren bis Tool geschlossen wird
            print("✅ Bildkontrolle-Tool geschlossen.")
        except Exception as e:
            print(f"⚠️  Fehler beim Starten des Bildkontrolle-Tools: {e}")
    else:
        print(f"\n⚠️  Bildkontrolle-Tool nicht gefunden: {image_review_tool}")

    # Schleife: wartet bis Lock-Datei weg UND ENTER gedrückt
    print()
    while True:
        try:
            input("\n⏳ Drücke ENTER wenn die Sichtkontrolle abgeschlossen ist...")
        except KeyboardInterrupt:
            print("\n⚠️  Abgebrochen. Workflow gestoppt.")
            sys.exit(1)

        if lock_file.exists():
            print()
            print("⚠️  Lock-Datei existiert noch!")
            print(f"   Bitte erst Sichtkontrolle UND Mockups abschließen, dann die Datei löschen:")
            print(f"   {lock_file}")
            print("   Danach ENTER drücken um fortzufahren.")
        else:
            break

    print()
    print("✅ Sichtkontrolle bestätigt.")
    print()

    # =================================================================
    # FILTER: Vom Nutzer gelöschte Bilder aus prompts_pending.json entfernen
    # Muss direkt nach der Sichtkontrolle laufen, damit Step_07/07a/08
    # nur noch mit den tatsächlich überlebenden Bildern arbeiten.
    # (Früher in Step_09 Phase 1 – dort wirkungslos, weil der Status dort
    # bereits "YouTube Done" war und der Filter auf "Renamed" prüft.)
    # =================================================================
    PENDING_FILE  = Path(cfg["PENDING_FILE"])
    STATUSES      = cfg["STATUSES"]
    renamed_status = STATUSES.get("renamed", "Renamed")

    print("=" * 60)
    print("🔍 Filter: Prüfe welche Bilder noch vorhanden sind...")
    print("=" * 60)

    if not PENDING_FILE.exists():
        print(f"ℹ️  {PENDING_FILE.name} nicht gefunden – Filter übersprungen.")
    else:
        try:
            with PENDING_FILE.open("r", encoding="utf-8") as _f:
                _pending = json.load(_f)
            if not isinstance(_pending, list):
                raise ValueError("prompts_pending.json ist kein Listen-Format")
        except Exception as _e:
            print(f"⚠️  {PENDING_FILE.name} konnte nicht gelesen werden: {_e}")
            _pending = None

        if _pending is not None:
            _removed = 0
            for _entry in _pending:
                if _entry.get("status") != renamed_status:
                    continue
                _images = _entry.get("images", [])
                if not _images:
                    continue
                _before = len(_images)
                _surviving = []
                for _img in _images:
                    _lp = _img.get("local_path", "")
                    if _lp and Path(_lp).exists():
                        _surviving.append(_img)
                    else:
                        print(f"🗑️  Entfernt (Datei gelöscht): {Path(_lp).name if _lp else 'unbekannt'}")
                        _removed += 1
                _entry["images"] = _surviving
                _after = len(_surviving)
                if _before != _after:
                    print(f"   → {_before} Bilder vorher, {_after} nach Filterung für: {_entry.get('id', '?')}")

            if _removed > 0:
                try:
                    atomic_write_json(PENDING_FILE, _pending)
                    print(f"✅ {_removed} gelöschte Bilder aus {PENDING_FILE.name} entfernt.")
                except Exception as _e:
                    print(f"❌ Schreiben von {PENDING_FILE.name} fehlgeschlagen: {_e}")
                    sys.exit(1)
            else:
                print("✅ Alle Bilder vorhanden, nichts zu filtern.")

    # =================================================================
    # NOLIST-STATUS: Produkte mit < 5 Bildern markieren
    # Muss nach der Bildkontrolle laufen, damit nur überlebende Bilder
    # gezählt werden. Im Staging-Modus wird diese Prüfung übersprungen.
    # =================================================================
    print("=" * 60)
    print("🔢 Prüfe Bildanzahl pro Produkt...")
    print("=" * 60)

    if STAGING_ISOLATION:
        print("\n🎭 STAGING-MODUS: Nolist-Prüfung übersprungen.")
    else:
        _master = load_master_listings(day_folder)
        _items = _master.get("items", [])
        _nolist_count = 0

        for _item in _items:
            _item_id = _item.get("id", "")
            _folder_name = _item.get("folder") or _item.get("marketing_title", "")
            if not _folder_name:
                continue

            # Zähle Bilder im Produktordner (nicht in Mockups/)
            _prod_folder = day_folder / _folder_name
            if not _prod_folder.exists():
                continue

            # Zähle .jpg, .png, .webp Dateien (nicht rekursiv, nur direkt im Produktordner)
            _image_count = 0
            for _f in _prod_folder.iterdir():
                if _f.is_file() and _f.suffix.lower() in {".jpg", ".png", ".webp"}:
                    _image_count += 1

            if _image_count < 5:
                _item["status"] = "nolist"
                _nolist_count += 1
                print(f"🗑️  {_folder_name}: nur {_image_count} Bilder → Status: nolist")
            else:
                print(f"✅ {_folder_name}: {_image_count} Bilder → OK")

        if _nolist_count > 0:
            try:
                save_master_listings(day_folder, _master)
                print(f"\n✅ {_nolist_count} Produkt(e) mit Status 'nolist' markiert.")
            except Exception as _e:
                print(f"❌ Speichern von master-listings.json fehlgeschlagen: {_e}")
                sys.exit(1)
        else:
            print("\n✅ Alle Produkte haben >= 5 Bilder.")

    print()
    print("▶️  Workflow wird fortgesetzt.")
    print()

if __name__ == "__main__":
    main()