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

import sys
import zipfile
from pathlib import Path
from datetime import date as _date
from config_loader import load_config, get_day_folder, load_master_listings

try:
    import pyperclip
    _CLIPBOARD_OK = True
except ImportError:
    _CLIPBOARD_OK = False


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

    # === CLIPBOARD-ASSISTENT ===
    # Liest aus master-listings.json (Single Source of Truth, seit Refactor 2026-04).
    _master_file = day_folder / "master-listings.json"
    if not _CLIPBOARD_OK:
        print("\nℹ️  pyperclip nicht installiert – Clipboard-Assistent übersprungen.")
        print("   pip install pyperclip")
    elif not _master_file.exists():
        print(f"\nℹ️  master-listings.json nicht gefunden – Clipboard-Assistent übersprungen.")
    else:
        _fields = [
            ("etsy_title",          "Etsy Titel (kurz)"),
            ("etsy_title_de",       "Etsy Titel DE (SEO)"),
            ("etsy_title_en",       "Etsy Titel EN (SEO)"),
            ("short_line_en",       "Short Line EN"),
            ("short_line_de",       "Short Line DE"),
            ("etsy_description_en", "Etsy Beschreibung EN"),
            ("etsy_description_de", "Etsy Beschreibung DE"),
            ("etsy_tags_en",        "Etsy Tags EN"),
            ("etsy_tags_de",        "Etsy Tags DE"),
            ("social_hashtags",     "Social Hashtags"),
            ("stock_tags",          "Stock Tags"),
        ]
        try:
            _master = load_master_listings(day_folder)
            _rows = _master.get("items", [])
        except Exception as e:
            print(f"⚠️  master-listings.json konnte nicht gelesen werden: {e}")
            _rows = []

        if _rows:
            print("\n" + "="*60)
            print("📋 CLIPBOARD-ASSISTENT")
            print("   ENTER = Feld kopieren  |  s = Feld überspringen  |  q = Beenden")
            print("="*60)
            _done = False
            for _idx, _row in enumerate(_rows, 1):
                if _done:
                    break
                _header = _row.get("marketing_title") or _row.get("folder") or _row.get("etsy_title", "?")
                print(f"\n── Eintrag {_idx}/{len(_rows)}: {_header} ──")
                for _key, _label in _fields:
                    _value = _row.get(_key) or ""
                    if not _value:
                        continue
                    _preview = _value[:60] + "…" if len(_value) > 60 else _value
                    print(f"  [{_label}] {_preview}")
                    try:
                        _antwort = input("  ENTER kopieren / s überspringen / q beenden: ").strip().lower()
                    except KeyboardInterrupt:
                        print("\n⚠️  Abgebrochen.")
                        _done = True
                        break
                    if _antwort == "q":
                        print("  ✅ Clipboard-Assistent beendet.")
                        _done = True
                        break
                    if _antwort == "s":
                        print("  ⏭️  Übersprungen.")
                        continue
                    pyperclip.copy(_value)
                    print(f"  ✅ Kopiert ({len(_value)} Zeichen)")

    # Lock-Datei erstellen
    try:
        lock_file.parent.mkdir(parents=True, exist_ok=True)
        lock_file.write_text(
            "Sichtkontrolle ausstehend.\n"
            "Bitte prüfe die Bilder im Tagesordner und lösche unerwünschte Bilder.\n"
            "Erstelle außerdem die Canva-Mockups und lege die PNGs in die Mockups-Unterordner.\n"
            "Lösche diese Datei wenn beides abgeschlossen ist.\n"
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

    # Schleife: wartet bis Lock-Datei weg UND ENTER gedrückt
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
    print("✅ Sichtkontrolle & Mockups bestätigt.")
    print()

    # =================================================================
    # ZIP-IMPORT AUS DOWNLOAD-ORDNER
    # Prüft ob passende Canva-ZIPs im Download-Ordner liegen und
    # extrahiert deren Inhalt in die Mockups-Unterordner.
    # =================================================================
    config       = cfg["config"]
    download_dir = Path(config.get("download_folder", r"D:\WTF\OneDrive\Downloads"))

    print("=" * 60)
    print("📦 Prüfe Download-Ordner auf Mockup-ZIPs...")
    print(f"   Quelle: {download_dir}")
    print("=" * 60)

    if not download_dir.exists():
        print(f"⚠️  Download-Ordner nicht gefunden: {download_dir}")
    else:
        # Marketing-Ordner mit Mockups/ Unterordner sammeln
        marketing_folders = sorted([
            d for d in day_folder.iterdir()
            if d.is_dir() and (d / "Mockups").is_dir()
        ])

        if not marketing_folders:
            print("ℹ️  Keine Marketing-Ordner mit Mockups/ gefunden – übersprungen.")
        else:
            def _normalize(s: str) -> str:
                """Für Namensvergleich: lowercase, Leerzeichen/Unterstriche/Bindestriche vereinheitlichen."""
                return s.lower().replace("_", " ").replace("-", " ").strip()

            target_date_only = TARGET_DATE.date() if hasattr(TARGET_DATE, "date") else TARGET_DATE
            zips_found    = 0
            zips_imported = 0

            for mf in marketing_folders:
                folder_name = mf.name
                mockups_dir = mf / "Mockups"
                norm_folder = _normalize(folder_name)

                # Passendes ZIP suchen (Name = Folder-Name, Datum = heute/Target-Datum)
                matched_zip = None
                for zf in download_dir.glob("*.zip"):
                    if _normalize(zf.stem) != norm_folder:
                        continue
                    zip_date = _date.fromtimestamp(zf.stat().st_mtime)
                    if zip_date == target_date_only:
                        matched_zip = zf
                        break

                if not matched_zip:
                    continue

                zips_found += 1
                print(f"\n   📦 {matched_zip.name} → {folder_name}/Mockups/")

                try:
                    with zipfile.ZipFile(matched_zip, "r") as zf:
                        extracted = 0
                        for member in zf.namelist():
                            # Ordner-Einträge und macOS-Metadaten überspringen
                            if member.endswith("/") or "__MACOSX" in member:
                                continue
                            # Nur Dateiname verwenden (ZIP-interne Ordnerstruktur ignorieren)
                            filename = Path(member).name
                            if not filename:
                                continue
                            target = mockups_dir / filename
                            with zf.open(member) as src, target.open("wb") as dst:
                                dst.write(src.read())
                            extracted += 1
                            print(f"      ✅ {filename}")

                    # ZIP bei Erfolg löschen
                    matched_zip.unlink()
                    print(f"   🗑️  {matched_zip.name} gelöscht.")
                    zips_imported += 1

                except zipfile.BadZipFile:
                    print(f"   ❌ {matched_zip.name} ist keine gültige ZIP-Datei.")
                except Exception as e:
                    print(f"   ❌ Fehler beim Entpacken von {matched_zip.name}: {e}")

            if zips_found == 0:
                print("\nℹ️  Keine passenden ZIPs gefunden (gleicher Name + heutiges Datum).")
            else:
                print(f"\n📦 {zips_imported}/{zips_found} ZIP(s) erfolgreich importiert.")

    print()
    print("▶️  Workflow wird fortgesetzt.")
    print()

if __name__ == "__main__":
    main()