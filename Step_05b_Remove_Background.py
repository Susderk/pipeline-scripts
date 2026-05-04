#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Step_05b_Remove_Background.py

Hintergrundentfernung für Clip-Art-Produkte (rembg).

- Nur aktiv wenn product_types.clip_art > 0 in der Config
- Input: JPG/PNG-Bilder aus dem Produktordner (Hauptordner, nicht Mockups/)
- Output: PNG mit transparentem Hintergrund in Unterordner Transparent/
- Qualitätsprüfung: Wenn >5% der Randpixel noch nicht transparent sind → Warnung
- Läuft nach Step_05 (Rename) und vor Step_06 (Review)
- Config-Key: remove_background (default: true, aber nur effektiv wenn clip_art > 0)

Abhängigkeit: pip install rembg
"""

import sys
import os
from pathlib import Path

from config_loader import (
    load_config,
    get_day_folder,
    atomic_write_json,
)

cfg    = load_config()
config = cfg["config"]

IMAGES_PATH  = cfg["IMAGES_PATH"]
PENDING_FILE = cfg["PENDING_FILE"]
DATE_FORMAT  = cfg["DATE_FORMAT"]
TARGET_DATE  = cfg["TARGET_DATE"]
STATUSES     = cfg["STATUSES"]
STAGING_ISOLATION = cfg["STAGING_ISOLATION"]

flags       = cfg["get_script_flags"]("remove_bg")
RUN_ENABLED = bool(flags["run"])
DRYRUN      = bool(flags["dry_run"])

# Nur für clip_art Produkte aktiv
product_types = config.get("product_types", {})
CLIP_ART_COUNT = product_types.get("clip_art", 0)

# Rand-Pixel-Schwellwert für Qualitätsprüfung (5%)
BORDER_ALPHA_WARN_THRESHOLD = 0.05


def _check_border_transparency(img_rgba, filename: str) -> None:
    """
    Prüft ob die Randpixel des Bildes transparent sind.
    Warnt wenn >5% der Randpixel einen Alpha-Wert > 10 haben
    (= noch nicht transparent).
    """
    try:
        import numpy as np
        arr = np.array(img_rgba)
        h, w = arr.shape[:2]

        if arr.shape[2] < 4:
            print(f"   ⚠️  {filename}: Kein Alpha-Kanal — Transparenzprüfung übersprungen.")
            return

        # Randpixel sammeln: oberste + unterste Zeile, linke + rechte Spalte
        border_pixels = []
        border_pixels.extend(arr[0, :, 3].tolist())      # oben
        border_pixels.extend(arr[h-1, :, 3].tolist())    # unten
        border_pixels.extend(arr[1:h-1, 0, 3].tolist())  # links (ohne Ecken)
        border_pixels.extend(arr[1:h-1, w-1, 3].tolist()) # rechts (ohne Ecken)

        if not border_pixels:
            return

        non_transparent = sum(1 for p in border_pixels if p > 10)
        ratio = non_transparent / len(border_pixels)

        if ratio > BORDER_ALPHA_WARN_THRESHOLD:
            print(
                f"   ⚠️  Qualitätswarnung {filename}: "
                f"{ratio*100:.1f}% der Randpixel sind nicht transparent "
                f"({non_transparent}/{len(border_pixels)} Pixel, Schwellwert: {BORDER_ALPHA_WARN_THRESHOLD*100:.0f}%). "
                f"Bitte Ergebnis manuell prüfen."
            )
        else:
            print(f"   ✓ Randtransparenz OK: {ratio*100:.1f}% nicht-transparent ({filename})")

    except ImportError:
        print(f"   ℹ️  numpy nicht verfügbar — Qualitätsprüfung übersprungen ({filename}).")
    except Exception as e:
        print(f"   ⚠️  Qualitätsprüfung fehlgeschlagen ({filename}): {e}")


def remove_background_from_folder(folder_path: Path) -> tuple[int, int]:
    """
    Entfernt Hintergrund aus allen JPG/PNG im Produktordner.
    Speichert Ergebnisse in Transparent/ Unterordner.

    Returns: (success_count, failed_count)
    """
    try:
        from rembg import remove as rembg_remove
        from PIL import Image
        import io
    except ImportError as e:
        print(f"❌ Pflicht-Abhängigkeit fehlt: {e}")
        print("   Bitte installieren mit: pip install rembg pillow")
        return 0, 0

    if not folder_path.is_dir():
        print(f"⚠️  Ordner existiert nicht: {folder_path}")
        return 0, 0

    transparent_dir = folder_path / "Transparent"
    transparent_dir.mkdir(exist_ok=True)

    extensions = {'.jpg', '.jpeg', '.png'}
    image_files = [
        f for f in folder_path.iterdir()
        if f.is_file() and f.suffix.lower() in extensions
    ]

    if not image_files:
        print(f"   ℹ️  Keine Bilder in {folder_path.name} — übersprungen.")
        return 0, 0

    success, failed = 0, 0
    for img_path in sorted(image_files):
        # Output-Dateiname: immer .png (für Alpha-Kanal)
        out_name = img_path.stem + ".png"
        out_path = transparent_dir / out_name

        try:
            print(f"   🔄 Hintergrund entfernen: {img_path.name} → Transparent/{out_name}")

            with open(img_path, "rb") as f:
                input_data = f.read()

            output_data = rembg_remove(input_data)

            # Als PIL-Image laden für Qualitätsprüfung
            from PIL import Image
            result_img = Image.open(io.BytesIO(output_data)).convert("RGBA")

            # Qualitätsprüfung
            _check_border_transparency(result_img, img_path.name)

            # Speichern
            result_img.save(out_path, "PNG")
            print(f"   ✅ Gespeichert: {out_path}")
            success += 1

        except Exception as e:
            print(f"   ❌ Fehler bei {img_path.name}: {e}")
            failed += 1

    return success, failed


def main():
    if not RUN_ENABLED:
        print("ℹ️ [remove_bg] ist in run_scripts deaktiviert – nichts zu tun.")
        sys.exit(0)

    if CLIP_ART_COUNT == 0:
        print("ℹ️ product_types.clip_art = 0 — Hintergrundentfernung übersprungen "
              "(nur aktiv wenn clip_art > 0).")
        sys.exit(0)

    print(f"🎨 Step 05b — Hintergrundentfernung (clip_art={CLIP_ART_COUNT})")

    if DRYRUN:
        print("🧪 DRY-RUN: Hintergrundentfernung simuliert — keine Dateien geschrieben.")
        sys.exit(0)

    # Pending-Einträge laden
    if not PENDING_FILE.exists():
        print("❌ prompts_pending.json fehlt.")
        sys.exit(1)

    try:
        import json
        with PENDING_FILE.open("r", encoding="utf-8") as f:
            pending = json.load(f)
        if not isinstance(pending, list):
            print("❌ prompts_pending.json hat kein Listenformat.")
            sys.exit(1)
    except Exception as e:
        print(f"❌ prompts_pending.json konnte nicht gelesen werden: {e}")
        sys.exit(1)

    renamed_status = STATUSES.get("renamed", "Renamed")

    total_success, total_failed, processed_folders = 0, 0, 0

    for entry in pending:
        if entry.get("status") != renamed_status:
            continue

        # Produkttyp prüfen (nur clip_art)
        product_type = entry.get("product_type", "wallpaper")
        if product_type != "clip_art":
            continue

        folder = entry.get("folder") or entry.get("day_folder")
        if not folder:
            continue

        folder_path = Path(folder)
        if not folder_path.is_dir():
            print(f"⚠️  Ordner nicht gefunden: {folder_path} — übersprungen.")
            continue

        print(f"\n📂 Verarbeite: {folder_path.name}")
        s, f = remove_background_from_folder(folder_path)
        total_success += s
        total_failed += f
        processed_folders += 1

    if processed_folders == 0:
        print("ℹ️  Keine clip_art-Einträge mit Status 'Renamed' gefunden — nichts zu tun.")
    else:
        print(f"\n📊 Ergebnis: {processed_folders} Ordner verarbeitet, "
              f"{total_success} Bilder erfolgreich, {total_failed} Fehler.")

    if total_failed > 0:
        print("⚠️  Einige Bilder konnten nicht verarbeitet werden — bitte Logs prüfen.")
        sys.exit(1)


if __name__ == "__main__":
    main()
