#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
patch_ai_disclosure.py — Einmal-Skript: KI-Offenlegungstext in bestehende Etsy-Listings einfügen

Auftrag: Majo, Aufgabe 61 (2026-05-03)
Umsetzung: Indi

Funktion:
  Durchsucht alle master-listings.json Dateien in den Tagesordnern (images_path).
  Fügt den KI-Offenlegungstext in etsy_description_en und etsy_description_de ein,
  wenn er dort noch nicht vorhanden ist.

  Einfüge-Logik (pro Beschreibungsfeld):
    1. Wenn CTA erkennbar ist (📩-Marker): Text VOR dem CTA einfügen
    2. Wenn kein CTA gefunden: Text ans Ende anhängen

  Dry-Run-Modus (--dry-run):
    Zeigt alle Änderungen (Diff), schreibt NICHTS.

Verwendung:
  python patch_ai_disclosure.py [--dry-run] [--images-path PATH]

  --dry-run          Nur anzeigen, keine Dateien schreiben
  --images-path PATH Überschreibt den images_path aus config.yaml

Hinweise:
  - Korrupte oder nicht lesbare JSON-Dateien werden übersprungen (Warnung).
  - Items mit nolist-Status werden mitgepflegt (Etsy ignoriert sie, schadet nicht).
  - Atomar via config_loader.atomic_write_json (fsync-gehärtet, 3 Retries).
  - Idempotent: bereits gepatchte Items werden nicht doppelt gepflegt.
"""

import sys
import os
import argparse
import json
from pathlib import Path

# Sicherstellen, dass pipeline/ im Suchpfad liegt (Skript liegt in pipeline/tools/)
_pipeline_dir = Path(__file__).resolve().parent.parent
if str(_pipeline_dir) not in sys.path:
    sys.path.insert(0, str(_pipeline_dir))

from config_loader import (
    load_config,
    load_master_listings,
    save_master_listings,
    atomic_write_json,
)

# === TEXTKONSTANTEN (identisch zu Step_02) ===
# Festgelegt durch Majo, Aufgabe 61, 2026-05-03
AI_DISCLOSURE_EN = (
    "\n\nThis product was created using AI image generation (Leonardo AI) and manually curated."
)
AI_DISCLOSURE_DE = (
    "\n\nDieses Produkt wurde mithilfe von KI-Bildgenerierung (Leonardo AI) erstellt und manuell kuratiert."
)

# Marker zum Erkennen eines vorhandenen CTA-Blocks
_CTA_MARKER = "📩"

# Eindeutiger Substring zum Erkennen ob Disclosure bereits vorhanden
_DISCLOSURE_MARKER_EN = "AI image generation (Leonardo AI)"
_DISCLOSURE_MARKER_DE = "KI-Bildgenerierung (Leonardo AI)"


def _insert_before_cta(text: str, disclosure: str) -> str:
    """
    Fügt `disclosure` VOR dem CTA-Block ein (erkannt durch _CTA_MARKER).
    Falls kein CTA gefunden wird, wird disclosure ans Ende angehängt.

    Args:
        text:        Bestehender Beschreibungstext
        disclosure:  Einzufügender KI-Offenlegungstext (beginnt mit \\n\\n)

    Returns:
        Modifizierter Text mit eingefügtem Offenlegungstext
    """
    idx = text.find(_CTA_MARKER)
    if idx == -1:
        # Kein CTA gefunden — ans Ende anhängen
        return text + disclosure

    # Trenne Text vor und nach CTA
    before_cta = text[:idx].rstrip()
    cta_and_rest = text[idx:]

    return before_cta + disclosure + "\n\n" + cta_and_rest


def _needs_patch_en(description: str) -> bool:
    """True wenn der EN-Offenlegungstext noch nicht in der Beschreibung ist."""
    return _DISCLOSURE_MARKER_EN not in description


def _needs_patch_de(description: str) -> bool:
    """True wenn der DE-Offenlegungstext noch nicht in der Beschreibung ist."""
    return _DISCLOSURE_MARKER_DE not in description


def _patch_item(item: dict) -> tuple[bool, dict]:
    """
    Patcht etsy_description_en und etsy_description_de eines Items.

    Returns:
        (changed: bool, changes: dict) — changed=True wenn mindestens ein Feld geändert wurde,
        changes enthält die alten/neuen Werte für den Diff.
    """
    changes = {}
    changed = False

    desc_en = item.get("etsy_description_en", "")
    desc_de = item.get("etsy_description_de", "")

    if desc_en and _needs_patch_en(desc_en):
        new_en = _insert_before_cta(desc_en, AI_DISCLOSURE_EN)
        changes["etsy_description_en"] = {"old": desc_en, "new": new_en}
        item["etsy_description_en"] = new_en
        changed = True

    if desc_de and _needs_patch_de(desc_de):
        new_de = _insert_before_cta(desc_de, AI_DISCLOSURE_DE)
        changes["etsy_description_de"] = {"old": desc_de, "new": new_de}
        item["etsy_description_de"] = new_de
        changed = True

    return changed, changes


def _find_all_master_listings(images_path: Path) -> list[Path]:
    """Findet alle master-listings.json Dateien rekursiv unter images_path."""
    return sorted(images_path.rglob("master-listings.json"))


def _print_diff(item_id: str, day_folder: str, field: str, old: str, new: str) -> None:
    """Gibt einen lesbaren Diff für ein Feld aus."""
    # Zeige nur die relevante Änderung (letzten 200 Zeichen vor und nach dem Disclosure)
    marker = AI_DISCLOSURE_EN if "en" in field else AI_DISCLOSURE_DE
    insertion_point = new.find(marker)
    if insertion_point >= 0:
        context_before = new[max(0, insertion_point - 60):insertion_point].replace("\n", "↵")
        inserted = new[insertion_point:insertion_point + len(marker)].replace("\n", "↵")
        print(f"    [{field}] Einfügepunkt: ...{context_before!r}")
        print(f"    [{field}] Einfügen:     {inserted!r}")
    else:
        # Fallback: Zeige letzten Teil des neuen Textes
        tail = new[-120:].replace("\n", "↵")
        print(f"    [{field}] Neu (Ende):   ...{tail!r}")


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Fügt KI-Offenlegungstext in bestehende master-listings.json ein."
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        default=False,
        help="Nur anzeigen was geändert würde — keine Dateien schreiben.",
    )
    parser.add_argument(
        "--images-path",
        type=str,
        default=None,
        help="Überschreibt images_path aus config.yaml.",
    )
    args = parser.parse_args()

    dry_run: bool = args.dry_run

    # Config laden — optional wenn --images-path explizit gesetzt ist
    images_path_str: str | None = args.images_path
    if not images_path_str:
        try:
            cfg = load_config()
            images_path_str = cfg.get("IMAGES_PATH") or cfg["config"].get("images_path")
        except Exception as e:
            print(f"❌ Config konnte nicht geladen werden: {e}")
            print(f"   Tipp: --images-path PATH explizit angeben um Config-Load zu umgehen.")
            sys.exit(1)

    if not images_path_str:
        print("❌ images_path nicht ermittelbar (weder --images-path noch config.yaml).")
        sys.exit(1)

    images_path = Path(images_path_str)
    if not images_path.exists():
        print(f"❌ images_path existiert nicht: {images_path}")
        sys.exit(1)

    mode_label = "[DRY-RUN]" if dry_run else "[LIVE]"
    print(f"\n{'='*60}")
    print(f"patch_ai_disclosure.py — {mode_label}")
    print(f"Suche master-listings.json in: {images_path}")
    print(f"{'='*60}\n")

    all_files = _find_all_master_listings(images_path)
    print(f"Gefundene master-listings.json: {len(all_files)}")

    total_items = 0
    patched_items = 0
    skipped_corrupt = 0
    skipped_already_done = 0
    patched_files: list[Path] = []

    for json_path in all_files:
        # Tagesordner = übergeordneter Ordner der Datei
        day_folder = json_path.parent

        try:
            with open(json_path, encoding="utf-8") as f:
                data = json.load(f)
        except Exception as e:
            print(f"⚠️  SKIP (korrupt): {json_path.name} in {day_folder.name} — {e}")
            skipped_corrupt += 1
            continue

        if not isinstance(data, dict) or "items" not in data:
            print(f"⚠️  SKIP (ungültiges Schema): {json_path}")
            skipped_corrupt += 1
            continue

        items = data.get("items", [])
        file_has_changes = False

        for item in items:
            total_items += 1
            item_id = item.get("id", "<unbekannt>")

            # Prüfen ob Disclosure bereits vorhanden
            en = item.get("etsy_description_en", "")
            de = item.get("etsy_description_de", "")

            already_en = not en or not _needs_patch_en(en)
            already_de = not de or not _needs_patch_de(de)

            if already_en and already_de:
                skipped_already_done += 1
                continue

            changed, changes = _patch_item(item)

            if changed:
                patched_items += 1
                file_has_changes = True
                print(f"  PATCH: {item_id} ({day_folder.name})")
                for field, diff in changes.items():
                    _print_diff(item_id, day_folder.name, field, diff["old"], diff["new"])

        if file_has_changes:
            patched_files.append(json_path)
            if not dry_run:
                try:
                    save_master_listings(day_folder, data)
                    print(f"  ✅ Gespeichert: {json_path}")
                except Exception as e:
                    print(f"  ❌ Speichern fehlgeschlagen: {json_path} — {e}")
                    sys.exit(1)

    # === ZUSAMMENFASSUNG ===
    print(f"\n{'='*60}")
    print(f"ZUSAMMENFASSUNG {mode_label}")
    print(f"{'='*60}")
    print(f"  Dateien durchsucht:         {len(all_files)}")
    print(f"  Korrupte Dateien (SKIP):    {skipped_corrupt}")
    print(f"  Items gesamt:               {total_items}")
    print(f"  Bereits gepflegt (SKIP):    {skipped_already_done}")
    print(f"  Items mit Änderungsbedarf:  {patched_items}")

    if dry_run:
        print(f"\n  ⚠️  DRY-RUN — keine Dateien wurden geschrieben.")
        print(f"  Zum Anwenden: python patch_ai_disclosure.py (ohne --dry-run)")
    else:
        print(f"  Dateien aktualisiert:       {len(patched_files)}")
        if patched_items == 0:
            print(f"\n  ✅ Alle Items bereits gepflegt — nichts zu tun.")
        else:
            print(f"\n  ✅ Fertig. {patched_items} Item(s) aktualisiert.")


if __name__ == "__main__":
    main()
