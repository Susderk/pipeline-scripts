#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Step_02_Generate_Marketing_CSV.py

Marketing-Pipeline:
- Nutzt Claude API (Modell steuerbar über claude_model_csv in config.yaml)
- Erzeugt im Tagesordner:
    1. metricool_HHMM.csv       → Ein Feld pro Eintrag: Titel + Marketing-Text + Hashtags
    2. master-listings.json     → Single Source of Truth für alle Plattform-Daten (Refactor 2026-04)
    3. listings.csv             → DEPRECATED Dual-Write bis Step_06/08/10/11 auf JSON umgestellt sind
                                  (Reihenfolge-Punkt 6 im Refactor-Plan). Danach ersatzlos entfernen.
- Plattform-Listings (payhip/etsy/meta/stockportal/canva/facebook) entstehen NICHT mehr hier,
  sondern verteilt in Step_05/06/08/10/11 — siehe Session-Log 2026-04-07.
- Setzt Status auf "CSV generated" und speichert zurück in prompts_pending.json.
- master-listings.json wird MERGED: bestehende Items bleiben, neue per id ergänzt,
  Kollisionen per id überschreiben das alte Item (sollte dank HHMM-ID nicht vorkommen).
- Dry-Run: keine Dateien, nur Simulation.
"""

import sys
import json
import csv
import re
import os
import random
import time
import anthropic
from pathlib import Path

from config_loader import (
    load_config,
    get_day_folder,
    load_master_listings,
    save_master_listings,
    MASTER_LISTINGS_SCHEMA_VERSION,
)

cfg    = load_config()
config = cfg["config"]

IMAGES_PATH  = cfg["IMAGES_PATH"]
PENDING_FILE = cfg["PENDING_FILE"]
DATE_FORMAT  = cfg["DATE_FORMAT"]
TARGET_DATE  = cfg["TARGET_DATE"]
STATUSES     = cfg["STATUSES"]
LISTS_FILE   = cfg["LISTS_FILE"]
STAGING_ISOLATION = cfg["STAGING_ISOLATION"]
remap_pending_entries_to_staging = cfg["remap_pending_entries_to_staging"]

# Marketing-Winkel aus lists.json laden
try:
    with LISTS_FILE.open("r", encoding="utf-8") as _f:
        _lists = json.load(_f)
    MARKETING_ANGLES = _lists.get("marketing_angles", [])
except Exception:
    MARKETING_ANGLES = []

flags       = cfg["get_script_flags"]("csv")
RUN_ENABLED = bool(flags["run"])
DRYRUN      = bool(flags["dry_run"])

# Feste Shop-Links
PAYHIP_LINK = "payhip.com/digipicshop"
ETSY_LINK   = "etsy.com/shop/DigiPicShopDesigns"

# Feste Basis-Hashtags
BASE_HASHTAGS = ["#digipicshop", "#desktop", "#background", "#wallpaper", "#digitalart", "#wallpaperdesign"]

# CTA für Installationsanleitung (wird an Produktbeschreibungen in listings.csv angehängt)
CTA_DE = "\n\n📩 Schreib mir für eine kostenlose Installationsanleitung auf Englisch, Deutsch, Spanisch und Portugiesisch für Windows, Mac und Linux. @digipicshop"
CTA_EN = "\n\n📩 Message me for a free installation guide – available in English, German, Spanish, and Portuguese for Windows, Mac, and Linux. @digipicshop"

# Tool-Schema für strukturierten Claude-Output (ein Call statt 7)
MARKETING_TOOL = {
    "name": "marketing_content",
    "description": "Generate all marketing content for a digital wallpaper product",
    "input_schema": {
        "type": "object",
        "properties": {
            "title": {
                "type": "string",
                "description": "Short marketing title, max 20 characters, must NOT contain the word 'wallpaper'",
            },
            "marketing_text": {
                "type": "string",
                "description": (
                    "Social media text, max 400 characters, complete sentences, "
                    "vivid description motivating to buy. No resolution or file format mentions."
                ),
            },
            "social_hashtags": {
                "type": "array",
                "items": {"type": "string"},
                "description": (
                    "Exactly 5 relevant hashtags WITHOUT # prefix, no numbers. "
                    "Do NOT include: desktop, wallpaper, background, digipicshop (added separately)."
                ),
            },
            "etsy_description_de": {
                "type": "string",
                "description": (
                    "SEO-optimized German Etsy product text, max 500 characters, complete sentences. "
                    "Include: relevant keywords, mention of digital download and 4K resolution, "
                    "call-to-action at end."
                ),
            },
            "etsy_description_en": {
                "type": "string",
                "description": (
                    "SEO-optimized English Etsy product text, max 500 characters, complete sentences. "
                    "Include: relevant keywords, instant digital download, 4K resolution, "
                    "call-to-action at end."
                ),
            },
            "stock_tags": {
                "type": "array",
                "items": {"type": "string"},
                "description": "Exactly 20 stock portal tags, lowercase, no # prefix, no numbers.",
            },
            "etsy_tags_en": {
                "type": "array",
                "items": {"type": "string"},
                "description": (
                    "Exactly 9 English Etsy tags, max 20 chars each, buyer-intent focused, "
                    "multi-word allowed (e.g. 'forest wallpaper'). No hashtags, no special chars, "
                    "no numbers. Do NOT include: wallpaper, background, digipicshop (added separately)."
                ),
            },
            "etsy_tags_de": {
                "type": "array",
                "items": {"type": "string"},
                "description": (
                    "Exactly 9 German Etsy tags, max 20 chars each, buyer-intent focused, "
                    "multi-word allowed (e.g. 'Waldtapete'). No hashtags, no special chars, "
                    "no numbers. Do NOT include: Wallpaper, Hintergrund, digipicshop (added separately)."
                ),
            },
            "short_line_en": {
                "type": "string",
                "description": (
                    "One complete, catchy English sales sentence for this wallpaper set. "
                    "STRICT LIMIT: 160 characters maximum. "
                    "Write a grammatically finished sentence within this limit. "
                    "Vary structure and tone – do not always start with 'Set of 5'."
                ),
            },
            "short_line_de": {
                "type": "string",
                "description": (
                    "Ein vollständiger, prägnanter deutscher Verkaufssatz für dieses Wallpaper-Set. "
                    "STRIKTE GRENZE: maximal 160 Zeichen. "
                    "Grammatikalisch vollständiger Satz innerhalb dieses Limits. "
                    "Struktur und Ton variieren – nicht immer mit '5 wunderschöne' beginnen."
                ),
            },
            "etsy_title_de": {
                "type": "string",
                "description": (
                    "SEO-optimierter deutscher Etsy-Titel, max. 140 Zeichen. "
                    "Kein künstlerischer Name – stattdessen eine beschreibende Phrase mit klarem Use Case. "
                    "Use Case aus Scene/Style/Atmosphere ableiten: "
                    "Büro/Stadtpanorama/Meeting → VC-Fokus (z.B. '5 professionelle Zoom-Hintergründe | 4K Desktop Wallpaper Set'); "
                    "Natur/Landschaft/Tiere → Desktop-Fokus (z.B. '5 herbstliche Waldtapeten für Desktop und Laptop | 4K Set'); "
                    "Geometrisch/Abstrakt/Minimal → Präsentations-Fokus (z.B. '5 abstrakte Präsentationshintergründe | 4K Desktop Wallpaper'); "
                    "Alles andere → Desktop + Präsentation kombinieren. "
                    "Darf NICHT mit 'wallpaper' beginnen."
                ),
            },
            "etsy_title_en": {
                "type": "string",
                "description": (
                    "SEO-optimized English Etsy title, max. 140 characters. "
                    "No artistic name – use a descriptive phrase with a clear use case. "
                    "Derive use case from Scene/Style/Atmosphere: "
                    "Office/Cityscape/Meeting → VC focus (e.g. '5 Professional Zoom Backgrounds | 4K Desktop Wallpaper Set'); "
                    "Nature/Landscape/Animals → Desktop focus (e.g. '5 Autumn Forest Wallpapers for Desktop and Laptop | 4K Set'); "
                    "Geometric/Abstract/Minimal → Presentation focus (e.g. '5 Abstract Presentation Backgrounds | 4K Desktop Wallpaper'); "
                    "Everything else → combine Desktop + Presentation. "
                    "Must NOT start with 'wallpaper'. "
                    "Use keywords: Zoom Background, Teams Background, Desktop Wallpaper, Presentation Background, 4K Digital Download."
                ),
            },
        },
        "required": [
            "title", "marketing_text", "social_hashtags",
            "etsy_description_de", "etsy_description_en",
            "stock_tags", "etsy_tags_en", "etsy_tags_de",
            "short_line_en", "short_line_de",
            "etsy_title_de", "etsy_title_en",
        ],
    },
}

# === CLAUDE CLIENT ===
_api_key = os.environ.get("Claude_API_Key", "")
if not _api_key and not DRYRUN:
    print("❌ Umgebungsvariable 'Claude_API_Key' nicht gesetzt. Bitte API-Key hinterlegen.")
    sys.exit(1)

claude_client = anthropic.Anthropic(api_key=_api_key) if _api_key else None

# === HELPERS ===
def atomic_write_json(path: Path, data) -> None:
    tmp = path.with_suffix(path.suffix + ".tmp")
    path.parent.mkdir(parents=True, exist_ok=True)
    with tmp.open("w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)
    tmp.replace(path)

def sanitize_folder_name(name: str) -> str:
    cleaned = re.sub(r'[^A-Za-z0-9 ]+', '', name)
    cleaned = re.sub(r'\s+', ' ', cleaned).strip()
    return ' '.join(cleaned.split()[:5]) or "Untitled"

def truncate_to_word(text: str, max_chars: int) -> str:
    if len(text) <= max_chars:
        return text
    truncated  = text[:max_chars]
    last_space = truncated.rfind(' ')
    return truncated[:last_space].rstrip('.,;:!?') if last_space > 0 else truncated

def truncate_hashtag_block(hashtags: list, max_chars: int) -> str:
    result, length = [], 0
    for tag in hashtags:
        needed = len(tag) + (1 if result else 0)
        if length + needed > max_chars:
            break
        result.append(tag)
        length += needed
    return " ".join(result)

def truncate_tag(tag: str, max_chars: int) -> str:
    if len(tag) <= max_chars:
        return tag
    truncated  = tag[:max_chars]
    last_space = truncated.rfind(' ')
    return truncated[:last_space] if last_space > 0 else truncated

# === MASTER-LISTINGS HELPER ===

def build_master_item(entry_id: str, content: dict) -> dict:
    """
    Baut ein master-listings.json Item aus einer entry-id und dem
    content-Dict aus generate_all_content(). Felder, die erst später
    befüllt werden (folder, *_url, payhip_product_link, promo_code),
    starten als null bzw. leerer String.
    """
    return {
        "id":                  entry_id,
        "marketing_title":     content["folder_title"],
        "folder":              "",  # wird in Step_05/06 beim Image-Rename gesetzt
        "etsy_title":          content["etsy_title"],
        "etsy_title_en":       content["etsy_title_en"],
        "etsy_title_de":       content["etsy_title_de"],
        "etsy_description_en": content["etsy_en"],
        "etsy_description_de": content["etsy_de"],
        "etsy_tags_en":        content["etsy_tags_en"],
        "etsy_tags_de":        content["etsy_tags_de"],
        "short_line_en":       content["short_line_en"],
        "short_line_de":       content["short_line_de"],
        "social_hashtags":     content["social_hashtags"],
        "stock_tags":          content["stock_tags"],
        "youtube_url":         None,   # wird in Step_08 gesetzt
        "payhip_product_link": None,   # wird im Approval Gate gesetzt
        "promo_code":          None,   # wird im Approval Gate gesetzt
        "etsy_url":            None,   # wird in Step_10 gesetzt
    }


def merge_master_items(existing: list, new_items: list) -> list:
    """
    Mergt neue Items in eine bestehende Item-Liste.
    - Items mit neuer id werden angehängt.
    - Items mit bereits vorhandener id überschreiben das alte Item
      (sollte dank HHMM in der id nicht vorkommen, ist aber idempotent).
    Reihenfolge: bestehende Items bleiben an Ort und Stelle, neue werden
    am Ende angefügt.
    """
    by_id = {it.get("id"): idx for idx, it in enumerate(existing)}
    merged = list(existing)
    for item in new_items:
        item_id = item.get("id")
        if item_id in by_id:
            merged[by_id[item_id]] = item
        else:
            by_id[item_id] = len(merged)
            merged.append(item)
    return merged


# === CONTENT GENERATOR ===
def generate_all_content(entry: dict) -> dict:
    scene      = entry.get("scenes", "Untitled")
    style      = entry.get("styles", "")
    palette    = entry.get("palettes", "")
    atmosphere = entry.get("atmospheres", "")

    if DRYRUN:
        title = truncate_to_word(scene, 20)
        folder_title = sanitize_folder_name(title)
        all_hashtags_str = truncate_hashtag_block(BASE_HASHTAGS + ["#simulated"], 500)
        return {
            "folder_title":    folder_title,
            "title":           title,
            "metricool_field": f"{title}\n\nSIMULATED marketing text\n\n{all_hashtags_str}",
            "etsy_title":      title,
            "etsy_de":         "SIMULATED Etsy-Text DE" + CTA_DE,
            "etsy_en":         "SIMULATED Etsy text EN" + CTA_EN,
            "etsy_tags_de":    "Wallpaper, Hintergrund, digitaler Download",
            "etsy_tags_en":    "Wallpaper, Background, digital download",
            "stock_tags":      "simulated, tags",
            "short_line_de":   f"5 wunderschöne {folder_title} Wallpapers",
            "short_line_en":   f"Set of 5 beautiful {folder_title} wallpapers",
            "social_hashtags": all_hashtags_str,
            "etsy_title_de":   f"SIMULATED Etsy-Titel DE – {folder_title} | 4K Desktop Wallpaper Set",
            "etsy_title_en":   f"SIMULATED Etsy Title EN – {folder_title} | 4K Desktop Wallpaper Set",
        }

    if not claude_client:
        print("❌ Claude API Client nicht initialisiert.")
        sys.exit(1)

    angle = random.choice(MARKETING_ANGLES) if MARKETING_ANGLES else ""

    system_prompt = (
        "You are a creative copywriter for a digital art shop selling AI-generated 4K wallpapers. "
        "Every product description must feel fresh and unique. "
        "Vary sentence structure, vocabulary, and emotional tone across all text fields – "
        "descriptions, short lines, and marketing text alike. "
        "Never reuse the same opening phrases. "
        "All texts must stay true to the scene, style, palette and atmosphere of the image prompt provided. "
        "IMPORTANT for etsy_title_de and etsy_title_en: these are NOT artistic names. "
        "They must be search-optimized descriptions with a clear use case (e.g. Zoom background, "
        "desktop wallpaper, presentation background). Derive the use case from the scene, style and "
        "atmosphere. Include relevant search keywords buyers actually use on Etsy."
        + (f"\n\nCreative direction for this product: {angle}" if angle else "")
    )

    prompt = (
        f"Generate all marketing content for an AI-generated digital 4K wallpaper.\n"
        f"Scene: '{scene}'\n"
        f"Style: {style}\n"
        f"Palette: {palette}\n"
        f"Atmosphere: {atmosphere}"
    )

    try:
        message = claude_client.messages.create(
            model=config.get("claude_model_csv", config.get("claude_model", "claude-haiku-4-5-20251001")),
            max_tokens=3500,
            system=system_prompt,
            tools=[MARKETING_TOOL],
            tool_choice={"type": "tool", "name": "marketing_content"},
            messages=[{"role": "user", "content": prompt}],
        )
    except Exception as e:
        print(f"❌ Claude API Fehler: {e}")
        sys.exit(1)

    tool_use = next((b for b in message.content if b.type == "tool_use"), None)
    if not tool_use:
        raise ValueError("Claude hat kein tool_use-Ergebnis zurückgegeben.")
    c = tool_use.input

    # Titel: Strip "wallpaper", safety-truncate
    title = truncate_to_word(
        re.sub(r'(?i)wallpaper', '', c["title"]).strip().strip('"').strip("'"), 20
    )

    # Hashtags zusammenbauen
    custom_tags = [t.strip().lstrip('#').split('.')[0] for t in c["social_hashtags"]][:5]
    all_hashtags_str = truncate_hashtag_block(BASE_HASHTAGS + [f"#{t}" for t in custom_tags], 500)

    # Stock-Tags
    stock_tags_str = ", ".join([t.strip().lstrip('#') for t in c["stock_tags"]][:20])

    # Etsy-Tags EN + Pflicht-Tags
    etsy_tags_en_list = [truncate_tag(t.strip().lstrip('#'), 20) for t in c["etsy_tags_en"]][:9]
    for mandatory in ["Wallpaper", "Background", "digital download"]:
        if mandatory.lower() not in [t.lower() for t in etsy_tags_en_list]:
            etsy_tags_en_list.append(mandatory)
    etsy_tags_en_str = ", ".join(etsy_tags_en_list)

    # Etsy-Tags DE + Pflicht-Tags
    etsy_tags_de_list = [truncate_tag(t.strip().lstrip('#'), 20) for t in c["etsy_tags_de"]][:9]
    for mandatory in ["Wallpaper", "Hintergrund", "digitaler Download"]:
        if mandatory.lower() not in [t.lower() for t in etsy_tags_de_list]:
            etsy_tags_de_list.append(mandatory)
    etsy_tags_de_str = ", ".join(etsy_tags_de_list)

    folder_title   = sanitize_folder_name(title)
    short_line_en  = truncate_to_word(c["short_line_en"].strip(), 160)
    short_line_de  = truncate_to_word(c["short_line_de"].strip(), 160)
    etsy_title_de  = truncate_to_word(c["etsy_title_de"].strip(), 140)
    etsy_title_en  = truncate_to_word(c["etsy_title_en"].strip(), 140)
    metricool_field = (
        f"{title}\n\n{c['marketing_text'].replace(chr(92), '')}\n\n"
        f"Buy this and more on {PAYHIP_LINK} or {ETSY_LINK}\n\n"
        f"{all_hashtags_str}"
    )

    return {
        "folder_title":    folder_title,
        "title":           title,
        "metricool_field": metricool_field,
        "etsy_title":      title,
        "etsy_de":         c["etsy_description_de"].strip() + CTA_DE,
        "etsy_en":         c["etsy_description_en"].strip() + CTA_EN,
        "etsy_tags_de":    etsy_tags_de_str,
        "etsy_tags_en":    etsy_tags_en_str,
        "stock_tags":      stock_tags_str,
        "short_line_de":   short_line_de,
        "short_line_en":   short_line_en,
        "social_hashtags": all_hashtags_str,
        "etsy_title_de":   etsy_title_de,
        "etsy_title_en":   etsy_title_en,
    }

# === MAIN ===
def main():
    if not RUN_ENABLED:
        print("ℹ️ [csv] ist in run_scripts deaktiviert – nichts zu tun.")
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

    day_folder = Path(get_day_folder(IMAGES_PATH, DATE_FORMAT, TARGET_DATE))
    day_folder.mkdir(parents=True, exist_ok=True)

    from datetime import datetime as _dt
    _ts = _dt.now().strftime("%H%M")
    metricool_file = day_folder / f"metricool_{_ts}.csv"
    listings_file  = day_folder / "listings.csv"

    sim_status              = STATUSES.get("simulation",       "Simulation")
    prompt_generated_status = STATUSES.get("prompt_generated", "Prompt Generated")
    csv_generated_status    = STATUSES.get("csv_generated",    "CSV generated")

    metricool_rows, listings_rows, new_master_items, processed = [], [], [], 0

    for entry in pending:
        target_status = sim_status if DRYRUN else prompt_generated_status
        if entry.get("status") != target_status:
            continue
        try:
            content = generate_all_content(entry)
            metricool_rows.append([content["metricool_field"]])
            listings_rows.append([
                content["stock_tags"],
                content["social_hashtags"],
                content["etsy_tags_en"],
                content["short_line_en"],
                content["etsy_en"],
                content["etsy_tags_de"],
                content["short_line_de"],
                content["etsy_de"],
                content["etsy_title"],
                content["etsy_title_de"],
                content["etsy_title_en"],
                "",  # promo_code – wird später im Approval Gate eingetragen
            ])
            # Master-listings.json Item (Single Source of Truth)
            entry_id = entry.get("id")
            if not entry_id:
                raise ValueError("Eintrag hat keine id – Step_01 muss zuerst laufen.")
            new_master_items.append(build_master_item(entry_id, content))
            if not DRYRUN:
                entry["status"]          = csv_generated_status
                entry["marketing_title"] = content["folder_title"]
            processed += 1
            print(f"✅ Eintrag {entry_id} verarbeitet: '{content['folder_title']}'")
        except Exception as e:
            print(f"⚠️ Fehler bei Eintrag {entry.get('id')}: {e}")

    if DRYRUN:
        print(f"🧪 DRY-RUN: {processed} Einträge simuliert. Keine Dateien geschrieben.")
        return

    try:
        with metricool_file.open("w", newline="", encoding="utf-8-sig") as f:
            writer = csv.writer(f, quoting=csv.QUOTE_ALL)
            writer.writerows(metricool_rows)
        print(f"📄 Metricool-CSV erstellt: {metricool_file}")
    except Exception as e:
        print(f"❌ Schreiben von {metricool_file} fehlgeschlagen: {e}")
        sys.exit(1)

    # listings CSV schreiben – anhängen falls bereits vorhanden, mit Retry falls gesperrt
    listings_is_new = not listings_file.exists()
    while True:
        try:
            with listings_file.open("a", newline="", encoding="utf-8-sig") as f:
                writer = csv.writer(f, quoting=csv.QUOTE_ALL, delimiter=';')
                if listings_is_new:
                    writer.writerow(["stock_tags", "social_hashtags",
                                     "etsy_tags_en", "short_line_en", "etsy_description_en",
                                     "etsy_tags_de", "short_line_de", "etsy_description_de",
                                     "etsy_title", "etsy_title_de", "etsy_title_en",
                                     "promo_code"])
                writer.writerows(listings_rows)
            action = "erstellt" if listings_is_new else "ergänzt"
            print(f"📋 Listings-CSV {action}: {listings_file}")
            break

        except PermissionError:
            print()
            print(f"⚠️  {listings_file.name} ist gesperrt – wahrscheinlich in Excel geöffnet.")
            print(f"   Bitte schließe die Datei in Excel:")
            print(f"   {listings_file}")
            print()
            try:
                antwort = input("   Danach ENTER drücken um es erneut zu versuchen, oder 'q' zum Abbrechen: ").strip().lower()
            except KeyboardInterrupt:
                print("\n⚠️  Abgebrochen.")
                sys.exit(1)
            if antwort == 'q':
                print("❌ Abgebrochen.")
                sys.exit(1)
            time.sleep(1)
            continue

        except Exception as e:
            print(f"❌ Schreiben von {listings_file} fehlgeschlagen: {e}")
            sys.exit(1)

    # === MASTER-LISTINGS.JSON (Single Source of Truth) ===
    # Merge: bestehende Items behalten, neue per id anhängen bzw. überschreiben.
    try:
        master = load_master_listings(day_folder)
        master["schema_version"] = MASTER_LISTINGS_SCHEMA_VERSION
        master["day_folder"]     = str(day_folder)
        master["run_date"]       = TARGET_DATE.strftime(DATE_FORMAT) if hasattr(TARGET_DATE, "strftime") else str(TARGET_DATE)
        master["items"]          = merge_master_items(master.get("items", []), new_master_items)
        save_master_listings(day_folder, master)
        print(f"🗂️  master-listings.json geschrieben ({len(new_master_items)} neu, "
              f"{len(master['items'])} gesamt): {day_folder / 'master-listings.json'}")
    except Exception as e:
        print(f"❌ Schreiben von master-listings.json fehlgeschlagen: {e}")
        sys.exit(1)

    # === STAGING-ISOLATION: Remap day_folder/folder zu Staging-Temp-Ordner ===
    # Dies ist WICHTIG: Nachfolgende Steps (03, 07a) müssen den korrekten Staging-Pfad sehen
    if STAGING_ISOLATION and not DRYRUN:
        remap_pending_entries_to_staging(pending, IMAGES_PATH)
        print(f"🎭 Pending-Einträge zu Staging-Ordner remapped (nach CSV-Generierung).")

    try:
        atomic_write_json(PENDING_FILE, pending)
    except Exception as e:
        print(f"❌ Schreiben von {PENDING_FILE} fehlgeschlagen: {e}")
        sys.exit(1)

    print(f"🔄 {processed} Einträge auf '{csv_generated_status}' gesetzt.")


if __name__ == "__main__":
    main()