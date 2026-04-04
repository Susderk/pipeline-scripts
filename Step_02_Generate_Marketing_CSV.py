#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Step_02_Generate_Marketing_CSV.py

Marketing-Pipeline:
- Nutzt Claude API (Modell steuerbar über claude_model_csv in config.yaml)
- Erzeugt DREI CSV-Dateien + Plattform-Listings im Tagesordner:
    1. metricool.csv            → Ein Feld pro Eintrag: Titel + Marketing-Text + Hashtags
    2. listings.csv             → Strukturierte Felder (Gesamt-Quelle, bleibt für Kompatibilität)
    3. listings-master.yaml     → Menschenlesbare Übersicht nach Plattform
    4. payhip-listing.csv       → Payhip-spezifische Felder
    5. youtube-listing.csv      → YouTube-spezifische Felder
    6. etsy-listing.csv         → Etsy EN+DE Felder
    7. meta-listing.csv         → Meta-Post Felder (promo_code leer – manuell eintragen)
    8. stockportal-listing.csv  → Stock-Portal Felder
- Setzt Status auf "CSV generated" und speichert zurück in prompts_pending.json.
- Dry-Run: keine Dateien, nur Simulation.
- Config-Flags: open_listings_excel, open_platform_listings_excel
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

# PyYAML für listings-master.yaml
try:
    import yaml
except ImportError:
    print("❌ PyYAML fehlt. Bitte installieren:")
    print("   pip install pyyaml")
    sys.exit(1)

from config_loader import load_config, get_day_folder

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

OPEN_LISTINGS_EXCEL          = bool(config.get("open_listings_excel",          True))
OPEN_PLATFORM_LISTINGS_EXCEL = bool(config.get("open_platform_listings_excel", True))

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

# === PLATFORM LISTING HELPERS ===

def write_platform_csv(path: Path, fieldnames: list, rows: list) -> None:
    """Schreibt Plattform-CSV mit Semikolon-Trennung, UTF-8-BOM, QUOTE_ALL."""
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, delimiter=";", quoting=csv.QUOTE_ALL)
        writer.writeheader()
        writer.writerows(rows)


def write_master_yaml(path: Path, data: dict) -> None:
    """Schreibt listings-master.yaml (UTF-8, keine BOM)."""
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as f:
        f.write("# Plattform-Listings – generiert aus listings.csv\n")
        f.write("# Menschenlesbare Übersicht über alle Produkte nach Plattform\n\n")
        yaml.dump(data, f, default_flow_style=False, allow_unicode=True, sort_keys=False)


def open_file(path: Path) -> None:
    """Öffnet eine Datei im Standardprogramm (Excel für CSV)."""
    try:
        import subprocess, winreg
        excel_exe = None
        try:
            with winreg.OpenKey(winreg.HKEY_LOCAL_MACHINE,
                                r"SOFTWARE\Microsoft\Windows\CurrentVersion\App Paths\excel.exe") as k:
                excel_exe, _ = winreg.QueryValueEx(k, "")
        except OSError:
            pass
        if excel_exe:
            subprocess.Popen([excel_exe, str(path)])
        else:
            os.startfile(str(path))
        print(f"📊 {path.name} geöffnet.")
    except Exception as e:
        print(f"⚠️  Konnte {path.name} nicht öffnen: {e}")


def generate_platform_listings(rows: list, day_folder: Path, dryrun: bool = False) -> None:
    """
    Generiert alle Plattform-Listing-Dateien aus einer Liste von Dicts.
    Erwartet Felder: etsy_title, etsy_title_en, etsy_title_de,
                     etsy_description_en, etsy_description_de,
                     etsy_tags_en, etsy_tags_de, stock_tags,
                     short_line_en, short_line_de, social_hashtags,
                     promo_code (leer), youtube_url (leer)
    """
    if not rows:
        print("⚠️  Keine Rows für Plattform-Listings – übersprungen.")
        return

    # --- Payhip ---
    payhip_rows = []
    for row in rows:
        etsy_title = row.get("etsy_title", "").strip()
        payhip_rows.append({
            "title":           f"Set of 5 {etsy_title} | 4K Wallpaper Digital Download" if etsy_title else "",
            "youtube_url":     row.get("youtube_url", ""),
            "marketing_text":  row.get("etsy_description_en", "").strip(),
            "ai_disclosure":   "AI-crafted with a human touch ✨",
        })

    # --- YouTube ---
    youtube_rows = []
    for row in rows:
        title_base = row.get("etsy_title_en", row.get("etsy_title", "Wallpaper")).strip()
        yt_title = f"{title_base} | AI Art | 4K Wallpaper #AIWallpaper"[:100]
        youtube_rows.append({
            "title":           yt_title,
            "social_hashtags": row.get("social_hashtags", "").strip(),
        })

    # --- Etsy ---
    etsy_rows = []
    for row in rows:
        etsy_rows.append({
            "etsy_title_en":       row.get("etsy_title_en", "").strip(),
            "etsy_title_de":       row.get("etsy_title_de", "").strip(),
            "etsy_description_en": row.get("etsy_description_en", "").strip(),
            "etsy_description_de": row.get("etsy_description_de", "").strip(),
            "etsy_tags_en":        row.get("etsy_tags_en", "").strip(),
            "etsy_tags_de":        row.get("etsy_tags_de", "").strip(),
            "short_line_en":       row.get("short_line_en", "").strip(),
            "short_line_de":       row.get("short_line_de", "").strip(),
        })

    # --- Meta ---
    meta_rows = []
    for row in rows:
        meta_rows.append({
            "etsy_title":          row.get("etsy_title", "").strip(),
            "etsy_description_en": row.get("etsy_description_en", "").strip(),
            "social_hashtags":     row.get("social_hashtags", "").strip(),
            "promo_code":          row.get("promo_code", ""),  # Ingo trägt manuell ein
        })

    # --- Stockportal ---
    stock_rows = []
    for row in rows:
        stock_rows.append({
            "marketing_title": row.get("etsy_title", "").strip(),
            "stock_tags":      row.get("stock_tags", "").strip(),
        })

    # --- Master YAML ---
    master = {"platform_listings": {"payhip": [], "youtube": [], "etsy": [], "meta": [], "stockportal": []}}
    for idx, row in enumerate(rows, 1):
        etsy_title = row.get("etsy_title", f"Product {idx}").strip()
        title_base = row.get("etsy_title_en", etsy_title).strip()
        master["platform_listings"]["payhip"].append({
            "index": idx, "title": f"Set of 5 {etsy_title} | 4K Wallpaper Digital Download",
            "youtube_url": row.get("youtube_url", ""),
        })
        master["platform_listings"]["youtube"].append({
            "index": idx, "title": f"{title_base} | AI Art | 4K Wallpaper #AIWallpaper"[:100],
            "hashtags": row.get("social_hashtags", "").strip(),
        })
        master["platform_listings"]["etsy"].append({
            "index": idx, "title_en": row.get("etsy_title_en", "").strip(),
            "title_de": row.get("etsy_title_de", "").strip(),
        })
        master["platform_listings"]["meta"].append({
            "index": idx, "title": etsy_title,
            "promo_code": row.get("promo_code", "").strip() or "(Ingo trägt ein)",
        })
        master["platform_listings"]["stockportal"].append({
            "index": idx, "marketing_title": etsy_title,
            "tags": row.get("stock_tags", "").strip(),
        })

    # --- Ausgabe ---
    output_files = {
        "payhip-listing.csv":      (["title", "youtube_url", "marketing_text", "ai_disclosure"],    payhip_rows),
        "youtube-listing.csv":     (["title", "social_hashtags"],                                   youtube_rows),
        "etsy-listing.csv":        (["etsy_title_en", "etsy_title_de", "etsy_description_en",
                                     "etsy_description_de", "etsy_tags_en", "etsy_tags_de",
                                     "short_line_en", "short_line_de"],                             etsy_rows),
        "meta-listing.csv":        (["etsy_title", "etsy_description_en", "social_hashtags",
                                     "promo_code"],                                                  meta_rows),
        "stockportal-listing.csv": (["marketing_title", "stock_tags"],                              stock_rows),
    }

    if dryrun:
        print("\n🔍 DRY-RUN: Plattform-Listings würden geschrieben (nicht wirklich):")
        for filename in output_files:
            print(f"   • {day_folder / filename}")
        print(f"   • {day_folder / 'listings-master.yaml'}")
        return

    print("\n✍️  Schreibe Plattform-Listings:")
    for filename, (fieldnames, rows_data) in output_files.items():
        out_path = day_folder / filename
        write_platform_csv(out_path, fieldnames, rows_data)
        print(f"   ✓ {filename} ({len(rows_data)} Zeilen)")

    master_path = day_folder / "listings-master.yaml"
    write_master_yaml(master_path, master)
    print(f"   ✓ listings-master.yaml")

    if OPEN_PLATFORM_LISTINGS_EXCEL:
        for filename in output_files:
            open_file(day_folder / filename)


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

    metricool_rows, listings_rows, listings_dicts, processed = [], [], [], 0

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
                "",  # promo_code – manuell vor Meta-Post eintragen (optional)
            ])
            # Für Plattform-Listings als Dict (Felder wie in listings.csv-Header)
            listings_dicts.append({
                "stock_tags":          content["stock_tags"],
                "social_hashtags":     content["social_hashtags"],
                "etsy_tags_en":        content["etsy_tags_en"],
                "short_line_en":       content["short_line_en"],
                "etsy_description_en": content["etsy_en"],
                "etsy_tags_de":        content["etsy_tags_de"],
                "short_line_de":       content["short_line_de"],
                "etsy_description_de": content["etsy_de"],
                "etsy_title":          content["etsy_title"],
                "etsy_title_de":       content["etsy_title_de"],
                "etsy_title_en":       content["etsy_title_en"],
                "promo_code":          "",
                "youtube_url":         "",  # wird später von Step_08 befüllt
            })
            if not DRYRUN:
                entry["status"]          = csv_generated_status
                entry["marketing_title"] = content["folder_title"]
            processed += 1
            print(f"✅ Eintrag {entry.get('id')} verarbeitet: '{content['folder_title']}'")
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

    if OPEN_LISTINGS_EXCEL:
        open_file(listings_file)

    # === PLATTFORM-LISTINGS GENERIEREN ===
    generate_platform_listings(listings_dicts, day_folder, dryrun=DRYRUN)

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