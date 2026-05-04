#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Step_02_Generate_Marketing_CSV.py

Marketing-Pipeline:
- Nutzt Claude API (Modell steuerbar über claude_model_csv in config.yaml)
- Erzeugt im Tagesordner:
    1. metricool_HHMM.csv       → Ein Feld pro Eintrag: Titel + Marketing-Text + Hashtags
    2. master-listings.json     → Single Source of Truth für alle Plattform-Daten (Refactor 2026-04)
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
import anthropic
import yaml
from pathlib import Path

from config_loader import (
    load_config,
    get_day_folder,
    load_master_listings,
    save_master_listings,
    MASTER_LISTINGS_SCHEMA_VERSION,
    atomic_write_json,
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
ETSY_LINK   = "digipicshopdesigns.etsy.com"

# Feste Basis-Hashtags
BASE_HASHTAGS = ["#digipicshop", "#desktop", "#background", "#wallpaper", "#digitalart", "#wallpaperdesign"]

# CTA für Installationsanleitung (wird an Etsy-Descriptions angehängt, vor AI-Disclosure)
CTA_DE = "\n\n📩 Schreib mir für eine kostenlose Installationsanleitung auf Englisch, Deutsch, Spanisch und Portugiesisch für Windows, Mac und Linux. @digipicshop"
CTA_EN = "\n\n📩 Message me for a free installation guide – available in English, German, Spanish, and Portuguese for Windows, Mac, and Linux. @digipicshop"

# Fallback-Reservoir für DE-Tags (BL-075, 2026-05-04)
# Genau 13 allgemeine Tags die zu praktisch allen Wallpapers passen.
# Wird verwendet um etsy_tags_de programmatisch auf 13 aufzufüllen falls
# der LLM-Call zu wenige Tags liefert.
FALLBACK_DE_TAGS = [
    "Digitaler Download",
    "Hintergrundbild",
    "PC Hintergrund",
    "Desktop Wallpaper",
    "Digitale Kunst",
    "Sofortdownload",
    "4K Hintergrundbild",
    "Home Office Deko",
    "Büro Wandbild",
    "Bildschirmhintergrund",
    "Laptop Hintergrund",
    "Computerbild",
    "Wohndekor",
]

# Etsy KI-Offenlegung (Compliance-Pflicht seit 2023, Hiwi-Auftrag 2026-05-03)
# Wird vor dem CTA-Append eingefügt, gesteuert durch Config-Key etsy_ai_disclosure (default: true)
# Text festgelegt durch Majo (Aufgabe 61, 2026-05-03).
AI_DISCLOSURE_EN = (
    "\n\nThis product was created using AI image generation (Leonardo AI) and manually curated."
)
AI_DISCLOSURE_DE = (
    "\n\nDieses Produkt wurde mithilfe von KI-Bildgenerierung (Leonardo AI) erstellt und manuell kuratiert."
)
_AI_DISCLOSURE_ENABLED = bool(config.get("etsy_ai_disclosure", True))

# Tool-Schema für strukturierten Claude-Output (nur Marketing, nicht Etsy)
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
            "stock_tags": {
                "type": "array",
                "items": {"type": "string"},
                "description": "Exactly 20 stock portal tags, lowercase, no # prefix, no numbers.",
            },
        },
        "required": [
            "title", "marketing_text", "social_hashtags", "stock_tags",
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
# Hinweis: `atomic_write_json` wird aus `config_loader` importiert (oben).
# Gehärtete Variante mit Retry/Backoff gegen Windows-Dateilocks — keine lokale
# Kopie mehr. Migration 2026-04-20 (session-log-2026-04-20-d.md).

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

def filter_oversized_tags(tag_string: str, max_chars: int, language_label: str, entry_id: str) -> str:
    """
    Safety-Net für Etsy-Tag-Längenregel.

    Dropt (entfernt, nicht trunciert) alle Tags in einer komma-separierten
    Tag-Liste, die das Zeichenlimit überschreiten. Hintergrund: Mid-Word-Cuts
    sind SEO-schlechter als ein fehlendes Tag; Etsy akzeptiert auch <13 Tags.

    Args:
        tag_string:     Komma-separierte Tags (z. B. "tag1, tag2, tag3")
        max_chars:      Maximale Zeichenlänge pro Tag (inkl. Leerzeichen)
        language_label: "EN" oder "DE" (nur für Log-Ausgabe)
        entry_id:       ID des Eintrags (nur für Log-Ausgabe)

    Returns:
        Gefilterter Tag-String mit max. 13 Tags, alle ≤ max_chars.
    """
    if not tag_string:
        return tag_string

    raw_tags = [t.strip() for t in tag_string.split(",") if t.strip()]
    kept, dropped = [], []
    for tag in raw_tags:
        if len(tag) <= max_chars:
            kept.append(tag)
        else:
            dropped.append(tag)

    if dropped:
        dropped_fmt = ", ".join(f"'{t}' ({len(t)}Z)" for t in dropped)
        print(
            f"   ⚠️  Etsy-Tags {language_label} ({entry_id}): "
            f"{len(dropped)} Tag(s) > {max_chars} Zeichen gedropped: {dropped_fmt}"
        )
        if len(kept) < 13:
            print(
                f"      Hinweis: Nur {len(kept)}/13 Tags übrig für {language_label} "
                f"(Etsy akzeptiert weniger, Pipeline läuft weiter)."
            )

    return ", ".join(kept)


def _strip_ai_disclosure(text: str) -> str:
    """
    Entfernt LLM-generierte AI-Disclosure-Sätze aus Etsy-Descriptions,
    BEVOR der programmatische Standard-Text (AI_DISCLOSURE_EN/DE) angehängt wird.

    Hintergrund (BL-070, 2026-05-04): Haiku generiert trotz "Do NOT"-Prohibition
    in etsy_listing.yaml eigene Disclosure-Sätze, z.B.:
      "This product was created with AI assistance (Leonardo AI)."
    Danach hängt Step_02 den Standard-Text programmatisch an → Dopplung.

    Diese Funktion wird VOR dem Append aufgerufen, also ist unser Standard-Text
    (AI_DISCLOSURE_EN/DE) noch nicht im Text — kein versehentliches Entfernen möglich.

    Bereinigt auch doppelte Leerzeilen, die nach dem Strip entstehen können.
    """
    if not text:
        return text

    # Muster für bekannte LLM-generierte Disclosure-Sätze (case-insensitive).
    # Reihenfolge: spezifischere Muster zuerst.
    patterns = [
        # Englisch — "with AI assistance" Variante (häufigste Haiku-Formulierung)
        r"This product was created with AI assistance[^.]*\.",
        # Englisch — "All designs are curated" (Haiku-Folgesatz)
        r"All designs are curated[^.]*\.",
        # Englisch — "created using AI image generation" (unser eigener Text, zur Sicherheit
        # ebenfalls erfasst — aber da die Funktion VOR dem Append läuft, trifft das
        # nur LLM-generierte Varianten dieses Satzes, nicht unseren Standard-Text)
        r"[Cc]reated using AI image generation[^.]*\.",
        # Deutsch — "mit KI-Unterstützung erstellt"
        r"Dieses Produkt wurde mit KI-Unterstützung erstellt[^.]*\.",
        # Deutsch — "mithilfe von KI-Bildgenerierung" (LLM-Variante, vor dem Append)
        r"mithilfe von KI-Bildgenerierung[^.]*\.",
        # Deutsch — "Alle Designs werden von einem menschlichen"
        r"Alle Designs werden von einem menschlichen[^.]*\.",
    ]

    result = text
    for pattern in patterns:
        result = re.sub(pattern, "", result, flags=re.IGNORECASE)

    # Doppelte Leerzeilen (entstehen wenn ein Satz allein in einem Absatz stand)
    # auf maximal eine Leerzeile reduzieren, danach trailing Whitespace entfernen.
    result = re.sub(r"\n{3,}", "\n\n", result)
    result = result.strip()

    return result


def _fill_de_tags(tags: list, max_count: int = 13) -> list:
    """
    Füllt eine DE-Tag-Liste auf max_count auf, falls sie kürzer ist.

    Hintergrund (BL-075, 2026-05-04): Der LLM liefert konsequent <13 DE-Tags
    (Durchschnitt 11,7). Diese Funktion ergänzt fehlende Tags aus FALLBACK_DE_TAGS,
    sodass Etsy immer mit dem vollen Limit von 13 Tags bedient wird.

    Läuft NACH dem Safety-Net (filter_oversized_tags) — erst droppen, dann auffüllen.
    Nur DE-Tags betroffen, EN-Tags bleiben unverändert.

    Args:
        tags:      Liste von Tag-Strings (bereits gefiltert, ohne Komma-Trennung)
        max_count: Ziellänge (default: 13)

    Returns:
        Liste mit genau max_count Tags (oder weniger, falls Fallback erschöpft).
        Bei len(tags) >= max_count wird auf max_count trunciert.
    """
    if len(tags) >= max_count:
        return tags[:max_count]

    tags_lower = {t.lower() for t in tags}
    filled = list(tags)
    for fallback_tag in FALLBACK_DE_TAGS:
        if len(filled) >= max_count:
            break
        if fallback_tag.lower() not in tags_lower:
            filled.append(fallback_tag)
            tags_lower.add(fallback_tag.lower())
    return filled


# === MASTER-LISTINGS HELPER ===

def build_master_item(entry_id: str, content: dict) -> dict:
    """
    Baut ein master-listings.json Item aus einer entry-id und dem
    content-Dict aus generate_all_content(). Felder, die erst später
    befüllt werden (folder, *_url, product_link, promo_code),
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
        "product_link":        None,   # wird im Listings-Gate gesetzt
        "promo_code":          None,   # wird im Listings-Gate gesetzt
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
            "etsy_de":         "",
            "etsy_en":         "",
            "etsy_tags_de":    "",
            "etsy_tags_en":    "",
            "stock_tags":      "simulated, tags",
            "short_line_de":   "",
            "short_line_en":   "",
            "social_hashtags": all_hashtags_str,
            "etsy_title_de":   "",
            "etsy_title_en":   "",
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
        "All texts must stay true to the scene, style, palette and atmosphere of the image prompt provided."
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

    folder_title   = sanitize_folder_name(title)
    metricool_field = (
        f"{title}\n\n{c['marketing_text'].replace(chr(92), '')}\n\n"
        f"Buy this and more on {ETSY_LINK} or {PAYHIP_LINK}\n\n"
        f"{all_hashtags_str}"
    )

    return {
        "folder_title":    folder_title,
        "title":           title,
        "metricool_field": metricool_field,
        "etsy_title":      title,
        "etsy_de":         "",
        "etsy_en":         "",
        "etsy_tags_de":    "",
        "etsy_tags_en":    "",
        "stock_tags":      stock_tags_str,
        "short_line_de":   "",
        "short_line_en":   "",
        "social_hashtags": all_hashtags_str,
        "etsy_title_de":   "",
        "etsy_title_en":   "",
    }

# === ETSY LISTING GENERATOR ===
def generate_etsy_listing(entry: dict, etsy_prompt_config: dict) -> dict:
    """
    Generiert Etsy-Listing-Felder (8 Stück) via separaten Claude-API-Call.

    Args:
        entry: Ein pending-Entry mit mindestens "prompt" Feld
        etsy_prompt_config: Geladene YAML-Config mit model, max_tokens, system_prompt, user_prompt_template

    Returns:
        Dict mit 8 Etsy-Feldern oder None bei Fehler
    """
    if not claude_client:
        print("❌ Claude API Client nicht initialisiert.")
        return None

    try:
        # YAML-Konfiguration auslesen
        model = etsy_prompt_config.get("model", "claude-haiku-4-5-20251001")
        max_tokens = etsy_prompt_config.get("max_tokens", 2048)
        system_prompt = etsy_prompt_config.get("system_prompt", "")
        user_prompt_template = etsy_prompt_config.get("user_prompt_template", "")

        # Placeholder {art_prompt} mit echtem Prompt befüllen
        art_prompt = entry.get("prompt", "")
        user_prompt = user_prompt_template.format(art_prompt=art_prompt)

        # API-Call (kein Tool-Use, direktes JSON)
        message = claude_client.messages.create(
            model=model,
            max_tokens=max_tokens,
            system=system_prompt,
            messages=[{"role": "user", "content": user_prompt}],
        )

        # JSON aus Response parsen
        response_text = message.content[0].text if message.content else ""

        # Strip Markdown-Fencing falls vorhanden
        response_text = response_text.strip()
        if response_text.startswith("```"):
            # Remove opening fence (```json or ```)
            response_text = response_text.split("\n", 1)[1] if "\n" in response_text else response_text[3:]
            # Remove closing fence
            if response_text.endswith("```"):
                response_text = response_text[:-3]
            response_text = response_text.strip()

        etsy_data = json.loads(response_text)

        # Safety-Net: Tags > 20 Zeichen droppen (symmetrisch für EN+DE).
        # Haiku priorisiert gelegentlich SEO-Kompound-Keywords über die 20-Zeichen-Regel,
        # vor allem auf Englisch. Drop statt Truncate, weil Mid-Word-Cuts SEO-schlechter
        # sind als ein Tag weniger.
        entry_id = entry.get("id", "<unknown>")
        tags_en = filter_oversized_tags(
            etsy_data.get("etsy_tags_en", ""), 20, "EN", entry_id,
        )
        tags_de = filter_oversized_tags(
            etsy_data.get("etsy_tags_de", ""), 20, "DE", entry_id,
        )

        # Die 8 erwarteten Felder zurückgeben (oder None wenn nicht vorhanden)
        return {
            "etsy_title_en": etsy_data.get("etsy_title_en", ""),
            "etsy_title_de": etsy_data.get("etsy_title_de", ""),
            "short_line_en": etsy_data.get("short_line_en", ""),
            "short_line_de": etsy_data.get("short_line_de", ""),
            "etsy_description_en": etsy_data.get("etsy_description_en", ""),
            "etsy_description_de": etsy_data.get("etsy_description_de", ""),
            "etsy_tags_en": tags_en,
            "etsy_tags_de": tags_de,
        }

    except json.JSONDecodeError as e:
        print(f"⚠️  Etsy-Listing-Call fehlgeschlagen für {entry.get('id')}: Ungültiges JSON — {e}")
        return None
    except Exception as e:
        print(f"⚠️  Etsy-Listing-Call fehlgeschlagen für {entry.get('id')}: {e}")
        return None

# === MAIN ===
def main():
    if not RUN_ENABLED:
        print("ℹ️ [csv] ist in run_scripts deaktiviert – nichts zu tun.")
        sys.exit(0)

    if not PENDING_FILE.exists():
        print("❌ prompts_pending.json fehlt.")
        sys.exit(1)

    # YAML-Datei für Etsy-Listing-Prompt laden
    etsy_yaml_path = Path(__file__).parent / "prompts" / "etsy_listing.yaml"
    if not etsy_yaml_path.exists():
        print(f"❌ Etsy-Prompt YAML fehlt: {etsy_yaml_path}")
        sys.exit(1)

    try:
        with etsy_yaml_path.open("r", encoding="utf-8") as f:
            etsy_prompt_config = yaml.safe_load(f)
    except Exception as e:
        print(f"❌ Fehler beim Laden der Etsy-Prompt YAML: {e}")
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

    sim_status              = STATUSES.get("simulation",       "Simulation")
    prompt_generated_status = STATUSES.get("prompt_generated", "Prompt Generated")
    csv_generated_status    = STATUSES.get("csv_generated",    "CSV generated")

    metricool_rows, new_master_items, processed = [], [], 0

    for entry in pending:
        target_status = sim_status if DRYRUN else prompt_generated_status
        if entry.get("status") != target_status:
            continue
        try:
            content = generate_all_content(entry)

            # Etsy-Listing-Call (separat, auch im DRYRUN)
            etsy_data = generate_etsy_listing(entry, etsy_prompt_config)
            if etsy_data:
                # Die 8 Etsy-Felder mergen (überschreiben die Defaults aus generate_all_content)
                content["etsy_title_en"] = etsy_data.get("etsy_title_en", "")
                content["etsy_title_de"] = etsy_data.get("etsy_title_de", "")
                content["short_line_en"] = etsy_data.get("short_line_en", "")
                content["short_line_de"] = etsy_data.get("short_line_de", "")
                content["etsy_en"] = etsy_data.get("etsy_description_en", "")
                content["etsy_de"] = etsy_data.get("etsy_description_de", "")
                content["etsy_tags_en"] = etsy_data.get("etsy_tags_en", "")
                content["etsy_tags_de"] = etsy_data.get("etsy_tags_de", "")

                # DE-Tags auf 13 auffüllen (BL-075, 2026-05-04).
                # Safety-Net (filter_oversized_tags) hat bereits zu lange Tags gedroppt —
                # jetzt aus FALLBACK_DE_TAGS auffüllen wenn < 13 Tags vorhanden.
                _raw_de = [t.strip() for t in content["etsy_tags_de"].split(",") if t.strip()]
                _orig_count = len(_raw_de)
                _filled_de = _fill_de_tags(_raw_de, 13)
                if len(_filled_de) > _orig_count:
                    import logging as _logging
                    _log = _logging.getLogger(__name__)
                    _log.info(
                        f"DE-Tags aufgefüllt von {_orig_count} auf {len(_filled_de)} "
                        f"({entry.get('id', '<unknown>')})"
                    )
                    print(
                        f"   ℹ️  DE-Tags aufgefüllt: {_orig_count} → {len(_filled_de)} Tags "
                        f"({entry.get('id', '<unknown>')})"
                    )
                content["etsy_tags_de"] = ", ".join(_filled_de)

            # LLM-generierte AI-Disclosure-Sätze entfernen (BL-070, 2026-05-04).
            # Muss VOR dem programmatischen Append laufen, damit unser Standard-Text
            # (AI_DISCLOSURE_EN/DE) nicht versehentlich entfernt wird.
            if content["etsy_en"]:
                content["etsy_en"] = _strip_ai_disclosure(content["etsy_en"])
            if content["etsy_de"]:
                content["etsy_de"] = _strip_ai_disclosure(content["etsy_de"])

            # KI-Offenlegung an Etsy-Descriptions anhängen (vor CTA, wenn aktiviert)
            if _AI_DISCLOSURE_ENABLED:
                if content["etsy_en"]:
                    content["etsy_en"] += AI_DISCLOSURE_EN
                if content["etsy_de"]:
                    content["etsy_de"] += AI_DISCLOSURE_DE

            # CTA an Etsy-Descriptions anhängen (immer, auch bei leeren Defaults)
            if content["etsy_en"]:
                content["etsy_en"] += CTA_EN
            if content["etsy_de"]:
                content["etsy_de"] += CTA_DE

            metricool_rows.append([content["metricool_field"]])
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