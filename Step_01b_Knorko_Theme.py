#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Step_01b_Knorko_Theme.py

Notion Theme-Generierung via Claude API (Anthropic).
Wählt zufällig 1 Eintrag pro Kategorie aus themes.json.
Ruft Claude API auf mit Knorko-System-Prompt.
Speichert Ergebnis in knorko_themes.json (eigene Datei, nicht prompts_pending.json).

Abhängigkeiten:
- pip install anthropic
"""

import os
import sys
import json
import random
from pathlib import Path

from config_loader import load_config

# Config laden
cfg = load_config()
config = cfg.get("config", {})
SCRIPT_PATH        = cfg.get("SCRIPT_PATH")
JSON_PATH          = cfg.get("JSON_PATH")
KNORKO_FILE        = cfg.get("JSON_PATH") / "knorko_themes.json"
THEMES_FILE        = cfg.get("THEMES_FILE")
KNORKO_PROMPT_FILE = cfg.get("KNORKO_PROMPT_FILE")

# Flags
flags = cfg.get("get_script_flags", lambda x: {})("knorko")
RUN_ENABLED = bool(flags.get("run", True))
DRYRUN = bool(flags.get("dry_run", False))

# Product Types
PRODUCT_TYPES = cfg.get("PRODUCT_TYPES", {})
NOTION_THEME_COUNT = PRODUCT_TYPES.get("notion_theme", 0)


def load_themes_json() -> dict:
    """Liest themes.json aus JSON Dateien/ (Pfad aus config)."""
    if not THEMES_FILE.exists():
        print(f"❌ themes.json nicht gefunden: {THEMES_FILE}")
        sys.exit(1)

    try:
        with open(THEMES_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception as e:
        print(f"❌ Fehler beim Lesen von themes.json: {e}")
        sys.exit(1)


def load_system_prompt() -> str:
    """Liest knorko_pipeline_system_prompt.txt aus pipeline/prompts/ (Pfad aus config)."""
    if not KNORKO_PROMPT_FILE.exists():
        print(f"❌ knorko_pipeline_system_prompt.txt nicht gefunden: {KNORKO_PROMPT_FILE}")
        sys.exit(1)

    try:
        with open(KNORKO_PROMPT_FILE, "r", encoding="utf-8") as f:
            return f.read().strip()
    except Exception as e:
        print(f"❌ Fehler beim Lesen von knorko_pipeline_system_prompt.txt: {e}")
        sys.exit(1)


def select_random_entries(themes: dict) -> dict:
    """Wählt zufällig 1 Eintrag pro Kategorie aus themes.json."""
    selected = {}

    categories = {
        "theme_categories": "theme_category",
        "color_palettes": "color_palette",
        "moods": "mood",
        "style_directions": "style_direction",
        "textures": "texture",
        "light_shadow_styles": "light_shadow_style",
        "composition_rules": "composition_rule",
        "icon_set_descriptions": "icon_set_description"
    }

    for list_key, output_key in categories.items():
        entries = themes.get(list_key, [])
        if not entries:
            print(f"⚠️  {list_key} ist leer – keine Auswahl möglich.")
            selected[output_key] = "default"
        else:
            selected[output_key] = random.choice(entries)

    return selected


def build_user_prompt(selected: dict) -> str:
    """Baut den User-Prompt als JSON-String zusammen."""
    return json.dumps(selected, ensure_ascii=False, indent=2)


def call_claude_api(system_prompt: str, user_prompt: str) -> dict:
    """Ruft Claude API auf und gibt geparste JSON zurück."""
    try:
        import anthropic
    except ImportError:
        print("❌ anthropic-Modul nicht installiert. Bitte ausführen: pip install anthropic")
        sys.exit(1)

    api_key = os.environ.get("ANTHROPIC_API_KEY")
    if not api_key:
        print("❌ ANTHROPIC_API_KEY ist nicht als Umgebungsvariable gesetzt.")
        sys.exit(1)

    try:
        client = anthropic.Anthropic(api_key=api_key)

        message = client.messages.create(
            model="claude-haiku-4-5-20251001",
            max_tokens=2000,
            system=system_prompt,
            messages=[
                {"role": "user", "content": user_prompt}
            ]
        )

        response_text = message.content[0].text

        # Entferne ```json ... ``` Wrapper falls vorhanden
        if response_text.startswith("```json"):
            response_text = response_text[7:]
        if response_text.startswith("```"):
            response_text = response_text[3:]
        if response_text.endswith("```"):
            response_text = response_text[:-3]

        response_text = response_text.strip()

        # Parse JSON
        try:
            return json.loads(response_text)
        except json.JSONDecodeError as e:
            print(f"❌ Fehler beim Parsen der Claude API-Response: {e}")
            print(f"Response-Text: {response_text[:200]}")
            sys.exit(1)

    except Exception as e:
        print(f"❌ Claude API-Fehler: {e}")
        sys.exit(1)


def validate_response(response: dict) -> bool:
    """Validiert Pflichtfelder in der Claude-Response."""
    required_fields = ["theme_name", "hex_palette", "moodboard_keywords", "style_guidelines", "asset_prompts"]

    for field in required_fields:
        if field not in response or not response[field]:
            print(f"❌ Pflichtfeld '{field}' fehlt oder ist leer in Claude-Response.")
            return False

    return True


def load_pending_json() -> dict:
    """Liest die aktuelle knorko_themes.json (oder {} wenn nicht vorhanden)."""
    if not KNORKO_FILE.exists():
        return {}

    try:
        with open(KNORKO_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception as e:
        print(f"⚠️  Fehler beim Lesen von knorko_themes.json: {e}")
        return {}


def save_pending_json(data: dict) -> None:
    """Speichert knorko_themes.json atomar."""
    KNORKO_FILE.parent.mkdir(parents=True, exist_ok=True)

    tmp_file = KNORKO_FILE.with_suffix(".tmp")
    try:
        with open(tmp_file, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2, ensure_ascii=False)
        tmp_file.replace(KNORKO_FILE)
    except Exception as e:
        print(f"❌ Fehler beim Speichern von knorko_themes.json: {e}")
        sys.exit(1)


def update_config_status() -> None:
    """Setzt knorko_done: true in config.yaml."""
    config_file = SCRIPT_PATH / "config.yaml"

    try:
        import yaml
    except ImportError:
        print("⚠️  PyYAML nicht installiert – config-Status wird nicht aktualisiert.")
        return

    try:
        with open(config_file, "r", encoding="utf-8") as f:
            config_data = yaml.safe_load(f)

        if "statuses" not in config_data:
            config_data["statuses"] = {}

        config_data["statuses"]["knorko_done"] = "Notion Theme Done"

        with open(config_file, "w", encoding="utf-8") as f:
            yaml.dump(config_data, f, default_flow_style=False, allow_unicode=True)
    except Exception as e:
        print(f"⚠️  Fehler beim Aktualisieren von config.yaml: {e}")


def main():
    print("=" * 60)
    print("🎨 Step 01b – Notion Theme Generierung (Knorko)")
    print("=" * 60)

    # 1. Prüfe, ob Step aktiv ist
    if NOTION_THEME_COUNT == 0:
        print("ℹ️  notion_theme: 0 – Step 01b deaktiviert (kein API-Call).")
        return

    if not RUN_ENABLED:
        print("ℹ️  knorko nicht in run_scripts aktiviert – übersprungen.")
        return

    print(f"Generiere {NOTION_THEME_COUNT} Notion Theme(s)...")

    # 2. Lade Abhängigkeiten
    print("\n[Schritt 1/5] Lade themes.json...")
    themes = load_themes_json()
    print(f"✅ {len(themes)} Kategorien geladen.")

    print("\n[Schritt 2/5] Lade Knorko-System-Prompt...")
    system_prompt = load_system_prompt()
    print("✅ System-Prompt geladen.")

    # 3. Generiere Themes
    notion_themes = []
    for i in range(NOTION_THEME_COUNT):
        print(f"\n[Schritt 3/5] Theme {i+1}/{NOTION_THEME_COUNT} – Wähle Bausteine...")
        selected = select_random_entries(themes)

        print(f"[Schritt 4/5] Theme {i+1}/{NOTION_THEME_COUNT} – Rufe Claude API auf...")
        user_prompt = build_user_prompt(selected)

        if DRYRUN:
            print(f"🔄 DRY-RUN: Claude würde mit folgendem Prompt aufgerufen:")
            print(user_prompt[:200] + "...")
            # Dummy-Antwort für Dry-Run
            theme_package = {
                "theme_name": f"Theme {i+1} (Dry-Run)",
                "hex_palette": [{"hex": "#000000", "role": "background"}],
                "moodboard_keywords": ["dry-run"],
                "style_guidelines": {"composition": "dry-run"},
                "asset_prompts": {"wallpaper_4k": "dry-run"}
            }
        else:
            theme_package = call_claude_api(system_prompt, user_prompt)

            # Validiere
            if not validate_response(theme_package):
                sys.exit(1)

            print(f"✅ Theme '{theme_package['theme_name']}' generiert.")

        notion_themes.append(theme_package)

    # 4. Speichere in pending.json
    print(f"\n[Schritt 5/5] Speichere in pending.json...")
    pending = load_pending_json()

    # Speichere all Themes unter "notion_theme_packages" (Array)
    pending["notion_theme_packages"] = notion_themes

    save_pending_json(pending)
    print(f"✅ {NOTION_THEME_COUNT} Theme(s) in pending.json gespeichert.")

    # 5. Aktualisiere Config-Status
    if not DRYRUN:
        update_config_status()
        print("✅ Config-Status aktualisiert (knorko_done).")

    print("\n" + "=" * 60)
    print("✅ Step 01b erfolgreich abgeschlossen!")
    print("=" * 60)


if __name__ == "__main__":
    main()
