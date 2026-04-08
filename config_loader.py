#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
config_loader.py

Zentrale Loader-Funktion:
- Lädt config.yaml aus dem SCRIPT_PATH (Ort der Skripte).
- Leitet BASE_PATH, JSON_PATH und IMAGES_PATH aus der Config ab.
- Stellt alle relevanten Pfade und Settings bereit.
"""

import sys
import os
import yaml   # PyYAML wird benötigt: pip install pyyaml
import json
import tempfile
import shutil
from datetime import datetime
from pathlib import Path

def get_day_folder(base_path, date_format="%Y-%m-%d", target_date=None):
    if target_date is None:
        target_date = datetime.today()
    year       = target_date.strftime("%Y")
    month_name = target_date.strftime("%B")
    day        = target_date.strftime(date_format)

    base_path    = Path(base_path)
    year_folder  = base_path / year
    month_folder = year_folder / f"{year} {month_name}"
    day_folder   = month_folder / day

    return day_folder

def load_config():
    SCRIPT_PATH = Path(__file__).resolve().parent

    # PIPELINE_CONFIG Env-Variable unterstützen (D1: Option A)
    config_filename = os.environ.get("PIPELINE_CONFIG", "config.yaml")
    CONFIG_FILE = SCRIPT_PATH / config_filename

    if not CONFIG_FILE.exists():
        print(f"❌ Config-Datei '{config_filename}' fehlt: {CONFIG_FILE}")
        sys.exit(1)

    try:
        with CONFIG_FILE.open("r", encoding="utf-8") as f:
            config = yaml.safe_load(f)
    except Exception as e:
        print(f"❌ config.yaml beschädigt oder nicht lesbar: {e}")
        sys.exit(1)

    # === SETTINGS ===
    DATE_FORMAT = config.get("date_format", "%Y-%m-%d")

    # Zieldatum: leer = heute
    target_date_str = str(config.get("target_date", "") or "").strip()
    if target_date_str:
        try:
            TARGET_DATE = datetime.strptime(target_date_str, DATE_FORMAT)
            print(f"📅 Zieldatum aus config.yaml: {target_date_str}")
        except ValueError:
            print(f"❌ Ungültiges Datum 'target_date': '{target_date_str}' (erwartet: {DATE_FORMAT})")
            sys.exit(1)
    else:
        TARGET_DATE = datetime.today()
    STATUSES    = config.get("statuses", {})
    BASE_PATH   = Path(config.get("base_path", SCRIPT_PATH))
    IMAGES_PATH = Path(config.get("images_path", BASE_PATH / "images"))
    JSON_PATH   = BASE_PATH / config.get("json_dir", "JSON Dateien")

    # === STAGING-ISOLATION: Isolierter Temp-Ordner für Staging-Läufe ===
    STAGING_ISOLATION = config.get("staging_isolation", False)
    STAGING_TEMP_DIR = None
    STAGING_TEMP_DIR_ENV = "PIPELINE_STAGING_TEMP_DIR"

    if STAGING_ISOLATION:
        # Prüfe: Hat ein früherer Step bereits einen Staging-Temp-Ordner erstellt?
        if os.environ.get(STAGING_TEMP_DIR_ENV):
            # Ja → wiederverwenden (kein neuer Ordner, keine Print-Meldung)
            STAGING_TEMP_DIR = Path(os.environ[STAGING_TEMP_DIR_ENV])
        else:
            # Nein → neuen Ordner erstellen und in Env-Variable speichern
            staging_base = config.get("staging_temp_dir", None)
            if staging_base:
                STAGING_TEMP_DIR = Path(staging_base)
            else:
                # Fallback: Temp-Ordner im System
                STAGING_TEMP_DIR = Path(tempfile.gettempdir()) / f"pipeline_staging_{datetime.now().strftime('%Y%m%d_%H%M%S')}"

            # Erstelle Staging-Temp-Ordner
            STAGING_TEMP_DIR.mkdir(parents=True, exist_ok=True)
            # Speichere Pfad in Env-Variable für nachfolgende Steps
            os.environ[STAGING_TEMP_DIR_ENV] = str(STAGING_TEMP_DIR)
            # Print-Meldung nur beim ERSTEN Aufruf
            print(f"🎭 STAGING-ISOLATION aktiv: {STAGING_TEMP_DIR}")

        # Leite IMAGES_PATH zu Staging-Ordner um
        IMAGES_PATH = STAGING_TEMP_DIR / "Generated pics"
        IMAGES_PATH.mkdir(parents=True, exist_ok=True)

    # Dateien – JSON Dateien/ oder fixtures/ (wenn Staging-Fixture verwendet wird)
    pending_file_name = config.get("pending_file", "prompts_pending.json")

    # Prüfe: Wurde von Start_Scripts.py eine Fixture-Arbeitskopie im Staging-Temp erstellt?
    if os.environ.get("PIPELINE_STAGING_PENDING_FILE"):
        # Ja → nutze Arbeitskopie (wird bei jedem Staging-Lauf frisch kopiert)
        PENDING_FILE = Path(os.environ["PIPELINE_STAGING_PENDING_FILE"])
    elif "_fixture" in pending_file_name:
        # Alle *_fixture.json Dateien liegen im fixtures/ Ordner im SCRIPT_PATH
        # (wird nur verwendet, wenn PIPELINE_STAGING_PENDING_FILE nicht gesetzt)
        PENDING_FILE = SCRIPT_PATH / "fixtures" / pending_file_name
    else:
        # Standard: JSON Dateien/ Ordner
        PENDING_FILE = JSON_PATH / pending_file_name

    LISTS_FILE    = JSON_PATH / config.get("lists_file",           "lists.json")
    NEGATIVE_FILE = JSON_PATH / config.get("negative_lists_file", "negative_lists.json")
    HOOKS_FILE    = JSON_PATH / config.get("hooks_file",           "hooks.json")
    THEMES_FILE   = JSON_PATH / config.get("themes_file",          "themes.json")

    # Dateien – pipeline/prompts/
    PROMPTS_PATH       = SCRIPT_PATH / config.get("prompts_dir", "prompts")
    KNORKO_PROMPT_FILE = PROMPTS_PATH / config.get("knorko_prompt_file", "knorko_pipeline_system_prompt.txt")
    ATTA_PROMPT_FILE   = PROMPTS_PATH / config.get("atta_prompt_file",   "atta_notion_pipeline_system_prompt.txt")

    # Skript-Flags
    run_scripts_raw = config.get("run_scripts", {})
    dry_run_global  = config.get("dry_run_global", False)
    dry_run_raw     = config.get("dry_run", {})

    # Normalisieren: run_scripts kann Dict oder Liste sein
    if isinstance(run_scripts_raw, list):
        run_scripts = {name: True for name in run_scripts_raw}
    elif isinstance(run_scripts_raw, dict):
        run_scripts = run_scripts_raw
    else:
        run_scripts = {}

    # Normalisieren: dry_run kann Dict oder Liste sein
    if isinstance(dry_run_raw, list):
        dry_run_scripts = {name: True for name in dry_run_raw}
    elif isinstance(dry_run_raw, dict):
        dry_run_scripts = dry_run_raw
    else:
        dry_run_scripts = {}

    # Hilfsfunktion: kombiniert run_scripts und dry_run
    def get_script_flags(script_name):
        run = run_scripts.get(script_name, False)
        dry = dry_run_scripts.get(script_name, False) or dry_run_global
        return {"run": run, "dry_run": dry}

    # === Produkttypen lesen ===
    product_types = config.get("product_types", {})
    if not isinstance(product_types, dict):
        product_types = {}

    # Validation: Wenn notion_theme > 0, prüfe ANTHROPIC_API_KEY
    notion_theme_count = product_types.get("notion_theme", 0)
    if notion_theme_count > 0:
        if not os.environ.get("ANTHROPIC_API_KEY"):
            print("❌ notion_theme > 0 benötigt ANTHROPIC_API_KEY als Umgebungsvariable.")
            sys.exit(1)

    # Hilfsfunktion: gibt nur aktivierte Produkttypen zurück
    def get_active_product_types():
        """Gibt Dict zurück: {product_type: count} – nur Typen mit count > 0"""
        return {k: v for k, v in product_types.items() if v > 0}

    return {
        "config":                config,
        "SCRIPT_PATH":           SCRIPT_PATH,
        "BASE_PATH":             BASE_PATH,
        "IMAGES_PATH":           IMAGES_PATH,
        "JSON_PATH":             JSON_PATH,
        "PENDING_FILE":      PENDING_FILE,
        "LISTS_FILE":        LISTS_FILE,
        "NEGATIVE_FILE":     NEGATIVE_FILE,
        "HOOKS_FILE":        HOOKS_FILE,
        "THEMES_FILE":       THEMES_FILE,
        "PROMPTS_PATH":      PROMPTS_PATH,
        "KNORKO_PROMPT_FILE": KNORKO_PROMPT_FILE,
        "ATTA_PROMPT_FILE":  ATTA_PROMPT_FILE,
        "DATE_FORMAT":           DATE_FORMAT,
        "TARGET_DATE":           TARGET_DATE,
        "STATUSES":              STATUSES,
        "CONFIG_FILE":           CONFIG_FILE,
        "RUN_SCRIPTS":           run_scripts,
        "DRY_RUN_GLOBAL":        dry_run_global,
        "DRY_RUN_SCRIPTS":       dry_run_scripts,
        "get_script_flags":      get_script_flags,
        "PRODUCT_TYPES":         product_types,
        "get_active_product_types": get_active_product_types,
        "STAGING_ISOLATION":     STAGING_ISOLATION,
        "STAGING_TEMP_DIR":      STAGING_TEMP_DIR,
        "remap_pending_entries_to_staging": remap_pending_entries_to_staging
    }


MASTER_LISTINGS_FILENAME = "master-listings.json"
MASTER_LISTINGS_SCHEMA_VERSION = 1


def master_listings_path(day_folder) -> Path:
    """Pfad zu master-listings.json für einen Tagesordner."""
    return Path(day_folder) / MASTER_LISTINGS_FILENAME


def load_master_listings(day_folder, exit_on_error: bool = False) -> dict:
    """
    Lädt master-listings.json eines Tagesordners.
    Gibt das vollständige Dict mit Schlüsseln 'schema_version', 'day_folder',
    'run_date', 'items' zurück. Bei Fehler/fehlender Datei: leeres Skelett.
    """
    path = master_listings_path(day_folder)
    try:
        with open(path, encoding="utf-8") as f:
            data = json.load(f)
        if not isinstance(data, dict) or "items" not in data:
            print(f"❌ {path} hat kein gültiges Schema (dict mit 'items' erwartet).")
            if exit_on_error:
                sys.exit(1)
            return _empty_master_listings(day_folder)
        return data
    except FileNotFoundError:
        print(f"⚠️  master-listings.json nicht gefunden: {path}")
        if exit_on_error:
            sys.exit(1)
        return _empty_master_listings(day_folder)
    except Exception as e:
        print(f"❌ Fehler beim Lesen von {path}: {e}")
        if exit_on_error:
            sys.exit(1)
        return _empty_master_listings(day_folder)


def _empty_master_listings(day_folder) -> dict:
    return {
        "schema_version": MASTER_LISTINGS_SCHEMA_VERSION,
        "day_folder": str(day_folder),
        "run_date": "",
        "items": [],
    }


def save_master_listings(day_folder, data: dict) -> None:
    """Schreibt master-listings.json atomar."""
    path = master_listings_path(day_folder)
    path.parent.mkdir(parents=True, exist_ok=True)
    atomic_write_json(path, data)


def find_master_item(data: dict, item_id: str) -> dict | None:
    """Sucht ein Item per id-Schlüssel. Gibt das dict (Referenz) oder None zurück."""
    if not data or "items" not in data:
        return None
    for it in data["items"]:
        if it.get("id") == item_id:
            return it
    return None


def update_master_item(day_folder, item_id: str, updates: dict) -> bool:
    """
    Aktualisiert Felder eines einzelnen Items in master-listings.json
    und persistiert die Datei. Gibt True bei Erfolg zurück.
    """
    data = load_master_listings(day_folder)
    item = find_master_item(data, item_id)
    if item is None:
        print(f"⚠️  update_master_item: id '{item_id}' nicht gefunden.")
        return False
    item.update(updates)
    save_master_listings(day_folder, data)
    return True


def atomic_write_json(path, data) -> None:
    """Schreibt JSON atomar (via Temp-Datei) um Datenverlust bei Abbruch zu vermeiden."""
    path = Path(path)
    tmp = path.with_suffix(".tmp")
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)
    tmp.replace(path)


def remap_pending_entries_to_staging(entries: list, staging_images_path: Path) -> None:
    """
    Schreibt 'day_folder' (und 'folder') in Pending-Einträgen vom Produktionspfad
    zu Staging-Temp-Ordner um.

    Wird aufgerufen, wenn staging_isolation: true ist.
    Modifiziert die Einträge in-place.

    Beispiel:
    - Input:  "C:/Users/ingos/Digital Pictures Shops/Generated pics/2026/2026 April/2026-04-04"
    - Output: "/tmp/pipeline_staging_YYYYMMDD_HHMMSS/Generated pics/2026/2026 April/2026-04-04"
    """
    if not entries or not isinstance(entries, list):
        return

    for entry in entries:
        # day_folder umschreiben
        if "day_folder" in entry and entry["day_folder"]:
            old_day_folder = Path(entry["day_folder"])
            # Extrahiere den relativen Pfad NACH "Generated pics"
            try:
                # Finde den Index von "Generated pics" im Pfad
                parts = old_day_folder.parts
                if "Generated pics" in parts:
                    idx = parts.index("Generated pics")
                    # relative_path = alles NACH "Generated pics"
                    relative_path = Path(*parts[idx+1:])
                    new_day_folder = staging_images_path / relative_path
                    entry["day_folder"] = str(new_day_folder)
            except (ValueError, IndexError):
                # Fallback: Falls "Generated pics" nicht im Pfad, Warnung und nichts ändern
                print(f"⚠️ remap_pending: 'Generated pics' nicht in Pfad gefunden: {old_day_folder} – Pfad bleibt unverändert")

        # folder umschreiben (falls vorhanden)
        if "folder" in entry and entry["folder"]:
            old_folder = Path(entry["folder"])
            try:
                parts = old_folder.parts
                if "Generated pics" in parts:
                    idx = parts.index("Generated pics")
                    relative_path = Path(*parts[idx+1:])
                    new_folder = staging_images_path / relative_path
                    entry["folder"] = str(new_folder)
            except (ValueError, IndexError):
                # Fallback: Falls "Generated pics" nicht im Pfad, Warnung und nichts ändern
                print(f"⚠️ remap_pending: 'Generated pics' nicht in Pfad gefunden: {old_folder} – Pfad bleibt unverändert")