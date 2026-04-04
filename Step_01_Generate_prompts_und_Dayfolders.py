#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Step_01_Generate_prompts_und_Dayfolders.py
Überarbeitet: sichere Auswahl, gewichtete Zufallsauswahl, 70% Seasonal Bias,
robustes Exclusion-Parsing, Fallbacks für leere Listen.

Unterstützt zwei AI-Backends, steuerbar über config.yaml:
  prompt_provider: "claude"   --> Anthropic Claude (API Key: Claude_API_Key)
  prompt_provider: "openai"   --> OpenAI GPT      (API Key: OPENAI_API_KEY)
"""

import os
import json
import random
import sys
import time
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional, Dict, Any, List

# Erwarte, dass load_config und get_day_folder in deiner Umgebung existieren
from config_loader import load_config, get_day_folder

cfg = load_config()
config = cfg.get("config", {})

# Pfade / Konstanten aus config
PENDING_FILE = cfg.get("PENDING_FILE")
LISTS_FILE = cfg.get("LISTS_FILE")
NEGATIVE_FILE = cfg.get("NEGATIVE_FILE")
IMAGES_PATH = cfg.get("IMAGES_PATH")
DATE_FORMAT = cfg.get("DATE_FORMAT", "%Y-%m-%d")
TARGET_DATE = cfg.get("TARGET_DATE")
JSON_PATH = Path(cfg.get("JSON_PATH", "."))

EXCLUSION_FILE = JSON_PATH / "scene_exclusions.json"
COUNTS_FILE = JSON_PATH / "scene_counts.json"

# Flags
flags = cfg.get("get_script_flags", lambda x: {} )("prompts")
RUN_ENABLED = bool(flags.get("run", True))
DRYRUN = bool(flags.get("dry_run", False))

# Settings mit Fallbacks
PROMPT_COUNT    = int(config.get("prompt_count", 5))
NEGATIVE_PROMPT = config.get("negative_prompt", "")
CHAT_TEMP       = float(config.get("chat_temperature", 0.2))
CHAT_MAXTOK     = int(config.get("chat_max_tokens", 600))
PAUSE_SEC       = float(config.get("chat_pause_sec", 0.12))
MODEL_ID        = config.get("model_id", "seedream-4.5")

# Provider-Auswahl aus config.yaml: "claude" oder "openai"
PROMPT_PROVIDER = config.get("prompt_provider", "claude").strip().lower()

if PROMPT_PROVIDER == "openai":
    from openai import OpenAI
    CHAT_MODEL = config.get("openai_chat_model", "gpt-4o-mini")
elif PROMPT_PROVIDER == "claude":
    import anthropic
    CHAT_MODEL = config.get("claude_chat_model", "claude-haiku-4-5-20251001")
else:
    print(f"❌ Unbekannter prompt_provider: '{PROMPT_PROVIDER}'. Erlaubt: 'claude' oder 'openai'.")
    sys.exit(1)

print(f"ℹ️  Prompt-Provider: {PROMPT_PROVIDER.upper()} | Modell: {CHAT_MODEL}")

SYSTEM_MSG = (
    f"You are an expert prompt engineer for image generation, producing concise, high-quality prompts "
    f"optimized for Leonardo AI's {MODEL_ID} model. Output only the final prompt text.\n\n"
    "STRICT RULES - these apply to every prompt you write:\n"
    "1. NEVER use words or phrases that may trigger content moderation filters, including but not limited to: "
    "'bleeding', 'blood', 'wound', 'injury', 'dead', 'dying', 'kill', 'gore', 'violence', 'shot', "
    "'newborn' combined with any bodily term, 'child' combined with any bodily term.\n"
    "2. For photographic/compositional terms use ONLY safe alternatives:\n"
    "   - Instead of 'light bleeding' use: 'light spill', 'soft light transition', 'luminous haze'\n"
    "   - Instead of 'blown out' use: 'overexposed highlights', 'washed-out background'\n"
    "   - Instead of 'dead space' use: 'empty foreground', 'negative space'\n"
    "   - Instead of 'shot' (photo) use: 'captured', 'photographed', 'framed'\n"
    "   - Instead of 'dying light' use: 'fading light', 'golden hour', 'dusk glow'\n"
    "3. Keep the prompt focused on visual aesthetics, lighting, composition, and style only.\n"
    "4. Output ONLY the final prompt text, no explanations, no preamble."
)

# Wörter/Phrasen, die den Leonardo Content-Filter auslösen können.
# Wird als Post-Processing-Check auf jeden generierten Prompt angewendet.
_FORBIDDEN_TERMS = [
    "bleeding", "blood", "wound", "wounded", "injury", "injured",
    "dead ", "dying", "killed", "gore", "violent", "violence",
    " shot ", "gunshot", "stab",
]

def sanitize_prompt(prompt: str) -> tuple[str, list[str]]:
    """
    Prüft den Prompt auf verbotene Begriffe.
    Gibt (bereinigter_prompt, liste_der_ersetzungen) zurück.
    Bekannte sichere Ersetzungen werden automatisch angewandt,
    unbekannte werden gemeldet.
    """
    replacements_made = []
    safe_replacements = {
        "light bleeding":  "light spill",
        "color bleeding":  "color spill",
        "edge bleeding":   "soft edge transition",
        "bleeding into":   "spilling into",
        "bleeding":        "spilling",
        "blown out":       "overexposed",
        "dead space":      "empty space",
        "dead center":     "centered",
        "dying light":     "fading light",
        "dying sun":       "setting sun",
        " shot ":          " captured ",
        "gunshot":         "impact",
        "blood":           "vivid red",
        "wound":           "mark",
    }

    result = prompt
    lower = result.lower()
    for bad, good in safe_replacements.items():
        if bad in lower:
            # case-insensitive replace
            import re
            result = re.sub(re.escape(bad), good, result, flags=re.IGNORECASE)
            replacements_made.append(f"'{bad}' → '{good}'")
            lower = result.lower()

    # Restliche verbotene Begriffe die keine Ersetzung haben
    remaining_flags = [t for t in _FORBIDDEN_TERMS if t in result.lower()]

    return result, replacements_made, remaining_flags

USER_TEMPLATE = (
    f"Create an image generation prompt for Leonardo AI's {MODEL_ID} model using these components. "
    "Enhance the visual quality and artistic detail, but strictly follow all system rules about forbidden words. "
    "Output only the improved prompt, nothing else.\n\n"
    "Components:\n"
    "Scene: {scene}; Style: {style}; Palette: {palette}; Atmosphere: {atmosphere}; "
    "Technique: {technique}; Seam: {seam}; Composition: {composition}; NegativeHint: {negative_hint}."
)

# -----------------------
# Hilfsfunktionen
# -----------------------
def ensure_dir(p: Path):
    p.mkdir(parents=True, exist_ok=True)

def load_json(path: Path):
    try:
        with path.open("r", encoding="utf-8") as f:
            return json.load(f)
    except FileNotFoundError:
        return None
    except Exception as e:
        print(f"[load_json] Fehler beim Laden {path}: {e}", file=sys.stderr)
        return None

def save_json(path: Path, data):
    ensure_dir(path.parent)
    tmp = path.with_suffix(".tmp")
    try:
        with tmp.open("w", encoding="utf-8") as f:
            json.dump(data, f, indent=2, ensure_ascii=False)
        tmp.replace(path)
    except Exception as e:
        print(f"[save_json] Fehler beim Speichern {path}: {e}", file=sys.stderr)

def clean_list(items):
    if not isinstance(items, list):
        return []
    return [s for s in items if isinstance(s, str) and s.strip()]

# Sicheres random.choice mit Fallback
def safe_choice(lst: List[str], fallback: str):
    if not lst:
        return fallback
    return random.choice(lst)

# -----------------------
# Laden von Listen & Daten
# -----------------------
def load_lists():
    raw = load_json(Path(LISTS_FILE)) or {}
    lists = {}
    # Standardkeys
    for key in ["scenes", "styles", "palettes", "atmospheres", "techniques", "seams", "compositions"]:
        lists[key] = clean_list(raw.get(key, []))

    # locale-unabhängiger Monatszugriff: benutze Monatsnummer des Zieldatums
    month = TARGET_DATE.month  # 1..12
    month_key = f"scenes_{month}"
    lists["seasonal_scenes"] = clean_list(raw.get(month_key, []))

    # fallback für compositions
    if not lists["compositions"]:
        lists["compositions"] = ["negative space", "normal composition"]

    return lists

def load_negative():
    return load_json(Path(NEGATIVE_FILE)) or {"text": [], "faces": []}

def load_counts():
    return load_json(COUNTS_FILE) or {}

def save_counts(counts):
    save_json(COUNTS_FILE, counts)

# Robustes Laden der Exclusions: ignoriert fehlerhafte Datumsstrings
def load_exclusions():
    data = load_json(EXCLUSION_FILE) or {}
    today = datetime.today().date()
    cleaned: Dict[str, str] = {}
    for scene, date_str in data.items():
        if not isinstance(scene, str) or not isinstance(date_str, str):
            continue
        try:
            d = datetime.fromisoformat(date_str).date()
            if d >= today:
                cleaned[scene] = date_str
        except Exception:
            # fehlerhafte Einträge ignorieren, aber loggen
            print(f"[load_exclusions] Ungültiges Datum für Szene '{scene}': {date_str}", file=sys.stderr)
            continue
    return cleaned

def save_exclusions(exclusions: Dict[str, str]):
    save_json(EXCLUSION_FILE, exclusions)

# -----------------------
# Gewichtete Szenenauswahl
# -----------------------
def weighted_scene(seasonal: List[str], normal: List[str], counts: Dict[str, int], exclusions: Dict[str, str], seasonal_bias: float = 0.7):
    """
    Wählt eine Szene aus seasonal oder normal mit Wahrscheinlichkeit seasonal_bias.
    Verwendet echte gewichtete Zufallsauswahl, Gewicht = 1/(1+count).
    Ignoriert ausgeschlossene Szenen (exclusions keys).
    """
    today = datetime.today().date()

    def filter_excluded(lst: List[str]) -> List[str]:
        return [s for s in lst if s not in exclusions]

    seasonal_f = filter_excluded(seasonal)
    normal_f = filter_excluded(normal)

    # Wähle Pool: entweder seasonal oder normal, abhängig von Verfügbarkeit und Bias
    if seasonal_f and normal_f:
        chosen_list = seasonal_f if random.random() < seasonal_bias else normal_f
    else:
        chosen_list = seasonal_f or normal_f or ["generic scene"]

    # Berechne Gewichte invers zu counts: seltenere Szenen haben höhere Wahrscheinlichkeit
    population = chosen_list
    weights = []
    for s in population:
        c = counts.get(s, 0)
        # Gewicht: 1/(1+count) -> bei count=0 Gewicht=1.0, bei count=9 Gewicht=0.1
        w = 1.0 / (1 + float(c))
        weights.append(w)

    # Falls alle Gewichte 0 (theoretisch nicht möglich), fallback auf uniform
    if not any(weights):
        return random.choice(population)

    # random.choices akzeptiert rohe Gewichte
    try:
        chosen = random.choices(population, weights=weights, k=1)[0]
    except Exception as e:
        print(f"[weighted_scene] random.choices Fehler: {e}", file=sys.stderr)
        chosen = random.choice(population)
    return chosen

# -----------------------
# AI-Aufruf (Claude oder OpenAI, je nach config.yaml)
# -----------------------
def client_call(system_msg, user_msg):
    if PROMPT_PROVIDER == "claude":
        key = os.environ.get("Claude_API_Key", "").strip()
        if not key:
            raise RuntimeError("Umgebungsvariable 'Claude_API_Key' nicht gesetzt.")
        client = anthropic.Anthropic(api_key=key)
        resp = client.messages.create(
            model=CHAT_MODEL,
            max_tokens=CHAT_MAXTOK,
            temperature=CHAT_TEMP,
            system=system_msg,
            messages=[{"role": "user", "content": user_msg}]
        )
        return resp.content[0].text.strip()

    elif PROMPT_PROVIDER == "openai":
        key = os.environ.get("OPENAI_API_KEY", "").strip()
        if not key:
            raise RuntimeError("Umgebungsvariable 'OPENAI_API_KEY' nicht gesetzt.")
        client = OpenAI(api_key=key)
        resp = client.chat.completions.create(
            model=CHAT_MODEL,
            messages=[
                {"role": "system", "content": system_msg},
                {"role": "user", "content": user_msg}
            ],
            temperature=CHAT_TEMP,
            max_tokens=CHAT_MAXTOK
        )
        return resp.choices[0].message.content.strip()


def build_prompt(scene, style, palette, atmosphere, technique, seam, composition, negative_hint, dryrun):
    user_msg = USER_TEMPLATE.format(
        scene=scene, style=style, palette=palette, atmosphere=atmosphere,
        technique=technique, seam=seam, composition=composition,
        negative_hint=negative_hint or ""
    )

    if dryrun:
        return {"prompt": f"{scene} in {style} ... [SIM]", "source": "simulated"}

    try:
        final = client_call(system_msg=SYSTEM_MSG, user_msg=user_msg)
        # Post-Processing: verbotene Begriffe prüfen und ersetzen
        final, replacements, remaining = sanitize_prompt(final)
        if replacements:
            print(f"[sanitize] Ersetzungen vorgenommen: {replacements}", file=sys.stderr)
        if remaining:
            print(f"[sanitize] ⚠️ Noch verbotene Begriffe im Prompt: {remaining}", file=sys.stderr)
            print(f"[sanitize] Prompt: {final[:120]}...", file=sys.stderr)
        return {"prompt": final, "source": PROMPT_PROVIDER}
    except Exception as e:
        print(f"[{PROMPT_PROVIDER.upper()}-Error] {e}", file=sys.stderr)
        fallback = f"{scene} in {style} style, {atmosphere}, palette {palette}, technique {technique}, {seam}, composition {composition}"
        if negative_hint:
            fallback += f", negative prompt: {negative_hint}"
        return {"prompt": fallback, "source": "fallback"}

# -----------------------
# MAIN
# -----------------------
def main():
    if not RUN_ENABLED:
        print("disabled")
        return

    # Optional: deterministischer Seed für Debugging (nicht in Produktion)
    debug_seed = config.get("debug_seed")
    if debug_seed is not None:
        try:
            random.seed(int(debug_seed))
            print(f"[main] random seed gesetzt: {debug_seed}")
        except Exception:
            pass

    lists = load_lists()
    exclusions = load_exclusions()
    counts = load_counts()

    day_folder = Path(get_day_folder(IMAGES_PATH, DATE_FORMAT, TARGET_DATE))
    ensure_dir(day_folder)

    pending_existing = load_json(Path(PENDING_FILE)) or []
    new_entries = []

    today_str = TARGET_DATE.strftime(DATE_FORMAT)

    # Idempotency guard: skip if entries for this day_folder already exist
    existing_for_date = [e for e in pending_existing if e.get("day_folder") == str(day_folder)]
    if existing_for_date:
        print(f"⚠️  {len(existing_for_date)} Einträge für {today_str} bereits vorhanden – Step 1 übersprungen.")
        return
    release_date = (TARGET_DATE + timedelta(days=60)).date().isoformat()

    # sichere Fallbacks für leere Listen
    default_style = "default style"
    default_palette = "default palette"
    default_atmosphere = "neutral"
    default_technique = "digital painting"
    default_seam = "no seam"
    default_composition = "normal composition"

    for i in range(PROMPT_COUNT):
        scene = weighted_scene(
            lists.get("seasonal_scenes", []),
            lists.get("scenes", []),
            counts,
            exclusions,
            seasonal_bias=0.7  # 70% Seasonal Bias wie gewünscht
        )

        style = safe_choice(lists.get("styles", []), default_style)
        palette = safe_choice(lists.get("palettes", []), default_palette)
        atmosphere = safe_choice(lists.get("atmospheres", []), default_atmosphere)
        technique = safe_choice(lists.get("techniques", []), default_technique)
        seam = safe_choice(lists.get("seams", []), default_seam)
        composition = safe_choice(lists.get("compositions", []), default_composition)

        result = build_prompt(scene, style, palette, atmosphere, technique, seam, composition, NEGATIVE_PROMPT, DRYRUN)

        # Count erhöhen
        counts[scene] = counts.get(scene, 0) + 1

        # Szene für 7 Tage ausschließen (ISO-String)
        exclusions[scene] = release_date

        entry = {
            "id": today_str + f"_{i+1:03d}",
            "timestamp": datetime.now().isoformat(),
            "scenes": scene,
            "styles": style,
            "palettes": palette,
            "atmospheres": atmosphere,
            "techniques": technique,
            "seams": seam,
            "composition": composition,
            "prompt": result["prompt"],
            "prompt_source": result["source"],
            "status": cfg.get("STATUSES", {}).get("prompt_generated", "Prompt Generated"),
            "day_folder": str(day_folder)
        }

        new_entries.append(entry)
        time.sleep(PAUSE_SEC)

    # speichern
    try:
        save_json(Path(PENDING_FILE), pending_existing + new_entries)
    except Exception as e:
        print(f"[main] Fehler beim Speichern PENDING_FILE: {e}", file=sys.stderr)

    save_counts(counts)
    save_exclusions(exclusions)

    print(f"Generated {len(new_entries)} prompts.")

if __name__ == "__main__":
    main()