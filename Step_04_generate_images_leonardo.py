#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Step_04_generate_images_leonardo.py

Erzeugt Bilder mit der Leonardo API basierend auf Einträgen in prompts_pending.json.
Unterstützt automatisch V1 (FLUX Dev, Phoenix etc.) und V2 (Seedream 4.5 etc.)

V1 Modelle (UUID-basiert):
  FLUX Dev:     b2614463-296c-462a-9586-aafdb8f00e36
  FLUX Schnell: b820ea11-02bf-4652-97ae-9ac0cc9e516c

V2 Modelle (String-ID):
  Seedream 4.5: seedream-4.5
  Seedream 4.0: seedream-4.0
  Lucid Origin: lucid-origin
  Lucid Realism:lucid-realism
"""

import sys, json, time, requests
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception
from datetime import datetime
from pathlib import Path
from typing import Dict, Any, List
from config_loader import load_config, get_day_folder

# === CONFIG ===
cfg = load_config()
config = cfg["config"]

BASE_PATH    = Path(config.get("base_path")).resolve()
JSON_DIR     = BASE_PATH / config.get("json_dir", "JSON Dateien")
IMAGES_PATH  = Path(config.get("images_path", BASE_PATH / "images")).resolve()
PENDING_FILE = JSON_DIR / "prompts_pending.json"
DONE_FILE    = JSON_DIR / "prompts_done.json"
DATE_FORMAT  = config.get("date_format", "%Y-%m-%d")
TARGET_DATE  = cfg["TARGET_DATE"]
STATUSES     = cfg["STATUSES"]

flags_images = cfg["get_script_flags"]("images")
RUN_ENABLED  = bool(flags_images.get("run", True))
DRYRUN       = bool(flags_images.get("dry_run", False))

LOGGING_EIN  = bool(config.get("logging_ein", False))
IMG_TOTAL    = int(config.get("image_total", config.get("image_count", 10)))
IMG_WIDTH    = int(config.get("width", 960))
IMG_HEIGHT   = int(config.get("height", 540))
MODEL_ID     = config.get("model_id", "b2614463-296c-462a-9586-aafdb8f00e36")

POLL_MAX_ATTEMPTS = int(config.get("poll_max_attempts", 30))
POLL_DELAY_SEC    = float(config.get("poll_delay_sec", 2.0))
INITIAL_DELAY_SEC = float(config.get("poll_initial_delay_sec", 8.0))

# V2-Modelle erkennen: String-IDs (keine UUIDs)
_V2_MODELS = {"seedream-4.5", "seedream-4.0", "lucid-origin", "lucid-realism",
              "nano-banana", "nano-banana-2", "nano-banana-pro",
              "phoenix", "ideogram-3.0"}
USE_V2 = MODEL_ID.lower() in _V2_MODELS

# API Endpoints
_API_V1 = "https://cloud.leonardo.ai/api/rest/v1/generations"
_API_V2 = "https://cloud.leonardo.ai/api/rest/v2/generations"
LEONARDO_API_URL = _API_V2 if USE_V2 else _API_V1

# === API KEY ===
def os_environ(key: str) -> str:
    import os
    return os.environ.get(key, "")

LEONARDO_API_KEY = None

if not DRYRUN:
    env_key = os_environ("LEONARDO_API_KEY")
    if env_key:
        LEONARDO_API_KEY = env_key.strip()
    else:
        key_file = Path.home() / ".leonardo_api_key"
        if key_file.is_file():
            try:
                LEONARDO_API_KEY = key_file.read_text(encoding="utf-8").strip()
            except Exception:
                LEONARDO_API_KEY = None

if not DRYRUN and not LEONARDO_API_KEY:
    print("❌ Leonardo API Key nicht gefunden!")
    sys.exit(1)

print(f"ℹ️  Leonardo API {'V2' if USE_V2 else 'V1'} | Modell: {MODEL_ID}")

# === RETRY HELPERS ===
def _is_retriable(exc: Exception) -> bool:
    if isinstance(exc, requests.exceptions.HTTPError):
        return exc.response is not None and exc.response.status_code in (429, 500, 502, 503, 504)
    return isinstance(exc, (requests.exceptions.ConnectionError, requests.exceptions.Timeout))

def _before_sleep(retry_state) -> None:
    exc  = retry_state.outcome.exception()
    wait = retry_state.next_action.sleep
    print(f"⏳ Retry {retry_state.attempt_number}/3 – {type(exc).__name__}: {exc} | warte {wait:.0f}s ...")

_api_retry = retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=4, max=30),
    retry=retry_if_exception(_is_retriable),
    before_sleep=_before_sleep,
    reraise=True,
)

@_api_retry
def _post_generation(headers: Dict, payload: Dict) -> Dict:
    response = requests.post(LEONARDO_API_URL, headers=headers, json=payload, timeout=60)
    response.raise_for_status()
    return response.json()

@_api_retry
def _poll_get(poll_url: str, headers: Dict) -> Dict:
    response = requests.get(poll_url, headers=headers, timeout=30)
    response.raise_for_status()
    return response.json()

@_api_retry
def _download_image(img_url: str) -> bytes:
    response = requests.get(img_url, timeout=60)
    response.raise_for_status()
    return response.content

# === HELPERS ===
def atomic_write_json(path: Path, data) -> None:
    tmp = path.with_suffix(path.suffix + ".tmp")
    path.parent.mkdir(parents=True, exist_ok=True)
    with tmp.open("w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)
    tmp.replace(path)

def ensure_dir(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)

# === LOGGING ===
def write_logfile(day_folder: Path, logname: str, payload: Dict[str, Any], start: bool) -> None:
    if not LOGGING_EIN:
        return
    ensure_dir(day_folder)
    logfile  = day_folder / f"{logname}.log"
    marker   = "START" if start else "END"
    mode_tag = "[DRY-RUN]" if DRYRUN else "[RUN]"
    try:
        with logfile.open("a", encoding="utf-8") as f:
            f.write(f"\n--- {marker} {logname} {mode_tag} ---\n")
            for k, v in payload.items():
                try:
                    if isinstance(v, (dict, list)):
                        f.write(f"{k}: {json.dumps(v, ensure_ascii=False)}\n")
                    else:
                        f.write(f"{k}: {v}\n")
                except Exception:
                    f.write(f"{k}: {str(v)}\n")
            f.write(f"--- {marker} {logname} {mode_tag} ---\n")
    except Exception as e:
        print(f"⚠️ Logfehler: {e}")

# === POLLING ===
def poll_generation(generation_id: str, headers: Dict, folder: Path, batch_index: int, day_folder: Path) -> List[Dict]:
    """
    Pollt die API bis die Generierung abgeschlossen ist.
    Polling-Endpoint ist immer V1, auch für V2-Modelle (Seedream etc.):
      GET https://cloud.leonardo.ai/api/rest/v1/generations/{id}
    Response-Struktur: {"generations_by_pk": {"status": "...", "generated_images": [{"url": "..."}]}}
    """
    poll_url = f"https://cloud.leonardo.ai/api/rest/v1/generations/{generation_id}"

    last_data    = None
    saved_images = []

    time.sleep(INITIAL_DELAY_SEC)

    for attempt in range(POLL_MAX_ATTEMPTS):
        try:
            data      = _poll_get(poll_url, headers)
            last_data = data

            # Polling-Response ist immer V1-Struktur (einziger dokumentierter GET-Endpoint)
            gen    = data.get("generations_by_pk", {})
            status = gen.get("status")
            images = gen.get("generated_images", [])

            print(f"⏳ Polling {attempt+1}/{POLL_MAX_ATTEMPTS} – Status: {status}")

            if status == "COMPLETE":
                if not images:
                    print("⚠️ Keine Bilder im Polling-Result gefunden.")
                    write_logfile(day_folder, "image_process", {"no_images_in_poll": data}, start=False)
                    sys.exit(1)

                ensure_dir(folder)
                for i, img in enumerate(images, start=1):
                    img_url = img.get("url")
                    if not img_url:
                        continue
                    try:
                        img_content = _download_image(img_url)
                        filename    = folder / f"image_batch{batch_index}_{i}.png"
                        with open(filename, "wb") as f:
                            f.write(img_content)
                        saved_images.append({
                            "local_path":   str(filename),
                            "filename":     filename.name,
                            "leonardo_url": img_url
                        })
                    except Exception as e:
                        print(f"❌ Download-Fehler ({img_url}): {e}")
                        write_logfile(day_folder, "image_process", {"download_error": str(e)}, start=False)
                        sys.exit(1)
                return saved_images

            if status == "FAILED":
                print("❌ Generierung fehlgeschlagen.")
                write_logfile(day_folder, "image_process", {"generation_failed": generation_id}, start=False)
                sys.exit(1)

        except Exception as e:
            print(f"❌ Polling-Fehler: {e}")
            write_logfile(day_folder, "image_process", {"polling_exception": str(e)}, start=False)
            sys.exit(1)

        time.sleep(POLL_DELAY_SEC)

    # Timeout
    print("⚠️ Timeout beim Polling.")
    if last_data:
        debug_file = day_folder / f"polling_timeout_{generation_id}.json"
        try:
            with debug_file.open("w", encoding="utf-8") as f:
                json.dump(last_data, f, indent=2, ensure_ascii=False)
            print(f"📄 Letzte Antwort gespeichert: {debug_file}")
        except Exception:
            pass
    sys.exit(1)

# === IMAGE GENERATION ===
def generate_images(prompt: str, folder: Path, total_count: int, width: int, height: int, day_folder: Path) -> List[Dict]:
    """
    Generiert Bilder via Leonardo API.
    V1 Payload: {"modelId": "UUID", "prompt": "...", "num_images": N, "width": W, "height": H}
    V2 Payload: {"model": "string-id", "parameters": {"prompt": "...", "quantity": N, ...}}
    """
    all_saved = []

    if DRYRUN:
        write_logfile(day_folder, "image_process", {
            "mode": "DRY-RUN", "total_count": total_count,
            "target_folder": str(folder), "prompt_preview": str(prompt)[:160]
        }, start=True)
        ensure_dir(folder)
        for i in range(total_count):
            filename = folder / f"dummy_image_{i+1}.png"
            filename.write_text("DUMMY IMAGE CONTENT", encoding="utf-8")
            print(f"🧪 Dummy-Bild erzeugt: {filename}")
            all_saved.append({"local_path": str(filename), "filename": filename.name})
        write_logfile(day_folder, "image_process", {"dryrun_written": total_count}, start=False)
        return all_saved

    headers = {
        "Authorization": f"Bearer {LEONARDO_API_KEY}",
        "accept":        "application/json",
        "content-type":  "application/json"
    }

    max_per_request = 4 if (width <= 768 and height <= 768) else 2
    remaining       = total_count
    batch_index     = 1

    while remaining > 0:
        num = min(max_per_request, remaining)

        if USE_V2:
            # V2 REST: verschachtelte parameters-Struktur (siehe docs.leonardo.ai/docs/seedream-4-5)
            payload = {
                "model": MODEL_ID,
                "parameters": {
                    "prompt":   prompt,
                    "quantity": num,
                    "width":    width,
                    "height":   height,
                },
                "public": False,
            }
        else:
            payload = {
                "prompt":     prompt,
                "modelId":    MODEL_ID,
                "num_images": num,
                "width":      width,
                "height":     height,
            }

        write_logfile(day_folder, "image_process", {"payload": payload, "batch_index": batch_index}, start=True)

        try:
            post_json = _post_generation(headers, payload)
            write_logfile(day_folder, "image_process", {"post_response": post_json}, start=False)
        except Exception as e:
            print(f"❌ Request-Fehler: {e}")
            write_logfile(day_folder, "image_process", {"request_exception": str(e)}, start=False)
            sys.exit(1)

        # GenerationId extrahieren – alle bekannten Response-Strukturen:
        # V2 GraphQL: {"generate": {"generationId": "..."}}
        # V2 REST:    {"id": "..."}  oder  [{"id": "..."}]
        # V1 REST:    {"sdGenerationJob": {"generationId": "..."}}
        if isinstance(post_json, list):
            post_json = post_json[0] if post_json else {}
        generate_block = post_json.get("generate") or {}
        job            = post_json.get("sdGenerationJob") or {}
        generation_id = (
            generate_block.get("generationId")
            or job.get("generationId")
            or post_json.get("generationId")
            or post_json.get("id")
        )

        if not generation_id:
            print(f"⚠️ Keine generationId erhalten. Response: {post_json}")
            write_logfile(day_folder, "image_process", {"missing_generationId": post_json}, start=False)
            sys.exit(1)

        batch_saved = poll_generation(generation_id, headers, folder, batch_index, day_folder)
        all_saved.extend(batch_saved)

        remaining   -= num
        batch_index += 1

    return all_saved

# === MAIN ===
def main() -> None:
    if not RUN_ENABLED:
        print("ℹ️ [images] ist in run_scripts deaktiviert – nichts zu tun.")
        sys.exit(0)

    if not PENDING_FILE.exists():
        print(f"❌ prompts_pending.json fehlt: {PENDING_FILE}")
        sys.exit(1)

    try:
        with PENDING_FILE.open("r", encoding="utf-8") as f:
            pending = json.load(f)
            if not isinstance(pending, list):
                pending = []
    except Exception:
        print("❌ prompts_pending.json beschädigt.")
        sys.exit(1)

    day_folder = Path(get_day_folder(IMAGES_PATH, DATE_FORMAT, TARGET_DATE))
    ensure_dir(day_folder)

    write_logfile(day_folder, "image_read", {
        "dry_run": DRYRUN, "pending_count": len(pending),
        "width": IMG_WIDTH, "height": IMG_HEIGHT,
        "image_total": IMG_TOTAL, "model_id": MODEL_ID,
        "api_version": "V2" if USE_V2 else "V1"
    }, start=True)

    processed             = 0
    sim_status            = STATUSES.get("simulation",     "Simulation")
    marketing_done_status = STATUSES.get("marketing_done", "Marketing Done")
    all_done_status       = STATUSES.get("all_done",       "All Done")

    for entry in pending:
        if DRYRUN:
            if entry.get("status") != sim_status:
                continue
            print(f"🧪 DRY-RUN: Würde Bilder für {entry.get('id')} simulieren.")
            processed += 1
            continue

        if entry.get("status") != marketing_done_status:
            continue

        folder_val = entry.get("folder")
        prompt     = entry.get("prompt")
        if not folder_val or not prompt:
            print("⚠️ Eintrag unvollständig:", entry.get("id"))
            continue

        folder = Path(folder_val)
        ensure_dir(folder)

        write_logfile(day_folder, "image_process", {
            "entry_id": entry.get("id"),
            "target_folder": str(folder),
            "prompt_preview": str(prompt)[:160]
        }, start=True)

        saved_images = generate_images(prompt, folder, IMG_TOTAL, IMG_WIDTH, IMG_HEIGHT, day_folder)

        entry["images"] = saved_images
        entry["status"] = all_done_status
        processed      += 1

        print(f"✅ {len(saved_images)} Bilder gespeichert für: {entry.get('id')}")

        write_logfile(day_folder, "image_process", {
            "entry_id": entry.get("id"),
            "status_after": entry["status"],
            "images_saved": len(saved_images)
        }, start=False)

    if DRYRUN:
        print(f"🧪 DRY-RUN: {processed} Einträge simuliert.")
    else:
        try:
            atomic_write_json(PENDING_FILE, pending)
            print(f"💾 Status und Bildpfade in {PENDING_FILE} aktualisiert.")
        except Exception as e:
            print(f"⚠️ Konnte {PENDING_FILE} nicht schreiben: {e}")
            sys.exit(1)
        print("✅ Alle Bilder verarbeitet.")

    write_logfile(day_folder, "image_read", {
        "processed_entries": processed, "dry_run": DRYRUN
    }, start=False)

if __name__ == "__main__":
    main()