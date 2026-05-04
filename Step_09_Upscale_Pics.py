#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Step_09_Upscale_Pics.py

Upscaling-Pipeline:
- Skaliert alle Bilder der Einträge mit Status "YouTube Done" per Real-ESRGAN hoch
- Speichert hochskalierte Bilder in 4k-Unterordner
- Aktualisiert upscaled_path in pending.json
- Setzt Status auf "Upscaled"

Phase 3 lädt in das Repo `Susderk/mockup-uploads` hoch:
- Mockup-Bilder nach `mockups/{YYYY-MM-DD}/{FolderName}/`
- Video (mp4) nach `mockups/{YYYY-MM-DD}/{FolderName}/`
(Der frühere Wallpaper-Upload nach `wallpapers/{YYYY-MM-DD}/{FolderName}/`
wurde am 2026-04-24 ersatzlos gestrichen — Archiv ohne Code-Consumer.
Die Leonardo-Originale werden seither in Step_05 in das Repo
`Susderk/original-uploads` hochgeladen.)

Hinweis: Das Filtern vom Nutzer gelöschter Bilder passiert in Step_06
(direkt nach der Sichtkontrolle), nicht hier.
"""

import sys
import os
import json
import subprocess
import base64
import time
from pathlib import Path

from config_loader import (
    load_config,
    atomic_write_json,
    load_master_listings,
    save_master_listings,
    find_master_item,
)

try:
    import requests as _requests
    _REQUESTS_OK = True
except ImportError:
    _REQUESTS_OK = False

# === CONFIG ===
cfg = load_config()
config = cfg["config"]

IMAGES_PATH  = Path(cfg["IMAGES_PATH"])
PENDING_FILE = Path(cfg["PENDING_FILE"])
STATUSES     = cfg["STATUSES"]

flags       = cfg["get_script_flags"]("upscale")
RUN_ENABLED = bool(flags["run"])
DRYRUN      = bool(flags["dry_run"])

UPSCALE_FACTOR   = int(config.get("upscale_factor", 4))
IMAGE_EXTENSIONS = tuple(config.get("image_extensions", [".jpg", ".jpeg", ".png"]))

REALESRGAN_EXE = config.get("realesrgan_path", "realesrgan-ncnn-vulkan").strip() or "realesrgan-ncnn-vulkan"
_model_raw = config.get("realesrgan_model", "realesrgan-x4plus-anime").strip() or "realesrgan-x4plus-anime"

if "/" in _model_raw or "\\" in _model_raw:
    _model_path = Path(_model_raw.replace("\\", "/"))
    REALESRGAN_MODEL      = _model_path.name
    REALESRGAN_MODEL_PATH = str(_model_path.parent)
else:
    REALESRGAN_MODEL      = _model_raw
    REALESRGAN_MODEL_PATH = config.get("realesrgan_model_path", "").strip()

DATE_FORMAT = cfg["DATE_FORMAT"]

# === GITHUB CONFIG ===
# Github_Token fehlt → Phase 3 wird stillschweigend übersprungen (kein Fehler)
GITHUB_TOKEN     = os.environ.get("Github_Token", "").strip()
GITHUB_REPO      = str(config.get("github_repo",             "Susderk/mockup-uploads"))
GITHUB_BRANCH    = str(config.get("github_branch",           "main"))
GITHUB_MK_FOLDER = str(config.get("github_mockup_folder",    "mockups"))
# Hinweis: `github_wallpaper_folder` + zugehörige Upload-Phase wurden am
# 2026-04-24 ersatzlos entfernt (Archiv ohne Code-Consumer).

# === HELPERS ===
def check_realesrgan() -> bool:
    try:
        subprocess.run([REALESRGAN_EXE, "-h"], capture_output=True, text=True, timeout=10)
        return True
    except FileNotFoundError:
        return False
    except Exception:
        return True

# === UPSCALING ===
def upscale_image(image_path: Path, dryrun: bool = False) -> Path | None:
    """
    Skaliert ein einzelnes Bild hoch.
    Gibt den Pfad des hochskalierten Bildes zurück, oder None bei Fehler.
    """
    target_folder = image_path.parent / "4k"
    target_folder.mkdir(parents=True, exist_ok=True)

    output_name = f"{image_path.stem}-4k{image_path.suffix}"
    output_path = target_folder / output_name

    if dryrun:
        print(f"🧪 DRY-RUN: Würde hochskalieren: {image_path.name} → 4k/{output_name}")
        return output_path

    if output_path.exists():
        print(f"⏭️  Bereits vorhanden, übersprungen: {output_path.name}")
        return output_path

    print(f"🔍 Upscaling: {image_path.name} → 4k/{output_name}")

    cmd = [
        REALESRGAN_EXE,
        "-i", str(image_path),
        "-o", str(output_path),
        "-n", REALESRGAN_MODEL,
        "-s", str(UPSCALE_FACTOR),
        "-f", "png"
    ]
    if REALESRGAN_MODEL_PATH:
        cmd += ["-m", REALESRGAN_MODEL_PATH]

    try:
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=300)
        if result.returncode != 0:
            print(f"❌ Fehler bei {image_path.name}:")
            print(f"   {result.stderr.strip()}")
            sys.exit(1)
        print(f"✅ Fertig: {output_path.name}")
        return output_path
    except subprocess.TimeoutExpired:
        print(f"❌ Timeout bei {image_path.name} (>5 Minuten)")
        sys.exit(1)
    except Exception as e:
        print(f"❌ Unbekannter Fehler bei {image_path.name}: {e}")
        sys.exit(1)

# === GITHUB UPLOAD ===

def _github_upload_file(gh_path: str, content_bytes: bytes, commit_msg: str) -> tuple:
    """
    Lädt eine Datei auf GitHub hoch (erstellt oder aktualisiert).
    Gibt (raw_url, sha) oder (None, None) bei Fehler zurück.
    Nutzt dieselbe Github_Token Umgebungsvariable wie fallback_creator.py.
    """
    if not _REQUESTS_OK:
        print("   ⚠️  'requests' nicht installiert (pip install requests) – GitHub-Upload übersprungen.")
        return None, None

    api_url = f"https://api.github.com/repos/{GITHUB_REPO}/contents/{gh_path}"
    headers = {
        "Authorization": f"Bearer {GITHUB_TOKEN}",
        "Accept":        "application/vnd.github+json",
    }

    # SHA für Update-Requests ermitteln (Datei schon vorhanden?)
    existing_sha = None
    try:
        r = _requests.get(api_url, headers=headers, timeout=30)
        if r.status_code == 200:
            existing_sha = r.json().get("sha")
    except Exception:
        pass

    payload = {
        "message": commit_msg,
        "content": base64.b64encode(content_bytes).decode("utf-8"),
        "branch":  GITHUB_BRANCH,
    }
    if existing_sha:
        payload["sha"] = existing_sha

    def _try_upload():
        try:
            resp = _requests.put(api_url, headers=headers, json=payload, timeout=60)
        except Exception as e:
            print(f"   ❌ Netzwerkfehler: {e}")
            return None, None
        if resp.status_code in (200, 201):
            sha     = resp.json().get("content", {}).get("sha", "")
            raw_url = f"https://raw.githubusercontent.com/{GITHUB_REPO}/{GITHUB_BRANCH}/{gh_path}"
            return raw_url, sha
        print(f"   ❌ GitHub API Fehler {resp.status_code}: {resp.text[:200]}")
        if resp.status_code == 401:
            print("   ℹ️  Tipp: Github_Token abgelaufen oder ungültig.")
        elif resp.status_code == 422 and existing_sha is None:
            print("   ℹ️  Tipp: Datei existiert bereits – SHA wird beim nächsten Lauf automatisch ermittelt.")
        return None, None

    raw_url, sha = _try_upload()
    if raw_url is None:
        print("   ↻ Warte 30s und versuche erneut...")
        time.sleep(30)
        raw_url, sha = _try_upload()
    return raw_url, sha


def phase3_github_upload(pending: list, upscaled_status: str, date_str: str) -> bool:
    """
    Phase 3: Lädt Mockup-Bilder und das Video (mp4) auf GitHub hoch.

    Struktur im Repo `Susderk/mockup-uploads`:
      mockups/{YYYY-MM-DD}/{FolderName}/{mockup_file}.jpg
      mockups/{YYYY-MM-DD}/{FolderName}/{video_file}.mp4

    Persistiert in master-listings.json (SSoT):
      item["github_mockup_urls"] = [{"file", "url", "sha"}, ...]
      item["video_github_url"]   = <raw-url>

    Der frühere Wallpaper-Upload nach `wallpapers/{YYYY-MM-DD}/{FolderName}/`
    wurde am 2026-04-24 ersatzlos entfernt — die 4k-Archiv-Uploads hatten
    keinen Code-Consumer (Recherche-Logs 2026-04-24 -a und -b). Die
    Leonardo-Originale werden stattdessen in Step_05 in das Repo
    `Susderk/original-uploads` hochgeladen (Leonardo-URL-Ablauf-Entkopplung).
    Damit entfällt auch das `entry["github_uploaded"]`-Idempotenz-Flag:
    Mockup-Idempotenz läuft über `already = {m["file"] for m in mk_urls}`,
    Video-Idempotenz über `entry.get("video_github_url")`.

    Fehlt Github_Token → Schritt wird übersprungen (kein Fehler).
    """
    if not GITHUB_TOKEN:
        print("ℹ️  Github_Token nicht gesetzt → GitHub-Upload übersprungen.")
        print("   Setze Umgebungsvariable Github_Token um den Upload zu aktivieren.")
        return False

    if not _REQUESTS_OK:
        print("⚠️  'requests' fehlt (pip install requests) → GitHub-Upload übersprungen.")
        return False

    entries = [e for e in pending if e.get("status") == upscaled_status]
    if not entries:
        print("ℹ️  Keine Einträge mit Status 'Upscaled' – nichts hochzuladen.")
        return True

    total_mk = 0   # hochgeladene Mockups
    total_vid = 0  # hochgeladene Videos
    failed   = 0

    # Sammelt master-listings.json Updates pro day_folder:
    # { day_folder: [(entry_id, video_github_url), ...] }
    master_updates: dict[Path, list[tuple[str, str]]] = {}

    for entry in entries:
        folder_path = entry.get("folder", "")
        folder_name = Path(folder_path).name if folder_path else entry.get("id", "unknown")

        print(f"\n📂 GitHub Upload: {folder_name}")

        # ── Mockup-Bilder (aus FolderName/Mockups/ ) ─────────────────────────
        mk_urls   = entry.get("github_mockup_urls", [])
        already   = {m["file"] for m in mk_urls}
        mockup_dir = Path(folder_path) / "Mockups" if folder_path else None

        if mockup_dir and mockup_dir.exists():
            image_suffixes = {".jpg", ".jpeg", ".png"}
            mockup_files = sorted(
                f for f in mockup_dir.iterdir()
                if f.suffix.lower() in image_suffixes
                and not f.name.endswith(".tmp")
                and f.name not in already
            )
            if mockup_files:
                for mk_file in mockup_files:
                    gh_path    = f"{GITHUB_MK_FOLDER}/{date_str}/{folder_name}/{mk_file.name}"
                    commit_msg = f"Mockup: {date_str}/{folder_name}/{mk_file.name}"
                    print(f"   ⬆️  Mockup: {mk_file.name}")

                    raw_url, sha = _github_upload_file(gh_path, mk_file.read_bytes(), commit_msg)
                    if raw_url:
                        mk_urls.append({"file": mk_file.name, "url": raw_url, "sha": sha})
                        total_mk += 1
                        print(f"   ✅ {raw_url}")
                    else:
                        failed += 1

                    time.sleep(0.3)
            else:
                print(f"   ℹ️  Keine neuen Mockup-Bilder in Mockups/")
        else:
            print(f"   ℹ️  Mockups/-Ordner nicht vorhanden (wird in Step 9 erstellt – dann erneut hochladen)")

        # GitHub-Infos in Eintrag speichern (wird in Schritt "master-listings Update"
        # unten in die SSoT übertragen).
        entry["github_mockup_urls"] = mk_urls

        # ── Video (.mp4) ─────────────────────────────────────────────────────
        # Step_07 legt das Video unter <folder_path>/Mockups/<safe_name>.mp4 ab
        # (output_path = subdir / output_name, subdir = <listing>/Mockups).
        # Fallback: <folder_path>/<safe_name>.mp4 (Altlasten / abweichende Ablage).
        # Idempotent über entry["video_github_url"].
        if folder_path and not entry.get("video_github_url"):
            listing_dir = Path(folder_path)
            if listing_dir.exists():
                # Primär: Mockups/-Unterordner (Step_07-Konvention)
                video_dir = listing_dir / "Mockups"
                video_files: list[Path] = []
                if video_dir.exists():
                    video_files = sorted(
                        f for f in video_dir.iterdir()
                        if f.is_file() and f.suffix.lower() == ".mp4"
                    )
                # Fallback: Listing-Ordner direkt
                if not video_files:
                    video_files = sorted(
                        f for f in listing_dir.iterdir()
                        if f.is_file() and f.suffix.lower() == ".mp4"
                    )

                if video_files:
                    vid_file = video_files[0]
                    if len(video_files) > 1:
                        print(f"   ⚠️  Mehrere mp4 für {listing_dir.name} gefunden – "
                              f"verwende {vid_file.name}.")
                    gh_path    = f"{GITHUB_MK_FOLDER}/{date_str}/{folder_name}/{vid_file.name}"
                    commit_msg = f"Video: {date_str}/{folder_name}/{vid_file.name}"
                    print(f"   ⬆️  Video: {vid_file.name}")

                    raw_url, _sha = _github_upload_file(gh_path, vid_file.read_bytes(), commit_msg)
                    if raw_url:
                        entry["video_github_url"] = raw_url
                        total_vid += 1
                        print(f"   ✅ {raw_url}")
                        # master-listings.json Update vormerken
                        entry_id = entry.get("id", "")
                        if entry_id:
                            day_folder = listing_dir.parent
                            master_updates.setdefault(day_folder, []).append(
                                (entry_id, raw_url)
                            )
                    else:
                        failed += 1
                    time.sleep(0.3)
                else:
                    print(f"   ℹ️  Keine .mp4 in {listing_dir.name}/Mockups/ "
                          f"(und nicht in {listing_dir.name}/) – Video-Upload uebersprungen.")
        elif entry.get("video_github_url"):
            print(f"   ⏭️  Video bereits hochgeladen: {entry['video_github_url']}")

    # ── master-listings.json mit video_github_url und github_mockup_urls updaten ──
    # Bug-Fix 2026-04-16: github_mockup_urls-Persistierung war an master_updates
    # gekoppelt (nur bei Video-Upload befüllt). Bei Re-Runs ohne neue Videos blieb
    # master_updates leer → github_mockup_urls erreichten master-listings.json nie.
    # Fix: day_folders werden jetzt UNABHÄNGIG aus Video- UND Mockup-Updates gesammelt.

    # Schritt 1: Sammle github_mockup_urls + zugehörige day_folders aus ALLEN pending-Einträgen
    mockup_updates: dict[str, list] = {}   # { entry_id: [...mockup dicts...] }
    mockup_day_folders: set[Path] = set()

    for entry in pending:
        entry_id = entry.get("id", "")
        mk_urls = entry.get("github_mockup_urls", [])
        if entry_id and mk_urls:
            mockup_updates[entry_id] = mk_urls
            folder_path = entry.get("folder", "")
            if folder_path:
                mockup_day_folders.add(Path(folder_path).parent)

    # Schritt 2: Vereinigung aller day_folders (Video-Uploads + Mockup-Updates)
    all_day_folders = set(master_updates.keys()) | mockup_day_folders

    for day_folder in all_day_folders:
        try:
            master = load_master_listings(day_folder)
            n_updated, n_missing = 0, 0

            # Update video_github_url für Einträge mit neuen Video-Uploads
            video_updates = master_updates.get(day_folder, [])
            video_ids = {e_id for e_id, _ in video_updates}

            for entry_id, video_url in video_updates:
                item = find_master_item(master, entry_id)
                if item is None:
                    n_missing += 1
                    print(f"   ⚠️  master-Item id '{entry_id}' nicht gefunden in "
                          f"{day_folder.name} – Video-URL nicht persistiert.")
                    continue
                item["video_github_url"] = video_url
                n_updated += 1

            # Persistiere github_mockup_urls FÜR ALLE EINTRÄGE, die welche haben
            # (unabhängig von Video-Uploads)
            for entry_id, mk_urls in mockup_updates.items():
                item = find_master_item(master, entry_id)
                if item is not None:
                    item["github_mockup_urls"] = mk_urls
                    if entry_id not in video_ids:
                        n_updated += 1

            if n_updated:
                save_master_listings(day_folder, master)
                print(f"   🗂️  master-listings.json ({day_folder.name}) aktualisiert: "
                      f"{n_updated} Einträge mit video_github_url / github_mockup_urls gesetzt.")
            if n_missing:
                print(f"   ⚠️  {n_missing} fehlende master-id(s) in {day_folder.name}.")
        except Exception as e:
            print(f"   ❌ KRITISCHER FEHLER beim Update von master-listings.json ({day_folder}): {e}")
            raise  # Fehler neu werfen — Schritt muss abbrechen

    print(f"\n{'─'*44}")
    print(f"🐙 GitHub: {total_mk} Mockup(s), {total_vid} Video(s) hochgeladen.", end="")
    if failed:
        print(f"  ⚠️  {failed} fehlgeschlagen.")
    else:
        print()
    return True


# === MAIN ===
def main():
    if not RUN_ENABLED:
        print("ℹ️ [upscale] ist in run_scripts deaktiviert – nichts zu tun.")
        sys.exit(0)

    if not DRYRUN:
        if not check_realesrgan():
            print(f"❌ Real-ESRGAN nicht gefunden: '{REALESRGAN_EXE}'")
            print("   Bitte herunterladen: https://github.com/xinntao/Real-ESRGAN/releases")
            sys.exit(1)
        print(f"✅ Real-ESRGAN gefunden. Modell: {REALESRGAN_MODEL}, Faktor: {UPSCALE_FACTOR}x")

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

    youtube_done_status = STATUSES.get("youtube_done", "YouTube Done")
    upscaled_status     = STATUSES.get("upscaled",     "Upscaled")
    sim_status          = STATUSES.get("simulation",   "Simulation")

    # === Upscaling ===
    # Hinweis: Filtern gelöschter Bilder passiert in Step_06 nach der Sichtkontrolle.
    print()
    print("🔍 Upscaling der Bilder...")
    total_images = 0
    updated = False

    for entry in pending:
        status = entry.get("status")

        if DRYRUN:
            if status != sim_status:
                continue
        else:
            if status != youtube_done_status:
                print(f"🚫 Eintrag übersprungen (Status: '{status}'): {entry.get('id')}")
                continue

        images = entry.get("images", [])
        if not images:
            print(f"⚠️  Keine Bilder mehr für Eintrag: {entry.get('id')} – übersprungen.")
            continue

        print(f"\n📂 Verarbeite: {entry.get('id', 'unbekannt')}")

        for img in images:
            local_path = img.get("local_path", "")
            if not local_path:
                continue

            image_path = Path(local_path)
            if not image_path.exists():
                print(f"⚠️  Datei nicht gefunden: {image_path.name} – übersprungen.")
                continue

            upscaled_path = upscale_image(image_path, dryrun=DRYRUN)

            if upscaled_path and not DRYRUN:
                img["upscaled_path"] = str(upscaled_path)

            total_images += 1

        if not DRYRUN:
            entry["status"] = upscaled_status
            updated = True

    # Pending nach Phase 2 speichern
    if DRYRUN:
        print(f"\n🧪 DRY-RUN: {total_images} Bilder simuliert. Keine Änderungen gespeichert.")
    elif updated:
        try:
            atomic_write_json(PENDING_FILE, pending)
            print(f"\n💾 Pending aktualisiert.")
        except Exception as e:
            print(f"❌ Fehler beim Speichern von {PENDING_FILE}: {e}")
            sys.exit(1)

    # === PHASE 3: GitHub Upload ===
    print()
    print("🐙 Phase 3: Bilder zu GitHub hochladen...")
    if DRYRUN:
        print("🧪 DRY-RUN – GitHub-Upload wird übersprungen.")
    else:
        date_str = cfg["TARGET_DATE"].strftime(DATE_FORMAT)
        phase3_github_upload(pending, upscaled_status, date_str)
        # Pending erneut speichern – GitHub-URLs wurden eingetragen
        try:
            atomic_write_json(PENDING_FILE, pending)
            print("💾 Pending mit GitHub-URLs aktualisiert.")
        except Exception as e:
            print(f"⚠️  Fehler beim Speichern nach GitHub-Upload: {e}")

    print(f"\n{'='*44}")
    print(f"🎯 Step 7 abgeschlossen: {total_images} Bilder verarbeitet.")
    print(f"{'='*44}")

if __name__ == "__main__":
    main()