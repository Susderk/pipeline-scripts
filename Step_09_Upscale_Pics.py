#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Step_09_Upscale_Pics.py

Upscaling-Pipeline:
- Skaliert alle Bilder der Einträge mit Status "YouTube Done" per Real-ESRGAN hoch
- Speichert hochskalierte Bilder in 4k-Unterordner
- Aktualisiert upscaled_path in pending.json
- Setzt Status auf "Upscaled"

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
GITHUB_WP_FOLDER = str(config.get("github_wallpaper_folder", "wallpapers"))
GITHUB_MK_FOLDER = str(config.get("github_mockup_folder",    "mockups"))

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
    Phase 3: Lädt upscalte Wallpapers und vorhandene Mockup-Bilder auf GitHub hoch.

    Struktur im Repo:
      wallpapers/{YYYY-MM-DD}/{FolderName}/{filename}-4k.png
      mockups/{YYYY-MM-DD}/{FolderName}/{mockup_file}.jpg

    GitHub-URLs werden in pending.json gespeichert:
      img["github_url"] + img["github_sha"]  – pro Wallpaper
      entry["github_mockup_urls"]            – Liste der Mockup-URL-Dicts
      entry["github_uploaded"] = True        – Idempotenz-Flag

    Fehlt Github_Token → Schritt wird übersprungen (kein Fehler).
    Mockups fehlen (noch nicht in Step 9 erstellt) → werden übersprungen,
      bei nächstem Lauf von Step 7 nachgetragen falls entry["github_uploaded"] False bleibt.
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

    total_wp = 0   # hochgeladene Wallpapers
    total_mk = 0   # hochgeladene Mockups
    total_vid = 0  # hochgeladene Videos
    failed   = 0

    # Sammelt master-listings.json Updates pro day_folder:
    # { day_folder: [(entry_id, video_github_url), ...] }
    master_updates: dict[Path, list[tuple[str, str]]] = {}

    for entry in entries:
        folder_path = entry.get("folder", "")
        folder_name = Path(folder_path).name if folder_path else entry.get("id", "unknown")

        # Idempotenz: bereits vollständig hochgeladen?
        if entry.get("github_uploaded"):
            print(f"⏭️  Bereits hochgeladen: {folder_name}")
            continue

        print(f"\n📂 GitHub Upload: {folder_name}")

        # ── Upscalte Wallpapers ──────────────────────────────────────────────
        for img in entry.get("images", []):
            up_path = img.get("upscaled_path", "")
            if not up_path:
                continue
            up_file = Path(up_path)
            if not up_file.exists():
                print(f"   ⚠️  Datei nicht gefunden: {up_file.name} – übersprungen.")
                continue

            gh_path    = f"{GITHUB_WP_FOLDER}/{date_str}/{folder_name}/{up_file.name}"
            commit_msg = f"Wallpaper: {date_str}/{folder_name}/{up_file.name}"
            print(f"   ⬆️  Wallpaper: {up_file.name}")

            raw_url, sha = _github_upload_file(gh_path, up_file.read_bytes(), commit_msg)
            if raw_url:
                img["github_url"] = raw_url
                img["github_sha"] = sha
                total_wp += 1
                print(f"   ✅ {raw_url}")
            else:
                failed += 1

            time.sleep(0.3)  # sanfter Rate-Limit-Schutz

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

        # GitHub-Infos in Eintrag speichern
        entry["github_mockup_urls"] = mk_urls
        # Nur als "fertig" markieren wenn Wallpapers erfolgreich
        wp_ok = sum(1 for img in entry.get("images", []) if img.get("github_url"))
        if wp_ok > 0:
            entry["github_uploaded"] = True

        # ── Video (.mp4) ─────────────────────────────────────────────────────
        # Video lebt direkt im Listing-Subordner. Step_07 erzeugt genau eine
        # mp4 pro Listing (safe_name.mp4). Idempotent über entry["video_github_url"].
        if folder_path and not entry.get("video_github_url"):
            listing_dir = Path(folder_path)
            if listing_dir.exists():
                video_files = sorted(
                    f for f in listing_dir.iterdir()
                    if f.is_file() and f.suffix.lower() == ".mp4"
                )
                if video_files:
                    vid_file = video_files[0]
                    if len(video_files) > 1:
                        print(f"   ⚠️  Mehrere mp4 in {listing_dir.name} gefunden – "
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
                    print(f"   ℹ️  Keine .mp4 in {listing_dir.name} – Video-Upload uebersprungen.")
        elif entry.get("video_github_url"):
            print(f"   ⏭️  Video bereits hochgeladen: {entry['video_github_url']}")

    # ── master-listings.json mit video_github_url und github_mockup_urls updaten ────────────────────
    for day_folder, updates in master_updates.items():
        try:
            master = load_master_listings(day_folder)
            n_updated, n_missing = 0, 0

            # Zusätzlich: Sammle github_mockup_urls für alle Einträge
            mockup_updates: dict[str, list] = {}  # { entry_id: [...mockup dicts...] }

            for entry in pending:
                if entry.get("status") == upscaled_status:
                    entry_id = entry.get("id", "")
                    mk_urls = entry.get("github_mockup_urls", [])
                    if entry_id and mk_urls:
                        mockup_updates[entry_id] = mk_urls

            for entry_id, video_url in updates:
                item = find_master_item(master, entry_id)
                if item is None:
                    n_missing += 1
                    print(f"   ⚠️  master-Item id '{entry_id}' nicht gefunden in "
                          f"{day_folder.name} – Video-URL nicht persistiert.")
                    continue
                item["video_github_url"] = video_url
                # Persistiere auch github_mockup_urls falls vorhanden
                if entry_id in mockup_updates:
                    item["github_mockup_urls"] = mockup_updates[entry_id]
                n_updated += 1
            if n_updated:
                save_master_listings(day_folder, master)
                print(f"   🗂️  master-listings.json ({day_folder.name}) aktualisiert: "
                      f"{n_updated} video_github_url + github_mockup_urls gesetzt.")
            if n_missing:
                print(f"   ⚠️  {n_missing} fehlende master-id(s) in {day_folder.name}.")
        except Exception as e:
            print(f"   ❌ master-listings.json Update fehlgeschlagen ({day_folder}): {e}")

    print(f"\n{'─'*44}")
    print(f"🐙 GitHub: {total_wp} Wallpaper(s), {total_mk} Mockup(s), {total_vid} Video(s) hochgeladen.", end="")
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