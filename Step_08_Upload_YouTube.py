#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Step_08_Upload_YouTube.py

Lädt die erstellten MP4-Videos (aus Step 07) als YouTube Shorts hoch.
Liest Titel, Beschreibung und Tags aus master-listings.json (Single Source
of Truth seit Refactor 2026-04). Matching: id-basiert via Item-Folder, kein
Substring-Matching mehr.

Schreibt nach erfolgreichem Upload:
- master-listings.json item.youtube_url (SSoT)
- payhip-listing.csv im Tagesordner (nur frisch hochgeladene Items),
  mit youtube_url und marketing_text fuer Payhip-Integration
- prompts_pending.json entry.youtube_url / .youtube_id (Uebergangs-Felder
  bis Refactor-Punkt 9 bestaetigt, dass kein Reader sie braucht)

Voraussetzungen:
  pip install google-api-python-client google-auth-oauthlib

Config-Parameter (config.yaml):
  youtube_credentials_file: "credentials.json"  # in JSON Dateien Ordner
  youtube_token_file:        "youtube_token.json" # in JSON Dateien Ordner (auto-erstellt)
  youtube_privacy:           "public"            # public / unlisted / private / scheduled
  youtube_schedule_time:     "18:00"             # Uhrzeit für geplante Uploads (HH:MM, UTC)
  youtube_category_id:       24                  # YouTube Kategorie-ID (24=Entertainment)
  youtube_language:          "en"                # Videosprache
  youtube_add_shorts_tag:    true                # #Shorts zu Titel/Beschreibung hinzufügen
  target_date:               ""                  # Datum (leer = heute, Format: YYYY-MM-DD)
"""

import sys
import json
import csv
import re
from pathlib import Path
from datetime import datetime

from config_loader import (
    load_config,
    atomic_write_json,
    get_day_folder,
    load_master_listings,
    save_master_listings,
    find_master_item,
)

# Konstanter Disclosure-Text (an marketing_text in payhip-listing.csv angehaengt)
PAYHIP_AI_DISCLOSURE = "AI-crafted with a human touch ✨"

# Google API – mit verständlicher Fehlermeldung falls nicht installiert
try:
    from google.oauth2.credentials import Credentials
    from google_auth_oauthlib.flow import InstalledAppFlow
    from google.auth.transport.requests import Request
    from googleapiclient.discovery import build
    from googleapiclient.http import MediaFileUpload
    import googleapiclient.errors
except ImportError:
    print("❌ Google API Bibliotheken fehlen. Bitte installieren:")
    print("   pip install google-api-python-client google-auth-oauthlib")
    sys.exit(1)


# === CONFIG ===
cfg = load_config()
config    = cfg["config"]
JSON_PATH = Path(cfg["JSON_PATH"])

PENDING_FILE = Path(cfg["PENDING_FILE"])
IMAGES_PATH  = Path(cfg["IMAGES_PATH"])
DATE_FORMAT  = cfg["DATE_FORMAT"]
STATUSES     = cfg["STATUSES"]

flags  = cfg["get_script_flags"]("youtube")
DRYRUN = bool(flags.get("dry_run", False))

CREDENTIALS_FILE = JSON_PATH / str(config.get("youtube_credentials_file", "credentials.json"))
TOKEN_FILE       = JSON_PATH / str(config.get("youtube_token_file",        "youtube_token.json"))
PRIVACY          = str(config.get("youtube_privacy",        "public")).lower()
SCHEDULE_TIME    = str(config.get("youtube_schedule_time",  "18:00")).strip()
CATEGORY_ID      = str(config.get("youtube_category_id",    24))
LANGUAGE         = str(config.get("youtube_language",       "en"))
ADD_SHORTS_TAG   = bool(config.get("youtube_add_shorts_tag", True))
VIDEO_FORMAT     = str(config.get("video_output_format",    "mp4"))

SCOPES           = ["https://www.googleapis.com/auth/youtube"]   # youtube.upload reicht nicht für delete
VIDEO_STATUS     = STATUSES.get("video_done",   "Video Done")
YOUTUBE_STATUS   = STATUSES.get("youtube_done", "YouTube Done")
UPLOADED_YT_FILE = JSON_PATH / "uploaded_to_yt.json"


# === HELPERS ===
def load_uploaded_yt() -> dict:
    """Lädt uploaded_to_yt.json; Schlüssel: 'day_folder|folder_name'."""
    if not UPLOADED_YT_FILE.exists():
        return {}
    try:
        with UPLOADED_YT_FILE.open("r", encoding="utf-8") as f:
            return json.load(f)
    except Exception as e:
        print(f"⚠️  uploaded_to_yt.json konnte nicht gelesen werden: {e}")
        return {}


def save_uploaded_yt(data: dict) -> None:
    """Speichert uploaded_to_yt.json atomar."""
    atomic_write_json(UPLOADED_YT_FILE, data)


def find_item_by_folder(items: list, folder_name: str) -> dict:
    """Deprecated: nutze find_master_item(master, entry_id) stattdessen.
    Diese Funktion wird nur noch für Fallback-Matching verwendet.
    """
    if not items or not folder_name:
        return None
    target = folder_name.strip().lower()
    for it in items:
        if (it.get("folder") or "").strip().lower() == target:
            return it
    return None


def build_title(row: dict) -> str:
    """Baut den YouTube-Titel (max. 100 Zeichen).
    Format: {etsy_title_en} | AI Art | 4K Wallpaper #AIWallpaper
    Nutzt etsy_title_en (SEO-Langtitel) statt etsy_title (Kurztitel).
    """
    base = row.get("etsy_title_en", row.get("etsy_title", "Wallpaper")) if row else "Wallpaper"
    title = f"{base} | AI Art | 4K Wallpaper #AIWallpaper"
    return title[:100]


def build_description(row: dict) -> str:
    """Baut die YouTube-Beschreibung aus den CSV-Feldern.

    Struktur:
      Zeile 1–2: short_line (Hook + Keyword – vor "mehr anzeigen" sichtbar)
      Zeile 3:   Shop-CTA (vor "mehr anzeigen" sichtbar)
      Danach:    etsy_description_en, Hashtags, #Shorts
    """
    shop_cta = str(config.get("shop_cta", "")).strip()

    parts = []
    if row:
        short_line  = row.get("short_line_en", "")
        description = row.get("etsy_description_en", "")
        hashtags    = row.get("social_hashtags", "")
        if short_line:
            parts.append(short_line)
        if shop_cta:
            parts.append("")
            parts.append(shop_cta)
        if description:
            parts.append("")
            parts.append(description)
        if hashtags:
            parts.append("")
            parts.append(hashtags)
    elif shop_cta:
        parts.append(shop_cta)
    if ADD_SHORTS_TAG:
        parts.append("")
        parts.append("#Shorts")
    return "\n".join(parts)


def build_tags(row: dict) -> list:
    """
    Baut die YouTube-Tag-Liste aus etsy_tags_en + youtube_base_tags aus config.
    Respektiert das YouTube-500-Zeichen-Limit (inkl. Kommas und Quotes).
    Tags, die nicht reinpassen, werden weggelassen (nicht abgeschnitten).
    #Shorts wird NICHT ins Tag-Array aufgenommen (nur noch in Beschreibung).
    """
    # Etsy-Tags aus CSV
    tags = []
    if row:
        raw = row.get("etsy_tags_en", "")
        tags = [t.strip().strip('"') for t in raw.split(",") if t.strip()]

    # YouTube-Basis-Tags aus config.yaml (ohne Duplikate, case-insensitive)
    base_tags = config.get("youtube_base_tags", []) or []
    existing_lower = {t.lower() for t in tags}
    for bt in base_tags:
        bt = bt.strip()
        if bt and bt.lower() not in existing_lower:
            tags.append(bt)
            existing_lower.add(bt.lower())

    # YouTube Tag-Budget: max. 500 Zeichen, inkl. Kommas und Quotes
    # Format in YouTube API: "tag1", "tag2", ... → 2 Zeichen Quotes pro Tag, Komma + Space zwischen Tags
    def estimate_budget(tag_list: list) -> int:
        """Schätzt die Zeichenlänge der formatierten Tag-Liste."""
        if not tag_list:
            return 0
        # Format: "tag1", "tag2", ..., "tagN"
        # = Summe(len(tag) + 2 für Quotes) + (len(tags)-1) * 2 für Komma + Space
        content_len = sum(len(t) + 2 for t in tag_list)
        sep_len = (len(tag_list) - 1) * 2  # ", "
        return content_len + sep_len

    # Entferne Tags von hinten, bis Budget unter 500 Zeichen
    while estimate_budget(tags) > 500 and tags:
        removed_tag = tags.pop()
        print(f"      ⚠️  Tag entfernt (Budget 500Z überschritten): '{removed_tag}'")

    return tags


def write_payhip_listing_csv(day_folder: Path, items_for_payhip: list) -> None:
    """
    Schreibt payhip-listing.csv im Tagesordner — eine Zeile pro frisch
    hochgeladenem master-Item. Spalten:
      id, folder, title, youtube_url, marketing_text
    marketing_text = etsy_description_en + "\n\n" + AI-Disclosure.
    """
    if not items_for_payhip:
        print("   ℹ️  Keine Items fuer payhip-listing.csv (keine erfolgreichen Uploads).")
        return

    csv_path = day_folder / "payhip-listing.csv"
    fieldnames = ["id", "folder", "title", "youtube_url", "marketing_text"]

    rows = []
    for it in items_for_payhip:
        etsy_title = (it.get("etsy_title") or "").strip()
        title = f"Set of 5 {etsy_title} | 4K Wallpaper Digital Download" if etsy_title else ""
        marketing_text = (it.get("etsy_description_en") or "").strip()
        if marketing_text:
            marketing_text = f"{marketing_text}\n\n{PAYHIP_AI_DISCLOSURE}"
        else:
            marketing_text = PAYHIP_AI_DISCLOSURE
        rows.append({
            "id":              it.get("id", ""),
            "folder":          it.get("folder", ""),
            "title":           title,
            "youtube_url":     it.get("youtube_url") or "",
            "marketing_text":  marketing_text,
        })

    tmp = csv_path.with_suffix(csv_path.suffix + ".tmp")
    try:
        with tmp.open("w", encoding="utf-8-sig", newline="") as f:
            writer = csv.DictWriter(
                f,
                fieldnames=fieldnames,
                delimiter=";",
                quotechar='"',
                quoting=csv.QUOTE_ALL,
            )
            writer.writeheader()
            writer.writerows(rows)
        tmp.replace(csv_path)
        print(f"   🛒 payhip-listing.csv geschrieben ({len(rows)} Zeilen): {csv_path}")
    except Exception as e:
        print(f"   ❌ payhip-listing.csv konnte nicht geschrieben werden: {e}")
        tmp.unlink(missing_ok=True)


def get_publish_at(target_date: datetime):
    """Gibt ISO-8601-Zeitstempel (UTC) für geplante Uploads zurück, sonst None."""
    if PRIVACY != "scheduled":
        return None
    try:
        h, m = map(int, SCHEDULE_TIME.split(":"))
        scheduled = target_date.replace(hour=h, minute=m, second=0, microsecond=0)
        return scheduled.strftime("%Y-%m-%dT%H:%M:%S.000Z")
    except Exception as e:
        print(f"⚠️  Ungültige youtube_schedule_time '{SCHEDULE_TIME}': {e}")
        return None


def get_youtube_service():
    """OAuth-Login + YouTube-Service aufbauen. Token wird in TOKEN_FILE gespeichert."""
    creds = None

    # Gespeichertes Token laden
    if TOKEN_FILE.exists():
        try:
            creds = Credentials.from_authorized_user_file(str(TOKEN_FILE), SCOPES)
        except Exception:
            creds = None

    # Token erneuern oder neu einloggen
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            print("🔄 Token wird erneuert...")
            creds.refresh(Request())
        else:
            if not CREDENTIALS_FILE.exists():
                print(f"❌ credentials.json nicht gefunden: {CREDENTIALS_FILE}")
                print("   Google Cloud Console → APIs & Dienste → Anmeldedaten")
                print("   → + Anmeldedaten erstellen → OAuth-Client-ID → Desktop-App")
                sys.exit(1)
            print("🌐 Browser öffnet sich für Google-Login (einmalig)...")
            flow = InstalledAppFlow.from_client_secrets_file(str(CREDENTIALS_FILE), SCOPES)
            creds = flow.run_local_server(port=0)

        # Token für spätere Läufe speichern
        TOKEN_FILE.parent.mkdir(parents=True, exist_ok=True)
        with TOKEN_FILE.open("w", encoding="utf-8") as f:
            f.write(creds.to_json())
        print(f"✅ Token gespeichert: {TOKEN_FILE.name}")

    return build("youtube", "v3", credentials=creds)


def delete_video(service, video_id: str) -> bool:
    """Löscht ein YouTube-Video anhand seiner Video-ID. Gibt True bei Erfolg zurück."""
    try:
        service.videos().delete(id=video_id).execute()
        return True
    except googleapiclient.errors.HttpError as e:
        if e.resp.status == 404:
            print(f"   ℹ️  Video {video_id} nicht mehr auf YouTube vorhanden (bereits gelöscht?).")
            return True   # Aus unserer Sicht trotzdem OK – ist weg
        print(f"   ❌ Löschen fehlgeschlagen (HTTP {e.resp.status}): {e}")
        return False
    except Exception as e:
        print(f"   ❌ Löschen fehlgeschlagen: {e}")
        return False


def upload_video(service, mp4_path: Path, title: str, description: str,
                 tags: list, publish_at) -> str:
    """
    Lädt ein Video via YouTube Data API v3 hoch (Resumable Upload).
    Gibt die Video-ID zurück oder None bei Fehler.
    """
    privacy_status = PRIVACY if PRIVACY != "scheduled" else "private"

    body = {
        "snippet": {
            "title":           title,
            "description":     description,
            "tags":            tags,
            "categoryId":      CATEGORY_ID,
            "defaultLanguage": LANGUAGE,
        },
        "status": {
            "privacyStatus":            privacy_status,
            "selfDeclaredMadeForKids":  False,
        }
    }

    # Geplante Veröffentlichung: muss privat sein + publishAt gesetzt
    if publish_at:
        body["status"]["publishAt"]      = publish_at
        body["status"]["privacyStatus"]  = "private"

    media = MediaFileUpload(
        str(mp4_path),
        mimetype="video/mp4",
        resumable=True,
        chunksize=5 * 1024 * 1024   # 5-MB-Chunks
    )

    try:
        request  = service.videos().insert(
            part="snippet,status",
            body=body,
            media_body=media
        )

        print("   ⬆️  Upload läuft...")
        response = None
        while response is None:
            status, response = request.next_chunk()
            if status:
                pct = int(status.progress() * 100)
                print(f"   📊 {pct}% hochgeladen...", end="\r")

        print()   # Zeilenumbruch nach Fortschrittsanzeige
        return response.get("id")

    except googleapiclient.errors.HttpError as e:
        print(f"   ❌ YouTube API Fehler: {e}")
        return None
    except Exception as e:
        print(f"   ❌ Upload-Fehler: {e}")
        return None


# === MAIN ===
def main():
    print("[Step 08 - YouTube Upload] wird gestartet...")

    # Zieldatum bestimmen: aus config.yaml oder heute
    target_date = cfg["TARGET_DATE"]
    print(f"📅 Zieldatum: {target_date.strftime(DATE_FORMAT)}")

    day_folder = get_day_folder(IMAGES_PATH, date_format=DATE_FORMAT, target_date=target_date)

    if not day_folder.exists():
        print(f"❌ Tagesordner nicht gefunden: {day_folder}")
        print("   Bitte zuerst Step 9 ausführen.")
        sys.exit(1)

    # master-listings.json laden (Single Source of Truth)
    master = load_master_listings(day_folder)
    master_items = master.get("items", [])
    if master_items:
        print(f"🗂️  master-listings.json geladen: {len(master_items)} Item(s)")
    else:
        print(f"⚠️  master-listings.json leer/nicht gefunden – Ordnernamen werden als Fallback verwendet")

    # --- DRY-RUN (vor der Dateiprüfung, damit kein abbruch bei fehlenden Videos) ---
    if DRYRUN:
        print("\n🧪 DRY-RUN – kein echter Upload.")
        print("   (Dateiprüfung übersprungen – keine echten Videos nötig)")
        print(f"\n{'='*52}")
        print("🧪 DRY-RUN abgeschlossen.")
        return

    # Alle <FolderName>/Mockups/*.mp4 sammeln (nur im echten Modus nötig)
    mp4_jobs = []
    for folder_dir in sorted(d for d in day_folder.iterdir() if d.is_dir()):
        mockups_dir = folder_dir / "Mockups"
        if not mockups_dir.exists():
            continue
        mp4_files = sorted(mockups_dir.glob(f"*.{VIDEO_FORMAT}"))
        if mp4_files:
            mp4_jobs.append((folder_dir.name, mp4_files[0]))

    if not mp4_jobs:
        print(f"❌ Keine .{VIDEO_FORMAT}-Videos in <FolderName>/Mockups/ gefunden.")
        print(f"   Pfad durchsucht: {day_folder}")
        print("   Bitte zuerst Step 07 (Video erstellen) ausführen.")
        sys.exit(1)

    print(f"\n🎬 {len(mp4_jobs)} Video(s) gefunden:")
    for folder_name, mp4_path in mp4_jobs:
        size_mb = mp4_path.stat().st_size / 1024 / 1024
        print(f"   • {folder_name}: {mp4_path.name} ({size_mb:.1f} MB)")

    # Sichtbarkeit/Zeitplan anzeigen
    publish_at = get_publish_at(target_date)
    if PRIVACY == "scheduled" and publish_at:
        print(f"\n📆 Geplante Veröffentlichung: {publish_at} (UTC)")
    else:
        print(f"\n🔒 Sichtbarkeit: {PRIVACY}")

    # --- Authentifizieren ---
    print("\n🔑 YouTube-Authentifizierung...")
    try:
        service = get_youtube_service()
        print("✅ Authentifiziert.")
    except SystemExit:
        raise
    except Exception as e:
        print(f"❌ Authentifizierung fehlgeschlagen: {e}")
        sys.exit(1)

    # --- Hochladen ---
    uploaded    = []
    failed      = []
    yt_tracking = load_uploaded_yt()

    # Build mapping: pending entry id → folder_name (für später, status-update)
    pending_by_id = {}
    if PENDING_FILE.exists():
        try:
            with PENDING_FILE.open("r", encoding="utf-8") as f:
                pending_list = json.load(f)
                if isinstance(pending_list, list):
                    for p_entry in pending_list:
                        p_id = p_entry.get("id", "")
                        p_folder = p_entry.get("folder", "")
                        if p_id and p_folder:
                            pending_by_id[p_id] = (p_folder, p_entry)
        except Exception:
            pass

    for folder_name, mp4_path in mp4_jobs:
        print(f"\n{'─'*52}")

        # Bereits hochgeladen? → altes Video löschen, dann neu hochladen (gleicher Tag)
        yt_key = f"{day_folder}|{folder_name}"
        if yt_key in yt_tracking:
            existing = yt_tracking[yt_key]
            old_id   = existing.get("video_id", "")
            print(f"🔄 Video bereits vorhanden (gleicher Tag): {folder_name}")
            print(f"   🗑️  Lösche altes Video: {old_id}  ({existing.get('url', '')})")
            if delete_video(service, old_id):
                del yt_tracking[yt_key]
                save_uploaded_yt(yt_tracking)
                print(f"   ✅ Altes Video entfernt – lade neu hoch.")
            else:
                print(f"   ⚠️  Löschen fehlgeschlagen – Upload wird trotzdem fortgesetzt.")

        size_mb = mp4_path.stat().st_size / 1024 / 1024
        print(f"📤 Lade hoch: {folder_name}  ({size_mb:.1f} MB)")

        # ID-basiertes Matching: Suche Entry-ID zuerst aus pending_by_id, dann aus Master
        item = None
        entry_id = None

        # Schritt 1: Suche in pending_by_id (Quelle der Wahrheit für entry_id)
        if folder_name in pending_by_id:
            p_folder, p_entry = pending_by_id[folder_name]
            entry_id = p_entry.get("id", "")

        # Schritt 2: Fallback zu Master-Item (nur wenn entry_id noch None ist)
        if not entry_id:
            for master_item in master_items:
                master_folder = master_item.get("folder", "")
                if master_folder and Path(master_folder).name == folder_name:
                    item = master_item
                    entry_id = master_item.get("id", "")
                    break

        # Schritt 3: Wenn entry_id gefunden, suche auch das matching master_item
        if entry_id and not item:
            for master_item in master_items:
                if master_item.get("id", "") == entry_id:
                    item = master_item
                    break

        if item and entry_id:
            print(f"   ✅ Master-Match: id={entry_id} folder='{item.get('folder')}'")
        elif entry_id:
            print(f"   ℹ️  Entry-ID gefunden: {entry_id} (master-Match nicht verfügbar)")
        else:
            print("   ⚠️  Kein Entry-Match – Ordnername als Fallback")

        title       = build_title(item)
        description = build_description(item)
        tags        = build_tags(item)

        print(f"   📝 Titel: {title}")

        video_id = upload_video(service, mp4_path, title, description, tags, publish_at)

        if video_id:
            url = f"https://www.youtube.com/shorts/{video_id}"
            print(f"   ✅ Hochgeladen! Video-ID: {video_id}")
            print(f"   🔗 {url}")
            # KRITISCH: entry_id MUSS gesetzt sein, damit Status-Update funktioniert
            if entry_id:
                uploaded.append({"folder": folder_name, "video_id": video_id, "url": url, "item": item, "entry_id": entry_id})
            else:
                # entry_id fehlt → kann Status nicht aktualisieren, aber master wird aktualisiert
                print(f"   ⚠️  Warnung: Keine entry_id für Status-Update in pending.json")
                uploaded.append({"folder": folder_name, "video_id": video_id, "url": url, "item": item, "entry_id": None})

            # SSoT: youtube_url direkt im Master-Item setzen (persistieren am Ende)
            if item is not None:
                item["youtube_url"] = url
            # Sofort nach erfolgreichem Upload persistieren
            yt_tracking[yt_key] = {"video_id": video_id, "url": url, "folder_name": folder_name, "day_folder": str(day_folder)}
            save_uploaded_yt(yt_tracking)
        else:
            failed.append(folder_name)

    # --- Zusammenfassung ---
    print(f"\n{'='*52}")
    print(f"🎯 Step 08 abgeschlossen: {len(uploaded)} hochgeladen, {len(failed)} fehlgeschlagen.")
    for u in uploaded:
        print(f"   ✅ {u['folder']}: {u['url']}")
    for fname in failed:
        print(f"   ❌ {fname}")
    print(f"{'='*52}")

    if not uploaded:
        sys.exit(1)

    # --- master-listings.json persistieren (youtube_url ist in den Items gesetzt) ---
    items_with_url = [u["item"] for u in uploaded if u.get("item") is not None]
    if items_with_url:
        try:
            save_master_listings(day_folder, master)
            print(f"\n🗂️  master-listings.json aktualisiert: "
                  f"{len(items_with_url)} youtube_url eingetragen.")
        except Exception as e:
            print(f"\n❌ master-listings.json konnte nicht geschrieben werden: {e}")

    # --- payhip-listing.csv schreiben (nur frisch hochgeladene Items) ---
    print("\n🛒 Erzeuge payhip-listing.csv...")
    write_payhip_listing_csv(day_folder, items_with_url)

    # --- Status in pending.json aktualisieren (id-basiert) ---
    if not PENDING_FILE.exists():
        return

    try:
        with PENDING_FILE.open("r", encoding="utf-8") as f:
            pending = json.load(f)

        if not isinstance(pending, list):
            return

        # Lade master-listings.json für Matching
        try:
            master = load_master_listings(day_folder)
        except Exception:
            master = None

        status_updated = False

        for entry in pending:
            if entry.get("status") == VIDEO_STATUS:
                entry_id = entry.get("id", "")
                if entry_id:
                    # Finde matching Upload via entry_id
                    matching_upload = None
                    for u in uploaded:
                        if u.get("entry_id") == entry_id:
                            matching_upload = u
                            break

                    if matching_upload:
                        entry["status"]      = YOUTUBE_STATUS
                        entry["youtube_url"] = matching_upload["url"]
                        entry["youtube_id"]  = matching_upload["video_id"]
                        status_updated = True

        if status_updated:
            atomic_write_json(PENDING_FILE, pending)
            print(f"\n💾 Status auf '{YOUTUBE_STATUS}' gesetzt.")
        else:
            print(f"\nℹ️  Keine passenden Einträge für Statusänderung in pending.json gefunden.")

    except Exception as e:
        print(f"⚠️  Konnte pending.json nicht aktualisieren: {e}")


if __name__ == "__main__":
    main()
