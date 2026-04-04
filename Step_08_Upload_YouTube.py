#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Step_08_Upload_YouTube.py

Lädt die erstellten MP4-Videos (aus Step 07) als YouTube Shorts hoch.
Liest Titel, Beschreibung und Tags aus listings.csv (aus Step 02).

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

from config_loader import load_config, normalize_name, load_listings_csv, atomic_write_json, get_day_folder

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


def find_csv_row(folder_name: str, csv_rows: list[dict]) -> dict:
    """Findet die beste CSV-Zeile für einen Folder-Namen via etsy_title-Vergleich."""
    norm_folder = normalize_name(folder_name)
    # Exakter Match zuerst
    for row in csv_rows:
        if normalize_name(row.get("etsy_title", "")) == norm_folder:
            return row
    # Teilstring-Match als Fallback
    for row in csv_rows:
        norm_title = normalize_name(row.get("etsy_title", ""))
        if norm_folder in norm_title or norm_title in norm_folder:
            return row
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
    """Baut die YouTube-Tag-Liste aus etsy_tags_en + youtube_base_tags aus config (max. 500 Zeichen gesamt)."""
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

    if ADD_SHORTS_TAG and "Shorts" not in tags:
        tags.append("Shorts")
    return tags[:500]


def write_youtube_urls_to_csv(csv_path: Path, csv_rows: list, uploaded: list) -> None:
    """
    Trägt die YouTube-Short-URLs in listings.csv ein (Spalte 'youtube_url').
    Schreibt die Datei im gleichen Format zurück (Semikolon-getrennt, gequotet).
    """
    if not csv_rows or not uploaded:
        return

    # Upload-Map: normalisierter Folder-Name → URL
    url_map = {normalize_name(u["folder"]): u["url"] for u in uploaded}

    # Spalte ergänzen falls noch nicht vorhanden
    for row in csv_rows:
        if "youtube_url" not in row:
            row["youtube_url"] = ""

    # Matching: etsy_title → URL eintragen
    matched = 0
    for row in csv_rows:
        norm_title = normalize_name(row.get("etsy_title", ""))
        # Exakter Match
        if norm_title in url_map:
            row["youtube_url"] = url_map[norm_title]
            matched += 1
            continue
        # Teilstring-Match als Fallback
        for norm_folder, url in url_map.items():
            if norm_folder in norm_title or norm_title in norm_folder:
                row["youtube_url"] = url
                matched += 1
                break

    if matched == 0:
        print("   ⚠️  Kein CSV-Match für YouTube-URL gefunden.")
        return

    # Spaltenreihenfolge: youtube_url ans Ende, alle anderen in Originalreihenfolge
    fieldnames = [k for k in csv_rows[0].keys() if k != "youtube_url"] + ["youtube_url"]

    # Atomar zurückschreiben
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
            writer.writerows(csv_rows)
        tmp.replace(csv_path)
        print(f"   📋 listings.csv aktualisiert: {matched} URL(s) eingetragen.")
    except Exception as e:
        print(f"   ⚠️  listings.csv konnte nicht geschrieben werden: {e}")
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

    # listings.csv laden
    csv_path = day_folder / "listings.csv"
    csv_rows = load_listings_csv(csv_path)
    if csv_rows:
        print(f"📋 listings.csv geladen: {len(csv_rows)} Zeile(n)")
    else:
        print(f"⚠️  listings.csv nicht gefunden – Ordnernamen werden als Fallback verwendet")

    # Alle <FolderName>/Mockups/*.mp4 sammeln
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
        print("   Bitte zuerst Step 9 ausführen.")
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

    # --- DRY-RUN ---
    if DRYRUN:
        print("\n🧪 DRY-RUN – kein echter Upload.")
        for folder_name, mp4_path in mp4_jobs:
            row   = find_csv_row(folder_name, csv_rows)
            title = build_title(row)
            desc  = build_description(row)
            tags  = build_tags(row)
            print(f"\n   📹 {mp4_path.name}")
            print(f"   Titel:       {title}")
            print(f"   Tags:        {', '.join(tags[:5])}{'...' if len(tags) > 5 else ''}")
            print(f"   Beschr.:     {desc[:80].replace(chr(10), ' ')}...")
        print(f"\n{'='*52}")
        print("🧪 DRY-RUN abgeschlossen.")
        return

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

        row = find_csv_row(folder_name, csv_rows)
        if row:
            print(f"   ✅ CSV-Match: '{row.get('etsy_title', '')}'")
        else:
            print("   ⚠️  Kein CSV-Match – Ordnername als Fallback")

        title       = build_title(row)
        description = build_description(row)
        tags        = build_tags(row)

        print(f"   📝 Titel: {title}")

        video_id = upload_video(service, mp4_path, title, description, tags, publish_at)

        if video_id:
            url = f"https://www.youtube.com/shorts/{video_id}"
            print(f"   ✅ Hochgeladen! Video-ID: {video_id}")
            print(f"   🔗 {url}")
            uploaded.append({"folder": folder_name, "video_id": video_id, "url": url})
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

    # --- YouTube-URLs in listings.csv eintragen ---
    if csv_rows:
        print("\n📋 Trage YouTube-URLs in listings.csv ein...")
        write_youtube_urls_to_csv(csv_path, csv_rows, uploaded)

    # --- Status in pending.json aktualisieren ---
    if not PENDING_FILE.exists():
        return

    try:
        with PENDING_FILE.open("r", encoding="utf-8") as f:
            pending = json.load(f)

        uploaded_map   = {u["folder"]: u for u in uploaded}
        status_updated = False

        for entry in pending:
            if entry.get("status") == VIDEO_STATUS:
                folder_path = entry.get("folder", "")
                folder_name = Path(folder_path).name if folder_path else ""
                if folder_name in uploaded_map:
                    u = uploaded_map[folder_name]
                    entry["status"]      = YOUTUBE_STATUS
                    entry["youtube_url"] = u["url"]
                    entry["youtube_id"]  = u["video_id"]
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
