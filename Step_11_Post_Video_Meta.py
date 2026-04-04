#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Step_11_Post_Video_Meta.py

Postet das in Step_07 erstellte Video als Reel auf Facebook und Instagram.

Ablauf:
  1. Alle pending.json-Einträge mit status="Video Done" suchen
  2. PAUSE – Lock-Datei erstellen, Workflow stoppt.
     → User pflegt Payhip-Produktseite (YT-Video einbetten)
     → User trägt optional promo_code in listings.csv ein
     → User löscht Lock-Datei und drückt ENTER
  3. listings.csv frisch lesen (promo_code-Spalte auswerten)
  4. Caption bauen: etsy_description_en + ggf. zufälliger Promo-Text mit Code
  5. Facebook Reel: Dreiphasiger Upload (start → Bytes → finish → publish)
  6. Instagram Reel: Resumable Upload → Container → Status-Polling → Publish
  7. pending.json: status = "Meta Posted"

Benötigte Umgebungsvariablen:
  META_ACCESS_TOKEN    – Meta Page Access Token
  FB_PAGE_ID           – Facebook Page ID
  INSTAGRAM_ACCOUNT_ID – Instagram Business Account ID

Fehlt META_ACCESS_TOKEN → Schritt wird übersprungen (kein Fehler).

Config-Parameter (config.yaml):
  meta_video_post_fb: true/false
  meta_video_post_ig: true/false
  promo_texts:
    - "✨ Limited offer: Use code {code} for a special discount!"
    - "🎁 Grab yours with code {code} and save today!"
    - "💥 Use code {code} at checkout for an exclusive deal!"
"""

import os
import sys
import csv
import json
import time
import random
from pathlib import Path
from datetime import datetime, timedelta, timezone

import requests

from config_loader import load_config, get_day_folder, normalize_name, load_listings_csv, atomic_write_json

# Import für Reel-Logging und Caption-Generierung
sys.path.insert(0, str(Path(__file__).parent.parent / "publisher"))
try:
    from repost_logger import log_reel
    from caption_generator import generate_captions
    HAVE_REPOST_LOGGER = True
except ImportError as e:
    print(f"⚠️  Warnung: repost_logger/caption_generator nicht importierbar: {e}")
    HAVE_REPOST_LOGGER = False

# =============================================================================
# CONFIG
# =============================================================================

cfg     = load_config()
config  = cfg["config"]

PENDING_FILE = Path(cfg["PENDING_FILE"])
IMAGES_PATH  = Path(cfg["IMAGES_PATH"])
DATE_FORMAT  = cfg["DATE_FORMAT"]
STATUSES     = cfg["STATUSES"]

flags  = cfg["get_script_flags"]("meta")
DRYRUN = bool(flags.get("dry_run", False))

META_TOKEN   = os.environ.get("META_ACCESS_TOKEN", "").strip()
PAGE_ID      = os.environ.get("FB_PAGE_ID", "").strip()
IG_ACCT_ID   = os.environ.get("INSTAGRAM_ACCOUNT_ID", "").strip()
META_VERSION = "v25.0"

POST_FB      = bool(config.get("meta_video_post_fb", True))
POST_IG      = bool(config.get("meta_video_post_ig", True))
PROMO_TEXTS  = config.get("promo_texts", [
    "✨ Limited offer: Use code {code} for a special discount!",
    "🎁 Grab yours with code {code} and save today!",
    "💥 Use code {code} at checkout for an exclusive deal!",
])
POST_TIMES   = config.get("meta_post_times", ["12:00", "18:00"])
SHOP_CTA     = config.get("shop_cta", "")  # Shop-CTA für Meta-Posts

ELIGIBLE_STATUSES  = {
    STATUSES.get("etsy_listed", "Etsy Listed"),  # Normalfall: Etsy-Step hat gelaufen
    STATUSES.get("upscaled",    "Upscaled"),     # Fallback: Upscaling gelaufen, Etsy übersprungen
}
META_POSTED_STATUS = STATUSES.get("meta_posted", "Meta Posted")

# =============================================================================
# HILFSFUNKTIONEN
# =============================================================================



def get_mockup_paths(article_folder: str | None) -> list[str]:
    """
    Rekonstruiert die lokalen Mockup-Pfade basierend auf dem Artikel-Ordner.

    Erwartet: article_folder = "2026/KW13" oder "Artikelspeicher/2026/KW13"
    Rückgabe: Liste von 5 lokalen Pfaden (mockup_1.jpg bis mockup_5.jpg)
    """
    if not article_folder:
        return []

    # Artikelspeicher-Pfad bestimmen
    workspace_path = Path(__file__).parent.parent / "Claude Workspace" / "Artikelspeicher"
    if not workspace_path.exists():
        # Alternative: Im Generationsordner
        workspace_path = Path(__file__).parent.parent / "Artikelspeicher"

    # Pfad zusammensetzen
    article_path = workspace_path / article_folder.replace("Artikelspeicher/", "")

    if not article_path.exists():
        return []

    # Mockup-Dateien sammeln (mockup_1.jpg bis mockup_5.jpg)
    mockups = []
    for i in range(1, 6):
        mockup_file = article_path / f"mockup_{i}.jpg"
        if mockup_file.exists():
            mockups.append(str(mockup_file))

    return mockups if len(mockups) == 5 else []


def find_csv_row(folder_name: str, csv_rows: list[dict]) -> dict | None:
    """Findet CSV-Zeile via etsy_title-Vergleich (exakt, dann Teilstring)."""
    norm_folder = normalize_name(folder_name)
    for row in csv_rows:
        if normalize_name(row.get("etsy_title", "")) == norm_folder:
            return row
    for row in csv_rows:
        norm_title = normalize_name(row.get("etsy_title", ""))
        if norm_folder in norm_title or norm_title in norm_folder:
            return row
    return None


def assign_post_times(entries: list, post_times: list) -> list:
    """
    Weist jedem Eintrag einen geplanten Post-Zeitpunkt zu.
    Slots werden zyklisch vergeben; nach Erschöpfung aller Tages-Slots
    wird auf den nächsten Tag gewechselt.

    Returns: Liste von (entry, scheduled_datetime | None)
             None = Zeitslot bereits vergangen → sofort posten
    """
    now        = datetime.now()
    today      = now.date()
    result     = []
    day_offset = 0
    slot_idx   = 0

    for entry in entries:
        time_str     = post_times[slot_idx % len(post_times)]
        hour, minute = map(int, time_str.split(":"))
        target_date  = today + timedelta(days=day_offset)
        target_dt    = datetime(target_date.year, target_date.month,
                                target_date.day, hour, minute)

        result.append((entry, None if target_dt <= now else target_dt))

        slot_idx += 1
        if slot_idx % len(post_times) == 0:
            day_offset += 1

    return result


def build_caption(row: dict) -> str:
    """
    Baut die Social-Media-Caption mit Shop-CTA.
    Basis: etsy_description_en + ggf. Promo-Text
    Dann: Shop-CTA vor den Hashtags hinzufügen
    Limit: 2.200 Zeichen (Instagram)

    Wenn Caption + CTA + Hashtags Limit überschreitet:
      Caption von hinten kürzen, CTA bleibt vollständig
    """
    base = (row.get("etsy_description_en") or "").strip()
    promo_code = (row.get("promo_code") or "").strip()
    hashtags = (row.get("social_hashtags") or "").strip()

    # Promo-Text einbauen falls vorhanden
    if promo_code and PROMO_TEXTS:
        template = random.choice(PROMO_TEXTS)
        promo_line = template.replace("{code}", promo_code)
        base = f"{base}\n\n{promo_line}" if base else promo_line

    # Shop-CTA vor den Hashtags einfügen
    caption_with_cta = base
    if SHOP_CTA:
        # Trennzeichen: Leerzeile zwischen Caption und CTA
        caption_with_cta = f"{base}\n\n{SHOP_CTA}" if base else SHOP_CTA

    # Hashtags am Ende
    if hashtags:
        caption_with_cta = f"{caption_with_cta}\n\n{hashtags}"

    # Instagram-Limit prüfen (2.200 Zeichen)
    max_chars = 2200
    if len(caption_with_cta) > max_chars:
        # Kürze Caption-Text von hinten, aber behalte CTA und Hashtags
        # Vorgehen: Trenne Caption-Text, CTA und Hashtags
        if hashtags:
            cta_and_hashtags = f"{SHOP_CTA}\n\n{hashtags}" if SHOP_CTA else hashtags
        else:
            cta_and_hashtags = SHOP_CTA if SHOP_CTA else ""

        cta_and_hashtags_len = len(cta_and_hashtags)
        if cta_and_hashtags_len >= max_chars:
            # Selbst CTA+Hashtags übersteigen Limit — kürze Hashtags
            available = max_chars - len(SHOP_CTA) - 4  # 4 = zwei Leerzeilen
            hashtags_shortened = hashtags[:available].rsplit(" ", 1)[0] if hashtags else ""
            return f"{SHOP_CTA}\n\n{hashtags_shortened}"

        # CTA+Hashtags passen, kürze nur die Base-Caption
        available_for_base = max_chars - cta_and_hashtags_len - 4  # 4 = zwei Leerzeilen
        if available_for_base > 0:
            base_shortened = base[:available_for_base].rsplit(" ", 1)[0] if base else ""
            return f"{base_shortened}\n\n{cta_and_hashtags}"

        # Fallback: nur CTA und Hashtags
        return cta_and_hashtags

    return caption_with_cta


# =============================================================================
# META API – FACEBOOK REEL (Dreiphasiger Upload)
# =============================================================================

def _fb_reel_upload(video_path: Path, caption: str, dry_run: bool,
                    scheduled_time: datetime = None) -> dict:
    """
    Postet ein Video als Facebook Reel über den dreiphasigen Upload.
    Phase 1: start   → video_id + upload_url
    Phase 2: Bytes   → Upload der Videodatei
    Phase 3: finish  → Reel veröffentlichen (sofort oder geplant)

    scheduled_time: None = sofort veröffentlichen
                    datetime = Meta Scheduled Publishing (video_state=SCHEDULED)
    """
    base_url = f"https://graph.facebook.com/{META_VERSION}/{PAGE_ID}/video_reels"
    file_size = video_path.stat().st_size

    if dry_run:
        when = scheduled_time.strftime("%d.%m.%Y %H:%M") if scheduled_time else "sofort"
        print(f"    [dry-run] Facebook Reel: {video_path.name} ({file_size // 1024} KB) → {when}")
        return {"status": "simulated"}

    # ── Phase 1: Upload initialisieren ────────────────────────────────────────
    resp = requests.post(base_url, params={
        "upload_phase": "start",
        "access_token": META_TOKEN,
    }, timeout=30)

    if resp.status_code != 200:
        print(f"    ✗ FB Reel start Fehler {resp.status_code}: {resp.text[:200]}")
        return {"status": "error", "phase": "start", "error": resp.text[:500]}

    data       = resp.json()
    video_id   = data.get("video_id")
    upload_url = data.get("upload_url")

    if not video_id or not upload_url:
        print(f"    ✗ FB Reel: Keine video_id/upload_url in Antwort: {data}")
        return {"status": "error", "phase": "start", "error": str(data)}

    print(f"    ↑ FB Reel Phase 1 OK – video_id={video_id}")

    # ── Phase 2: Bytes hochladen ──────────────────────────────────────────────
    with video_path.open("rb") as vf:
        upload_resp = requests.post(
            upload_url,
            headers={
                "Authorization": f"OAuth {META_TOKEN}",
                "offset":        "0",
                "file_size":     str(file_size),
            },
            data=vf,
            timeout=300,
        )

    if upload_resp.status_code not in (200, 201):
        print(f"    ✗ FB Reel Upload-Fehler {upload_resp.status_code}: {upload_resp.text[:200]}")
        return {"status": "error", "phase": "upload", "error": upload_resp.text[:500]}

    print(f"    ↑ FB Reel Phase 2 OK – Bytes hochgeladen")

    # ── Phase 3: Veröffentlichen (sofort oder geplant) ───────────────────────
    finish_params = {
        "upload_phase":    "finish",
        "video_id":        video_id,
        "video_state":     "SCHEDULED" if scheduled_time else "PUBLISHED",
        "description":     caption,
        "access_token":    META_TOKEN,
    }
    if scheduled_time:
        finish_params["scheduled_publish_time"] = int(scheduled_time.timestamp())

    finish_resp = requests.post(base_url, params=finish_params, timeout=60)

    if finish_resp.status_code == 200:
        when = scheduled_time.strftime("%d.%m.%Y %H:%M") if scheduled_time else "sofort"
        print(f"    ✓ Facebook Reel geplant für {when} – video_id={video_id}")
        return {"video_id": video_id, "status": "success", "scheduled": bool(scheduled_time)}
    else:
        print(f"    ✗ FB Reel finish Fehler {finish_resp.status_code}: {finish_resp.text[:200]}")
        return {"status": "error", "phase": "finish", "error": finish_resp.text[:500]}


# =============================================================================
# META API – INSTAGRAM REEL (Resumable Upload)
# =============================================================================

def _ig_reel_upload(video_path: Path, caption: str, dry_run: bool,
                    scheduled_time: datetime = None) -> dict:
    """
    Postet ein Video als Instagram Reel über den Resumable Upload.
    Phase 1: Container erstellen (upload_type=resumable) → upload_url + ig_container_id
    Phase 2: Bytes hochladen an upload_url
    Phase 3: Status pollen bis FINISHED (max. 5 Minuten)
    Phase 4: Veröffentlichen (media_publish, sofort oder geplant)

    scheduled_time: None = sofort veröffentlichen
                    datetime = scheduled_publish_time im Publish-Schritt
    """
    file_size = video_path.stat().st_size

    if dry_run:
        when = scheduled_time.strftime("%d.%m.%Y %H:%M") if scheduled_time else "sofort"
        print(f"    [dry-run] Instagram Reel: {video_path.name} ({file_size // 1024} KB) → {when}")
        return {"status": "simulated"}

    # ── Phase 1: Container erstellen ─────────────────────────────────────────
    container_url = f"https://graph.facebook.com/{META_VERSION}/{IG_ACCT_ID}/media"
    c_resp = requests.post(container_url, params={
        "media_type":    "REELS",
        "upload_type":   "resumable",
        "caption":       caption,
        "share_to_feed": "true",
        "access_token":  META_TOKEN,
    }, timeout=30)

    if c_resp.status_code != 200:
        print(f"    ✗ IG Reel Container Fehler {c_resp.status_code}: {c_resp.text[:200]}")
        return {"status": "error", "phase": "container", "error": c_resp.text[:500]}

    c_data        = c_resp.json()
    container_id  = c_data.get("id")
    upload_url    = c_data.get("uri")          # IG nennt es "uri"

    if not container_id or not upload_url:
        print(f"    ✗ IG Reel: Keine id/uri in Container-Antwort: {c_data}")
        return {"status": "error", "phase": "container", "error": str(c_data)}

    print(f"    ↑ IG Reel Phase 1 OK – container_id={container_id}")

    # ── Phase 2: Bytes hochladen ──────────────────────────────────────────────
    with video_path.open("rb") as vf:
        up_resp = requests.post(
            upload_url,
            headers={
                "Authorization":  f"OAuth {META_TOKEN}",
                "offset":         "0",
                "file_size":      str(file_size),
            },
            data=vf,
            timeout=300,
        )

    if up_resp.status_code not in (200, 201):
        print(f"    ✗ IG Reel Upload-Fehler {up_resp.status_code}: {up_resp.text[:200]}")
        return {"status": "error", "phase": "upload", "error": up_resp.text[:500]}

    print(f"    ↑ IG Reel Phase 2 OK – Bytes hochgeladen")

    # ── Phase 3: Status pollen ────────────────────────────────────────────────
    status_url = f"https://graph.facebook.com/{META_VERSION}/{container_id}"
    max_polls  = 30          # max. 30 × 10s = 5 Minuten
    poll_delay = 10          # Sekunden zwischen Versuchen

    for attempt in range(1, max_polls + 1):
        time.sleep(poll_delay)
        s_resp = requests.get(status_url, params={
            "fields":       "status_code",
            "access_token": META_TOKEN,
        }, timeout=30)

        if s_resp.status_code != 200:
            print(f"    ⚠ IG Status-Poll Fehler {s_resp.status_code}: {s_resp.text[:100]}")
            continue

        status_code = s_resp.json().get("status_code", "")
        print(f"    … IG Status [{attempt}/{max_polls}]: {status_code}")

        if status_code == "FINISHED":
            break
        elif status_code in ("ERROR", "EXPIRED"):
            print(f"    ✗ IG Reel Container-Status: {status_code}")
            return {"status": "error", "phase": "poll", "error": status_code}
    else:
        print(f"    ✗ IG Reel Timeout: Container nach {max_polls} Versuchen nicht FINISHED")
        return {"status": "error", "phase": "poll", "error": "timeout"}

    # ── Phase 4: Veröffentlichen (sofort oder geplant) ───────────────────────
    pub_url    = f"https://graph.facebook.com/{META_VERSION}/{IG_ACCT_ID}/media_publish"
    pub_params = {
        "creation_id":  container_id,
        "access_token": META_TOKEN,
    }
    if scheduled_time:
        pub_params["scheduled_publish_time"] = int(scheduled_time.timestamp())

    p_resp = requests.post(pub_url, params=pub_params, timeout=30)

    if p_resp.status_code == 200:
        post_id = p_resp.json().get("id")
        when    = scheduled_time.strftime("%d.%m.%Y %H:%M") if scheduled_time else "sofort"
        print(f"    ✓ Instagram Reel geplant für {when} – post_id={post_id}")
        return {"container_id": container_id, "post_id": post_id, "status": "success",
                "scheduled": bool(scheduled_time)}
    else:
        print(f"    ✗ IG Reel Publish Fehler {p_resp.status_code}: {p_resp.text[:200]}")
        return {
            "container_id": container_id,
            "status":       "error",
            "phase":        "publish",
            "error":        p_resp.text[:500],
        }


# =============================================================================
# PAUSE-MECHANISMUS
# =============================================================================

def _pause_for_payhip(day_folder: Path, video_entries: list) -> None:
    """
    Hält den Workflow an und wartet auf manuelle Payhip-Pflege durch den User.
    Lock-Datei: <day_folder>/META_POST_PENDING.lock
    """
    lock_file = day_folder / "META_POST_PENDING.lock"

    # Listings-CSV-Pfad ermitteln (für Anzeige)
    csv_path = day_folder / "listings.csv"

    try:
        lock_file.parent.mkdir(parents=True, exist_ok=True)
        lock_file.write_text(
            "Meta-Post ausstehend.\n"
            "Bitte Payhip-Produktseite pflegen (YouTube-Video einbetten).\n"
            "Optional: promo_code in listings.csv eintragen.\n"
            "Diese Datei löschen wenn fertig, dann ENTER drücken.\n",
            encoding="utf-8"
        )
    except Exception as e:
        print(f"❌ Konnte Lock-Datei nicht erstellen: {e}")
        sys.exit(1)

    print()
    print("=" * 65)
    print("⏸️  WORKFLOW PAUSIERT – PAYHIP-PFLEGE VOR META-POST")
    print("=" * 65)
    print()
    print("Bitte erledige BEIDES bevor du fortsetzt:")
    print()
    print("  🛒  Payhip-Produktseite:")
    print("      1. Payhip öffnen → Produkt bearbeiten")
    print("      2. YouTube-Video-Link einbetten")
    print("      3. Produktseite speichern")
    print()
    meta_csv_path = day_folder / "meta-listing.csv"
    print("  🏷️  Promo-Code (optional):")
    print(f"      4. meta-listing.csv öffnen: {meta_csv_path}")
    print("      5. Spalte 'promo_code' für dieses Produkt füllen")
    print("         (leer lassen = kein Promo-Text im Post)")
    print()

    print("  Produkte in dieser Runde:")
    for e in video_entries:
        print(f"   • {e.get('title', e.get('id', '?'))}")
    print()
    print(f"  🔓  Danach: Lock-Datei löschen ({lock_file.name})")
    print("      und ENTER drücken")
    print("=" * 65)

    while True:
        try:
            input("\n⏳ Drücke ENTER wenn Payhip-Pflege abgeschlossen ist...")
        except KeyboardInterrupt:
            print("\n⚠️  Abgebrochen. Workflow gestoppt.")
            sys.exit(1)

        if lock_file.exists():
            print()
            print("⚠️  Lock-Datei existiert noch!")
            print(f"   Bitte erst Payhip pflegen, dann Lock-Datei löschen:")
            print(f"   {lock_file}")
            print("   Danach ENTER drücken.")
        else:
            break

    print()
    print("✅ Payhip-Pflege bestätigt – Meta-Post wird vorbereitet.")
    print()


# =============================================================================
# HAUPTLOGIK
# =============================================================================

def main():
    print("[Step 11 – Meta Video Post] wird gestartet...")

    # ── Env-Vars prüfen ───────────────────────────────────────────────────────
    if not META_TOKEN:
        print("ℹ️  META_ACCESS_TOKEN nicht gesetzt – Step 12 wird übersprungen.")
        sys.exit(0)

    if not PAGE_ID and POST_FB:
        print("⚠️  FB_PAGE_ID nicht gesetzt – Facebook-Post wird übersprungen.")
    if not IG_ACCT_ID and POST_IG:
        print("⚠️  INSTAGRAM_ACCOUNT_ID nicht gesetzt – Instagram-Post wird übersprungen.")

    # ── run_scripts-Flag prüfen ───────────────────────────────────────────────
    run_scripts = config.get("run_scripts", [])
    if "meta" not in run_scripts:
        print("ℹ️  [meta] ist in run_scripts deaktiviert – Step 12 übersprungen.")
        sys.exit(0)

    # ── pending.json lesen ────────────────────────────────────────────────────
    if not PENDING_FILE.exists():
        print(f"❌ pending.json nicht gefunden: {PENDING_FILE}")
        sys.exit(1)

    with PENDING_FILE.open(encoding="utf-8") as f:
        pending = json.load(f)

    # Einträge nach Etsy-Step (normal) oder YouTube-Step (Step 11 übersprungen) suchen
    video_entries = [
        e for e in pending
        if e.get("status") in ELIGIBLE_STATUSES and e.get("video_path")
    ]

    if not video_entries:
        eligible_str = ", ".join(f"'{s}'" for s in sorted(ELIGIBLE_STATUSES))
        print(f"ℹ️  Keine Einträge mit Status {eligible_str} gefunden – nichts zu tun.")
        sys.exit(0)

    print(f"🎬 {len(video_entries)} Video(s) gefunden:")
    for e in video_entries:
        print(f"   • {e.get('title', e.get('id', '?'))}  →  {Path(e['video_path']).name}")

    # ── Tagesordner bestimmen (konsistent via config, wie alle anderen Steps) ──
    day_folder = get_day_folder(str(IMAGES_PATH), DATE_FORMAT, cfg["TARGET_DATE"])
    if not day_folder.exists():
        print(f"❌ Tagesordner nicht gefunden: {day_folder}")
        print("   Bitte zuerst Step 9 ausführen.")
        sys.exit(1)

    # ── PAUSE – Payhip-Pflege abwarten ────────────────────────────────────────
    if not DRYRUN:
        _pause_for_payhip(day_folder, video_entries)

    # ── listings.csv frisch laden (nach Pause, promo_code ggf. gesetzt) ───────
    csv_path = day_folder / "listings.csv"
    csv_rows = load_listings_csv(csv_path)
    if not csv_rows:
        print(f"⚠️  listings.csv nicht gefunden oder leer: {csv_path}")
        print("   Posts werden ohne Promo-Code erstellt.")

    # ── meta-listing.csv laden für promo_code (Ingo trägt dort den Code ein) ──
    meta_csv_path = day_folder / "meta-listing.csv"
    meta_rows = load_listings_csv(meta_csv_path)
    if meta_rows:
        # promo_code aus meta-listing.csv in csv_rows übertragen (Matching via etsy_title)
        meta_promo_map = {
            r.get("etsy_title", "").strip(): r.get("promo_code", "").strip()
            for r in meta_rows
        }
        for row in csv_rows:
            title = row.get("etsy_title", "").strip()
            if title in meta_promo_map and meta_promo_map[title]:
                row["promo_code"] = meta_promo_map[title]

    # ── Posting-Zeiten zuweisen ───────────────────────────────────────────────
    scheduled_entries = assign_post_times(video_entries, POST_TIMES)
    print(f"\n📅 Posting-Plan:")
    for e, t in scheduled_entries:
        when = t.strftime("%d.%m.%Y %H:%M") if t else "sofort (Zeitslot verpasst)"
        print(f"   • {e.get('title', e.get('id', '?'))}  →  {when}")

    # ── Einträge verarbeiten ──────────────────────────────────────────────────
    posted   = []
    failed   = []

    for entry, scheduled_time in scheduled_entries:
        title      = entry.get("title", entry.get("id", "?"))
        video_path = Path(entry["video_path"])

        print(f"\n{'─' * 60}")
        print(f"📹 Verarbeite: {title}")
        print(f"   Video: {video_path.name}")

        if not video_path.exists():
            print(f"   ❌ Videodatei nicht gefunden: {video_path}")
            failed.append({"title": title, "error": "video file not found"})
            continue

        # CSV-Zeile und Caption
        # Für den CSV-Abgleich marketing_title verwenden (enthält den Produktnamen),
        # da title/id nur die interne ID wie "2026-03-26_002" ist.
        csv_key = entry.get("marketing_title", title)
        csv_row = find_csv_row(csv_key, csv_rows) if csv_rows else {}
        if not csv_row:
            print(f"   ⚠️  Keine CSV-Zeile gefunden für '{csv_key}' – Caption leer.")
            csv_row = {}

        caption    = build_caption(csv_row)
        promo_code = (csv_row.get("promo_code") or "").strip()

        print(f"   Caption: {caption[:80].replace(chr(10), ' ')}{'...' if len(caption) > 80 else ''}")
        if promo_code:
            print(f"   Promo-Code: {promo_code}")
        else:
            print(f"   Promo-Code: keiner")

        entry_result = {"title": title, "fb": None, "ig": None}

        # Facebook Reel
        if POST_FB and PAGE_ID:
            print(f"\n   📘 Facebook Reel...")
            fb_result = _fb_reel_upload(video_path, caption, DRYRUN, scheduled_time)
            entry_result["fb"] = fb_result
            if fb_result.get("status") == "error":
                print(f"   ⚠️  Facebook fehlgeschlagen – fahre mit Instagram fort.")
        elif POST_FB and not PAGE_ID:
            print(f"   ⏭️  Facebook übersprungen (FB_PAGE_ID nicht gesetzt)")

        # Instagram Reel
        if POST_IG and IG_ACCT_ID:
            print(f"\n   📷 Instagram Reel...")
            ig_result = _ig_reel_upload(video_path, caption, DRYRUN, scheduled_time)
            entry_result["ig"] = ig_result
            if ig_result.get("status") == "error":
                print(f"   ⚠️  Instagram fehlgeschlagen.")
        elif POST_IG and not IG_ACCT_ID:
            print(f"   ⏭️  Instagram übersprungen (INSTAGRAM_ACCOUNT_ID nicht gesetzt)")

        # Ergebnis auswerten
        fb_ok = (not POST_FB) or (entry_result["fb"] or {}).get("status") in ("success", "simulated")
        ig_ok = (not POST_IG) or (entry_result["ig"] or {}).get("status") in ("success", "simulated")

        if fb_ok and ig_ok:
            posted.append(entry_result)

            # ── Log-Reel bei erfolgreichem Post ────────────────────────────────────────
            if HAVE_REPOST_LOGGER:
                # Generiere Captions falls noch nicht vorhanden
                captions = generate_captions(
                    product_title=csv_row.get("etsy_title", title),
                    etsy_description_en=csv_row.get("etsy_description_en", ""),
                )

                # Mockup-Pfade rekonstruieren
                mockup_paths = get_mockup_paths(entry.get("article_folder"))

                # Für jeden erfolgreich geposteten Reel: log_reel() aufrufen
                # Falls FB erfolgreich: Nutze FB video_id
                if entry_result.get("fb", {}).get("status") in ("success", "simulated"):
                    fb_reel_id = entry_result["fb"].get("video_id", "")
                    if fb_reel_id:
                        log_reel(
                            reel_id=fb_reel_id,
                            platform="facebook",
                            video_url=entry.get("github_video_url", entry["video_path"]),
                            permalink=None,
                            thumbnail_a=entry.get("github_mockup1_url"),
                            thumbnail_b=entry.get("github_mockup2_url"),
                            caption_a=captions.get("caption_a", ""),
                            caption_b=captions.get("caption_b", ""),
                            caption_c=captions.get("caption_c", ""),
                            caption_d=captions.get("caption_d", ""),
                            mockup_paths=mockup_paths,
                        )

                # Falls IG erfolgreich: Nutze IG post_id
                if entry_result.get("ig", {}).get("status") in ("success", "simulated"):
                    ig_post_id = entry_result["ig"].get("post_id", "")
                    if ig_post_id:
                        log_reel(
                            reel_id=ig_post_id,
                            platform="instagram",
                            video_url=entry.get("github_video_url", entry["video_path"]),
                            permalink=None,
                            thumbnail_a=entry.get("github_mockup1_url"),
                            thumbnail_b=entry.get("github_mockup2_url"),
                            caption_a=captions.get("caption_a", ""),
                            caption_b=captions.get("caption_b", ""),
                            caption_c=captions.get("caption_c", ""),
                            caption_d=captions.get("caption_d", ""),
                            mockup_paths=mockup_paths,
                        )
        else:
            failed.append(entry_result)

    # ── Zusammenfassung ───────────────────────────────────────────────────────
    print(f"\n{'=' * 60}")
    print(f"🎯 Step 11 abgeschlossen: {len(posted)} gepostet, {len(failed)} fehlgeschlagen.")

    if failed:
        print("   Fehlgeschlagen:")
        for f in failed:
            print(f"   ✗ {f.get('title', '?')}")

    # ── pending.json aktualisieren ────────────────────────────────────────────
    if DRYRUN:
        print("🧪 DRY-RUN – pending.json nicht verändert.")
        return

    posted_titles = {p["title"] for p in posted}
    status_updated = False

    for entry in pending:
        if (entry.get("status") in ELIGIBLE_STATUSES
                and entry.get("title", entry.get("id")) in posted_titles):
            entry["status"] = META_POSTED_STATUS
            entry["meta_posted_at"] = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
            status_updated = True

    if status_updated:
        atomic_write_json(PENDING_FILE, pending)
        print(f"💾 Status auf '{META_POSTED_STATUS}' gesetzt.")


if __name__ == "__main__":
    main()
