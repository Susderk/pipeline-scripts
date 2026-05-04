#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Step_10_List_On_Etsy.py

Erstellt Etsy-Listings (Entwürfe / draft) aus master-listings.json des
Tagesordners — inklusive Listing-Bilder-Upload (Mockups) und Digital-Product-
File-Upload (4k-PNGs). Listings bleiben im Status "draft" und werden NICHT
automatisch aktiviert. Manuelle Sichtkontrolle in der Etsy-UI ist Teil des
Workflows (Ingo-Entscheidung 2026-04-27).

Ablauf pro Item (master-listings.json):
  1. POST /v3/application/shops/{shop_id}/listings              — Draft erstellen
  2. POST /v3/application/shops/{shop_id}/listings/{listing_id}/images
                                                                 — 1..4 Mockups
                                                                   (Skip > 10 MB)
  3. POST /v3/application/shops/{shop_id}/listings/{listing_id}/files
                                                                 — bis zu 5 4k-PNGs
                                                                   (Skip > 20 MB)
  4. (NICHT implementiert) PATCH .../{listing_id} state=active.
     Manuelle Aktivierung in der Etsy-UI.

Idempotenz:
  - uploaded_to_etsy.json speichert pro (date|item_id):
      listing_id, url, image_ids[], file_ids[], images_uploaded, files_uploaded
  - Bei Re-Run: bereits existierendes Draft wird wiederverwendet; fehlende
    Bilder/Files werden nachgereicht. Komplett-erfolgte Items werden komplett
    übersprungen.

Voraussetzungen:
  pip install requests

Benoetigte Umgebungsvariablen:
  ETSY_API_KEY       – Etsy API Keystring (aus Etsy Developer Console)
  ETSY_ACCESS_TOKEN  – OAuth 2.0 Access Token mit Scope "listings_w"
  ETSY_SHOP_ID       – Numerische Shop-ID (alternativ: etsy_shop_id in config.yaml)

  → Fehlt einer dieser Werte, wird Step_10 sauber UEBERSPRUNGEN (kein Fehler).

Config-Parameter (config.yaml):
  etsy_shop_id, etsy_price, etsy_currency_code, etsy_quantity, etsy_taxonomy_id,
  etsy_who_made, etsy_when_made, etsy_listing_state, etsy_max_tags,
  etsy_max_image_bytes (default 10 MB), etsy_max_file_bytes (default 20 MB),
  etsy_max_listing_files (default 5).

Quellen Bilder/Files (lokal):
  Mockups:  <day_folder>/<folder>/Mockups/{1..4}.png  (Listing-Hero-Bilder)
  4k:       <day_folder>/<folder>/4k/*-4k.png         (Digital-Product-Files)

Refactor 2026-04-27 (Indi):
  - Draft-Erstellung erweitert um Image- und File-Upload.
  - Idempotency-Tracker um images_uploaded/files_uploaded + IDs erweitert.
  - Reader / Status-Update / Tracker-Format rückwärts-kompatibel.
"""

import sys
import os
import json
import re
import time
from pathlib import Path
from datetime import datetime

try:
    import requests
except ImportError:
    print("❌ 'requests' fehlt. Bitte installieren: pip install requests")
    sys.exit(1)

from config_loader import (
    load_config,
    atomic_write_json,
    load_master_listings,
    save_master_listings,
    find_master_item,
)

# === CONFIG ===
cfg = load_config()
config    = cfg["config"]
JSON_PATH = Path(cfg["JSON_PATH"])

PENDING_FILE = Path(cfg["PENDING_FILE"])
IMAGES_PATH  = Path(cfg["IMAGES_PATH"])
DATE_FORMAT  = cfg["DATE_FORMAT"]
STATUSES     = cfg["STATUSES"]

flags  = cfg["get_script_flags"]("etsy")
DRYRUN = bool(flags.get("dry_run", False))

# === ETSY KONFIGURATION ===
ETSY_API_KEY      = os.environ.get("ETSY_API_KEY", "").strip()
ETSY_ACCESS_TOKEN = os.environ.get("ETSY_ACCESS_TOKEN", "").strip()
ETSY_SHOP_ID      = (
    os.environ.get("ETSY_SHOP_ID", "").strip()
    or str(config.get("etsy_shop_id", "")).strip()
)

ETSY_PRICE         = float(config.get("etsy_price",         3.99))
ETSY_CURRENCY      = str(config.get("etsy_currency_code",   "USD"))
ETSY_QUANTITY      = int(config.get("etsy_quantity",        999))
ETSY_TAXONOMY_ID   = int(config.get("etsy_taxonomy_id",     2078))
ETSY_WHO_MADE      = str(config.get("etsy_who_made",        "i_did"))
ETSY_WHEN_MADE     = str(config.get("etsy_when_made",       "made_to_order"))
ETSY_STATE         = str(config.get("etsy_listing_state",   "draft")).lower()
ETSY_MAX_TAGS      = int(config.get("etsy_max_tags",        13))

# Asset-Limits (Etsy-Doku 2026-04: 10 MB Listing-Bild, 20 MB Digital-File,
# max 5 Files pro Listing). Konfigurierbar.
ETSY_MAX_IMAGE_BYTES   = int(config.get("etsy_max_image_bytes",   10 * 1024 * 1024))
ETSY_MAX_FILE_BYTES    = int(config.get("etsy_max_file_bytes",    20 * 1024 * 1024))
ETSY_MAX_LISTING_FILES = int(config.get("etsy_max_listing_files", 5))

ETSY_API_BASE      = "https://openapi.etsy.com/v3/application"
ETSY_LISTED_FILE   = JSON_PATH / "uploaded_to_etsy.json"

YOUTUBE_STATUS     = STATUSES.get("youtube_done", "YouTube Done")
ETSY_STATUS        = STATUSES.get("etsy_listed",  "Etsy Listed")


# ─────────────────────────────────────────────
# HELPERS
# ─────────────────────────────────────────────

def load_etsy_tracker() -> dict:
    """Laedt uploaded_to_etsy.json; Key: 'date|id'."""
    if not ETSY_LISTED_FILE.exists():
        return {}
    try:
        with ETSY_LISTED_FILE.open("r", encoding="utf-8") as f:
            return json.load(f)
    except Exception as e:
        print(f"⚠️  uploaded_to_etsy.json konnte nicht gelesen werden: {e}")
        return {}


def save_etsy_tracker(data: dict) -> None:
    atomic_write_json(ETSY_LISTED_FILE, data)


def _split_tags(raw: str) -> list:
    """Splittet einen Tag-String (',' separiert) in normalisierte Tag-Liste."""
    if not raw:
        return []
    tags = [
        re.sub(r"[^a-zA-Z0-9 \-]", "", t.strip().strip('"'))[:20]
        for t in raw.split(",")
        if t.strip()
    ]
    seen, clean = set(), []
    for t in tags:
        t = t.strip()
        if t and t.lower() not in seen:
            seen.add(t.lower())
            clean.append(t)
    return clean


def build_tags_from_item(item: dict) -> list:
    """
    Baut Tag-Liste fuer Etsy (max. ETSY_MAX_TAGS, max. 20 Zeichen je Tag).
    Bevorzugt etsy_tags_en; faellt auf etsy_tags_de zurueck.
    """
    raw = item.get("etsy_tags_en") or item.get("etsy_tags_de") or ""
    return _split_tags(raw)[:ETSY_MAX_TAGS]


def build_description_from_item(item: dict) -> str:
    """Etsy-Beschreibung aus etsy_description_en (Fallback DE)."""
    desc = item.get("etsy_description_en") or item.get("etsy_description_de") or ""
    return desc.strip()


def _resolve_listing_dir(item: dict, day_folder: Path) -> Path:
    """
    Resolved den Produktordner aus dem master-Item.

    `item["folder"]` ist mal ein Basename ("Neon Jungle Retreat"), mal ein
    absoluter Windows-Pfad mit Backslashes. Beides muss verlässlich auf
    <day_folder>/<basename> auflösen — auch wenn das Skript auf einer Posix-
    Plattform laeuft (Mount/Linux-Container), wo `pathlib.Path("C:\\foo\\bar").name`
    keinen Backslash als Trenner erkennt.
    """
    folder_val = (item.get("folder") or "").strip()
    if not folder_val:
        folder_val = (item.get("marketing_title") or "").strip()
    # Sowohl Backslash als auch Slash als Trenner behandeln (Cross-Platform)
    cleaned = folder_val.replace("\\", "/").rstrip("/")
    basename = cleaned.rsplit("/", 1)[-1] if "/" in cleaned else cleaned
    return day_folder / basename


# ─────────────────────────────────────────────
# ETSY API
# ─────────────────────────────────────────────

def etsy_headers_json() -> dict:
    """Header für JSON-POST/PATCH (Listing-Erstellung)."""
    return {
        "x-api-key":     ETSY_API_KEY,
        "Authorization": f"Bearer {ETSY_ACCESS_TOKEN}",
        "Content-Type":  "application/json",
    }


def etsy_headers_multipart() -> dict:
    """
    Header für multipart/form-data Uploads (Bilder + Files).
    KEIN Content-Type setzen — `requests` setzt ihn inkl. boundary automatisch.
    """
    return {
        "x-api-key":     ETSY_API_KEY,
        "Authorization": f"Bearer {ETSY_ACCESS_TOKEN}",
    }


def list_etsy_listings_by_shop(limit: int = 100) -> list:
    """Ruft die letzten 'limit' Listings des Shops ab (default 100)."""
    url = f"{ETSY_API_BASE}/shops/{ETSY_SHOP_ID}/listings"
    params = {"limit": limit, "sort_on": "created", "sort_order": "descending"}
    try:
        resp = requests.get(url, headers=etsy_headers_json(), params=params, timeout=30)
        if resp.status_code == 200:
            data = resp.json()
            return data.get("results", [])
    except Exception as e:
        print(f"   ⚠️  Fehler beim Abrufen von Listings: {e}")
    return []


def check_and_fix_duplicate_title(original_title: str, item: dict) -> str:
    """
    Prueft, ob ein Listing mit 'original_title' bereits existiert.
    Falls ja: Praefix "Neu " (DE) bzw. "New " (EN) voranstellen.
    """
    is_english = bool(item.get("etsy_description_en", ""))
    prefix = "New " if is_english else "Neu "

    existing_listings = list_etsy_listings_by_shop()
    existing_titles = {listing.get("title", "") for listing in existing_listings}

    if original_title in existing_titles:
        new_title = (prefix + original_title)[:140]
        print(f"   ℹ️  Duplikat erkannt: '{original_title}' → '{new_title}'")
        return new_title
    return original_title


def create_etsy_listing(title: str, description: str, tags: list) -> dict:
    """POST /v3/application/shops/{shop_id}/listings."""
    url = f"{ETSY_API_BASE}/shops/{ETSY_SHOP_ID}/listings"

    payload = {
        "title":       title[:140],
        "description": description,
        "price":       ETSY_PRICE,
        "quantity":    ETSY_QUANTITY,
        "who_made":    ETSY_WHO_MADE,
        "when_made":   ETSY_WHEN_MADE,
        "taxonomy_id": ETSY_TAXONOMY_ID,
        "type":        "download",
        "tags":        tags,
        "state":       ETSY_STATE,
        "is_supply":   False,
    }

    try:
        resp = requests.post(url, headers=etsy_headers_json(), json=payload, timeout=30)
    except requests.exceptions.RequestException as e:
        print(f"   ❌ Netzwerkfehler bei Listing-Erstellung: {e}")
        return None

    if resp.status_code in (200, 201):
        return resp.json()

    msg = _safe_error_msg(resp)
    print(f"   ❌ Etsy API Fehler {resp.status_code} bei Listing-POST: {msg}")
    _print_status_hint(resp.status_code)
    return None


def _safe_error_msg(resp) -> str:
    """Extrahiert einen lesbaren Fehlertext aus der Etsy-Response."""
    try:
        err = resp.json()
        return err.get("error", "") or err.get("message", "") or resp.text[:200]
    except Exception:
        return resp.text[:200]


def _print_status_hint(status_code: int) -> None:
    """Bekannte Etsy-Fehler-Hinweise."""
    if status_code == 401:
        print("   ℹ️  Tipp: ETSY_ACCESS_TOKEN abgelaufen oder ungueltig.")
    elif status_code == 403:
        print("   ℹ️  Tipp: ETSY_ACCESS_TOKEN hat keine 'listings_w'-Berechtigung.")
    elif status_code == 429:
        print("   ℹ️  Tipp: Rate-Limit erreicht. Kurz warten und erneut versuchen.")


def upload_listing_image(listing_id, image_path: Path, rank: int) -> dict:
    """
    POST /v3/application/shops/{shop_id}/listings/{listing_id}/images

    Multipart/form-data mit Feldern:
      - image: binary
      - rank:  Position in Listing (1 = Hero)

    Rückgabe: dict mit 'listing_image_id' bei Erfolg, sonst None.
    """
    url = f"{ETSY_API_BASE}/shops/{ETSY_SHOP_ID}/listings/{listing_id}/images"
    try:
        with image_path.open("rb") as f:
            files = {"image": (image_path.name, f, "image/png")}
            data  = {"rank": str(rank)}
            resp  = requests.post(
                url,
                headers=etsy_headers_multipart(),
                files=files,
                data=data,
                timeout=120,
            )
    except FileNotFoundError:
        print(f"   ❌ Bilddatei nicht gefunden: {image_path}")
        return None
    except requests.exceptions.RequestException as e:
        print(f"   ❌ Netzwerkfehler beim Bild-Upload ({image_path.name}): {e}")
        return None

    if resp.status_code in (200, 201):
        return resp.json()

    msg = _safe_error_msg(resp)
    print(f"   ❌ Bild-Upload {resp.status_code} ({image_path.name}): {msg}")
    _print_status_hint(resp.status_code)
    return None


def upload_listing_file(listing_id, file_path: Path, rank: int) -> dict:
    """
    POST /v3/application/shops/{shop_id}/listings/{listing_id}/files

    Multipart/form-data mit Feldern:
      - file: binary
      - name: Dateiname (für Käufer sichtbar)
      - rank: Reihenfolge (optional)

    Rückgabe: dict mit 'listing_file_id' bei Erfolg, sonst None.
    """
    url = f"{ETSY_API_BASE}/shops/{ETSY_SHOP_ID}/listings/{listing_id}/files"
    try:
        with file_path.open("rb") as f:
            files = {"file": (file_path.name, f, "application/octet-stream")}
            data  = {"name": file_path.name, "rank": str(rank)}
            resp  = requests.post(
                url,
                headers=etsy_headers_multipart(),
                files=files,
                data=data,
                timeout=180,
            )
    except FileNotFoundError:
        print(f"   ❌ Datei nicht gefunden: {file_path}")
        return None
    except requests.exceptions.RequestException as e:
        print(f"   ❌ Netzwerkfehler beim File-Upload ({file_path.name}): {e}")
        return None

    if resp.status_code in (200, 201):
        return resp.json()

    msg = _safe_error_msg(resp)
    print(f"   ❌ File-Upload {resp.status_code} ({file_path.name}): {msg}")
    _print_status_hint(resp.status_code)
    return None


# ─────────────────────────────────────────────
# ASSET-PHASEN
# ─────────────────────────────────────────────

def collect_mockup_images(listing_dir: Path) -> list:
    """
    Sammelt 1.png..4.png aus <listing_dir>/Mockups/ (aufsteigend nach Name).
    Skippt fehlende.
    """
    mockup_dir = listing_dir / "Mockups"
    if not mockup_dir.is_dir():
        return []
    return sorted(
        [p for p in mockup_dir.iterdir()
         if p.is_file() and p.suffix.lower() == ".png" and p.stem.isdigit()],
        key=lambda p: int(p.stem),
    )


def collect_4k_files(listing_dir: Path) -> list:
    """
    Sammelt *-4k.png aus <listing_dir>/4k/ (alphabetisch).
    """
    fk_dir = listing_dir / "4k"
    if not fk_dir.is_dir():
        return []
    return sorted(
        [p for p in fk_dir.iterdir()
         if p.is_file() and p.suffix.lower() == ".png" and p.stem.endswith("-4k")],
        key=lambda p: p.name,
    )


def upload_images_for_listing(listing_id, listing_dir: Path) -> tuple:
    """
    Lädt 1..4 Mockups als Listing-Bilder hoch (Rang 1..4).
    Skippt Bilder > ETSY_MAX_IMAGE_BYTES.

    Rückgabe: (image_ids: list, all_ok: bool)
      all_ok=True wenn KEIN Bild hard-gefailed (Skips zählen nicht als Fail).
    """
    images = collect_mockup_images(listing_dir)
    if not images:
        print(f"   ⚠️  Keine Mockup-Bilder gefunden in {listing_dir / 'Mockups'}.")
        return [], True  # leere Bilder = kein Fail, aber Listing bleibt unvollständig

    image_ids = []
    any_failed = False

    for img_path in images:
        size = img_path.stat().st_size
        rank = int(img_path.stem)
        if size > ETSY_MAX_IMAGE_BYTES:
            print(f"   ⏭️  {img_path.name} = {size/1024/1024:.1f} MB > "
                  f"{ETSY_MAX_IMAGE_BYTES/1024/1024:.0f} MB Etsy-Limit — geskippt.")
            continue
        print(f"   📷 Upload Bild rank={rank}: {img_path.name} ({size/1024/1024:.1f} MB)")
        result = upload_listing_image(listing_id, img_path, rank=rank)
        if result is None:
            any_failed = True
            continue
        img_id = result.get("listing_image_id") or result.get("image_id")
        if img_id:
            image_ids.append(int(img_id) if isinstance(img_id, (int, str)) and str(img_id).isdigit() else img_id)
        time.sleep(0.5)

    return image_ids, not any_failed


def upload_files_for_listing(listing_id, listing_dir: Path) -> tuple:
    """
    Lädt bis zu ETSY_MAX_LISTING_FILES 4k-PNGs als Digital-Product-Files hoch.
    Skippt Dateien > ETSY_MAX_FILE_BYTES.

    Rückgabe: (file_ids: list, all_ok: bool)
    """
    files_all = collect_4k_files(listing_dir)
    if not files_all:
        print(f"   ⚠️  Keine 4k-PNGs gefunden in {listing_dir / '4k'}.")
        return [], True  # leer = kein Fail

    if len(files_all) > ETSY_MAX_LISTING_FILES:
        print(f"   ℹ️  {len(files_all)} 4k-PNGs gefunden — nur erste "
              f"{ETSY_MAX_LISTING_FILES} (Etsy-Limit) werden hochgeladen.")
        for skipped in files_all[ETSY_MAX_LISTING_FILES:]:
            print(f"      ⏭️  Übersprungen (Limit): {skipped.name}")
        files_all = files_all[:ETSY_MAX_LISTING_FILES]

    file_ids = []
    any_failed = False

    for rank, f_path in enumerate(files_all, start=1):
        size = f_path.stat().st_size
        if size > ETSY_MAX_FILE_BYTES:
            print(f"   ⏭️  {f_path.name} = {size/1024/1024:.1f} MB > "
                  f"{ETSY_MAX_FILE_BYTES/1024/1024:.0f} MB Etsy-Limit — geskippt.")
            continue
        print(f"   📦 Upload File rank={rank}: {f_path.name} ({size/1024/1024:.1f} MB)")
        result = upload_listing_file(listing_id, f_path, rank=rank)
        if result is None:
            any_failed = True
            continue
        file_id = result.get("listing_file_id") or result.get("file_id")
        if file_id:
            file_ids.append(int(file_id) if isinstance(file_id, (int, str)) and str(file_id).isdigit() else file_id)
        time.sleep(0.5)

    return file_ids, not any_failed


# ─────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────

def main():
    print("[Step 10 – Etsy Listing] wird gestartet...")

    # ── API-Verfuegbarkeit pruefen ───────────────────────────────────────────
    # Fehlende Keys/Tokens brechen den Schritt NICHT ab – der Schritt wird
    # sauber uebersprungen. Grund: Etsy-Developer-Approval liegt noch nicht
    # vor; bis dahin ist Step_10 ein No-Op. Die Content-CSVs (etsy-listing.csv
    # + facebook-listing.csv) werden seit 2026-04-09 am Anfang von Step_11
    # aus master-listings.json erzeugt, unabhaengig vom Etsy-API-Status.
    missing = []
    if not ETSY_API_KEY:      missing.append("ETSY_API_KEY")
    if not ETSY_ACCESS_TOKEN: missing.append("ETSY_ACCESS_TOKEN")
    if not ETSY_SHOP_ID:      missing.append("ETSY_SHOP_ID")
    etsy_api_available = not missing

    if not etsy_api_available:
        print()
        print("ℹ️  Etsy-API nicht verfuegbar – fehlende Werte: " + ", ".join(missing))
        print("    → Step_10 wird uebersprungen. CSVs erzeugt Step_11.")
        print()
        return

    # ── Zieldatum & Tagesordner ──────────────────────────────────────────────
    target_date = cfg["TARGET_DATE"]
    print(f"📅 Zieldatum: {target_date.strftime(DATE_FORMAT)}")

    year       = target_date.strftime("%Y")
    month_name = target_date.strftime("%B")
    date_str   = target_date.strftime(DATE_FORMAT)
    day_folder = Path(IMAGES_PATH) / year / f"{year} {month_name}" / date_str

    if not day_folder.exists():
        print(f"❌ Tagesordner nicht gefunden: {day_folder}")
        sys.exit(1)

    # ── master-listings.json laden ───────────────────────────────────────────
    master = load_master_listings(day_folder)
    items = master.get("items", [])

    if not items:
        print("❌ master-listings.json leer oder nicht vorhanden.")
        print("   Bitte zuerst Step 02 ausfuehren.")
        sys.exit(1)

    print(f"📋 master-listings.json geladen: {len(items)} Item(s)")

    # ── Listings vorbereiten (nolist filtern, kein etsy_title → skip) ───────
    jobs = []
    for item in items:
        if item.get("status") == "nolist":
            print(f"   ⏭️  nolist-Item uebersprungen: id={item.get('id', '?')}")
            continue
        title = (item.get("etsy_title") or item.get("etsy_title_en")
                 or item.get("etsy_title_de") or "").strip()
        if not title:
            print(f"   ⚠️  Item ohne etsy_title uebersprungen: id={item.get('id', '?')}")
            continue
        jobs.append({
            "id":          item.get("id", ""),
            "title":       title,
            "description": build_description_from_item(item),
            "tags":        build_tags_from_item(item),
            "item":        item,
            "listing_dir": _resolve_listing_dir(item, day_folder),
        })

    if not jobs:
        print("❌ Keine verwertbaren Items in master-listings.json gefunden.")
        sys.exit(1)

    print(f"\n🛍️  {len(jobs)} Listing(s) werden verarbeitet:")
    for j in jobs:
        tag_preview = ", ".join(j["tags"][:5])
        if len(j["tags"]) > 5:
            tag_preview += "..."
        print(f"   • [{j['id']}] {j['title']} | Tags: {tag_preview}")

    print(f"\n💶 Preis: {ETSY_PRICE} {ETSY_CURRENCY}  |  Zustand: {ETSY_STATE}")
    print(f"🏷️  Taxonomie-ID: {ETSY_TAXONOMY_ID}")
    print(f"📐 Asset-Limits: Bild ≤ {ETSY_MAX_IMAGE_BYTES/1024/1024:.0f} MB, "
          f"File ≤ {ETSY_MAX_FILE_BYTES/1024/1024:.0f} MB, "
          f"max {ETSY_MAX_LISTING_FILES} Files/Listing")

    # ── DRY-RUN ──────────────────────────────────────────────────────────────
    if DRYRUN:
        print("\n🧪 DRY-RUN – keine echten Etsy-Listings werden erstellt.")
        for j in jobs:
            print(f"\n   📦 [{j['id']}] {j['title']}")
            print(f"   Beschreibung: {j['description'][:80].replace(chr(10), ' ')}...")
            print(f"   Tags ({len(j['tags'])}): {', '.join(j['tags'])}")
            mockups = collect_mockup_images(j["listing_dir"])
            files_4k = collect_4k_files(j["listing_dir"])
            print(f"   Mockups vorhanden: {len(mockups)}; 4k-Files: {len(files_4k)}")
        print(f"\n{'='*52}")
        print("🧪 DRY-RUN abgeschlossen.")
        return

    # ── Etsy-Listings erstellen ──────────────────────────────────────────────
    listed = []
    failed = []

    etsy_tracker = load_etsy_tracker()

    for j in jobs:
        print(f"\n{'─'*52}")
        tracker_key = f"{date_str}|{j['id']}"
        existing = etsy_tracker.get(tracker_key)

        # ─── Phase 1: Draft-Listing erstellen oder wiederverwenden ───────────
        if existing and existing.get("listing_id"):
            listing_id = existing["listing_id"]
            listing_url = existing.get("url", f"https://www.etsy.com/listing/{listing_id}")
            print(f"♻️  Bestehender Draft wiederverwendet: [{j['id']}] listing_id={listing_id}")
            # Falls beide Phases bereits done → komplett skippen
            if existing.get("images_uploaded") and existing.get("files_uploaded"):
                print(f"   ✅ Bereits vollständig. Skip.")
                listed.append({
                    "id":         j["id"],
                    "listing_id": listing_id,
                    "url":        listing_url,
                })
                # Sicherheitsnetz: master-Item nachpflegen
                j["item"]["etsy_url"]    = listing_url
                j["item"]["listing_id"]  = listing_id
                continue
        else:
            # Duplikat-Pruefung: Titel ggf. mit Praefix
            final_title = check_and_fix_duplicate_title(j["title"], j["item"])
            print(f"📤 Erstelle Listing: {final_title}")
            print(f"   Tags: {', '.join(j['tags'][:5])}{'...' if len(j['tags']) > 5 else ''}")
            result = create_etsy_listing(final_title, j["description"], j["tags"])
            if not result:
                failed.append(j["title"])
                continue
            listing_id = result.get("listing_id") or result.get("id")
            listing_url = f"https://www.etsy.com/listing/{listing_id}"
            print(f"   ✅ Draft erstellt! Listing-ID: {listing_id}")
            print(f"   🔗 {listing_url}")

            # Tracker initial schreiben (vor Asset-Uploads, damit Re-Run klappt)
            etsy_tracker[tracker_key] = {
                "listing_id":       listing_id,
                "url":              listing_url,
                "title":            final_title,
                "original_title":   j["title"],
                "id":               j["id"],
                "date":             date_str,
                "created_at":       datetime.now().isoformat(),
                "image_ids":        [],
                "file_ids":         [],
                "images_uploaded":  False,
                "files_uploaded":   False,
            }
            save_etsy_tracker(etsy_tracker)

        tracker_entry = etsy_tracker[tracker_key]

        # ─── Phase 2: Listing-Bilder (Mockups) ───────────────────────────────
        if not tracker_entry.get("images_uploaded"):
            print(f"   ── Phase 2: Listing-Bilder ──")
            image_ids, images_ok = upload_images_for_listing(listing_id, j["listing_dir"])
            tracker_entry["image_ids"] = image_ids
            tracker_entry["images_uploaded"] = images_ok and len(image_ids) > 0
            save_etsy_tracker(etsy_tracker)
            if images_ok and image_ids:
                print(f"   ✅ {len(image_ids)} Bild(er) hochgeladen.")
            elif not image_ids:
                print(f"   ⚠️  Keine Bilder hochgeladen (siehe oben).")
            else:
                print(f"   ⚠️  Bild-Upload teilweise fehlgeschlagen ({len(image_ids)} OK).")
        else:
            print(f"   ⏭️  Bilder bereits hochgeladen (Re-Run): {len(tracker_entry.get('image_ids', []))} ID(s).")

        # ─── Phase 3: Digital-Product-Files (4k-PNGs) ────────────────────────
        if not tracker_entry.get("files_uploaded"):
            print(f"   ── Phase 3: Digital-Files ──")
            file_ids, files_ok = upload_files_for_listing(listing_id, j["listing_dir"])
            tracker_entry["file_ids"] = file_ids
            tracker_entry["files_uploaded"] = files_ok and len(file_ids) > 0
            save_etsy_tracker(etsy_tracker)
            if files_ok and file_ids:
                print(f"   ✅ {len(file_ids)} File(s) hochgeladen.")
            elif not file_ids:
                print(f"   ⚠️  Keine Files hochgeladen (siehe oben).")
            else:
                print(f"   ⚠️  File-Upload teilweise fehlgeschlagen ({len(file_ids)} OK).")
        else:
            print(f"   ⏭️  Files bereits hochgeladen (Re-Run): {len(tracker_entry.get('file_ids', []))} ID(s).")

        # ─── Listing-Status-Hinweis ──────────────────────────────────────────
        if tracker_entry.get("images_uploaded") and tracker_entry.get("files_uploaded"):
            print(f"   ✅ Listing vollstaendig (draft). Manuelle Aktivierung in Etsy-UI noetig.")
        else:
            print(f"   ⚠️  Listing unvollstaendig — Etsy verbietet 'active' bis Bilder + Files vorhanden.")

        listed.append({
            "id":         j["id"],
            "listing_id": listing_id,
            "url":        listing_url,
        })

        # master-Item direkt im Speicher updaten
        j["item"]["etsy_url"]   = listing_url
        j["item"]["listing_id"] = listing_id

        time.sleep(0.5)

    # ── Zusammenfassung ──────────────────────────────────────────────────────
    print(f"\n{'='*52}")
    print(f"🎯 Step 10 abgeschlossen: {len(listed)} Listing(s) gelistet, {len(failed)} fehlgeschlagen.")
    for entry in listed:
        print(f"   ✅ [{entry['id']}] {entry['url']}")
    for title in failed:
        print(f"   ❌ {title}")
    print(f"{'='*52}")

    if not listed:
        sys.exit(1)

    # ── master-listings.json persistieren (etsy_url + listing_id) ────────────
    try:
        save_master_listings(day_folder, master)
        print(f"\n🗂️  master-listings.json aktualisiert: "
              f"{len(listed)} Item(s) mit etsy_url / listing_id.")
    except Exception as e:
        print(f"❌ master-listings.json konnte nicht geschrieben werden: {e}")
        sys.exit(1)

    # ── Status in pending.json aktualisieren (id-basiert) ────────────────────
    if not PENDING_FILE.exists():
        return

    try:
        with PENDING_FILE.open("r", encoding="utf-8") as f:
            pending = json.load(f)

        listed_by_id = {item["id"]: item for item in listed if item.get("id")}
        status_updated = False

        for entry in pending:
            entry_id = entry.get("id", "")
            if not entry_id or entry_id not in listed_by_id:
                continue
            # B2 FIX (2026-04-15): Nur Upscaled + YouTube Done akzeptieren.
            # "Video Done" und "All Done" sind zu früh in Pipeline — Normalfluss ist:
            # Step_09 → Upscaled, oder Legacy nach Step_08 → YouTube Done.
            # Zu breites Filter-Spektrum verstößt gegen Status-Quelle-Konsistenz.
            if entry.get("status") not in (YOUTUBE_STATUS, "Upscaled"):
                continue
            li = listed_by_id[entry_id]
            entry["status"]     = ETSY_STATUS
            entry["etsy_url"]   = li["url"]
            entry["listing_id"] = li["listing_id"]
            status_updated = True

        if status_updated:
            atomic_write_json(PENDING_FILE, pending)
            print(f"\n💾 pending.json: Status auf '{ETSY_STATUS}' gesetzt (id-basiert).")
        else:
            print("\nℹ️  Keine passenden id-Treffer in pending.json fuer Statusaenderung.")

    except Exception as e:
        print(f"⚠️  Konnte pending.json nicht aktualisieren: {e}")


if __name__ == "__main__":
    main()
