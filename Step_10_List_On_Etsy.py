#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Step_10_List_On_Etsy.py

Erstellt Etsy-Listings (Entwürfe) aus master-listings.json des Tagesordners
und schreibt zwei Content-Exporte:

  • etsy-listing.csv     (DE+EN Content fuer manuelles Etsy-Listing-Backup)
  • facebook-listing.csv (Commerce-Manager-Import, EN-only, mit Festwerten)

Refactor-Punkt 6 (master-listings.json als SSoT):
  - Reader: master-listings.json (kein listings.csv mehr)
  - Matching: id-basiert via find_master_item (kein Substring-Matching)
  - Ergebnis: etsy_url + listing_id werden ins master-Item zurueckgeschrieben
  - pending.json-Status-Update ebenfalls per id

Voraussetzungen:
  pip install requests

Benoetigte Umgebungsvariablen:
  ETSY_API_KEY       – Etsy API Keystring (aus Etsy Developer Console)
  ETSY_ACCESS_TOKEN  – OAuth 2.0 Access Token (aus Etsy OAuth-Flow)
  ETSY_SHOP_ID       – Numerische Shop-ID (alternativ: etsy_shop_id in config.yaml)

  → Fehlt ETSY_API_KEY, wird der Schritt UEBERSPRUNGEN (kein Fehler).

Config-Parameter (config.yaml):
  etsy_shop_id, etsy_price, etsy_currency_code, etsy_quantity, etsy_taxonomy_id,
  etsy_who_made, etsy_when_made, etsy_listing_state, etsy_max_tags

Hinweis zu digitalen Produkten:
  Etsy erfordert, dass nach dem Erstellen des Listings die Produktdatei
  manuell (oder via API /v3/.../files) hochgeladen wird, bevor das Listing
  auf "active" gesetzt werden kann. Dieses Skript erstellt zunaechst Entwuerfe.
"""

import sys
import os
import json
import csv
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

ETSY_API_BASE      = "https://openapi.etsy.com/v3/application"
ETSY_LISTED_FILE   = JSON_PATH / "uploaded_to_etsy.json"

YOUTUBE_STATUS     = STATUSES.get("youtube_done", "YouTube Done")
ETSY_STATUS        = STATUSES.get("etsy_listed",  "Etsy Listed")

# Festwerte fuer facebook-listing.csv (Commerce-Manager-Import)
FB_PRICE        = "2,99"
FB_AVAILABILITY = "auf Lager"
FB_CONDITION    = "neu"


# ─────────────────────────────────────────────
# HELPERS
# ─────────────────────────────────────────────

def load_etsy_tracker() -> dict:
    """Laedt uploaded_to_etsy.json; Key: 'day_folder|id'."""
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


def _split_tags(raw: str) -> list[str]:
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


def build_tags_from_item(item: dict) -> list[str]:
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


# ─────────────────────────────────────────────
# ETSY API
# ─────────────────────────────────────────────

def etsy_headers() -> dict:
    return {
        "x-api-key":     ETSY_API_KEY,
        "Authorization": f"Bearer {ETSY_ACCESS_TOKEN}",
        "Content-Type":  "application/json",
    }


def list_etsy_listings_by_shop(limit: int = 100) -> list:
    """Ruft die letzten 'limit' Listings des Shops ab (default 100)."""
    url = f"{ETSY_API_BASE}/shops/{ETSY_SHOP_ID}/listings"
    params = {"limit": limit, "sort_on": "created", "sort_order": "descending"}
    try:
        resp = requests.get(url, headers=etsy_headers(), params=params, timeout=30)
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


def create_etsy_listing(title: str, description: str, tags: list) -> dict | None:
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
        resp = requests.post(url, headers=etsy_headers(), json=payload, timeout=30)
    except requests.exceptions.RequestException as e:
        print(f"   ❌ Netzwerkfehler: {e}")
        return None

    if resp.status_code in (200, 201):
        return resp.json()

    try:
        err = resp.json()
        msg = err.get("error", "") or err.get("message", "") or resp.text[:200]
    except Exception:
        msg = resp.text[:200]

    print(f"   ❌ Etsy API Fehler {resp.status_code}: {msg}")
    if resp.status_code == 401:
        print("   ℹ️  Tipp: ETSY_ACCESS_TOKEN abgelaufen oder ungueltig.")
    elif resp.status_code == 403:
        print("   ℹ️  Tipp: ETSY_ACCESS_TOKEN hat keine 'listings_w'-Berechtigung.")
    elif resp.status_code == 429:
        print("   ℹ️  Tipp: Rate-Limit erreicht. Kurz warten und erneut versuchen.")
    return None


# ─────────────────────────────────────────────
# CSV-Exporte
# ─────────────────────────────────────────────

ETSY_CSV_FIELDS = [
    "title_de", "description_de", "short_line_de", "tags_de",
    "title_en", "description_en", "short_line_en", "tags_en",
]

FB_CSV_FIELDS = [
    "product_id", "video_link_github", "title", "price",
    "payhip_link", "availability", "condition", "description",
]


def write_etsy_listing_csv(day_folder: Path, items: list[dict]) -> Path | None:
    """
    Schreibt etsy-listing.csv (Content-Backup, DE+EN).
    Bewusst KEIN id/folder/listing_id/etsy_url – nur Content-Felder.
    """
    csv_path = day_folder / "etsy-listing.csv"
    try:
        with csv_path.open("w", encoding="utf-8-sig", newline="") as f:
            writer = csv.DictWriter(
                f, fieldnames=ETSY_CSV_FIELDS,
                delimiter=";", quotechar='"', quoting=csv.QUOTE_ALL,
            )
            writer.writeheader()
            for it in items:
                writer.writerow({
                    "title_de":        it.get("etsy_title_de") or it.get("etsy_title", ""),
                    "description_de":  it.get("etsy_description_de", ""),
                    "short_line_de":   it.get("short_line_de", ""),
                    "tags_de":         it.get("etsy_tags_de", ""),
                    "title_en":        it.get("etsy_title_en") or it.get("etsy_title", ""),
                    "description_en":  it.get("etsy_description_en", ""),
                    "short_line_en":   it.get("short_line_en", ""),
                    "tags_en":         it.get("etsy_tags_en", ""),
                })
        print(f"   📋 etsy-listing.csv geschrieben: {len(items)} Zeile(n).")
        return csv_path
    except Exception as e:
        print(f"   ⚠️  etsy-listing.csv konnte nicht geschrieben werden: {e}")
        return None


def write_facebook_listing_csv(day_folder: Path, items: list[dict]) -> Path | None:
    """
    Schreibt facebook-listing.csv (Commerce-Manager-Import, EN-only, Festwerte).
    Spalten: product_id | video_link_github | title | price | payhip_link |
             availability | condition | description
    Wird NICHT in Excel geoeffnet (manueller Import in Commerce Manager).
    Auch Items ohne payhip_product_link / video_github_url werden geschrieben
    (Commerce-Manager-Validierung uebernimmt der User).
    """
    csv_path = day_folder / "facebook-listing.csv"
    try:
        with csv_path.open("w", encoding="utf-8-sig", newline="") as f:
            writer = csv.DictWriter(
                f, fieldnames=FB_CSV_FIELDS,
                delimiter=";", quotechar='"', quoting=csv.QUOTE_ALL,
            )
            writer.writeheader()
            for it in items:
                writer.writerow({
                    "product_id":        it.get("id", ""),
                    "video_link_github": it.get("video_github_url") or "",
                    "title":             it.get("etsy_title_en") or it.get("etsy_title", ""),
                    "price":             FB_PRICE,
                    "payhip_link":       it.get("payhip_product_link") or "",
                    "availability":      FB_AVAILABILITY,
                    "condition":         FB_CONDITION,
                    "description":       it.get("etsy_description_en") or "",
                })
        print(f"   📋 facebook-listing.csv geschrieben: {len(items)} Zeile(n).")
        return csv_path
    except Exception as e:
        print(f"   ⚠️  facebook-listing.csv konnte nicht geschrieben werden: {e}")
        return None


# ─────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────

def main():
    print("[Step 10 – Etsy Listing] wird gestartet...")

    # ── API-Verfuegbarkeit pruefen ───────────────────────────────────────────
    # Fehlende Keys/Tokens brechen den Schritt NICHT ab. Der Schritt laeuft
    # weiter und erzeugt zumindest die Content-CSVs (etsy-listing.csv +
    # facebook-listing.csv). Nur der eigentliche API-Upload (Listing-Erstellung,
    # Tracker, etsy_url/listing_id Write-Back) wird uebersprungen.
    # Grund: Etsy-Developer-Approval liegt noch nicht vor; bis dahin laeuft
    # Step_10 "blind" mit.
    missing = []
    if not ETSY_API_KEY:      missing.append("ETSY_API_KEY")
    if not ETSY_ACCESS_TOKEN: missing.append("ETSY_ACCESS_TOKEN")
    if not ETSY_SHOP_ID:      missing.append("ETSY_SHOP_ID")
    etsy_api_available = not missing

    if not etsy_api_available:
        print()
        print("ℹ️  Etsy-API nicht verfuegbar – fehlende Werte: " + ", ".join(missing))
        print("    → API-Upload wird uebersprungen, CSV-Exporte laufen normal.")
        print()

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

    # ── Listings vorbereiten ─────────────────────────────────────────────────
    jobs = []
    for item in items:
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

    # ── DRY-RUN ──────────────────────────────────────────────────────────────
    if DRYRUN:
        print("\n🧪 DRY-RUN – keine echten Etsy-Listings werden erstellt.")
        for j in jobs:
            print(f"\n   📦 [{j['id']}] {j['title']}")
            print(f"   Beschreibung: {j['description'][:80].replace(chr(10), ' ')}...")
            print(f"   Tags ({len(j['tags'])}): {', '.join(j['tags'])}")
        # Im DRY-RUN trotzdem die Content-CSVs erzeugen (kein API-Call noetig)
        print()
        write_etsy_listing_csv(day_folder, items)
        write_facebook_listing_csv(day_folder, items)
        print(f"\n{'='*52}")
        print("🧪 DRY-RUN abgeschlossen.")
        return

    # ── Etsy-Listings erstellen (nur wenn API verfuegbar) ────────────────────
    listed: list = []
    failed: list = []

    if not etsy_api_available:
        print("\n⏭️  API-Upload uebersprungen (Keys/Tokens fehlen).")
        print("    master-listings.json bleibt bei etsy_url/listing_id unveraendert.")
        print()
        write_etsy_listing_csv(day_folder, items)
        write_facebook_listing_csv(day_folder, items)
        print(f"\n{'='*52}")
        print("✅ Step 10 abgeschlossen (blind / nur CSV-Export).")
        print(f"{'='*52}")
        return

    etsy_tracker = load_etsy_tracker()

    for j in jobs:
        print(f"\n{'─'*52}")
        tracker_key = f"{date_str}|{j['id']}"

        # Idempotency Guard – bereits gelistet?
        if tracker_key in etsy_tracker:
            existing = etsy_tracker[tracker_key]
            print(f"⏭️  Bereits gelistet: [{j['id']}] {j['title']}")
            print(f"   🔗 {existing.get('url', '?')}")
            listed.append({
                "id":         j["id"],
                "listing_id": existing["listing_id"],
                "url":        existing["url"],
            })
            # Sicherheitsnetz: master-Item ggf. nachpflegen
            j["item"]["etsy_url"]   = existing.get("url")
            j["item"]["listing_id"] = existing.get("listing_id")
            continue

        # Duplikat-Pruefung: Titel ggf. mit Praefix
        final_title = check_and_fix_duplicate_title(j["title"], j["item"])

        print(f"📤 Erstelle Listing: {final_title}")
        print(f"   Tags: {', '.join(j['tags'][:5])}{'...' if len(j['tags']) > 5 else ''}")

        result = create_etsy_listing(final_title, j["description"], j["tags"])

        if result:
            listing_id = result.get("listing_id") or result.get("id")
            url = f"https://www.etsy.com/listing/{listing_id}"
            print(f"   ✅ Erstellt! Listing-ID: {listing_id}")
            print(f"   🔗 {url}")
            listed.append({"id": j["id"], "listing_id": listing_id, "url": url})

            # master-Item direkt im Speicher updaten
            j["item"]["etsy_url"]   = url
            j["item"]["listing_id"] = listing_id

            # Tracker persistieren
            etsy_tracker[tracker_key] = {
                "listing_id":     listing_id,
                "url":            url,
                "title":          final_title,
                "original_title": j["title"],
                "id":             j["id"],
                "date":           date_str,
                "created_at":     datetime.now().isoformat(),
            }
            save_etsy_tracker(etsy_tracker)
        else:
            failed.append(j["title"])

        time.sleep(0.5)

    # ── Zusammenfassung ──────────────────────────────────────────────────────
    print(f"\n{'='*52}")
    print(f"🎯 Step 10 abgeschlossen: {len(listed)} gelistet, {len(failed)} fehlgeschlagen.")
    for item in listed:
        print(f"   ✅ [{item['id']}] {item['url']}")
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

    # ── Content-Exporte (etsy + facebook) ────────────────────────────────────
    print()
    write_etsy_listing_csv(day_folder, items)
    write_facebook_listing_csv(day_folder, items)

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
            if entry.get("status") not in (YOUTUBE_STATUS, "All Done", "Video Done", "Upscaled"):
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
