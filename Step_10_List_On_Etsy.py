#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Step_10_List_On_Etsy.py

Erstellt Etsy-Listings (Entwürfe) aus listings.csv des Tagesordners.
Duplikat-Handling: Wenn ein Listing mit gleichem Titel bereits existiert,
wird automatisch "Neu " (DE) oder "New " (EN) vorangestellt.

Voraussetzungen:
  pip install requests

Benötigte Umgebungsvariablen:
  ETSY_API_KEY       – Etsy API Keystring (aus Etsy Developer Console)
  ETSY_ACCESS_TOKEN  – OAuth 2.0 Access Token (aus Etsy OAuth-Flow)
  ETSY_SHOP_ID       – Numerische Shop-ID (alternativ: etsy_shop_id in config.yaml)

  → Fehlt ETSY_API_KEY, wird der Schritt ÜBERSPRUNGEN (kein Fehler, nächster Step läuft).

Config-Parameter (config.yaml):
  etsy_shop_id:           ""             # Shop-ID (alternativ: Env-Var ETSY_SHOP_ID)
  etsy_price:             3.99           # Preis pro Listing
  etsy_currency_code:     "USD"          # Währung (ISO 4217)
  etsy_quantity:          999            # Verfügbare Menge
  etsy_taxonomy_id:       2078           # Etsy Kategorie-ID (2078 = Digital Prints)
  etsy_who_made:          "i_did"        # Wer hat's gemacht (i_did / collective / someone_else)
  etsy_when_made:         "made_to_order"# Wann gemacht (made_to_order / 2020_2025 / ...)
  etsy_listing_state:     "draft"        # draft / active (active erst wenn Datei angehängt)
  etsy_max_tags:          13             # Etsy erlaubt max. 13 Tags pro Listing

Hinweis zu digitalen Produkten:
  Etsy erfordert, dass nach dem Erstellen des Listings die Produktdatei
  manuell (oder via API /v3/.../files) hochgeladen wird, bevor das Listing
  auf "active" gesetzt werden kann. Dieses Skript erstellt zunächst Entwürfe.

Duplikat-Handling:
  • Prüft vor der Erstellung alle existierenden Shop-Listings (max. 100)
  • Falls Titel bereits vorhanden: Präfix hinzufügen
  • Sprache für Präfix (DE "Neu " oder EN "New ") aus CSV ermittelt
  • Aktualisierte Titel werden in tracker (uploaded_to_etsy.json) mit original_title gespeichert
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

from config_loader import load_config, normalize_name, load_listings_csv, atomic_write_json

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


# ─────────────────────────────────────────────
# HELPERS
# ─────────────────────────────────────────────

def load_etsy_tracker() -> dict:
    """Lädt uploaded_to_etsy.json; Key: 'day_folder|folder_name'."""
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


def build_tags(row: dict) -> list:
    """
    Baut Tag-Liste für Etsy (max. ETSY_MAX_TAGS Tags, max. 20 Zeichen je Tag).
    Bevorzugt etsy_tags_en; fällt auf etsy_tags_de zurück.
    Etsy erlaubt keine Sonderzeichen außer Bindestrich und Leerzeichen.
    """
    raw = row.get("etsy_tags_en") or row.get("etsy_tags_de", "")
    tags = [
        re.sub(r"[^a-zA-Z0-9 \-]", "", t.strip().strip('"'))[:20]
        for t in raw.split(",")
        if t.strip()
    ]
    # Duplikate & Leerstrings entfernen
    seen, clean = set(), []
    for t in tags:
        t = t.strip()
        if t and t.lower() not in seen:
            seen.add(t.lower())
            clean.append(t)
    return clean[:ETSY_MAX_TAGS]


def build_description(row: dict) -> str:
    """Baut Etsy-Beschreibung aus etsy_description_en + CTA."""
    desc = row.get("etsy_description_en", "") or row.get("etsy_description_de", "")
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
    """
    Ruft alle Listings des Shops ab (begrenzt auf 'limit', default 100).
    Gibt Liste der Listing-Dicts zurück oder leere Liste bei Fehler.
    """
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


def check_and_fix_duplicate_title(original_title: str, row: dict) -> str:
    """
    Prüft, ob ein Listing mit 'original_title' bereits existiert.
    Falls ja: Präfix "Neu " (DE) bzw. "New " (EN) voranstellen.
    Rückgabe: Möglicherweise geänderter Titel (max. 140 Zeichen).
    """
    # Sprache ermitteln (aus CSV: etsy_description_en vs. etsy_description_de)
    is_english = bool(row.get("etsy_description_en", ""))
    prefix = "New " if is_english else "Neu "

    # Existierende Listings abrufen
    existing_listings = list_etsy_listings_by_shop()
    existing_titles = {listing.get("title", "") for listing in existing_listings}

    if original_title in existing_titles:
        new_title = prefix + original_title
        # Etsy-Limit einhalten (max. 140 Zeichen)
        new_title = new_title[:140]
        print(f"   ℹ️  Duplikat erkannt: '{original_title}' → '{new_title}'")
        return new_title

    return original_title


def create_etsy_listing(title: str, description: str, tags: list) -> dict | None:
    """
    Erstellt ein Etsy-Listing via POST /v3/application/shops/{shop_id}/listings.
    Gibt das API-Response-Dict zurück oder None bei Fehler.
    """
    url = f"{ETSY_API_BASE}/shops/{ETSY_SHOP_ID}/listings"

    payload = {
        "title":       title[:140],       # Etsy-Limit: 140 Zeichen
        "description": description,
        "price":       ETSY_PRICE,
        "quantity":    ETSY_QUANTITY,
        "who_made":    ETSY_WHO_MADE,
        "when_made":   ETSY_WHEN_MADE,
        "taxonomy_id": ETSY_TAXONOMY_ID,
        "type":        "download",        # Digitales Produkt
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

    # Fehler-Detail ausgeben
    try:
        err = resp.json()
        msg = err.get("error", "") or err.get("message", "") or resp.text[:200]
    except Exception:
        msg = resp.text[:200]

    print(f"   ❌ Etsy API Fehler {resp.status_code}: {msg}")

    # Hilfreiche Hinweise bei bekannten Fehlercodes
    if resp.status_code == 401:
        print("   ℹ️  Tipp: ETSY_ACCESS_TOKEN abgelaufen oder ungültig.")
    elif resp.status_code == 403:
        print("   ℹ️  Tipp: ETSY_ACCESS_TOKEN hat keine 'listings_w'-Berechtigung.")
    elif resp.status_code == 429:
        print("   ℹ️  Tipp: Rate-Limit erreicht. Kurz warten und erneut versuchen.")

    return None


def write_etsy_urls_to_csv(csv_path: Path, csv_rows: list, listed: list) -> None:
    """Trägt Etsy-Listing-URLs in listings.csv ein (Spalte 'etsy_url')."""
    if not csv_rows or not listed:
        return

    url_map = {normalize_name(item["folder"]): item["url"] for item in listed}

    for row in csv_rows:
        if "etsy_url" not in row:
            row["etsy_url"] = ""

    matched = 0
    for row in csv_rows:
        norm_title = normalize_name(row.get("etsy_title", ""))
        if norm_title in url_map:
            row["etsy_url"] = url_map[norm_title]
            matched += 1
            continue
        for norm_folder, url in url_map.items():
            if norm_folder in norm_title or norm_title in norm_folder:
                row["etsy_url"] = url
                matched += 1
                break

    if matched == 0:
        print("   ⚠️  Kein CSV-Match für Etsy-URL gefunden.")
        return

    fieldnames = [k for k in csv_rows[0].keys() if k != "etsy_url"] + ["etsy_url"]
    tmp = csv_path.with_suffix(csv_path.suffix + ".tmp")
    try:
        with tmp.open("w", encoding="utf-8-sig", newline="") as f:
            writer = csv.DictWriter(
                f, fieldnames=fieldnames,
                delimiter=";", quotechar='"', quoting=csv.QUOTE_ALL,
            )
            writer.writeheader()
            writer.writerows(csv_rows)
        tmp.replace(csv_path)
        print(f"   📋 listings.csv aktualisiert: {matched} Etsy-URL(s) eingetragen.")
    except Exception as e:
        print(f"   ⚠️  listings.csv konnte nicht geschrieben werden: {e}")
        tmp.unlink(missing_ok=True)


# ─────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────

def main():
    print("[Step 10 – Etsy Listing] wird gestartet...")

    # ── API-Key-Check: Fehlt der Key → Schritt überspringen ──────────────────
    if not ETSY_API_KEY:
        print()
        print("ℹ️  ETSY_API_KEY nicht gesetzt → Etsy-Schritt wird übersprungen.")
        print("   Sobald du deinen API-Key hast, setze diese Umgebungsvariablen:")
        print("     ETSY_API_KEY       = <Keystring aus Etsy Developer Console>")
        print("     ETSY_ACCESS_TOKEN  = <OAuth 2.0 Access Token>")
        print("     ETSY_SHOP_ID       = <Deine numerische Shop-ID>")
        print("   Alternativ: etsy_shop_id in config.yaml eintragen.")
        print()
        sys.exit(0)   # Kein Fehler – nächster Step läuft normal weiter

    if not ETSY_ACCESS_TOKEN:
        print()
        print("⚠️  ETSY_ACCESS_TOKEN nicht gesetzt → Etsy-Schritt wird übersprungen.")
        print("   ETSY_API_KEY ist vorhanden, aber für Write-Operationen wird")
        print("   zusätzlich ein OAuth 2.0 Access Token benötigt.")
        print("   Setze: ETSY_ACCESS_TOKEN = <OAuth 2.0 Access Token>")
        print()
        sys.exit(0)

    if not ETSY_SHOP_ID:
        print()
        print("⚠️  ETSY_SHOP_ID weder als Env-Var noch in config.yaml gefunden.")
        print("   Bitte setzen: ETSY_SHOP_ID = <Deine numerische Shop-ID>")
        print("   oder: etsy_shop_id in config.yaml eintragen.")
        print()
        sys.exit(0)

    # ── Zieldatum & Tagesordner ───────────────────────────────────────────────
    target_date = cfg["TARGET_DATE"]
    print(f"📅 Zieldatum: {target_date.strftime(DATE_FORMAT)}")

    year       = target_date.strftime("%Y")
    month_name = target_date.strftime("%B")
    date_str   = target_date.strftime(DATE_FORMAT)
    day_folder = Path(IMAGES_PATH) / year / f"{year} {month_name}" / date_str

    if not day_folder.exists():
        print(f"❌ Tagesordner nicht gefunden: {day_folder}")
        print("   Bitte zuerst Step 7 oder 9 ausführen.")
        sys.exit(1)

    # ── listings.csv laden ────────────────────────────────────────────────────
    csv_path = day_folder / "listings.csv"
    csv_rows = load_listings_csv(csv_path)

    if not csv_rows:
        print(f"❌ listings.csv nicht gefunden oder leer: {csv_path}")
        print("   Bitte zuerst Step 2 ausführen.")
        sys.exit(1)

    print(f"📋 listings.csv geladen: {len(csv_rows)} Zeile(n)")

    # ── Listings vorbereiten ──────────────────────────────────────────────────
    jobs = []
    for row in csv_rows:
        title = row.get("etsy_title", "").strip()
        if not title:
            print(f"   ⚠️  Zeile ohne etsy_title übersprungen: {row}")
            continue
        desc = build_description(row)
        tags = build_tags(row)
        jobs.append({"title": title, "description": desc, "tags": tags, "row": row})

    if not jobs:
        print("❌ Keine verwertbaren Zeilen in listings.csv gefunden.")
        sys.exit(1)

    print(f"\n🛍️  {len(jobs)} Listing(s) werden verarbeitet:")
    for j in jobs:
        tag_preview = ", ".join(j["tags"][:5])
        if len(j["tags"]) > 5:
            tag_preview += "..."
        print(f"   • {j['title']} | Tags: {tag_preview}")

    print(f"\n💶 Preis: {ETSY_PRICE} {ETSY_CURRENCY}  |  Zustand: {ETSY_STATE}")
    print(f"🏷️  Taxonomie-ID: {ETSY_TAXONOMY_ID}")

    # ── DRY-RUN ───────────────────────────────────────────────────────────────
    if DRYRUN:
        print("\n🧪 DRY-RUN – keine echten Etsy-Listings werden erstellt.")
        for j in jobs:
            print(f"\n   📦 {j['title']}")
            print(f"   Beschreibung: {j['description'][:80].replace(chr(10), ' ')}...")
            print(f"   Tags ({len(j['tags'])}): {', '.join(j['tags'])}")
        print(f"\n{'='*52}")
        print("🧪 DRY-RUN abgeschlossen.")
        return

    # ── Etsy-Listings erstellen ───────────────────────────────────────────────
    etsy_tracker   = load_etsy_tracker()
    listed         = []
    failed         = []

    for j in jobs:
        print(f"\n{'─'*52}")
        folder_name = normalize_name(j["title"])
        tracker_key = f"{date_str}|{folder_name}"

        # Idempotency Guard – bereits gelistet?
        if tracker_key in etsy_tracker:
            existing = etsy_tracker[tracker_key]
            print(f"⏭️  Bereits gelistet: {j['title']}")
            print(f"   🔗 {existing.get('url', '?')}")
            listed.append({
                "folder": folder_name,
                "listing_id": existing["listing_id"],
                "url": existing["url"],
            })
            continue

        # Duplikat-Prüfung: Titel ggf. mit Präfix versehen
        final_title = check_and_fix_duplicate_title(j["title"], j["row"])

        print(f"📤 Erstelle Listing: {final_title}")
        print(f"   Tags: {', '.join(j['tags'][:5])}{'...' if len(j['tags']) > 5 else ''}")

        result = create_etsy_listing(final_title, j["description"], j["tags"])

        if result:
            listing_id = result.get("listing_id") or result.get("id")
            url = f"https://www.etsy.com/listing/{listing_id}"
            print(f"   ✅ Erstellt! Listing-ID: {listing_id}")
            print(f"   🔗 {url}")
            listed.append({"folder": folder_name, "listing_id": listing_id, "url": url})
            # Sofort persistieren (mit möglicherweise geändertem Titel)
            etsy_tracker[tracker_key] = {
                "listing_id": listing_id,
                "url": url,
                "title": final_title,  # Titel kann "Neu " / "New " prefix haben
                "original_title": j["title"],
                "date": date_str,
                "created_at": datetime.now().isoformat(),
            }
            save_etsy_tracker(etsy_tracker)
        else:
            failed.append(j["title"])

        # Kurze Pause zwischen API-Calls (Rate-Limit-Schutz)
        time.sleep(0.5)

    # ── Zusammenfassung ───────────────────────────────────────────────────────
    print(f"\n{'='*52}")
    print(f"🎯 Step 10 abgeschlossen: {len(listed)} gelistet, {len(failed)} fehlgeschlagen.")
    for item in listed:
        print(f"   ✅ {item['url']}")
    for title in failed:
        print(f"   ❌ {title}")
    print(f"{'='*52}")

    if not listed:
        sys.exit(1)

    # ── Etsy-URLs in listings.csv eintragen ───────────────────────────────────
    print("\n📋 Trage Etsy-URLs in listings.csv ein...")
    write_etsy_urls_to_csv(csv_path, csv_rows, listed)

    # ── Status in pending.json aktualisieren ──────────────────────────────────
    if not PENDING_FILE.exists():
        return

    try:
        with PENDING_FILE.open("r", encoding="utf-8") as f:
            pending = json.load(f)

        listed_map     = {item["folder"]: item for item in listed}
        status_updated = False

        for entry in pending:
            if entry.get("status") in (YOUTUBE_STATUS, "All Done", "Video Done"):
                folder_name = normalize_name(
                    Path(entry.get("folder", "")).name or entry.get("title", "")
                )
                if folder_name in listed_map:
                    item = listed_map[folder_name]
                    entry["status"]     = ETSY_STATUS
                    entry["etsy_url"]   = item["url"]
                    entry["listing_id"] = item["listing_id"]
                    status_updated = True

        if status_updated:
            atomic_write_json(PENDING_FILE, pending)
            print(f"\n💾 Status auf '{ETSY_STATUS}' gesetzt.")
        else:
            print(f"\nℹ️  Keine passenden Einträge für Statusänderung in pending.json gefunden.")

    except Exception as e:
        print(f"⚠️  Konnte pending.json nicht aktualisieren: {e}")


if __name__ == "__main__":
    main()
