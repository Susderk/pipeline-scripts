#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Step_05_rename_images.py

- Benennt Bilder im Marketing-Ordner nach dem Schema: <Ordnername> wallpaper NNN.ext
- Aktualisiert die lokalen Pfade und Dateinamen in prompts_pending.json
  (entry["images"] Liste wird mit neuen Pfaden und Dateinamen aktualisiert)
- Setzt Status auf "Renamed"
- **NEU (2026-04-24):** Lädt die umbenannten Leonardo-Originale auf GitHub in das
  Repo `Susderk/original-uploads` (Konfig-Key `github_repo_originals`) hoch —
  blockierend, VOR der pending-Disk-Persistierung. Struktur im Repo:
  `{YYYY-MM-DD}/{FolderName}/{filename}`. Persistiert pro Item in
  master-listings.json das Feld `github_original_urls = [{file, url, sha}, ...]`
  (analog `github_mockup_urls`). Hebel für Leonardo-URL-Ablauf-Entkopplung:
  sobald das Bild uploaded ist, hängt die Pipeline nicht mehr an den befristeten
  Leonardo-Download-URLs.
- Schreibt neue Dateinamen und Pfade zurück in pending.json
- Synct den Ordnernamen (Basename) aus pending-Eintrag in master-listings.json
  (per id, Feld "folder") — Single Source of Truth aktuell halten.
- Schreibt canva-listing.csv im Tagesordner (eine Spalte "folder", Basename pro
  Marketing-Ordner) als Referenz für die Canva-Mockup-Erstellung in Step_06.
"""

import os
import sys
import csv
import json
import base64
import time
from pathlib import Path
from config_loader import (
    load_config,
    load_master_listings,
    save_master_listings,
    find_master_item,
    atomic_write_json,
)

try:
    import requests as _requests
    _REQUESTS_OK = True
except ImportError:
    _REQUESTS_OK = False


# Hinweis: `atomic_write_json` wird aus `config_loader` importiert (oben).
# Gehärtete Variante mit Retry/Backoff gegen Windows-Dateilocks — keine lokale
# Kopie mehr. Migration 2026-04-20 (session-log-2026-04-20-d.md).


def rename_images_in_folder(folder_path: str, dryrun: bool = False) -> list:
    """
    Benennt Bilder um und gibt eine Liste von Dicts zurück:
    [{"old_path": ..., "new_path": ..., "new_filename": ...}, ...]
    """
    print(f"📂 Starte Bearbeitung des Ordners: {folder_path}")
    if not os.path.isdir(folder_path):
        print(f"⚠️ Ordner existiert nicht: {folder_path}")
        return []

    extensions = ('.jpg', '.jpeg', '.png', '.gif', '.bmp', '.tiff', '.webp')
    all_files = os.listdir(folder_path)
    files = [f for f in all_files if f.lower().endswith(extensions)]

    if not files:
        print(f"🔎 Keine Bilder im Ordner: {folder_path}. Gefundene Dateien: {all_files}")
        return []

    renamed_list = []
    counter = 1
    folder_basename = os.path.basename(folder_path)

    for filename in sorted(files):
        old_path = os.path.join(folder_path, filename)
        ext = os.path.splitext(filename)[1].lower()
        new_filename = f"{folder_basename} wallpaper {counter:03d}{ext}"
        new_path = os.path.join(folder_path, new_filename)

        if filename != new_filename:
            if dryrun:
                print(f"🧪 DRY-RUN: Würde umbenennen: {filename} → {new_filename}")
            else:
                try:
                    os.rename(old_path, new_path)
                except Exception as e:
                    print(f"❌ Konnte {filename} nicht umbenennen: {e}")
                    counter += 1
                    continue

        renamed_list.append({
            "old_path": old_path,
            "new_path": new_path,
            "new_filename": new_filename
        })
        counter += 1

    print(f"✅ Bearbeitet: {folder_path}. {'Simuliert' if dryrun else 'Umbenannt'}: {len(renamed_list)}")
    return renamed_list


def update_images_in_entry(entry: dict, renamed_list: list) -> None:
    """
    Aktualisiert die 'images' Liste im pending-Eintrag mit den neuen Pfaden.
    Matcht alte Pfade mit neuen Pfaden anhand der Sortierreihenfolge.
    """
    images = entry.get("images", [])
    if not images:
        return

    # Sortiere beide Listen nach altem Pfad / Dateiname für konsistentes Matching
    images_sorted = sorted(images, key=lambda x: x.get("local_path", ""))
    renamed_sorted = sorted(renamed_list, key=lambda x: x.get("old_path", ""))

    # Gleiche Anzahl? Dann 1:1 matchen
    if len(images_sorted) == len(renamed_sorted):
        for img, renamed in zip(images_sorted, renamed_sorted):
            img["local_path"] = renamed["new_path"]
            img["filename"] = renamed["new_filename"]
    else:
        # Fallback: anhand old_path matchen
        old_to_new = {r["old_path"]: r for r in renamed_list}
        for img in images:
            old = img.get("local_path", "")
            if old in old_to_new:
                img["local_path"] = old_to_new[old]["new_path"]
                img["filename"] = old_to_new[old]["new_filename"]


# === GITHUB UPLOAD (Leonardo-Originale → Susderk/original-uploads) =====================
# Neu seit 2026-04-24. Hebel für Leonardo-URL-Ablauf-Entkopplung: sobald die
# umbenannten Originale im Repo liegen, ist die Pipeline unabhängig von den
# befristeten Leonardo-Download-URLs.
#
# Design-Entscheidungen (siehe session-log-2026-04-24-c.md):
# - Repo-interne Struktur: {YYYY-MM-DD}/{FolderName}/{filename}. Kein zusätzlicher
#   `originals/`-Unterordner — das Repo `Susderk/original-uploads` ist dediziert
#   für diesen Zweck, ein Typ-Namespace im Pfad wäre redundant.
# - Dry-Run-Hebel: `cfg["get_script_flags"]("images")["dry_run"]`. Wenn Leonardo
#   selbst simuliert wurde (staging/dev), gibt es keine echten Bilder auf Disk —
#   Upload wird symmetrisch simuliert. Kein neuer Config-Key nötig.
# - Partial-Success-Gate (pro Entry, analog Step_07 vom 2026-04-20):
#     any_failed  (mindestens ein Bild failed nach 3 Retries) → sys.exit(1)
#     0/N Entries erfolgreich (bei erwartet > 0)              → sys.exit(1)
#     0 < k < N                                               → sichtbar loggen,
#                                                               `github_original_urls`
#                                                               nur für erfolgreiche
#                                                               Entries, Exit 0.
# - Retry: 3 Versuche mit Exponential Backoff (0.5/1/2s) — symmetrisch zum
#   `atomic_write_json`-Pattern. Bei persistenten Netzwerk-/Auth-Fehlern nach
#   allen Retries: sys.exit(1). KEIN silent-swallow (Lektion Aufgabe 47 +
#   repost_log.json-Truncation-Serie 2026-04-19).


_UPLOAD_RETRY_BACKOFFS = (0.5, 1.0, 2.0)


def _github_upload_original(
    gh_path: str,
    content_bytes: bytes,
    commit_msg: str,
    repo: str,
    branch: str,
    token: str,
) -> tuple:
    """
    Lädt eine Datei auf GitHub hoch (Create oder Update).
    3 Retry-Versuche mit Exponential Backoff (0.5/1/2s) gegen transiente
    Netzwerk-/API-Fehler. Bei Rückgabe (None, None) → Aufrufer entscheidet
    (Partial-Success-Gate oder sys.exit(1)).

    Rückgabe: (raw_url, sha) oder (None, None).
    """
    if not _REQUESTS_OK:
        print("   ⚠️  'requests' nicht installiert (pip install requests) – Upload nicht möglich.")
        return None, None

    api_url = f"https://api.github.com/repos/{repo}/contents/{gh_path}"
    headers = {
        "Authorization": f"Bearer {token}",
        "Accept":        "application/vnd.github+json",
    }

    for attempt_idx, backoff in enumerate(_UPLOAD_RETRY_BACKOFFS):
        # SHA existierender Datei ermitteln (für Idempotenz beim Re-Run)
        existing_sha = None
        try:
            r = _requests.get(api_url, headers=headers, timeout=30)
            if r.status_code == 200:
                existing_sha = r.json().get("sha")
        except Exception:
            # Transient → wird im PUT erneut geprüft
            pass

        payload = {
            "message": commit_msg,
            "content": base64.b64encode(content_bytes).decode("utf-8"),
            "branch":  branch,
        }
        if existing_sha:
            payload["sha"] = existing_sha

        try:
            resp = _requests.put(api_url, headers=headers, json=payload, timeout=60)
        except Exception as e:
            if attempt_idx < len(_UPLOAD_RETRY_BACKOFFS) - 1:
                print(f"   ↻ Netzwerkfehler ({e}) – retry in {backoff}s ({attempt_idx + 2}/{len(_UPLOAD_RETRY_BACKOFFS)}).")
                time.sleep(backoff)
                continue
            print(f"   ❌ Netzwerkfehler nach {len(_UPLOAD_RETRY_BACKOFFS)} Versuchen: {e}")
            return None, None

        if resp.status_code in (200, 201):
            sha     = resp.json().get("content", {}).get("sha", "")
            raw_url = f"https://raw.githubusercontent.com/{repo}/{branch}/{gh_path}"
            return raw_url, sha

        # Fehlerdiagnose
        if resp.status_code == 401:
            # Auth-Fehler sind nicht transient → kein Retry
            print(f"   ❌ GitHub API 401 (Github_Token ungültig/abgelaufen): {resp.text[:200]}")
            return None, None
        if resp.status_code == 403:
            # Rate-Limit oder Permission — potentiell transient, retrien
            if attempt_idx < len(_UPLOAD_RETRY_BACKOFFS) - 1:
                print(f"   ↻ GitHub API 403 ({resp.text[:100]}) – retry in {backoff}s.")
                time.sleep(backoff)
                continue
            print(f"   ❌ GitHub API 403 nach {len(_UPLOAD_RETRY_BACKOFFS)} Versuchen: {resp.text[:200]}")
            return None, None
        if resp.status_code == 422 and existing_sha is None:
            # Datei existiert bereits — beim Retry wird existing_sha neu ermittelt
            if attempt_idx < len(_UPLOAD_RETRY_BACKOFFS) - 1:
                print(f"   ↻ GitHub API 422 (Datei existiert) – retry mit frischem SHA in {backoff}s.")
                time.sleep(backoff)
                continue
            print(f"   ❌ GitHub API 422 nach {len(_UPLOAD_RETRY_BACKOFFS)} Versuchen: {resp.text[:200]}")
            return None, None

        # Andere 4xx/5xx → ebenfalls retrien (kann transient sein)
        if attempt_idx < len(_UPLOAD_RETRY_BACKOFFS) - 1:
            print(f"   ↻ GitHub API {resp.status_code} – retry in {backoff}s.")
            time.sleep(backoff)
            continue
        print(f"   ❌ GitHub API {resp.status_code} nach {len(_UPLOAD_RETRY_BACKOFFS)} Versuchen: {resp.text[:200]}")
        return None, None

    return None, None


def phase_upload_originals(entries: list, cfg: dict, dryrun: bool) -> tuple:
    """
    Lädt Leonardo-Originale der in diesem Lauf umbenannten Entries auf GitHub hoch.

    Zählt pro Entry: vollständig erfolgreicher Upload aller Bilder = Entry-Erfolg.
    Schreibt Ergebnisse pro erfolgreichem Entry in master-listings.json unter
    `item["github_original_urls"] = [{"file", "url", "sha"}, ...]`.

    Rückgabe: (entries_total, entries_ok, entries_failed, any_file_failed)
      - entries_total: Anzahl Entries mit Status 'Renamed' (Upload-Kandidaten)
      - entries_ok:    komplett erfolgreich (alle Bilder hochgeladen)
      - entries_failed: mindestens ein Bild failed nach Retry
      - any_file_failed: bool — Hard-Fail-Indikator

    Aufrufer entscheidet über sys.exit(1) anhand der Aggregat-Zahlen.
    """
    if dryrun:
        print("🧪 DRY-RUN: Originale-Upload wird simuliert (dry_run.images=true).")
        # Simuliere Erfolg für alle renamed entries
        renamed_status = cfg["STATUSES"].get("renamed", "Renamed")
        cand = [e for e in entries if e.get("status") == renamed_status]
        for entry in cand:
            n = len(entry.get("images", []))
            print(f"   🧪 Würde {n} Bild(er) für {entry.get('id', '?')} hochladen.")
        return (len(cand), len(cand), 0, False)

    token  = os.environ.get("Github_Token", "").strip()
    if not token:
        print("❌ Github_Token nicht gesetzt → Originale-Upload nicht möglich.")
        print("   Setze Umgebungsvariable Github_Token (siehe CREDENTIALS.md).")
        sys.exit(1)

    if not _REQUESTS_OK:
        print("❌ 'requests' fehlt (pip install requests) → Originale-Upload nicht möglich.")
        sys.exit(1)

    config = cfg["config"]
    repo    = str(config.get("github_repo_originals", "Susderk/original-uploads"))
    branch  = str(config.get("github_branch",         "main"))
    date_fmt = cfg["DATE_FORMAT"]

    renamed_status = cfg["STATUSES"].get("renamed", "Renamed")
    candidates = [e for e in entries if e.get("status") == renamed_status]

    entries_total = len(candidates)
    entries_ok = 0
    entries_failed = 0
    any_file_failed = False

    if entries_total == 0:
        print("ℹ️  Keine Einträge mit Status 'Renamed' – nichts hochzuladen.")
        return (0, 0, 0, False)

    # Sammelt Master-Updates pro day_folder
    # { day_folder_path: [(entry_id, [{file,url,sha}, ...]), ...] }
    master_updates: dict[Path, list] = {}

    print(f"\n🐙 Originale-Upload zu {repo} (Branch {branch})...")

    for entry in candidates:
        entry_id    = entry.get("id", "")
        folder_path = entry.get("folder", "")
        day_folder  = entry.get("day_folder", "")
        folder_name = Path(folder_path).name if folder_path else entry_id

        if not folder_path or not day_folder or not entry_id:
            print(f"   ⚠️  Eintrag unvollständig (id/folder/day_folder fehlt) – übersprungen: {entry_id or '?'}")
            entries_failed += 1
            any_file_failed = True
            continue

        # Tagesdatum aus day_folder-Basename ableiten (z. B. '2026-04-24').
        date_str = Path(day_folder).name

        images = entry.get("images", [])
        if not images:
            print(f"   ⚠️  Keine Bilder in entry.images für {folder_name} – übersprungen.")
            entries_failed += 1
            any_file_failed = True
            continue

        print(f"\n📂 {folder_name} ({len(images)} Bild(er))")

        uploaded_for_entry: list[dict] = []
        entry_had_failure = False

        for img in images:
            local_path = img.get("local_path", "")
            if not local_path:
                print(f"   ⚠️  Bild ohne local_path übersprungen (Entry {entry_id}).")
                entry_had_failure = True
                break
            img_file = Path(local_path)
            if not img_file.exists():
                print(f"   ⚠️  Datei nicht gefunden: {img_file} – Entry failed.")
                entry_had_failure = True
                break

            gh_path    = f"{date_str}/{folder_name}/{img_file.name}"
            commit_msg = f"Original: {date_str}/{folder_name}/{img_file.name}"
            print(f"   ⬆️  {img_file.name}")

            raw_url, sha = _github_upload_original(
                gh_path, img_file.read_bytes(), commit_msg,
                repo=repo, branch=branch, token=token,
            )
            if raw_url:
                uploaded_for_entry.append({"file": img_file.name, "url": raw_url, "sha": sha})
                print(f"   ✅ {raw_url}")
            else:
                entry_had_failure = True
                break  # Abbruch für dieses Entry — Partial-Entry-Upload lohnt sich nicht

            time.sleep(0.3)  # sanfter Rate-Limit-Schutz (wie in Step_09)

        if entry_had_failure:
            entries_failed += 1
            any_file_failed = True
            print(f"   ❌ Entry {folder_name}: Upload fehlgeschlagen – github_original_urls wird NICHT persistiert.")
            continue

        # Entry komplett erfolgreich → für Master-Update vormerken
        entries_ok += 1
        master_updates.setdefault(Path(day_folder), []).append((entry_id, uploaded_for_entry))
        print(f"   ✅ Entry {folder_name}: {len(uploaded_for_entry)} Bild(er) hochgeladen.")

    # === master-listings.json Updates persistieren (nur erfolgreiche Entries) ===
    for day_path, id_urls_pairs in master_updates.items():
        try:
            master = load_master_listings(day_path)
            master_updated = False
            n_missing = 0
            for entry_id, urls in id_urls_pairs:
                item = find_master_item(master, entry_id)
                if item is None:
                    n_missing += 1
                    print(f"   ⚠️  master-Item id '{entry_id}' nicht gefunden in "
                          f"{day_path.name} – github_original_urls nicht persistiert.")
                    continue
                item["github_original_urls"] = urls
                master_updated = True
            if master_updated:
                save_master_listings(day_path, master)
                print(f"   🗂️  master-listings.json ({day_path.name}) aktualisiert: "
                      f"{len(id_urls_pairs) - n_missing} Entry(s) mit github_original_urls.")
            if n_missing:
                print(f"   ⚠️  {n_missing} fehlende master-id(s) in {day_path.name}.")
        except Exception as e:
            print(f"   ❌ KRITISCHER FEHLER beim Update von master-listings.json ({day_path}): {e}")
            raise  # Laut-Fail, kein silent-swallow

    # === Aggregat-Report ===
    print(f"\n{'─'*44}")
    print(f"🐙 Originale-Upload: {entries_ok}/{entries_total} Entry(s) erfolgreich"
          f", {entries_failed} fehlgeschlagen.")
    return (entries_total, entries_ok, entries_failed, any_file_failed)


def main():
    print("[Step 5 - Rename] wird gestartet...")
    cfg = load_config()

    flags_rename = cfg["get_script_flags"]("rename")
    RUN_ENABLED  = bool(flags_rename["run"])
    DRYRUN       = bool(flags_rename["dry_run"])
    STAGING_ISOLATION = cfg["STAGING_ISOLATION"]
    STAGING_IMAGES_PATH = cfg["IMAGES_PATH"]
    remap_pending_entries_to_staging = cfg["remap_pending_entries_to_staging"]

    if not RUN_ENABLED:
        print("ℹ️ [rename] ist in run_scripts deaktiviert – nichts zu tun.")
        sys.exit(0)

    prompts_pending_path = cfg['PENDING_FILE']
    all_done_status   = cfg["STATUSES"].get("all_done", "All Done")
    renamed_status    = cfg["STATUSES"].get("renamed", "Renamed")
    simulation_status = cfg["STATUSES"].get("simulation", "Simulation")

    if not os.path.exists(prompts_pending_path):
        print(f"❌ Datei nicht gefunden: {prompts_pending_path}")
        sys.exit(1)

    try:
        with open(prompts_pending_path, 'r', encoding='utf-8') as f:
            entries = json.load(f)
        if not isinstance(entries, list):
            print("❌ prompts_pending.json hat kein Listenformat.")
            sys.exit(1)
    except Exception:
        print("❌ prompts_pending.json beschädigt.")
        sys.exit(1)

    # === STAGING-ISOLATION: Remap day_folder zu Staging-Temp-Ordner ===
    if STAGING_ISOLATION:
        remap_pending_entries_to_staging(entries, STAGING_IMAGES_PATH)
        print(f"🎭 Pending-Einträge zu Staging-Ordner remapped.")

    total_renamed = 0
    updated = False
    # day_folder → Liste von (id, folder_basename) für Master-Sync und canva-listing.csv
    per_day_folder: dict[str, list[tuple[str, str]]] = {}

    for entry in entries:
        status = entry.get("status")

        if DRYRUN:
            if status != simulation_status:
                print(f"🚫 Eintrag wird übersprungen (Status ist '{status}')")
                continue
            folder_path = entry.get("folder")
            if folder_path:
                renamed = rename_images_in_folder(folder_path, dryrun=True)
                total_renamed += len(renamed)
            continue

        if status != all_done_status:
            print(f"🚫 Eintrag wird übersprungen (Status ist '{status}')")
            continue

        folder_path = entry.get("folder")
        if not folder_path:
            print(f"⚠️ Kein Ordnerpfad im Eintrag vorhanden: {entry}")
            continue

        renamed_list = rename_images_in_folder(folder_path, dryrun=False)
        if renamed_list:
            update_images_in_entry(entry, renamed_list)
            entry["status"] = renamed_status
            updated = True
            total_renamed += len(renamed_list)
            # Für Master-Sync und canva-listing.csv merken
            day_key = entry.get("day_folder", "")
            entry_id = entry.get("id", "")
            folder_basename = os.path.basename(folder_path)
            if day_key and entry_id and folder_basename:
                per_day_folder.setdefault(day_key, []).append((entry_id, folder_basename))
        else:
            print(f"⚠️ Keine Bilder umbenannt für: {folder_path}")

    if DRYRUN:
        print(f"🧪 DRY-RUN: {total_renamed} Umbenennungen simuliert. Keine Änderungen gespeichert.")
    elif updated:
        # === NEUE PHASE: Leonardo-Originale zu Susderk/original-uploads hochladen ===
        # Läuft VOR atomic_write_json(PENDING_FILE) — bei Hard-Fail (exit(1)) bleibt
        # die pending.json auf dem Pre-Rename-Stand (Rename-Operationen auf Disk
        # sind idempotent, Re-Run von Step_05 erkennt „bereits umbenannt" und
        # macht nichts).
        #
        # Dry-Run-Hebel: dry_run.images — wenn Leonardo simuliert wurde,
        # existieren keine echten Bilder, daher Upload ebenfalls simulieren.
        flags_images = cfg["get_script_flags"]("images")
        upload_dryrun = bool(flags_images["dry_run"])
        entries_total, entries_ok, entries_failed, any_file_failed = \
            phase_upload_originals(entries, cfg, dryrun=upload_dryrun)

        # Partial-Success-Gate (analog Step_07 vom 2026-04-20):
        if not upload_dryrun and entries_total > 0:
            if any_file_failed and entries_ok == 0:
                print("❌ Originale-Upload: 0 erfolgreiche Entries – Pipeline bricht ab.")
                sys.exit(1)
            if any_file_failed:
                # Partial-Success: sichtbar loggen, aber weiterlaufen
                print(f"⚠️  Partial-Success: {entries_ok}/{entries_total} Entries hochgeladen, "
                      f"{entries_failed} fehlgeschlagen. Pipeline läuft weiter "
                      f"(fehlgeschlagene Entries haben kein github_original_urls).")

        # pending.json erst NACH erfolgreichem Upload-Gate persistieren
        try:
            atomic_write_json(Path(prompts_pending_path), entries)
            print(f"✅ {prompts_pending_path} aktualisiert (Status auf '{renamed_status}', Pfade aktualisiert).")
        except Exception as e:
            print(f"❌ Fehler beim Speichern von {prompts_pending_path}: {e}")

    # === MASTER-LISTINGS.JSON SYNC + canva-listing.csv ===
    # Pro Tagesordner: Ordnernamen in master items[].folder setzen und canva-listing.csv
    # schreiben. Nur im Produktivlauf — DRY-RUN schreibt keine Dateien.
    if not DRYRUN and per_day_folder:
        for day_key, id_folder_pairs in per_day_folder.items():
            day_path = Path(day_key)
            try:
                master = load_master_listings(day_path)
                master_updated = False
                for entry_id, folder_basename in id_folder_pairs:
                    item = find_master_item(master, entry_id)
                    if item is None:
                        print(f"⚠️  master-listings.json: id '{entry_id}' nicht gefunden – Sync übersprungen.")
                        continue
                    if item.get("folder") != folder_basename:
                        item["folder"] = folder_basename
                        master_updated = True
                if master_updated:
                    save_master_listings(day_path, master)
                    print(f"🗂️  master-listings.json folder-Feld synchronisiert "
                          f"({len(id_folder_pairs)} Eintrag/Einträge): {day_path / 'master-listings.json'}")
            except Exception as e:
                print(f"❌ master-listings.json Sync fehlgeschlagen für {day_path}: {e}")

            # canva-listing.csv schreiben (eine Spalte "folder", Basename)
            canva_path = day_path / "canva-listing.csv"
            try:
                canva_path.parent.mkdir(parents=True, exist_ok=True)
                with canva_path.open("w", encoding="utf-8-sig", newline="") as f:
                    writer = csv.writer(f, delimiter=";", quoting=csv.QUOTE_ALL)
                    writer.writerow(["folder"])
                    for _, folder_basename in id_folder_pairs:
                        writer.writerow([folder_basename])
                print(f"🎨 canva-listing.csv geschrieben ({len(id_folder_pairs)} Zeilen): {canva_path}")
            except Exception as e:
                print(f"❌ Schreiben von {canva_path} fehlgeschlagen: {e}")

    print(f"=== Insgesamt umbenannte Bilder: {total_renamed} ===")
    print("✅ Step 5 abgeschlossen (inkl. Originale-Upload zu GitHub).")

if __name__ == "__main__":
    main()