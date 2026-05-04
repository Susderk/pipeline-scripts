#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Step_07a_Generate_Music.py

Generiert Hintergrundmusik für Videos mittels Meta MusicGen (Hugging Face transformers).
Läuft VOR Step_07 (Video-Erstellung).

Workflow:
1. Liest Tagesordner + Mockups-Unterordner (wie Step_07)
2. Berechnet Video-Dauer aus PNG-Anzahl
3. Ableitet Musik-Prompt aus prompts_pending.json (oder Fallback)
4. Generiert WAV-Datei mittels MusicGen lokal
5. Speichert als <FolderName>_music.wav neben dem Video

Config-Parameter (config.yaml):
  music_model: "facebook/musicgen-small"  # small (~430MB), medium (~1.5GB), large (~3.3GB)
  music_style_prefix: "cinematic, atmospheric, ambient"
  music_fallback_prompt: "cinematic, atmospheric, ambient background music"
  video_duration_per_image: 1.3  # Sekunden pro Bild
  video_crossfade_duration: 0.3  # Sekunden Überblendung

Hinweis: Beim ersten Lauf wird das Modell automatisch von Hugging Face
heruntergeladen (~430 MB für small). Speicherort: ~/.cache/huggingface/

Abhängigkeiten:
  pip install transformers torch torchaudio
  (bei GPU-Nutzung: torch mit CUDA-Support empfohlen)
"""

import sys
import os
import json
import random
from pathlib import Path
from datetime import datetime

from config_loader import (
    load_config,
    load_master_listings,
    save_master_listings,
    find_master_item,
    atomic_write_json,
)

# === CONFIG ===
cfg = load_config()
config = cfg["config"]

PENDING_FILE = Path(cfg["PENDING_FILE"])
IMAGES_PATH  = Path(cfg["IMAGES_PATH"])
DATE_FORMAT  = cfg["DATE_FORMAT"]
STATUSES     = cfg["STATUSES"]
STAGING_ISOLATION = cfg["STAGING_ISOLATION"]
remap_pending_entries_to_staging = cfg["remap_pending_entries_to_staging"]

flags  = cfg["get_script_flags"]("music")
DRYRUN = bool(flags.get("dry_run", False))

MUSIC_MODEL          = config.get("music_model", "facebook/musicgen-small")
MUSIC_STYLE_PREFIX   = config.get("music_style_prefix", "cinematic, atmospheric, ambient")
MUSIC_FALLBACK_PROMPT = config.get("music_fallback_prompt", "cinematic, atmospheric, ambient background music")

DURATION_PER_IMAGE = float(config.get("video_duration_per_image", 1.3))
CROSSFADE_DURATION = float(config.get("video_crossfade_duration", 0.3))


# === HELPERS ===
# Hinweis: `atomic_write_json` wird aus `config_loader` importiert (oben).
# Gehärtete Variante mit Retry/Backoff gegen Windows-Dateilocks — keine lokale
# Kopie mehr. Migration 2026-04-20 (session-log-2026-04-20-d.md).


def load_pending_json() -> list:
    """Lädt prompts_pending.json. Gibt [] zurück bei Fehler/nicht vorhanden."""
    if not PENDING_FILE.exists():
        return []
    try:
        with PENDING_FILE.open("r", encoding="utf-8") as f:
            data = json.load(f)
            if isinstance(data, list):
                # === STAGING-ISOLATION: Remap day_folder zu Staging-Temp-Ordner ===
                # Hinweis: Option B (Remap + IMAGES_PATH-basiert)
                # Der Remap ändert entry["day_folder"], aber main() rekonstruiert day_folder
                # NEU aus IMAGES_PATH / year / month / date_str. Das ist bewusst:
                # - IMAGES_PATH zeigt bereits auf Staging (config_loader.py)
                # - Daher funktioniert die Rekonstruktion ohne entry["day_folder"] zu nutzen
                if STAGING_ISOLATION:
                    remap_pending_entries_to_staging(data, IMAGES_PATH)
                return data
            return []
    except Exception as e:
        print(f"   ⚠️ Fehler beim Laden von pending.json: {e}")
        return []


def get_music_prompt(folder_name: str) -> str:
    """
    Versucht, den Musik-Prompt aus prompts_pending.json abzuleiten.
    Matching-Logik: Ordnername mit folder-Feld vergleichen.
    Fallback: MUSIC_FALLBACK_PROMPT verwenden.
    """
    pending = load_pending_json()
    if not pending:
        return MUSIC_FALLBACK_PROMPT

    # Versuche Match: folder-Feld enthält den Ordnernamen
    for entry in pending:
        folder_path = entry.get("folder", "")
        if folder_path:
            folder_name_from_entry = Path(folder_path).name
            if folder_name_from_entry == folder_name:
                # Prompt-Feld suchen (Annahme: "prompt" oder "image_prompt")
                image_prompt = entry.get("prompt") or entry.get("image_prompt") or ""
                if image_prompt:
                    return f"{MUSIC_STYLE_PREFIX}, {image_prompt}"
                break

    return MUSIC_FALLBACK_PROMPT


def generate_music_musicgen(prompt: str, duration: float, dryrun: bool = False) -> tuple:
    """
    Generiert Musik mittels MusicGen (Hugging Face transformers).
    Gibt Tuple (audio_tensor, sample_rate) zurück.
    Wirft Exception bei Fehler.
    """
    if dryrun:
        # Dummy für Dry-Run: (Tensor, sample_rate)
        import torch
        return (torch.zeros(1, int(16000 * duration)), 16000)

    try:
        from transformers import AutoProcessor, MusicgenForConditionalGeneration
        import torch
        import torchaudio
    except ImportError as e:
        print(f"   ❌ transformers oder PyTorch nicht installiert:")
        print(f"      {e}")
        print(f"   Bitte installieren:")
        print(f"      pip install transformers torch torchaudio")
        raise

    try:
        # Gerät bestimmen (GPU oder CPU)
        device = "cuda" if torch.cuda.is_available() else "cpu"
        print(f"   🖥️  Gerät: {device.upper()}")

        # Modell und Processor laden (werden gecacht nach erstem Download)
        print(f"   📥 Lade Modell: {MUSIC_MODEL}")
        processor = AutoProcessor.from_pretrained(MUSIC_MODEL)
        model = MusicgenForConditionalGeneration.from_pretrained(MUSIC_MODEL)
        model = model.to(device)

        # Generierungsparameter berechnen
        # MusicGen erzeugt ~51.2 Token/Sekunde
        print(f"   ⏱️  Generierungsdauer: {duration:.1f}s")
        max_new_tokens = max(100, int(duration * 51.2))

        # Generiere Musik
        print(f"   🎵 Generiere Musik: {prompt}")
        inputs = processor(
            text=[prompt],
            padding=True,
            return_tensors="pt",
        ).to(device)

        audio_values = model.generate(**inputs, max_new_tokens=max_new_tokens)

        # Sample rate aus Modell-Config lesen
        sample_rate = model.config.audio_encoder.sampling_rate

        return audio_values[0].cpu(), sample_rate
    except Exception as e:
        print(f"   ❌ MusicGen Fehler: {e}")
        raise


def get_png_files(folder: Path) -> list:
    """Gibt alle PNG und JPG Dateien in einem Ordner zurück."""
    return sorted([f for f in folder.iterdir() if f.is_file() and f.suffix.lower() in {".png", ".jpg", ".jpeg"}])


# === MAIN ===
def main():
    print("[Step 07a - Musik generieren (MusicGen)] wird gestartet...")

    # Zieldatum bestimmen
    target_date = cfg["TARGET_DATE"]
    print(f"📅 Zieldatum: {target_date.strftime(DATE_FORMAT)}")

    # WICHTIG: IMAGES_PATH ist bereits auf Staging umgeleitet, wenn staging_isolation aktiv ist
    # (gemacht in config_loader.py bei Laden)
    year       = target_date.strftime("%Y")
    month_name = target_date.strftime("%B")
    date_str   = target_date.strftime(DATE_FORMAT)
    day_folder = IMAGES_PATH / year / f"{year} {month_name}" / date_str

    if not day_folder.exists():
        print(f"❌ Tagesordner nicht gefunden: {day_folder}")
        print(f"   IMAGES_PATH: {IMAGES_PATH}")
        print(f"   Staging-Isolation: {STAGING_ISOLATION}")
        print("   Bitte zuerst Step 03 (Marketing Ordner) ausführen.")
        sys.exit(1)

    # --- DRY-RUN (vor der Dateiprüfung, damit kein Abbruch bei fehlenden Ordnern) ---
    if DRYRUN:
        print("\n🧪 DRY-RUN – keine echte Musik-Generierung.")
        print("   (Dateiprüfung übersprungen – keine echten Mockup-Ordner nötig)")
        print(f"\n{'='*52}")
        print("🧪 DRY-RUN abgeschlossen.")
        return

    # Alle <FolderName>/Mockups/ Verzeichnisse im Tagesordner finden (nur im echten Modus nötig)
    subdirs = sorted([
        d / "Mockups"
        for d in day_folder.iterdir()
        if d.is_dir() and (d / "Mockups").exists()
    ])

    if not subdirs:
        print(f"❌ Keine <FolderName>/Mockups/ Unterordner in {day_folder} gefunden.")
        print("   Erwartet wird: Tagesordner/<FolderName>/Mockups/<Bild-Dateien (PNG/JPG/JPEG)>")
        print("   Bitte zuerst Step 05 (Bilder umbenennen) ausführen.")
        sys.exit(1)

    print(f"\n📁 {len(subdirs)} Folder-Unterordner gefunden:")
    for d in subdirs:
        imgs = get_png_files(d)
        n_imgs = len(imgs)
        if n_imgs > 0:
            duration = (DURATION_PER_IMAGE * n_imgs) - (CROSSFADE_DURATION * (n_imgs - 1))
            print(f"   • {d.parent.name}/Mockups: {n_imgs} Bild(er) → {duration:.1f}s Video")
        else:
            print(f"   • {d.parent.name}/Mockups: 0 Bild(er) (wird übersprungen)")

    music_created = []
    any_failed    = False

    for subdir in subdirs:
        print(f"\n{'─'*52}")
        print(f"🎵 Verarbeite Folder: {subdir.parent.name}")

        png_files = get_png_files(subdir)

        if len(png_files) == 0:
            print(f"   ⚠️  Keine Bild-Dateien (PNG/JPG/JPEG) – übersprungen.")
            continue

        # Video-Dauer berechnen
        n = len(png_files)
        video_duration = (DURATION_PER_IMAGE * n) - (CROSSFADE_DURATION * (n - 1))
        print(f"   ⏱️  Video-Dauer: {video_duration:.1f}s ({n} Bilder)")

        # Musik-Prompt ableiten
        folder_name = subdir.parent.name
        music_prompt = get_music_prompt(folder_name)
        print(f"   📝 Musik-Prompt: {music_prompt}")

        # Musik-Dateiname: <FolderName>_music.wav
        safe_name = "".join(c for c in folder_name if c.isalnum() or c in " _-").strip().replace(" ", "_")
        output_name = f"{safe_name}_music.wav"
        output_path = subdir / output_name

        # Musik generieren
        try:
            audio_tensor, sample_rate = generate_music_musicgen(music_prompt, video_duration, dryrun=False)

            # Speichern via scipy (kein torchcodec erforderlich)
            import numpy as np
            import scipy.io.wavfile
            audio_np = audio_tensor.numpy()  # Shape: [channels, samples]
            if audio_np.shape[0] == 1:
                audio_np = audio_np[0]       # Mono: [samples]
            else:
                audio_np = audio_np.T        # Stereo: [samples, channels]
            audio_int16 = (audio_np * 32767).astype(np.int16)
            scipy.io.wavfile.write(str(output_path), int(sample_rate), audio_int16)
            print(f"   ✅ Musik generiert: {output_name} ({video_duration:.1f}s)")
            music_created.append(str(output_path))
        except Exception as e:
            print(f"   ❌ Musik-Generierung fehlgeschlagen: {e}")
            any_failed = True
            # Fehler ist kritisch → Workflow abbrechhen
            sys.exit(1)

    print(f"\n{'='*52}")
    print(f"🎯 Step 07a abgeschlossen: {len(music_created)} Musik-Datei(en) generiert.")
    for m in music_created:
        print(f"   🎵 {Path(m).name}")
    print(f"{'='*52}")

    # Status in pending.json aktualisieren
    if not PENDING_FILE.exists():
        return

    try:
        with PENDING_FILE.open("r", encoding="utf-8") as f:
            pending = json.load(f)

        if not isinstance(pending, list):
            return

        video_done_status = STATUSES.get("video_done", "Video Done")
        music_done_status = STATUSES.get("music_done", "Music Done")
        status_updated = False

        # Lade master-listings.json für master-Update
        try:
            master = load_master_listings(day_folder)
            master_updated = False
        except Exception:
            master = None
            master_updated = False

        for entry in pending:
            # Aktualisiere nur Einträge mit video_done Status → music_done
            if entry.get("status") == video_done_status:
                entry_id = entry.get("id", "")
                folder_path = entry.get("folder", "")

                # ID-basiertes Matching: Musik-Datei liegt im selben Ordner wie Video
                if entry_id and folder_path:
                    matching = [m for m in music_created if str(folder_path) in m]
                    if matching:
                        entry["status"] = music_done_status
                        entry["music_path"] = matching[0]
                        status_updated = True

                        # Update master-listings.json mit music_path
                        if master:
                            item = find_master_item(master, entry_id)
                            if item:
                                item["music_path"] = matching[0]
                                master_updated = True
                    else:
                        # B1 FIX (2026-04-15): Status UNCONDITIONAL setzen
                        # Auch wenn kein matching gefunden — Eintrag hat video_done Status,
                        # muss mindestens auf music_done aktualisiert werden (Laufzeit-SSoT)
                        entry["status"] = music_done_status
                        status_updated = True

        if status_updated:
            atomic_write_json(PENDING_FILE, pending)
            print(f"\n💾 Status auf '{music_done_status}' gesetzt.")
        else:
            print(f"\nℹ️  Keine Einträge mit Status '{video_done_status}' gefunden.")

        # Persistiere master-listings.json wenn aktualisiert
        if master and master_updated:
            try:
                save_master_listings(day_folder, master)
                print(f"   🗂️  master-listings.json aktualisiert (music_path).")
            except Exception as e:
                print(f"   ⚠️  master-listings.json konnte nicht aktualisiert werden: {e}")

    except Exception as e:
        print(f"⚠️ Konnte pending.json nicht aktualisieren: {e}")


if __name__ == "__main__":
    main()
