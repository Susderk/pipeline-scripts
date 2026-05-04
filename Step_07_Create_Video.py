#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Step_07_Create_Video.py

Erstellt PRO MARKETING-FOLDER ein Video aus den 5 Mockup-PNGs.

- Liest alle Unterordner in Tagesordner/Mockups/
- Pro Unterordner: die PNG-Dateien in ZUFÄLLIGER Reihenfolge zu Video zusammenfügen
- Sanfte Überblendung zwischen den Bildern (crossfade)
- Videoname enthält Datum + Folder-Name

Bei 2 Marketing-Foldern → 2 Videos

Config-Parameter (config.yaml):
  video_duration_per_image: 0.8   # Sekunden pro Bild
  video_crossfade_duration: 0.3   # Sekunden Überblendung
  video_fps: 30                   # Frames pro Sekunde
  video_output_format: "mp4"      # Ausgabeformat
  ffmpeg_path: ""                 # Pfad zu ffmpeg.exe (leer = aus PATH)
"""

import sys
import os
import json
import random
import subprocess
import textwrap
from pathlib import Path
from datetime import datetime

from config_loader import (
    load_config,
    load_master_listings,
    save_master_listings,
    find_master_item,
    update_master_item,
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

flags  = cfg["get_script_flags"]("video")
DRYRUN = bool(flags.get("dry_run", False))

DURATION_PER_IMAGE = float(config.get("video_duration_per_image", 1.2))
CROSSFADE_DURATION = float(config.get("video_crossfade_duration", 0.6))
VIDEO_FPS          = int(config.get("video_fps", 30))
VIDEO_FORMAT       = config.get("video_output_format", "mp4")
FFMPEG_EXE         = config.get("ffmpeg_path", "ffmpeg").strip() or "ffmpeg"
VIDEO_W            = int(config.get("video_width", 1080))
VIDEO_H            = int(config.get("video_height", 1920))
KEN_BURNS          = bool(config.get("ken_burns", False))
KEN_BURNS_STEP     = float(config.get("ken_burns_intensity", 0.002))

# Hook-Text-Overlay-Config (ffmpeg drawtext)
HOOK_CONFIG        = config.get("hook", {})
HOOK_FONT_NAME     = HOOK_CONFIG.get("font_name", "Kristen ITC")
HOOK_FONT_STYLE    = HOOK_CONFIG.get("font_style", "Bold")
HOOK_FONT_SIZE     = int(HOOK_CONFIG.get("font_size", 64))
HOOK_BAR_OPACITY   = float(HOOK_CONFIG.get("bar_opacity", 0.60))
HOOK_BAR_PADDING   = int(HOOK_CONFIG.get("bar_padding_px", 15))
HOOK_TEXT_MARGIN   = int(HOOK_CONFIG.get("text_margin_px", 80))
HOOK_POSITION      = HOOK_CONFIG.get("position", "top")
HOOKS_FILE         = Path(cfg["HOOKS_FILE"])   # aus config_loader → JSON Dateien/hooks.json


# === HELPERS ===
# Hinweis: `atomic_write_json` wird aus `config_loader` importiert (oben).
# Gehärtete Variante mit Retry/Backoff gegen Windows-Dateilocks — keine lokale
# Kopie mehr. Migration 2026-04-20 (session-log-2026-04-20-c.md).


def check_ffmpeg() -> bool:
    try:
        result = subprocess.run(
            [FFMPEG_EXE, "-version"],
            capture_output=True, text=True, timeout=10
        )
        return result.returncode == 0
    except FileNotFoundError:
        return False
    except Exception:
        return True


def find_font(font_name: str, font_style: str) -> tuple:
    """
    Sucht eine Schriftart auf Windows-Systemen.
    Versucht zuerst Kristen ITC, fällt auf Arial zurück.
    Gibt (font_path, font_name_used) zurück.
    """
    win_font_dirs = [
        Path("C:/Windows/Fonts"),
        Path(os.environ.get("WINDIR", "C:/Windows")) / "Fonts",
        Path(os.environ.get("LOCALAPPDATA", "")) / "Microsoft/Windows/Fonts" if os.environ.get("LOCALAPPDATA") else None,
    ]
    win_font_dirs = [d for d in win_font_dirs if d is not None and d.exists()]

    # Kristen ITC Varianten
    kristen_candidates = [
        "ITCKRIST.TTF", "itckrist.ttf",
        "KRISTAB_.TTF", "KRISTEN.TTF", "KristenITC-Regular.ttf", "KristenITC Bold.ttf",
        "kristab.ttf", "kristen.ttf",
    ]
    for font_dir in win_font_dirs:
        for candidate in kristen_candidates:
            fp = font_dir / candidate
            if fp.exists():
                return str(fp), font_name

    # Arial Bold Fallback
    arial_candidates = ["arialbd.ttf", "Arial Bold.ttf", "arial.ttf", "ARIALBD.TTF"]
    for font_dir in win_font_dirs:
        for candidate in arial_candidates:
            fp = font_dir / candidate
            if fp.exists():
                print(f"   [ALERT] {font_name} nicht gefunden → Fallback: Arial Bold ({fp})")
                return str(fp), "Arial"

    # Letzter Fallback: ffmpeg sucht selbst
    print(f"   [ALERT] Kein Font gefunden ({font_name}) — ffmpeg wird System-Default verwenden")
    return None, "Arial"


def load_hooks() -> list:
    """Lädt Hook-Texte aus hooks.json. Fallback bei Fehler."""
    hooks_path = HOOKS_FILE   # vollständiger Pfad aus config_loader
    if hooks_path.exists():
        try:
            with hooks_path.open("r", encoding="utf-8") as f:
                hooks = json.load(f)
                if isinstance(hooks, list) and len(hooks) > 0:
                    return hooks
        except (json.JSONDecodeError, IOError) as e:
            print(f"   ⚠️ hooks.json konnte nicht gelesen werden: {e}")
    # Fallback-Text wenn hooks.json leer, fehlerhaft oder nicht vorhanden
    fallback_text = "Download. Set. Impress!"
    print(f"   📌 Fallback-Hook verwendet: '{fallback_text}'")
    return [fallback_text]


def apply_hook_overlay(video_path: Path, dryrun: bool = False) -> bool:
    """
    Erzeugt einen ffmpeg-Durchlauf, um einen zufälligen Hook-Text
    über die gesamte Video-Dauer einzubrennen.

    Hook = horizontaler halbtransparenter Balken (volle Videobreite) mit zentriertem Text.
    """
    hooks = load_hooks()

    # load_hooks() gibt jetzt immer eine Liste zurück (mit Fallback, nie leer)
    hook_text = random.choice(hooks)
    print(f"   🎣 Hook-Text gewählt: '{hook_text}'")

    if dryrun:
        print(f"   🧪 DRY-RUN: Würde Hook-Overlay hinzufügen")
        return True

    # Font suchen
    font_path, font_name_used = find_font(HOOK_FONT_NAME, HOOK_FONT_STYLE)

    # ffmpeg drawtext-Parameter vorbereiten
    def esc(s: str) -> str:
        """Escape-Funktion für ffmpeg drawtext-Filter."""
        return s.replace("\\", "\\\\").replace("'", "\\'").replace(":", "\\:")

    # Text umbrechen damit er in die Videobreite passt.
    # Faktor 0.7: Kristen ITC Bold ist deutlich breiter als ein Standard-Font.
    # Gibt bei 1080px / 64pt ≈ 24 Zeichen pro Zeile.
    max_chars = max(10, int(VIDEO_W / (HOOK_FONT_SIZE * 0.7)))
    wrapped_lines = textwrap.wrap(hook_text, width=max_chars) or [hook_text]
    hook_esc = "\\n".join(esc(line) for line in wrapped_lines)
    if len(wrapped_lines) > 1:
        print(f"   📐 Text umgebrochen: {len(wrapped_lines)} Zeile(n) (max. {max_chars} Zeichen/Zeile)")

    # Balken-Höhe berechnen: für alle Zeilen + Padding
    bar_h = (HOOK_FONT_SIZE * len(wrapped_lines)) + HOOK_BAR_PADDING * 2

    # fontfile-Parameter (Windows: Doppelpunkt im Laufwerksbuchstaben escapen, z.B. C: → C\:)
    def esc_path(p: str) -> str:
        return p.replace("\\", "/").replace(":", "\\:")

    fontfile_param = f":fontfile='{esc_path(font_path)}'" if font_path else ""

    # drawbox: halbtransparenter Balken (volle Breite, t=fill für alle ffmpeg-Versionen)
    drawbox_filter = (
        f"drawbox="
        f"x=0:"
        f"y={HOOK_TEXT_MARGIN}:"
        f"w=iw:"
        f"h={bar_h}:"
        f"color=black@{HOOK_BAR_OPACITY}:"
        f"t=fill"
    )

    # Pro Zeile ein eigener drawtext-Filter (zuverlässiger als \n über alle ffmpeg-Versionen)
    drawtext_filters = []
    for i, line in enumerate(wrapped_lines):
        line_esc = esc(line)
        y_pos = HOOK_TEXT_MARGIN + HOOK_BAR_PADDING + (i * HOOK_FONT_SIZE)
        drawtext_filters.append(
            f"drawtext=text='{line_esc}'{fontfile_param}"
            f":fontsize={HOOK_FONT_SIZE}"
            f":fontcolor=white"
            f":x=max(20\\,(w-text_w)/2)"
            f":y={y_pos}"
        )

    # Kombiniert: erst Box, dann eine drawtext-Instanz pro Zeile
    vf_filter = ", ".join([drawbox_filter] + drawtext_filters)

    # Temporäre Ausgabedatei
    output_path = video_path.with_stem(video_path.stem + "_with_hook")

    # ffmpeg-Befehl
    cmd = [
        FFMPEG_EXE, "-y",
        "-i", str(video_path),
        "-vf", vf_filter,
        "-c:v", "libx264",
        "-crf", "18",
        "-preset", "slow",
        str(output_path)
    ]

    print(f"   ▶️  ffmpeg-Overlay wird ausgeführt...")
    try:
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=600)
        if result.returncode != 0:
            print(f"   ❌ ffmpeg Overlay-Fehler:")
            print(f"      {result.stderr[-500:].strip()}")
            return False

        # Original durch das mit Hook versehene Video ersetzen
        video_path.unlink()
        output_path.rename(video_path)
        print(f"   ✅ Hook-Overlay angewendet: {video_path.name}")
        return True
    except subprocess.TimeoutExpired:
        print(f"   ❌ ffmpeg Overlay-Timeout (>10 Minuten)")
        return False
    except Exception as e:
        print(f"   ❌ ffmpeg Overlay-Fehler: {e}")
        return False


def mix_audio_into_video(video_path: Path, folder_name: str, dryrun: bool = False) -> bool:
    """
    Mischt eine Musikdatei (<folder_name>_music.wav) in das fertige Video ein.
    Falls keine Musikdatei vorhanden ist: Video bleibt stumm (defensiver Fallback).
    Gibt True zurück bei Erfolg, False bei Fehler.
    """
    music_path = video_path.parent / f"{folder_name}_music.wav"

    if not music_path.exists():
        print(f"   ⚠️ Keine Musikdatei gefunden ({music_path.name}) — Video bleibt stumm")
        return True  # Kein Fehler — Step_07a hätte sonst abgebrochen

    if dryrun:
        print(f"   🧪 DRY-RUN: Würde Musik einmischen: {music_path.name}")
        return True

    output_path = video_path.with_stem(video_path.stem + "_with_audio")

    cmd = [
        FFMPEG_EXE, "-y",
        "-i", str(video_path),           # Video-Input
        "-stream_loop", "-1",            # Musik loopen falls kürzer als Video
        "-i", str(music_path),           # Musik-Input
        "-map", "0:v",                   # Video-Stream aus Input 0
        "-map", "1:a",                   # Audio-Stream aus Input 1
        "-c:v", "copy",                  # Video nicht re-encodieren
        "-c:a", "aac",                   # Audio zu AAC
        "-b:a", "192k",
        "-shortest",                     # Video-Ende bestimmt Gesamtlänge
        str(output_path)
    ]

    print(f"   🎵 Musik wird eingemischt: {music_path.name}")
    try:
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=300)
        if result.returncode != 0:
            print(f"   ❌ FFmpeg Audio-Mix Fehler:")
            print(f"      {result.stderr[-500:].strip()}")
            return False

        video_path.unlink()
        output_path.rename(video_path)
        print(f"   ✅ Musik eingemischt: {video_path.name}")
        return True
    except subprocess.TimeoutExpired:
        print(f"   ❌ FFmpeg Audio-Mix Timeout (>5 Minuten)")
        return False
    except Exception as e:
        print(f"   ❌ FFmpeg Audio-Mix Fehler: {e}")
        return False


def get_folder_subdirs(mockups_root: Path) -> list:
    """Gibt alle Unterordner im Mockups-Root zurück (je ein Folder = ein Video)."""
    if not mockups_root.exists():
        return []
    subdirs = [d for d in mockups_root.iterdir() if d.is_dir()]
    return sorted(subdirs)


def get_png_files(folder: Path) -> list:
    """Gibt alle PNG und JPG Dateien in einem Ordner zurück."""
    return sorted([f for f in folder.iterdir() if f.is_file() and f.suffix.lower() in {".png", ".jpg", ".jpeg"}])


def create_video_ffmpeg(png_files: list, output_path: Path, dryrun: bool = False) -> bool:
    """
    Erstellt ein Video aus PNG-Dateien mit sanften Crossfade-Übergängen via FFmpeg.

    Ken-Burns-Modus (config: ken_burns=true):
      Jedes Bild erhält eine langsame Zoom-Animation — gerade Indizes zoomen in,
      ungerade zoomen heraus. Intensität steuerbar via ken_burns_intensity (Zoom-Inkrement
      pro Frame). Wenn ken_burns=false (default): exakt identische Filter-Chain wie zuvor.
    """
    n = len(png_files)
    if n == 0:
        print("   ❌ Keine PNG-Dateien gefunden.")
        return False

    total_duration = (DURATION_PER_IMAGE * n) - (CROSSFADE_DURATION * (n - 1))

    if dryrun:
        print(f"   🧪 DRY-RUN: Würde Video erstellen aus {n} PNGs → {output_path.name}")
        print(f"      Dauer pro Bild: {DURATION_PER_IMAGE}s | Crossfade: {CROSSFADE_DURATION}s")
        print(f"      Gesamtdauer ca.: {total_duration:.1f}s")
        return True

    print(f"   🎬 Erstelle Video aus {n} Bildern (ca. {total_duration:.1f}s)...")

    # FFmpeg Kommando aufbauen
    cmd = [FFMPEG_EXE, "-y"]

    # Inputs: jedes Bild als Loop mit Dauer
    for png in png_files:
        cmd += ["-loop", "1", "-t", str(DURATION_PER_IMAGE), "-i", str(png)]

    # Filter-Graph: Skalierung + Crossfade-Kette
    scale_parts = []

    if KEN_BURNS:
        # Konstanten für Ken-Burns vorab berechnen (einmal, nicht pro Frame-Iteration)
        frames_per_image = int(DURATION_PER_IMAGE * VIDEO_FPS)
        overscan_w = int(VIDEO_W * 1.3)
        overscan_h = int(VIDEO_H * 1.3)

    for i in range(n):
        if KEN_BURNS:
            # Zoom-in für gerade Indizes (0, 2, …), Zoom-out für ungerade (1, 3, …)
            if i % 2 == 0:
                # Zoom-in: startet bei 1.0, läuft bis 1.2
                zoom_expr = f"'min(zoom+{KEN_BURNS_STEP},1.2)'"
            else:
                # Zoom-out: startet bei 1.2, läuft bis 1.0
                zoom_expr = f"'if(eq(on,1),1.2,max(1.0,zoom-{KEN_BURNS_STEP}))'"
            x_expr = "'iw/2-(iw/zoom/2)'"
            y_expr = "'ih/2-(ih/zoom/2)'"
            scale_parts.append(
                # Schritt 1: auf 130 % der Zielgröße hochskalieren (Platz für Zoom-Pan)
                f"[{i}:v]scale={overscan_w}:{overscan_h}:"
                f"force_original_aspect_ratio=decrease,"
                f"pad={overscan_w}:{overscan_h}:(ow-iw)/2:(oh-ih)/2,setsar=1,"
                # Schritt 2: zoompan erzeugt frames_per_image Frames in Zielgröße
                f"zoompan=z={zoom_expr}:x={x_expr}:y={y_expr}:"
                f"d={frames_per_image}:s={VIDEO_W}x{VIDEO_H}:fps={VIDEO_FPS}[v{i}]"
            )
        else:
            scale_parts.append(
                f"[{i}:v]scale={VIDEO_W}:{VIDEO_H}:force_original_aspect_ratio=decrease,"
                f"pad={VIDEO_W}:{VIDEO_H}:(ow-iw)/2:(oh-ih)/2,setsar=1,fps={VIDEO_FPS}[v{i}]"
            )

    filter_complex = "; ".join(scale_parts)

    if n == 1:
        filter_complex += "; [v0]copy[outv]"
    else:
        offset = DURATION_PER_IMAGE - CROSSFADE_DURATION
        filter_complex += (
            f"; [v0][v1]xfade=transition=fade:"
            f"duration={CROSSFADE_DURATION}:offset={offset:.3f}[xf0]"
        )
        for i in range(2, n):
            offset_i = offset * i
            prev = "[xf0]" if i == 2 else f"[xf{i-2}]"
            filter_complex += (
                f"; {prev}[v{i}]xfade=transition=fade:"
                f"duration={CROSSFADE_DURATION}:offset={offset_i:.3f}[xf{i-1}]"
            )
        last = "[xf0]" if n == 2 else f"[xf{n-2}]"
        filter_complex += f"; {last}copy[outv]"

    cmd += [
        "-filter_complex", filter_complex,
        "-map", "[outv]",
        "-c:v", "libx264",
        "-pix_fmt", "yuv420p",
        "-crf", "18",
        "-preset", "slow",
        str(output_path)
    ]

    # Alte Datei löschen damit Windows ein neues Erstellungsdatum setzt
    output_path.unlink(missing_ok=True)

    print(f"   ▶️  FFmpeg wird ausgeführt...")
    try:
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=300)
        if result.returncode != 0:
            print(f"   ❌ FFmpeg Fehler:")
            print(f"      {result.stderr[-500:].strip()}")
            return False
        print(f"   ✅ Video erstellt: {output_path.name}")
        return True
    except subprocess.TimeoutExpired:
        print("   ❌ FFmpeg Timeout (>5 Minuten)")
        return False
    except Exception as e:
        print(f"   ❌ FFmpeg Fehler: {e}")
        return False


# === MAIN ===
def main():
    print("[Step 9 - Video erstellen] wird gestartet...")

    if not DRYRUN:
        if not check_ffmpeg():
            print(f"❌ FFmpeg nicht gefunden: '{FFMPEG_EXE}'")
            print("   Bitte FFmpeg installieren: https://ffmpeg.org/download.html")
            print("   Oder Pfad in config.yaml unter 'ffmpeg_path' eintragen.")
            sys.exit(1)
        print(f"✅ FFmpeg gefunden.")

    # Zieldatum bestimmen: aus config.yaml oder heute
    target_date = cfg["TARGET_DATE"]
    print(f"📅 Zieldatum: {target_date.strftime(DATE_FORMAT)}")

    year       = target_date.strftime("%Y")
    month_name = target_date.strftime("%B")
    date_str   = target_date.strftime(DATE_FORMAT)
    day_folder = IMAGES_PATH / year / f"{year} {month_name}" / date_str

    if not day_folder.exists():
        print(f"❌ Tagesordner nicht gefunden: {day_folder}")
        print("   Bitte zuerst Step 03 (Marketing Ordner) ausführen.")
        sys.exit(1)

    # --- DRY-RUN (vor der Dateiprüfung, damit kein Abbruch bei fehlenden Ordnern) ---
    if DRYRUN:
        print("\n🧪 DRY-RUN – keine echte Video-Erstellung.")
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
        print("   Erwartet wird: Tagesordner/<FolderName>/Mockups/<PNG-Dateien>")
        print("   Bitte zuerst Step 05 (Bilder umbenennen) ausführen.")
        sys.exit(1)

    print(f"\n📁 {len(subdirs)} Folder-Unterordner gefunden:")
    for d in subdirs:
        pngs = get_png_files(d)
        print(f"   • {d.parent.name}/Mockups: {len(pngs)} PNG(s)")

    videos_created = []
    any_failed     = False

    for subdir in subdirs:
        print(f"\n{'─'*52}")
        print(f"🎬 Verarbeite Folder: {subdir.name}")

        png_files = get_png_files(subdir)

        if len(png_files) == 0:
            print(f"   ⚠️  Keine PNG-Dateien – übersprungen.")
            continue

        # Bilder in zufälliger Reihenfolge
        random.shuffle(png_files)
        print(f"   🎲 Reihenfolge ({len(png_files)} Bilder):")
        for i, f in enumerate(png_files, 1):
            print(f"      {i}. {f.name}")

        # Video-Dateiname: nur Folder-Name, kein Datum
        folder_label = subdir.parent.name
        safe_name    = "".join(c for c in folder_label if c.isalnum() or c in " _-").strip().replace(" ", "_")
        output_name  = f"{safe_name}.{VIDEO_FORMAT}"
        output_path  = subdir / output_name

        success = create_video_ffmpeg(png_files, output_path, dryrun=DRYRUN)

        # Hook-Overlay hinzufügen (falls Video erfolgreich erstellt)
        if success:
            hook_success = apply_hook_overlay(output_path, dryrun=DRYRUN)
            if not hook_success:
                print(f"   ⚠️ Hook-Overlay fehlgeschlagen, aber Video wird beibehalten")
                # Fehler beim Hook ist nicht kritisch — Video bleibt bestehen

            # Musik einmischen (neu)
            # Wichtig: safe_name (nicht folder_label) verwenden — Step_07a
            # speichert die Musikdatei ebenfalls unter safe_name_music.wav.
            audio_success = mix_audio_into_video(output_path, safe_name, dryrun=DRYRUN)
            if not audio_success:
                print(f"   ⚠️ Audio-Mix fehlgeschlagen, aber Video wird beibehalten")
                # Fehler beim Audio-Mix ist nicht kritisch — Video bleibt bestehen

        if success and not DRYRUN:
            videos_created.append(str(output_path))
        elif not success and not DRYRUN:
            any_failed = True

    print(f"\n{'='*52}")
    if DRYRUN:
        print(f"🧪 DRY-RUN abgeschlossen.")
        print(f"{'='*52}")
        return

    print(f"🎯 Step 9 abgeschlossen: {len(videos_created)} Video(s) erstellt.")
    for v in videos_created:
        print(f"   🎬 {Path(v).name}")
    print(f"{'='*52}")

    # Gate vor Status-Write:
    # - any_failed=True (echter Fehler beim Video-Bau) bricht den Lauf hart ab.
    # - Wenn kein einziges Video erstellt wurde (z. B. alle Mockups-Ordner leer),
    #   gibt es nichts zu persistieren → stiller Return.
    # - Partial-Success (mind. 1 Video, aber < expected_videos) ist KEIN Fehler
    #   mehr: seit 2026-04-15 darf der Cowork-Task „canva-mockups-today" Produkte
    #   unter der 5-Punkte-Schwelle überspringen; die betroffenen Ordner haben
    #   dann legitim leere Mockups/. Status-Write läuft trotzdem, aber nur für
    #   die Einträge, deren Video tatsächlich in `videos_created` gelandet ist
    #   (ID-basiertes Matching weiter unten). Einträge ohne Video bleiben auf
    #   ihrem vorherigen Status — sie fallen ohnehin später via `nolist`-Filter
    #   bzw. fehlende Artefakte aus der Pipeline heraus.
    expected_videos = len(subdirs)
    if any_failed:
        print(f"\nℹ️ Keine Statusänderung – Fehler beim Video-Bau "
              f"({len(videos_created)}/{expected_videos} erfolgreich).")
        sys.exit(1)

    if not videos_created:
        print(f"\nℹ️ Keine Statusänderung – kein Video erstellt "
              f"(0/{expected_videos}).")
        return

    if len(videos_created) < expected_videos:
        skipped = expected_videos - len(videos_created)
        print(f"\nℹ️ Teilerfolg: {len(videos_created)}/{expected_videos} Video(s) erstellt, "
              f"{skipped} Folder übersprungen (leerer Mockups-Ordner). "
              f"Status-Update läuft nur für die erfolgreich erzeugten Videos.")

    if not PENDING_FILE.exists():
        return

    try:
        with PENDING_FILE.open("r", encoding="utf-8") as f:
            pending = json.load(f)

        # Im Staging-Modus: Pending-Einträge auf Staging-Temp-Ordner remappen
        if STAGING_ISOLATION:
            remap_pending_entries_to_staging(pending, IMAGES_PATH)

        renamed_status = STATUSES.get("renamed", "Renamed")
        video_status    = STATUSES.get("video_done", "Video Done")
        status_updated  = False

        # Lade master-listings.json für master-Update
        try:
            master = load_master_listings(day_folder)
            master_updated = False
        except Exception:
            master = None
            master_updated = False

        for entry in pending:
            if entry.get("status") == renamed_status:
                entry_id = entry.get("id", "")
                folder_path = entry.get("folder", "")

                # ID-basiertes Matching: finde Video nach entry.id
                if entry_id and folder_path:
                    # Das Video lebt in <folder_path>/safe_name.mp4
                    # Matching: folder_path muss mit Video-Pfad-Prefix übereinstimmen
                    matching = [v for v in videos_created if str(folder_path) in v]

                    if matching:
                        entry["status"]     = video_status
                        entry["video_path"] = matching[0]
                        status_updated      = True

                        # Update master-listings.json mit video_path
                        if master:
                            item = find_master_item(master, entry_id)
                            if item:
                                item["video_path"] = matching[0]
                                master_updated = True

        if status_updated:
            atomic_write_json(PENDING_FILE, pending)
            print(f"\n💾 Status auf '{video_status}' gesetzt.")
        else:
            print(f"\nℹ️ Keine passenden Einträge für Statusänderung gefunden.")

        # Persistiere master-listings.json wenn aktualisiert
        if master and master_updated:
            try:
                save_master_listings(day_folder, master)
                print(f"   🗂️  master-listings.json aktualisiert (video_path).")
            except Exception as e:
                print(f"   ⚠️  master-listings.json konnte nicht aktualisiert werden: {e}")

    except Exception as e:
        print(f"⚠️ Konnte pending.json nicht aktualisieren: {e}")


if __name__ == "__main__":
    main()