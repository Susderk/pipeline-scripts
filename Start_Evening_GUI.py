#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Start_Evening_GUI.py

Mini-GUI für den Vorabend-Lauf der DPS-Pipeline.

Funktionen:
- Datums-Picker (YYYY-MM-DD strikt) mit Default 'morgen' (Berlin-Lokalzeit)
- Min-Wert: heute (Berlin-Lokalzeit)
- Checkbox "Neustart Pipeline" — nur aktivierbar wenn Datum == heute
- Im Neustart-Modus: Vollständigkeits-Check pro Listing
  (Hauptordner-Bilder + alle Bilder rekursiv unter <listing>/_kill/<tag>/
   sollten == image_count sein, plus <listing>/Mockups/ mit ≥1 Bild)
- Submit-Button startet Subprocess auf Start_Scripts.py mit passenden Flags

Submit-Logik:
- Standard (Datum != heute, oder Checkbox aus): voller Vorabend-Lauf
    → python Start_Scripts.py --evening --target-date=<picked>
- Resume-Modus (Datum == heute, Checkbox an, Vollständigkeits-Check OK):
    → python Start_Scripts.py --resume --target-date=<picked>
  (--resume impliziert --evening, run_scripts wird auf [review] reduziert)
- Resume-Modus mit unvollständigem Status, "Verstanden, trotzdem starten":
    → python Start_Scripts.py --evening --target-date=<picked>
  (voller Vorabend-Lauf ab Step 01 mit explizitem Hinweis akzeptiert)

Persona-Bootstrap-Hinweis: tkinter ist Standardbibliothek (keine externe
Dependency). zoneinfo ist seit Python 3.9 stdlib; falls IANA-Datenbank auf
Windows fehlt: pip install tzdata.

Letzte Änderung: 2026-04-26 (Indi, Pipeline-Split-Patch).
"""

import os
import sys
import subprocess
import re
from pathlib import Path
from datetime import datetime, timedelta

import tkinter as tk
from tkinter import ttk, messagebox

try:
    from zoneinfo import ZoneInfo
    BERLIN_TZ = ZoneInfo("Europe/Berlin")
    _ZONEINFO_OK = True
except Exception:
    BERLIN_TZ = None
    _ZONEINFO_OK = False

SCRIPT_PATH = Path(__file__).resolve().parent
START_SCRIPTS = SCRIPT_PATH / "Start_Scripts.py"

DATE_FORMAT = "%Y-%m-%d"
DATE_PATTERN = re.compile(r"^\d{4}-\d{2}-\d{2}$")
IMAGE_SUFFIXES = {".jpg", ".jpeg", ".png", ".webp"}

# Default-Bezugsgröße für Vollständigkeits-Check; wird aus config.evening.yaml
# gezogen (Fallback 10), damit kein Drift, wenn Ingo image_count ändert.
def _load_image_count() -> int:
    try:
        import yaml
        cfg_path = SCRIPT_PATH / "config.evening.yaml"
        if not cfg_path.exists():
            cfg_path = SCRIPT_PATH / "config.yaml"
        with cfg_path.open("r", encoding="utf-8") as f:
            cfg = yaml.safe_load(f)
        return int(cfg.get("image_count", 10))
    except Exception:
        return 10


def _today_berlin_date():
    if _ZONEINFO_OK:
        return datetime.now(BERLIN_TZ).date()
    # Fallback ohne tzdata: lokale Maschinen-Zeit.
    return datetime.now().date()


def _tomorrow_berlin_date():
    return _today_berlin_date() + timedelta(days=1)


def _images_path_from_config() -> Path:
    """Liest images_path aus config.evening.yaml (oder config.yaml als Fallback)."""
    try:
        import yaml
        cfg_path = SCRIPT_PATH / "config.evening.yaml"
        if not cfg_path.exists():
            cfg_path = SCRIPT_PATH / "config.yaml"
        with cfg_path.open("r", encoding="utf-8") as f:
            cfg = yaml.safe_load(f)
        return Path(cfg.get("images_path",
                            "C:/Companies/DPS/Generated pics"))
    except Exception:
        return Path("C:/Companies/DPS/Generated pics")


def _day_folder_for(date_obj) -> Path:
    """Tagesordner-Pfad analog config_loader.get_day_folder()."""
    base = _images_path_from_config()
    year = date_obj.strftime("%Y")
    month_name = date_obj.strftime("%B")
    day = date_obj.strftime(DATE_FORMAT)
    return base / year / f"{year} {month_name}" / day


def _count_listing_images(listing_dir: Path) -> int:
    """
    Zählt Bilder eines Listings für den Vollständigkeits-Check:
    Hauptordner-Bilder + alle Bilder rekursiv unter <listing>/_kill/<tag>/.
    Mockups/ wird ausgeklammert (Canva-Output, nicht Step_04-Output).

    Pfad-Lage `<listing>/_kill/<tag>/` ist verifiziert in
    image_review_tool.py Z.636.
    """
    count = 0
    # Hauptordner (flach)
    for f in listing_dir.iterdir():
        if f.is_file() and f.suffix.lower() in IMAGE_SUFFIXES:
            count += 1
    # _kill/<tag>/ (rekursiv)
    kill_dir = listing_dir / "_kill"
    if kill_dir.exists() and kill_dir.is_dir():
        for f in kill_dir.rglob("*"):
            if f.is_file() and f.suffix.lower() in IMAGE_SUFFIXES:
                count += 1
    return count


def _check_completeness(day_folder: Path, expected_per_listing: int) -> tuple[bool, list[str]]:
    """
    Vollständigkeits-Check für den Restart-Modus.

    Liest master-listings.json, prüft pro non-nolist-Listing:
      - <listing>/ existiert,
      - Bild-Anzahl (Hauptordner + _kill/ rekursiv) >= expected_per_listing,
      - <listing>/Mockups/ mit ≥1 Bild.

    Returns (all_ok, issues_list). issues_list ist eine Liste lesbarer
    Strings (eine Zeile pro Problem); leer wenn alles ok.
    """
    issues = []
    if not day_folder.exists():
        return False, [f"Tagesordner existiert nicht: {day_folder}"]

    master_path = day_folder / "master-listings.json"
    if not master_path.exists():
        return False, [f"master-listings.json fehlt im Tagesordner: {master_path}"]

    try:
        import json
        with master_path.open("r", encoding="utf-8") as f:
            master = json.load(f)
    except Exception as e:
        return False, [f"master-listings.json nicht lesbar: {e}"]

    items = master.get("items", []) if isinstance(master, dict) else []
    non_nolist = [it for it in items if it.get("status") != "nolist"]

    if not non_nolist:
        return False, ["Keine non-nolist-Items in master-listings.json gefunden."]

    for item in non_nolist:
        title = item.get("marketing_title") or item.get("folder") or item.get("id", "?")
        folder_name = item.get("folder") or item.get("marketing_title", "")
        if not folder_name:
            issues.append(f"  • {title}: kein 'folder'/'marketing_title' im Item.")
            continue
        listing_dir = day_folder / folder_name
        if not listing_dir.exists():
            issues.append(f"  • {title}: Listing-Ordner fehlt ({listing_dir.name}).")
            continue
        n_images = _count_listing_images(listing_dir)
        if n_images < expected_per_listing:
            issues.append(f"  • {title}: nur {n_images} Bilder gezählt "
                          f"(Hauptordner + _kill/ rekursiv); erwartet "
                          f">= {expected_per_listing}. Step 04/05 lief evtl. nicht durch.")
        mockups_dir = listing_dir / "Mockups"
        if not mockups_dir.exists():
            issues.append(f"  • {title}: Mockups-Ordner fehlt.")
            continue
        n_mock = sum(1 for f in mockups_dir.iterdir()
                     if f.is_file() and f.suffix.lower() in IMAGE_SUFFIXES)
        if n_mock < 1:
            issues.append(f"  • {title}: Mockups-Ordner ist leer "
                          f"(Cowork-Task evtl. nicht gelaufen).")
    return (len(issues) == 0), issues


# =============================================================================
# GUI-Klasse
# =============================================================================
class EveningGUI:
    def __init__(self, root: tk.Tk):
        self.root = root
        root.title("DPS Pipeline — Vorabend-Lauf starten")
        root.geometry("560x520")
        root.minsize(540, 480)

        self.image_count = _load_image_count()
        self.tomorrow = _tomorrow_berlin_date()
        self.today = _today_berlin_date()

        # === Header ===
        header = ttk.Label(root, text="DPS Pipeline — Vorabend-Lauf",
                           font=("Segoe UI", 14, "bold"))
        header.pack(pady=(12, 4))

        sub = ttk.Label(root,
                        text="Step 01–06: Prompts → Bilder → Sichtkontrolle",
                        foreground="#555")
        sub.pack(pady=(0, 12))

        if not _ZONEINFO_OK:
            warn = ttk.Label(root,
                             text="⚠️  zoneinfo/tzdata fehlt — nutze System-Zeitzone "
                                  "(pip install tzdata empfohlen)",
                             foreground="#aa6600")
            warn.pack(pady=(0, 6))

        # === Datums-Eingabe ===
        date_frame = ttk.LabelFrame(root, text="Tagesordner-Datum (YYYY-MM-DD)")
        date_frame.pack(fill="x", padx=14, pady=(4, 8))

        info = ttk.Label(date_frame,
                         text=f"Default: morgen ({self.tomorrow.strftime(DATE_FORMAT)}). "
                              f"Min: heute ({self.today.strftime(DATE_FORMAT)}).",
                         foreground="#555")
        info.pack(anchor="w", padx=8, pady=(6, 4))

        self.date_var = tk.StringVar(value=self.tomorrow.strftime(DATE_FORMAT))
        self.date_entry = ttk.Entry(date_frame, textvariable=self.date_var,
                                    width=20, font=("Consolas", 11))
        self.date_entry.pack(anchor="w", padx=8, pady=(0, 8))
        self.date_var.trace_add("write", lambda *_: self._on_date_change())

        # === Restart-Checkbox ===
        restart_frame = ttk.LabelFrame(root, text="Modus")
        restart_frame.pack(fill="x", padx=14, pady=(4, 8))

        self.restart_var = tk.BooleanVar(value=False)
        self.restart_cb = ttk.Checkbutton(
            restart_frame,
            text="Neustart Pipeline (nur wenn Datum = heute, "
                 "z.B. Step 06 nochmal nach Crash)",
            variable=self.restart_var,
            command=self._on_restart_toggle,
        )
        self.restart_cb.pack(anchor="w", padx=8, pady=8)

        # === Status-Bereich ===
        status_frame = ttk.LabelFrame(root, text="Status")
        status_frame.pack(fill="both", expand=True, padx=14, pady=(4, 8))

        self.status_text = tk.Text(status_frame, height=10, wrap="word",
                                   font=("Consolas", 9), state="disabled")
        self.status_text.pack(fill="both", expand=True, padx=6, pady=6)

        # === Hinweis-Block ===
        hint = ttk.Label(root,
                         text="Step 06 Bedienreihenfolge: Review-Tool öffnet automatisch → "
                              "Tool schließen → Cowork-Task 'canva-mockup-pipeline-creation' "
                              "manuell starten → ENTER drücken → Lock-Datei löschen.",
                         foreground="#444", wraplength=520, justify="left",
                         font=("Segoe UI", 8))
        hint.pack(padx=14, pady=(0, 4))

        # === Submit-Button ===
        btn_frame = ttk.Frame(root)
        btn_frame.pack(fill="x", padx=14, pady=(4, 12))

        self.submit_btn = ttk.Button(btn_frame, text="▶ Vorabend-Lauf starten",
                                     command=self._on_submit)
        self.submit_btn.pack(side="right")

        cancel_btn = ttk.Button(btn_frame, text="Abbrechen",
                                command=self.root.destroy)
        cancel_btn.pack(side="right", padx=(0, 8))

        # Initial: Datum-Validierung + Checkbox-Verfügbarkeit
        self._on_date_change()

    # ----- Helfer -----
    def _set_status(self, lines):
        self.status_text.config(state="normal")
        self.status_text.delete("1.0", "end")
        if isinstance(lines, str):
            lines = [lines]
        self.status_text.insert("end", "\n".join(lines))
        self.status_text.config(state="disabled")

    def _parse_date(self) -> tuple[bool, str, object]:
        """Returns (ok, msg, date_obj_or_None)."""
        s = self.date_var.get().strip()
        if not DATE_PATTERN.match(s):
            return False, "Format ungültig (erwartet: YYYY-MM-DD).", None
        try:
            d = datetime.strptime(s, DATE_FORMAT).date()
        except ValueError as e:
            return False, f"Datum ungültig: {e}", None
        if d < self.today:
            return False, (f"Datum liegt in der Vergangenheit "
                           f"(heute: {self.today.strftime(DATE_FORMAT)})."), None
        return True, "ok", d

    def _on_date_change(self):
        ok, msg, d = self._parse_date()
        if not ok:
            self.submit_btn.config(state="disabled")
            self._set_status(f"❌ {msg}")
            self.restart_cb.config(state="disabled")
            self.restart_var.set(False)
            return
        self.submit_btn.config(state="normal")
        # Restart-Checkbox nur verfügbar wenn Datum == heute
        if d == self.today:
            self.restart_cb.config(state="normal")
            self._set_status(f"✓ Datum {d.strftime(DATE_FORMAT)} (= heute). "
                             f"Restart-Modus möglich.")
        else:
            self.restart_cb.config(state="disabled")
            self.restart_var.set(False)
            self._set_status(f"✓ Datum {d.strftime(DATE_FORMAT)} (= morgen oder später). "
                             f"Standard-Vorabend-Lauf.")

    def _on_restart_toggle(self):
        ok, _, d = self._parse_date()
        if not ok or d != self.today:
            return
        if self.restart_var.get():
            # Sofortigen Vollständigkeits-Check anzeigen
            day_folder = _day_folder_for(d)
            all_ok, issues = _check_completeness(day_folder, self.image_count)
            if all_ok:
                self._set_status([
                    f"🔁 Neustart-Modus aktiv für {d.strftime(DATE_FORMAT)}.",
                    f"   Tagesordner: {day_folder}",
                    f"   Vollständigkeits-Check: ✅ alles ok.",
                    f"   → Submit ruft '--resume --target-date={d.strftime(DATE_FORMAT)}' "
                    f"(nur Step 06).",
                ])
            else:
                self._set_status([
                    f"🔁 Neustart-Modus aktiv für {d.strftime(DATE_FORMAT)}.",
                    f"   Tagesordner: {day_folder}",
                    f"   Vollständigkeits-Check: ⚠️ {len(issues)} Problem(e):",
                    *issues,
                    "",
                    f"   → Submit zeigt Hinweis-Dialog (voller Vorabend ab Step 01 mit "
                    f"Konsequenzen-Warnung).",
                ])
        else:
            self._set_status(f"✓ Datum {d.strftime(DATE_FORMAT)} (= heute). "
                             f"Standard-Vorabend-Lauf.")

    # ----- Submit -----
    def _on_submit(self):
        ok, msg, d = self._parse_date()
        if not ok:
            messagebox.showerror("Datum ungültig", msg)
            return

        date_str = d.strftime(DATE_FORMAT)
        is_restart = self.restart_var.get() and d == self.today

        if is_restart:
            day_folder = _day_folder_for(d)
            all_ok, issues = _check_completeness(day_folder, self.image_count)
            if all_ok:
                # Resume: nur Step 06
                self._launch(["--resume", f"--target-date={date_str}"])
            else:
                # Hinweis-Dialog: Konsequenzen voller Lauf
                consequence_msg = (
                    "Der Tagesordner ist UNVOLLSTÄNDIG:\n\n"
                    + "\n".join(issues[:8])
                    + ("\n  ..." if len(issues) > 8 else "")
                    + "\n\nKonsequenzen bei vollem Vorabend-Lauf ab Step 01:\n\n"
                    "• Step 04 generiert NEUE Leonardo-Bilder zu den vorhandenen "
                    "dazu → Credit-Verschwendung.\n"
                    "• Step 05 benennt nur die neuen um, alte sind bereits "
                    "umbenannt → potenzielles Chaos im Tagesordner.\n\n"
                    "Trotzdem starten?"
                )
                resp = messagebox.askyesno("Unvollständig — trotzdem starten?",
                                           consequence_msg, icon="warning")
                if resp:
                    # Voller Vorabend-Lauf ab Step 01
                    self._launch(["--evening", f"--target-date={date_str}"])
                # else: Abbruch ohne Aktion
        else:
            # Standard-Vorabend-Lauf
            self._launch(["--evening", f"--target-date={date_str}"])

    def _launch(self, extra_args):
        cmd = [sys.executable, str(START_SCRIPTS)] + extra_args
        # Konsole sichtbar lassen — wir spawnen ohne stdout-Capture, damit die
        # Step-Logs in dem Konsolenfenster erscheinen, aus dem die GUI gestartet
        # wurde (via Start_Evening.bat).
        try:
            self.root.destroy()  # GUI schließen, Konsole übernimmt
        except Exception:
            pass
        # Starte Subprocess im Vordergrund (blocking) — PowerShell/CMD-Fenster
        # bleibt offen weil Start_Evening.bat ein "pause" am Ende hat.
        rc = subprocess.call(cmd, cwd=str(SCRIPT_PATH))
        sys.exit(rc)


# =============================================================================
# Entry-Point
# =============================================================================
def main():
    root = tk.Tk()
    # Versuche besseres Theme
    try:
        style = ttk.Style()
        if "vista" in style.theme_names():
            style.theme_use("vista")
        elif "clam" in style.theme_names():
            style.theme_use("clam")
    except Exception:
        pass
    EveningGUI(root)
    root.mainloop()


if __name__ == "__main__":
    main()
