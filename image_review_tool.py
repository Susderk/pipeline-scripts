#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
image_review_tool.py

Desktop-GUI für Bildkontrolle in der Pipeline.
Lädt Bilder aus einem Tagesordner, zeigt Prompts an, ermöglicht Bewertung und Verschiebung.

Nutzung:
  python image_review_tool.py                    # Dialog fragt nach Tagesordner
  python image_review_tool.py /path/to/day_folder  # Direkter Tagesordner

Abhängigkeiten:
  - PyQt6 (pip install PyQt6)
  - Pillow (pip install Pillow)
"""

import sys
import json
import os
from pathlib import Path
from datetime import datetime
from typing import Optional, Tuple, List

from PyQt6.QtWidgets import (
    QApplication, QMainWindow, QWidget, QVBoxLayout, QHBoxLayout,
    QLabel, QFileDialog, QScrollArea, QFrame, QPushButton, QDialog, QTextEdit
)
from PyQt6.QtGui import (
    QPixmap, QImage, QIcon, QFont, QColor, QPalette, QWheelEvent
)
from PyQt6.QtCore import Qt, QSize, QTimer, QPoint, QRect, QThread, pyqtSignal
from PyQt6.QtGui import QPixmap as QPixmapType

from config_loader import atomic_write_json


# ============================================================================
# MODUL-KONSTANTEN
# ============================================================================

SCRIPT_DIR = Path(__file__).resolve().parent  # .../pipeline/
JSON_DIR = SCRIPT_DIR.parent / "JSON Dateien"  # .../01 Python Skript/JSON Dateien/


# ============================================================================
# KONFIGURATION & HELPER
# ============================================================================

def load_pending_file(pending_path: Path) -> List[dict]:
    """Laden von prompts_pending.json."""
    if not pending_path.exists():
        return []
    try:
        with open(pending_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
        return data if isinstance(data, list) else []
    except Exception as e:
        print(f"❌ Fehler beim Laden von {pending_path}: {e}")
        return []


def find_prompt_for_image(image_path: Path, pending_entries: List[dict], folder_name: Optional[str] = None) -> dict:
    """
    Sucht den Prompt für ein Bild basierend auf dem Marketing-Ordner.

    Parameter:
      image_path      — Pfad des Bildes (wird nur als Fallback für den Ordnernamen genutzt).
      pending_entries — Liste aus prompts_pending.json.
      folder_name     — optionaler expliziter Produktordner-Name. MUSS gesetzt werden,
                        wenn das Bild zwischen Sammeln und Matching bewegt wurde
                        (z. B. nach rename() in _kill/<tag>/). Andernfalls zeigt
                        image_path.parent.name auf den Kill-Tag-Ordner und Matching schlägt fehl.

    Rückgabe: {"prompt": str, "generator": str, "id": str}
              bzw. {"prompt": "Kein Prompt gefunden", "generator": "unknown", "id": ""} wenn kein Match.
    """
    compare_folder = (folder_name if folder_name is not None else image_path.parent.name).lower()

    for entry in pending_entries:
        marketing_title = entry.get("marketing_title", "").lower()

        if marketing_title and marketing_title in compare_folder:
            return {
                "prompt": entry.get("prompt", "Kein Prompt vorhanden"),
                "generator": entry.get("generator", "unknown"),
                "id": entry.get("id", "")
            }

    return {
        "prompt": "Kein Prompt gefunden",
        "generator": "unknown",
        "id": ""
    }


def _heal_truncated_journal(text: str) -> Optional[List[dict]]:
    """
    Versucht eine truncated feedback-journal.json zu heilen, indem das letzte
    vollstaendige Item gesucht wird. Pattern: '},\\r\\n  {' (CRLF, Windows-Output)
    oder '},\\n  {' (LF, Linux-Output). Ist analog zu den Daten-Reparaturen
    Indi-g (2026-04-26) und Indi-23-d (NPP-Truncation).

    Rueckgabe: Liste mit allen vollstaendigen Items, oder None wenn nicht heilbar.
    """
    for boundary in ('},\r\n  {', '},\n  {'):
        idx = text.rfind(boundary)
        if idx < 0:
            continue
        # Schnitt an der Position des '}' am Ende des letzten vollstaendigen Items.
        # Plus '\r\n]' bzw. '\n]' fuer den Array-Abschluss.
        suffix = '\r\n]' if '\r\n' in boundary else '\n]'
        healed = text[:idx + 1] + suffix
        try:
            data = json.loads(healed)
            if isinstance(data, list):
                return data
        except json.JSONDecodeError:
            continue
    return None


def load_journal(journal_path: Path) -> List[dict]:
    """Laden des Feedback-Journals.

    Defensive Healing-Strategie (Patch 2026-04-26-h, Sektion X.3):
    - Wenn die Datei nicht existiert: leere Liste zurueck (Erstanlage).
    - Wenn json.load erfolgreich: Liste zurueckgeben (oder leere Liste bei Typ-Mismatch).
    - Wenn json.load failed: Healing-Versuch via _heal_truncated_journal.
      Bei Erfolg: laute Warnung + geheilte Liste zurueckgeben.
      Bei Fehlschlag: GUI-Modal-Dialog (falls QApplication aktiv ist) der
      den User vor Daten-Verlust warnt; sonst lauter Print. In KEINEM Fall
      stillschweigend leere Liste zurueckgeben (alte bare-except-Variante
      hatte 319 historische Eintraege beim naechsten Save vernichten koennen).

    Hintergrund: Indi-g (2026-04-26-g) hat eine truncated feedback-journal.json
    repariert. Eine zweite Sichtkontrolle hat sie erneut beschaedigt (5 Crystalline-
    Eintraege fehlten), ohne dass das Tool den User warnte. Diese Healing-Funktion
    schliesst die Luecke.
    """
    if not journal_path.exists():
        return []
    try:
        with open(journal_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
            return data if isinstance(data, list) else []
    except (json.JSONDecodeError, OSError) as e:
        # Healing-Versuch
        try:
            raw = journal_path.read_bytes()
            text = raw.decode('utf-8', errors='replace')
            healed = _heal_truncated_journal(text)
        except Exception as heal_err:
            healed = None
            heal_err_msg = str(heal_err)
        else:
            heal_err_msg = ""

        if healed is not None:
            warn = (f"⚠️  feedback-journal.json war truncated (json.load: {e}). "
                    f"Healing erfolgreich: {len(healed)} historische Eintraege gerettet. "
                    f"Neue Bewertungen werden ans geheilte Journal angehaengt.")
            print(warn)
            # Optionales Modal-Dialog (nur wenn QApplication initialisiert ist)
            try:
                from PyQt6.QtWidgets import QApplication, QMessageBox
                if QApplication.instance() is not None:
                    QMessageBox.warning(
                        None, "Journal-Healing",
                        f"feedback-journal.json war truncated.\n\n"
                        f"Healing erfolgreich: {len(healed)} historische Eintraege gerettet.\n\n"
                        f"Original-Fehler: {e}\n\n"
                        f"Neue Bewertungen werden ans geheilte Journal angehaengt."
                    )
            except Exception:
                pass
            return healed

        # Healing fehlgeschlagen — User MUSS gewarnt werden, sonst gehen
        # historische Eintraege beim naechsten Save verloren.
        crit = (f"❌ KRITISCH: feedback-journal.json defekt UND nicht heilbar "
                f"(json.load: {e}; healing: {heal_err_msg or 'kein Cut-Boundary gefunden'}). "
                f"Wenn das Tool jetzt einen Save macht, werden ALLE historischen "
                f"Bewertungen UEBERSCHRIEBEN.")
        print(crit)
        try:
            from PyQt6.QtWidgets import QApplication, QMessageBox
            if QApplication.instance() is not None:
                reply = QMessageBox.critical(
                    None, "Journal-Defekt",
                    f"feedback-journal.json ist defekt UND nicht heilbar.\n\n"
                    f"Original-Fehler: {e}\n"
                    f"Healing-Fehler: {heal_err_msg or 'kein Cut-Boundary gefunden'}\n\n"
                    f"⚠️  WARNUNG: Wenn die Sichtkontrolle fortgesetzt wird, werden\n"
                    f"ALLE historischen Bewertungen UEBERSCHRIEBEN.\n\n"
                    f"Empfehlung: Tool ABBRECHEN, Datei manuell pruefen,\n"
                    f"ggf. aus Backup wiederherstellen.\n\n"
                    f"Trotzdem mit leerem Journal fortsetzen?",
                    QMessageBox.StandardButton.No | QMessageBox.StandardButton.Yes,
                    QMessageBox.StandardButton.No
                )
                if reply == QMessageBox.StandardButton.No:
                    # User entscheidet sich gegen Datenverlust — Tool soll abbrechen.
                    # Der Aufrufer bekommt eine Empty-List, aber wir setzen ein
                    # Markier-Attribut auf die Liste, damit _apply_all_decisions
                    # entscheiden kann, KEINEN Save zu machen.
                    sys.exit(2)
        except SystemExit:
            raise
        except Exception:
            pass
        # Fallback ohne GUI: leere Liste, aber laut.
        return []


def save_journal(journal_path: Path, entries: List[dict]) -> None:
    """Speichert Feedback-Journal atomar.

    Nutzt den gehärteten Writer aus config_loader (Retry/Backoff gegen
    Windows-Dateilocks, Replace-Schutz). Vormals lokale Temp+Replace-Variante
    ohne Retry und mit Silent-Swallow (print+weiter) wurde am 2026-04-20
    im Rahmen des Restbefund-Cleanups migriert — siehe Session-Log -f.

    Patch 2026-04-26-h (Sektion X.4): Bei Save-Fehler GUI-Modal-Dialog
    statt nur Print. Der User soll explizit erfahren, dass die in der GUI
    gemachten Bewertungen NICHT auf Disk gelandet sind. Dialog bietet
    Retry und Abbrechen — Retry ruft atomic_write_json erneut auf
    (entlang `max_retries=3` im Writer wartet sonst niemand mehr).
    """
    try:
        atomic_write_json(journal_path, entries)
        return
    except Exception as e:
        print(f"❌ Fehler beim Speichern von {journal_path}: {e}")
        # GUI-Modal-Dialog mit Retry/Abbrechen
        try:
            from PyQt6.QtWidgets import QApplication, QMessageBox
            if QApplication.instance() is not None:
                reply = QMessageBox.critical(
                    None, "Save-Fehler",
                    f"feedback-journal.json konnte NICHT gespeichert werden.\n\n"
                    f"Fehler: {e}\n\n"
                    f"Die in dieser Session gemachten Bewertungen sind NICHT\n"
                    f"auf Disk gelandet.\n\n"
                    f"Erneut versuchen?",
                    QMessageBox.StandardButton.Retry | QMessageBox.StandardButton.Abort,
                    QMessageBox.StandardButton.Retry
                )
                if reply == QMessageBox.StandardButton.Retry:
                    try:
                        atomic_write_json(journal_path, entries)
                        print(f"✅ Save nach Retry erfolgreich.")
                        return
                    except Exception as e2:
                        QMessageBox.critical(
                            None, "Save endgültig fehlgeschlagen",
                            f"Auch der zweite Speicher-Versuch ist gescheitert.\n\n"
                            f"Fehler: {e2}\n\n"
                            f"Bewertungen gehen verloren. Bitte Datei manuell pruefen."
                        )
                        print(f"❌ Save nach Retry erneut fehlgeschlagen: {e2}")
        except Exception as gui_err:
            print(f"⚠️  GUI-Dialog konnte nicht gezeigt werden: {gui_err}")


def collect_images(day_folder: Path) -> List[Tuple[Path, str]]:
    """
    Sammelt alle Bilder aus day_folder/*/
    Gibt Liste von (full_path, folder_name) zurück, sortiert nach Ordner, dann Dateiname.
    Ignoriert Unterordner wie 4k/, Mockups/, _kill/
    """
    images = []

    if not day_folder.exists():
        return images

    # Iteriere über Marketing-Ordner (eine Ebene tief)
    for marketing_folder in sorted(day_folder.iterdir()):
        if not marketing_folder.is_dir():
            continue

        if marketing_folder.name.startswith('_'):
            continue

        # Suche Bilder direkt im Ordner (nicht in Unterordnern wie 4k/)
        for file_path in sorted(marketing_folder.glob('*')):
            if file_path.suffix.lower() in ['.jpg', '.jpeg', '.png', '.webp']:
                images.append((file_path, marketing_folder.name))

    return images


def collect_images_multi(day_folders: List[Path]) -> List[Tuple[Path, str, Path]]:
    """
    Sammelt Bilder aus mehreren Tagesordnern (Multi-Day-Modus).
    Gibt Liste von (full_path, folder_name, day_folder) zurück.
    Sortiert pro Tagesordner nach Ordnername, dann Dateiname.
    Tagesordner werden in der übergebenen Reihenfolge verarbeitet (aufsteigend nach Datum).
    Ignoriert Unterordner wie 4k/, Mockups/, _kill/
    """
    images = []
    for day_folder in day_folders:
        if not day_folder.exists():
            continue
        for marketing_folder in sorted(day_folder.iterdir()):
            if not marketing_folder.is_dir():
                continue
            if marketing_folder.name.startswith('_'):
                continue
            for file_path in sorted(marketing_folder.glob('*')):
                if file_path.suffix.lower() in ['.jpg', '.jpeg', '.png', '.webp']:
                    images.append((file_path, marketing_folder.name, day_folder))
    return images


def _apply_nolist_filter_for_day(day_folder: Path, decisions_for_day: dict, images_for_day: List) -> dict:
    """
    Wertet nolist-Status für alle Produktordner eines Tagesordners aus.
    Zählt Bilder im Produkt-Hauptordner (nur iterdir(), nicht rekursiv,
    .jpg/.png/.webp case-insensitive). Bei <5: setzt status='nolist' in
    master-listings.json via config_loader-Helpers.

    Gibt Dict zurück: {product_folder_name: {"count": N, "nolist": bool}}
    """
    # Importiere config_loader-Helpers (lazy, damit import-Fehler nicht den Start blocken)
    try:
        from config_loader import load_master_listings, save_master_listings, find_master_item
        config_loader_available = True
    except ImportError:
        config_loader_available = False

    # Sammle distinct Produktordner aus diesem Tagesordner
    product_folders = set()
    for entry in images_for_day:
        # entry ist entweder (path, folder_name) oder (path, folder_name, day_folder)
        folder_name = entry[1]
        product_folders.add(folder_name)

    result = {}
    for folder_name in sorted(product_folders):
        product_path = day_folder / folder_name
        if not product_path.is_dir():
            result[folder_name] = {"count": 0, "nolist": False}
            continue

        # Bilder zählen (nur iterdir(), nicht rekursiv, bekannte Bildformate)
        count = sum(
            1 for f in product_path.iterdir()
            if f.is_file() and f.suffix.lower() in {'.jpg', '.jpeg', '.png', '.webp'}
        )
        is_nolist = count < 5
        result[folder_name] = {"count": count, "nolist": is_nolist}

        # nolist in master-listings.json setzen
        if is_nolist and config_loader_available:
            master_path = day_folder / "master-listings.json"
            if master_path.exists():
                try:
                    master_data = load_master_listings(master_path)
                    item = find_master_item(master_data, folder=folder_name)
                    if item is not None:
                        item["status"] = "nolist"
                        save_master_listings(master_path, master_data)
                        print(f"  ⚠️  nolist gesetzt: {folder_name} ({count} Bilder)")
                except Exception as e:
                    print(f"  ❌ nolist-Schreib-Fehler für {folder_name}: {e}")

    return result


# ============================================================================
# BACKGROUND IMAGE LOADER (QThread)
# ============================================================================

class ImageLoaderThread(QThread):
    """Lädt Bilder im Hintergrund."""
    image_loaded = pyqtSignal(QPixmap, str)  # pixmap, image_path

    def __init__(self, image_path: Path):
        super().__init__()
        self.image_path = image_path

    def run(self):
        """Lade Bild."""
        pixmap = QPixmap(str(self.image_path))
        self.image_loaded.emit(pixmap, str(self.image_path))


# ============================================================================
# KILL-OVERLAY DIALOG
# ============================================================================

class KillOverlay(QDialog):
    """
    Dialog für Kill-Tagging.
    Zeigt Chips für Tags, Freitext-Feld, OK/Cancel-Buttons.
    Modal, frameless, mit dunklem Overlay-Stil.
    """

    KILL_TAGS = [
        ("Anatomie", "A"),
        ("Verschmolzen", "V"),
        ("Langweilig", "L"),
        ("Komposition", "K"),
        ("Artefakte", "R"),
        ("Sonstiges", "S"),
    ]

    def __init__(self, parent=None):
        super().__init__(parent)
        self.selected_tag = None
        self.custom_note = ""

        # Dialog-Konfiguration: Modal, Frameless, auf Parent zentriert
        self.setWindowTitle("Kill-Tag Dialog")
        self.setWindowFlags(Qt.WindowType.Dialog | Qt.WindowType.FramelessWindowHint)
        self.setModal(True)
        self.setStyleSheet("""
            QDialog {
                background-color: rgba(0, 0, 0, 220);
                border: 2px solid white;
                border-radius: 10px;
            }
        """)

        layout = QVBoxLayout()
        layout.setSpacing(15)
        layout.setContentsMargins(20, 20, 20, 20)

        # Titel
        title = QLabel("Grund für Deletion:")
        title.setFont(QFont("Arial", 14, QFont.Weight.Bold))
        title.setStyleSheet("color: white;")
        layout.addWidget(title)

        # Tag-Chips
        chips_layout = QHBoxLayout()
        chips_layout.setSpacing(10)
        for tag_label, shortcut in self.KILL_TAGS:
            btn = QPushButton(f"{tag_label}\n({shortcut})")
            btn.setFont(QFont("Arial", 10))
            btn.setMinimumSize(80, 60)
            btn.setStyleSheet("""
                QPushButton {
                    background-color: #444;
                    color: white;
                    border: 1px solid #888;
                    border-radius: 5px;
                }
                QPushButton:hover {
                    background-color: #666;
                }
                QPushButton:pressed {
                    background-color: #0066cc;
                }
            """)
            btn.clicked.connect(lambda checked, tag=tag_label.lower().replace("ö", "oe").replace("ü", "ue").replace("ß", "ss"): self._select_tag(tag))
            chips_layout.addWidget(btn)

        layout.addLayout(chips_layout)

        # Freitext-Feld
        note_label = QLabel("Optionale Notiz:")
        note_label.setStyleSheet("color: white;")
        layout.addWidget(note_label)

        self.note_field = QTextEdit()
        self.note_field.setMaximumHeight(60)
        self.note_field.setStyleSheet("""
            QTextEdit {
                background-color: #222;
                color: white;
                border: 1px solid #666;
            }
        """)
        layout.addWidget(self.note_field)

        # OK/Cancel
        btn_layout = QHBoxLayout()
        ok_btn = QPushButton("OK (ENTER)")
        cancel_btn = QPushButton("Abbrechen (ESC)")

        for btn in [ok_btn, cancel_btn]:
            btn.setFont(QFont("Arial", 10))
            btn.setStyleSheet("""
                QPushButton {
                    background-color: #0066cc;
                    color: white;
                    border: 1px solid #004499;
                    border-radius: 5px;
                    padding: 5px;
                }
                QPushButton:hover {
                    background-color: #0080ff;
                }
            """)

        ok_btn.clicked.connect(self.accept_kill)
        cancel_btn.clicked.connect(self.reject)

        btn_layout.addWidget(ok_btn)
        btn_layout.addWidget(cancel_btn)
        layout.addLayout(btn_layout)

        self.setLayout(layout)
        self.setMinimumWidth(600)

    def _select_tag(self, tag_name):
        """Tag-Callback."""
        self.selected_tag = tag_name

    def accept_kill(self):
        """OK-Button Callback."""
        if self.selected_tag:
            self.custom_note = self.note_field.toPlainText().strip()
            # Dialog akzeptieren und schließen
            self.accept()
            # Callback nach dem Schließen
            if hasattr(self, 'on_kill_confirmed'):
                self.on_kill_confirmed()

    def show_overlay(self):
        """Zeige Dialog an (modal)."""
        self.selected_tag = None
        self.custom_note = ""
        self.note_field.clear()
        self.note_field.setFocus()
        # Dialog zentriert auf Parent positionieren
        self.exec()

    def keyPressEvent(self, event):
        """Tastatur-Handler für Dialog."""
        key = event.key()
        text = event.text().upper()

        # ENTER: OK bestätigen
        if text == "RETURN" or key == Qt.Key.Key_Return:
            self.accept_kill()
            return

        # ESC: Dialog abbrechen
        if key == Qt.Key.Key_Escape:
            self.reject()
            return

        # Tag-Shortcuts (A, V, L, K, R, S)
        for tag_label, shortcut in self.KILL_TAGS:
            if text == shortcut:
                self._select_tag(tag_label.lower().replace("ö", "oe").replace("ü", "ue").replace("ß", "ss"))
                return

        # Standard-Handler für andere Keys
        super().keyPressEvent(event)


# ============================================================================
# HAUPTFENSTER
# ============================================================================

class ImageReviewTool(QMainWindow):
    def __init__(self, day_folder: Optional[Path] = None, multi_day_folders: Optional[List[Path]] = None):
        """
        day_folder          — Einzelner Tagesordner (Single-Day-Modus, rückwärtskompatibel).
        multi_day_folders   — Liste von Tagesordnern (Multi-Day-Modus, neu ab 2026-05-02).
                              Wenn gesetzt, wird day_folder ignoriert.
        """
        super().__init__()

        # Pfade
        self.day_folder = Path(day_folder) if day_folder else None
        self.json_dir = JSON_DIR
        self.journal_path = JSON_DIR / "feedback-journal.json"

        # Multi-Day-Modus Daten
        # images enthält im Single-Day-Modus: List[Tuple[Path, str]]
        # Im Multi-Day-Modus: List[Tuple[Path, str, Path]] (Path, folder_name, day_folder)
        self._multi_day_mode: bool = multi_day_folders is not None and len(multi_day_folders) > 0
        self._multi_day_folders: List[Path] = multi_day_folders or []
        # Mapping: Index im images-Array → Tag-Abschnitts-Anfangs-Index
        # Wird in _load_multi_day() befüllt für den Day-Banner-Check
        self._day_section_starts: dict = {}  # {day_folder_str: first_index}

        # Daten
        self.pending_entries = []
        self.images = []
        self.current_index = 0
        self.journal = []
        self.decisions = {}  # {image_index: {"action": "top"|"kill"|"pass", "kill_tag": ..., "kill_note": ...}}

        # UI
        self.image_label = QLabel()
        self.prompt_label = QLabel()
        self.info_label = QLabel()
        self.kill_overlay = None

        # Zoom & Pan State
        self.original_pixmap = None
        self.zoom_factor = 1.0
        self.max_zoom = 5.0
        self.pan_offset = QPoint(0, 0)
        self.is_panning = False
        self.pan_start_pos = None

        # Hintergrund-Vorlade-Thread
        self.loader_thread = None

        self._init_ui()

        # Versuche Tagesordner zu laden
        if self._multi_day_mode:
            self._load_multi_day()
        elif self.day_folder:
            self._load_day_folder()
        else:
            self._ask_for_folder()

    def _init_ui(self):
        """Initialisiere UI."""
        self.setWindowTitle("Image Review Tool")
        self.setGeometry(100, 100, 1200, 800)

        # Central widget
        central = QWidget()
        self.setCentralWidget(central)

        main_layout = QVBoxLayout()
        main_layout.setSpacing(10)
        main_layout.setContentsMargins(10, 10, 10, 10)

        # Bild (zentriert, mit Zoom)
        scroll = QScrollArea()
        scroll.setWidgetResizable(True)
        scroll.setStyleSheet("QScrollArea { background-color: #222; }")
        scroll.setFocusPolicy(Qt.FocusPolicy.NoFocus)

        self.image_label.setAlignment(Qt.AlignmentFlag.AlignCenter)
        self.image_label.setStyleSheet("background-color: #111;")
        self.image_label.setFocusPolicy(Qt.FocusPolicy.NoFocus)
        scroll.setWidget(self.image_label)
        main_layout.addWidget(scroll, 20)

        # Info & Prompt
        info_frame = QFrame()
        info_frame.setStyleSheet("background-color: #f0f0f0; border-radius: 5px;")
        info_layout = QVBoxLayout()
        info_layout.setSpacing(2)

        self.info_label.setFont(QFont("Arial", 9, QFont.Weight.Bold))
        self.info_label.setStyleSheet("color: #333;")
        info_layout.addWidget(self.info_label)

        self.prompt_label.setWordWrap(True)
        self.prompt_label.setFont(QFont("Courier", 8))
        self.prompt_label.setStyleSheet("color: #666;")
        info_layout.addWidget(self.prompt_label)

        info_frame.setLayout(info_layout)
        main_layout.addWidget(info_frame, 1)

        # Tasten-Info
        keys_frame = QFrame()
        keys_frame.setStyleSheet("background-color: #e8f4f8; border-radius: 5px; padding: 10px;")
        keys_layout = QVBoxLayout()
        keys_layout.setSpacing(3)

        instructions = [
            "↑ = TOP  |  → = PASS  |  ↓ = KILL  |  ← = Zurück  |  ESC = Schließen  |  Mausrad = Zoom",
        ]
        for instruction in instructions:
            lbl = QLabel(instruction)
            lbl.setFont(QFont("Arial", 9))
            lbl.setStyleSheet("color: #333;")
            keys_layout.addWidget(lbl)

        keys_frame.setLayout(keys_layout)
        main_layout.addWidget(keys_frame)

        central.setLayout(main_layout)

        # Kill-Dialog (wird als modal angezeigt)
        self.kill_overlay = KillOverlay(self)
        self.kill_overlay.on_kill_confirmed = self._confirm_kill

        # Shortcuts
        self.setFocusPolicy(Qt.FocusPolicy.StrongFocus)

    def _ask_for_folder(self):
        """Frage Benutzer nach Tagesordner."""
        folder = QFileDialog.getExistingDirectory(
            self, "Wähle Tagesordner", ""
        )
        if folder:
            self.day_folder = Path(folder)
            self.json_dir = JSON_DIR
            self.journal_path = JSON_DIR / "feedback-journal.json"
            self._load_day_folder()
        else:
            self.close()

    def _load_day_folder(self):
        """Lade Bilder aus Tagesordner."""
        if not self.day_folder or not self.day_folder.exists():
            print(f"❌ Tagesordner nicht gefunden: {self.day_folder}")
            self.close()
            return

        # Lade Pending-Datei
        pending_path = JSON_DIR / "prompts_pending.json"
        self.pending_entries = load_pending_file(pending_path)

        # Sammle Bilder
        self.images = collect_images(self.day_folder)

        # Lade Journal
        self.journal = load_journal(self.journal_path)

        if not self.images:
            print(f"❌ Keine Bilder gefunden in {self.day_folder}")
            self.close()
            return

        self.current_index = 0
        self._display_image()

    def _load_multi_day(self):
        """
        Multi-Day-Modus: Lade Bilder aus mehreren Tagesordnern.
        Befüllt self.images als List[Tuple[Path, str, Path]].
        Setzt self._day_section_starts für Tag-Banner-Erkennung.
        """
        # Lade Pending-Datei (für Prompt-Matching)
        pending_path = JSON_DIR / "prompts_pending.json"
        self.pending_entries = load_pending_file(pending_path)

        # Sammle Bilder aus allen Tagesordnern
        all_images = collect_images_multi(self._multi_day_folders)

        # Lade Journal
        self.journal = load_journal(self.journal_path)

        if not all_images:
            print("❌ Keine Bilder gefunden in den übergebenen Tagesordnern.")
            self.close()
            return

        self.images = all_images

        # Day-Section-Starts berechnen: erster Index jedes neuen Tagesordners
        self._day_section_starts = {}
        for idx, entry in enumerate(self.images):
            day_folder_path = entry[2]  # (path, folder_name, day_folder)
            key = str(day_folder_path)
            if key not in self._day_section_starts:
                self._day_section_starts[key] = idx

        total_days = len(self._multi_day_folders)
        total_images = len(self.images)
        print(f"📅 Multi-Day-Modus: {total_days} Tag(e), {total_images} Bilder gesamt.")

        self.current_index = 0
        self._display_image()

    def _get_current_day_folder(self) -> Optional[Path]:
        """Gibt den Tagesordner des aktuell angezeigten Bildes zurück."""
        if not self.images or self.current_index >= len(self.images):
            return self.day_folder
        entry = self.images[self.current_index]
        if len(entry) == 3:
            return entry[2]  # Multi-Day: (path, folder_name, day_folder)
        return self.day_folder  # Single-Day

    def _show_day_transition_banner(self, day_folder: Path):
        """
        Zeigt kurz einen Fenstertitel-Banner an, wenn ein neuer Tagesordner beginnt.
        Minimaler Eingriff: nur Fenstertitel-Update für 2 Sekunden via QTimer.
        """
        date_str = day_folder.name  # = 'YYYY-MM-DD'
        day_index = list(self._day_section_starts.keys()).index(str(day_folder)) + 1
        total_days = len(self._multi_day_folders)
        banner = f"📅 Neuer Tag: {date_str} ({day_index}. von {total_days} Tagen)"
        self.setWindowTitle(banner)
        # Nach 2 Sekunden wird der Titel beim nächsten _display_image() überschrieben
        # (kein extra Timer nötig — der nächste _display_image()-Call setzt ihn zurück)

    def _show_day_end_dialog(self, day_folder: Path):
        """
        Zeigt Tages-Abschluss-Dialog nach dem letzten Bild eines Tagesordners.
        Wendet nolist-Filter für diesen Tag an.
        Wird aufgerufen, bevor zum nächsten Tag gewechselt wird.
        """
        date_str = day_folder.name  # = 'YYYY-MM-DD'

        # Bilder dieses Tags aus decisions sammeln
        images_this_day = [
            (idx, entry) for idx, entry in enumerate(self.images)
            if len(entry) == 3 and str(entry[2]) == str(day_folder)
        ]
        # Single-Day: alle Bilder
        if not self._multi_day_mode:
            images_this_day = list(enumerate(self.images))

        n_top = sum(1 for idx, _ in images_this_day
                    if self.decisions.get(idx, {}).get("action") == "top")
        n_pass = sum(1 for idx, _ in images_this_day
                     if self.decisions.get(idx, {}).get("action") == "pass")
        n_kill = sum(1 for idx, _ in images_this_day
                     if self.decisions.get(idx, {}).get("action") == "kill")

        # nolist-Filter anwenden
        entries_this_day = [(entry[0], entry[1]) for _, entry in images_this_day]
        nolist_result = _apply_nolist_filter_for_day(day_folder, self.decisions, entries_this_day)

        # Dialog-Text aufbauen
        nolist_lines = []
        for product_name, info in nolist_result.items():
            count = info["count"]
            is_nolist = info["nolist"]
            status_text = "⚠️ NOLIST gesetzt" if is_nolist else "✅ genug"
            nolist_lines.append(f"  - {product_name}: {count} Bilder — {status_text}")

        nolist_section = "\n".join(nolist_lines) if nolist_lines else "  (keine Produkte gefunden)"

        msg_text = (
            f"Tag {date_str} abgeschlossen.\n"
            f"{n_top} Top, {n_pass} Pass, {n_kill} Kill.\n\n"
            f"nolist-Status:\n{nolist_section}"
        )

        from PyQt6.QtWidgets import QMessageBox
        QMessageBox.information(self, f"Tag {date_str} abgeschlossen", msg_text)

    def _display_image(self):
        """Zeige aktuelles Bild an."""
        if not self.images or self.current_index >= len(self.images):
            # Im Multi-Day-Modus: nolist für letzten Tag + Summary
            if self._multi_day_mode and self.images:
                last_entry = self.images[-1]
                last_day = last_entry[2] if len(last_entry) == 3 else self.day_folder
                if last_day is not None:
                    self._show_day_end_dialog(last_day)
            self._apply_all_decisions()
            self._show_summary()
            return

        # Multi-Day-Modus: Tuple hat 3 Elemente (path, folder_name, day_folder)
        # Single-Day-Modus: Tuple hat 2 Elemente (path, folder_name)
        entry = self.images[self.current_index]
        if len(entry) == 3:
            image_path, folder_name, current_day_folder = entry
        else:
            image_path, folder_name = entry
            current_day_folder = self.day_folder

        # Multi-Day: Prüfe ob neuer Tag beginnt (Banner + Day-End-Dialog für vorherigen Tag)
        if self._multi_day_mode and self._day_section_starts:
            day_key = str(current_day_folder)
            section_start = self._day_section_starts.get(day_key, 0)

            if self.current_index == section_start and self.current_index > 0:
                # Neuer Tag — erst Abschluss-Dialog für vorherigen Tag zeigen
                prev_entry = self.images[self.current_index - 1]
                prev_day = prev_entry[2] if len(prev_entry) == 3 else self.day_folder
                if prev_day is not None and str(prev_day) != day_key:
                    self._show_day_end_dialog(prev_day)
                # Banner für neuen Tag
                self._show_day_transition_banner(current_day_folder)

        # Lade Bild
        pixmap = QPixmap(str(image_path))
        if pixmap.isNull():
            self._next_image()
            return

        # Speichere Original-Pixmap für Zoom
        self.original_pixmap = pixmap
        self.zoom_factor = 1.0
        self.pan_offset = QPoint(0, 0)

        # Skaliere auf Fenster (Fit-to-Window)
        scaled = pixmap.scaledToWidth(
            self.image_label.width() or 800,
            Qt.TransformationMode.SmoothTransformation
        )
        self.image_label.setPixmap(scaled)
        self.image_label.setScaledContents(False)

        # Info & Prompt (folder_name aus collect_images, nicht aus Pfad ableiten)
        prompt_data = find_prompt_for_image(image_path, self.pending_entries, folder_name=folder_name)

        # Zeige aktuelle Entscheidung (falls vorhanden)
        decision_text = self._get_decision_text()

        # Tag-Header: YYYY-MM-DD — marketing_title — Bild N/Gesamt
        if self._multi_day_mode and current_day_folder is not None:
            date_str = current_day_folder.name  # = 'YYYY-MM-DD'
            info_text = (f"{date_str} — {folder_name} — "
                         f"Bild {self.current_index + 1}/{len(self.images)}")
        else:
            info_text = f"{self.current_index + 1}/{len(self.images)} | {folder_name} | {image_path.name}"

        if decision_text:
            info_text += f"  |  {decision_text}"
        self.info_label.setText(info_text)

        prompt_text = f"Prompt: {prompt_data['prompt']}\n[Generator: {prompt_data['generator']}]"
        self.prompt_label.setText(prompt_text)

        if self._multi_day_mode and current_day_folder is not None:
            date_str = current_day_folder.name
            self.setWindowTitle(f"Image Review — {date_str} — {folder_name} — {image_path.name}")
        else:
            self.setWindowTitle(f"Image Review - {image_path.name}")

        # Holster Focus für Tastatur-Eingaben
        self.setFocus()

    def _next_image(self):
        """Nächstes Bild."""
        self.current_index += 1
        self._display_image()

    def _prev_image(self):
        """Vorheriges Bild."""
        if self.current_index > 0:
            self.current_index -= 1
            self._display_image()

    def _get_decision_text(self) -> str:
        """Gibt visuellen Text der aktuellen Entscheidung für Bild zurück (falls vorhanden)."""
        decision = self.decisions.get(self.current_index)
        if not decision:
            return ""

        action = decision.get("action", "")
        if action == "top":
            return "✓ TOP"
        elif action == "pass":
            return "→ PASS"
        elif action == "kill":
            tag = decision.get("kill_tag", "unknown")
            return f"✗ KILL ({tag})"
        return ""

    def _preload_next_image(self):
        """Lade nächstes Bild im Hintergrund vor."""
        next_index = self.current_index + 1
        if next_index < len(self.images):
            # Kompatibel mit 2-Tuple (Single-Day) und 3-Tuple (Multi-Day)
            image_path = self.images[next_index][0]
            if self.loader_thread is None or not self.loader_thread.isRunning():
                self.loader_thread = ImageLoaderThread(image_path)
                self.loader_thread.image_loaded.connect(self._on_preload_complete)
                self.loader_thread.start()

    def _rate_top(self):
        """Bewertung: TOP (nur merken, nicht ausführen)."""
        if not self.images:
            return

        self.decisions[self.current_index] = {"action": "top"}
        self._next_image()
        self._preload_next_image()

    def _rate_pass(self):
        """Bewertung: PASS (akzeptiert, wird im Journal festgehalten)."""
        if not self.images:
            return

        self.decisions[self.current_index] = {"action": "pass"}
        self._next_image()
        self._preload_next_image()

    def _show_kill_overlay(self):
        """Zeige Kill-Dialog (modal)."""
        self.kill_overlay.show_overlay()

    def _confirm_kill(self):
        """Bestätige Kill-Aktion (nur merken, nicht verschieben)."""
        if not self.images or not self.kill_overlay.selected_tag:
            return

        self.decisions[self.current_index] = {
            "action": "kill",
            "kill_tag": self.kill_overlay.selected_tag,
            "kill_note": self.kill_overlay.custom_note or None
        }

        self._next_image()
        self._preload_next_image()

    def _on_preload_complete(self, pixmap: QPixmap, image_path: str):
        """Callback wenn Vorladen abgeschlossen ist."""
        # Die Pixmap ist bereits im Cache der nächsten Anzeige
        pass

    def _apply_all_decisions(self):
        """Wende alle Entscheidungen auf einmal an (am Ende der Session)."""
        # Sortiere nach Index in absteigender Reihenfolge, um sicher zu verschieben/löschen
        sorted_indices = sorted(self.decisions.keys(), reverse=True)

        for idx in sorted_indices:
            if idx < 0 or idx >= len(self.images):
                continue

            image_path, folder_name = self.images[idx]
            # Produktordner-Namen VOR jeder Dateioperation sichern.
            # Nach image_path.rename(...) in _kill/<tag>/ ist image_path.parent.name
            # der Kill-Tag-Ordner, nicht mehr der Produktordner -> Matching würde fehlschlagen.
            product_folder_name = folder_name
            original_filename = image_path.name
            decision = self.decisions[idx]
            action = decision.get("action", "")

            if action == "kill":
                # Verschiebe Bild nach _kill/<tag>/
                kill_tag = decision.get("kill_tag", "unknown")
                kill_folder = image_path.parent / "_kill" / kill_tag
                kill_folder.mkdir(parents=True, exist_ok=True)

                new_path = kill_folder / image_path.name
                try:
                    image_path.rename(new_path)
                except Exception as e:
                    print(f"❌ Fehler beim Verschieben von {image_path.name}: {e}")
                    continue

                # Journal-Eintrag für Kill — folder_name explizit übergeben,
                # da image_path.parent nach rename() auf _kill/<tag>/ zeigt.
                prompt_data = find_prompt_for_image(
                    image_path, self.pending_entries, folder_name=product_folder_name
                )
                journal_entry = {
                    "filename": original_filename,
                    "folder": product_folder_name,
                    "prompt": prompt_data["prompt"],
                    "generator": prompt_data["generator"],
                    "rating": "kill",
                    "kill_tag": kill_tag,
                    "kill_note": decision.get("kill_note") or None,
                    "timestamp": datetime.now().isoformat()
                }
                self.journal.append(journal_entry)

            elif action == "top":
                # Journal-Eintrag für Top
                prompt_data = find_prompt_for_image(
                    image_path, self.pending_entries, folder_name=product_folder_name
                )
                journal_entry = {
                    "filename": original_filename,
                    "folder": product_folder_name,
                    "prompt": prompt_data["prompt"],
                    "generator": prompt_data["generator"],
                    "rating": "top",
                    "kill_tag": None,
                    "kill_note": None,
                    "timestamp": datetime.now().isoformat()
                }
                self.journal.append(journal_entry)

            elif action == "pass":
                # Journal-Eintrag für Pass
                prompt_data = find_prompt_for_image(
                    image_path, self.pending_entries, folder_name=product_folder_name
                )
                journal_entry = {
                    "filename": original_filename,
                    "folder": product_folder_name,
                    "prompt": prompt_data["prompt"],
                    "generator": prompt_data["generator"],
                    "rating": "pass",
                    "kill_tag": None,
                    "kill_note": None,
                    "timestamp": datetime.now().isoformat()
                }
                self.journal.append(journal_entry)

        # Speichere Journal am Ende
        save_journal(self.journal_path, self.journal)

    def _calculate_stats(self) -> dict:
        """Berechnet Statistiken aus self.decisions."""
        stats = {"top": 0, "kill": 0, "pass": 0, "kill_tags": {}}

        for decision in self.decisions.values():
            action = decision.get("action", "")
            if action == "top":
                stats["top"] += 1
            elif action == "kill":
                stats["kill"] += 1
                tag = decision.get("kill_tag", "unknown")
                stats["kill_tags"][tag] = stats["kill_tags"].get(tag, 0) + 1
            elif action == "pass":
                stats["pass"] += 1

        return stats

    def _show_summary(self):
        """Zeige Zusammenfassung."""
        stats = self._calculate_stats()

        session_total = stats['top'] + stats['kill'] + stats['pass']
        summary_text = (
            f"Bildkontrolle abgeschlossen!\n\n"
            f"Diese Session: {session_total}\n"
            f"Top: {stats['top']}\n"
            f"Kill: {stats['kill']}\n"
            f"Pass: {stats['pass']}\n\n"
            f"Kill-Tags:"
        )

        for tag, count in sorted(stats["kill_tags"].items()):
            summary_text += f"\n  {tag}: {count}"

        from PyQt6.QtWidgets import QMessageBox
        QMessageBox.information(self, "Fertig", summary_text)
        self.close()

    def keyPressEvent(self, event):
        """Tastatur-Handler."""
        key = event.key()
        text = event.text().upper()

        # Hauptfenster-Shortcuts (Dialog hat eigene keyPressEvent)
        if key == Qt.Key.Key_Escape:
            self.close()
        elif key == Qt.Key.Key_Up:
            self._rate_top()
        elif key == Qt.Key.Key_Right:
            self._rate_pass()
        elif key == Qt.Key.Key_Down:
            self._show_kill_overlay()
        elif key == Qt.Key.Key_Left:
            self._prev_image()

    def wheelEvent(self, event: QWheelEvent):
        """Mausrad-Zoom mit Zoom-Anker auf Mausposition."""
        if not self.original_pixmap:
            return

        delta = event.angleDelta().y()
        old_zoom = self.zoom_factor

        # Zoom-Änderung: 10% pro Schritt
        if delta > 0:
            self.zoom_factor = min(self.zoom_factor * 1.1, self.max_zoom)
        elif delta < 0:
            self.zoom_factor = max(self.zoom_factor / 1.1, 1.0)

        # Wenn zoom_factor = 1.0, Fit-to-Window
        if self.zoom_factor == 1.0:
            self.pan_offset = QPoint(0, 0)
            scaled = self.original_pixmap.scaledToWidth(
                self.image_label.width() or 800,
                Qt.TransformationMode.SmoothTransformation
            )
            self.image_label.setPixmap(scaled)
        else:
            # Zoom: skaliere Original-Pixmap
            new_width = int(self.original_pixmap.width() * self.zoom_factor)
            scaled = self.original_pixmap.scaledToWidth(
                new_width,
                Qt.TransformationMode.SmoothTransformation
            )
            self.image_label.setPixmap(scaled)

    def mousePressEvent(self, event):
        """Start Pan bei linker Maustaste (nur wenn reingezoomt)."""
        if event.button() == Qt.MouseButton.LeftButton and self.zoom_factor > 1.0:
            self.is_panning = True
            self.pan_start_pos = event.pos()

    def mouseMoveEvent(self, event):
        """Pan bei Mausbewegung."""
        if self.is_panning and self.pan_start_pos:
            delta = event.pos() - self.pan_start_pos
            self.pan_offset += delta
            self.pan_start_pos = event.pos()

    def mouseReleaseEvent(self, event):
        """End Pan bei Mausfreigabe."""
        if event.button() == Qt.MouseButton.LeftButton:
            self.is_panning = False
            self.pan_start_pos = None

    def mouseDoubleClickEvent(self, event):
        """Doppelklick: Zoom zurücksetzen auf Fit-to-Window."""
        if not self.original_pixmap:
            return
        self.zoom_factor = 1.0
        self.pan_offset = QPoint(0, 0)
        scaled = self.original_pixmap.scaledToWidth(
            self.image_label.width() or 800,
            Qt.TransformationMode.SmoothTransformation
        )
        self.image_label.setPixmap(scaled)


# ============================================================================
# MAIN
# ============================================================================

def main():
    """
    Einstiegspunkt.

    Startverhalten:
    1. sys.argv[1] ist ein gültiger Pfad → Single-Day-Modus (rückwärtskompatibel,
       identisches Verhalten wie vor Multi-Day-Erweiterung). Day-End-Dialog
       läuft ebenfalls im Single-Day-Modus.

    2. Kein Argument → Multi-Day-Modus (neu ab 2026-05-02):
       Liest prompts_pending.json, filtert status == "Renamed", gruppiert nach
       day_folder, sortiert alphabetisch aufsteigend (= chronologisch).
       Wenn Einträge gefunden: Alle Tagesordner in einer GUI-Session.
       Wenn keine "Renamed"-Einträge: Fallback auf Dialog (Single-Day, wie bisher).
    """
    app = QApplication(sys.argv)

    # Single-Day-Modus wenn Pfad-Argument übergeben
    if len(sys.argv) > 1:
        day_folder = sys.argv[1]
        tool = ImageReviewTool(day_folder=day_folder)
        tool.showMaximized()
        tool.setFocus()
        sys.exit(app.exec())

    # Multi-Day-Modus: prompts_pending.json auswerten
    pending_path = JSON_DIR / "prompts_pending.json"
    multi_day_folders: List[Path] = []

    try:
        pending_entries = load_pending_file(pending_path)
        # Filtere nach status == "Renamed", gruppiere nach day_folder
        day_folder_map: dict = {}  # day_folder_str → True (ordered insertion Python 3.7+)
        for entry in pending_entries:
            if entry.get("status") == "Renamed":
                df = entry.get("day_folder", "")
                if df and df not in day_folder_map:
                    day_folder_map[df] = True

        if day_folder_map:
            # Sortiere alphabetisch aufsteigend (= chronologisch nach Datum)
            sorted_day_folders = sorted(day_folder_map.keys())
            # Nur existierende Ordner übernehmen
            for df_str in sorted_day_folders:
                df_path = Path(df_str)
                if df_path.exists():
                    multi_day_folders.append(df_path)
                else:
                    print(f"⚠️  Tagesordner nicht gefunden, übersprungen: {df_str}")

    except Exception as e:
        print(f"⚠️  Fehler beim Lesen von prompts_pending.json: {e}")

    if multi_day_folders:
        print(f"📅 Multi-Day-Modus: {len(multi_day_folders)} Tagesordner gefunden.")
        for df in multi_day_folders:
            print(f"   - {df}")
        tool = ImageReviewTool(multi_day_folders=multi_day_folders)
    else:
        # Fallback: Single-Day mit Dialog (wie bisher wenn kein Argument)
        print("ℹ️  Keine 'Renamed'-Einträge in prompts_pending.json — Single-Day-Modus (Dialog).")
        tool = ImageReviewTool()

    tool.showMaximized()
    tool.setFocus()
    sys.exit(app.exec())


if __name__ == "__main__":
    main()
