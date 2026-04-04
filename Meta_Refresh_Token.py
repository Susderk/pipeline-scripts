#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Meta_Refresh_Token.py

Erneuert den Meta Page Access Token (Facebook + Instagram).

Ablauf:
  1. META_APP_ID und META_APP_SECRET abfragen (oder aus Umgebung lesen)
  2. Short-Lived User Token vom User entgegennehmen
     (manuell aus https://developers.facebook.com/tools/accesstoken/)
  3. Short-Lived → Long-Lived User Token tauschen (gültig 60 Tage)
  4. Nie ablaufenden Page Access Token aus /me/accounts holen
  5. Beide .env-Dateien aktualisieren (YAML-Format)
  6. Windows-Umgebungsvariable META_ACCESS_TOKEN via setx setzen

Benötigt:
  - Meta App ID + App Secret (aus Meta Developer Portal)
  - Einmalig kurzzeitiger User Token (aus Token-Tool oder Login)

Wichtig:
  - APP_ID und APP_SECRET werden NICHT gespeichert
  - Der neue Page Token läuft nicht ab (solange App-Verbindung aktiv)
"""

import os
import re
import sys
import subprocess
from pathlib import Path

try:
    import requests
except ImportError:
    print("❌ 'requests' nicht installiert. Bitte: pip install requests")
    sys.exit(1)

# =============================================================================
# KONFIGURATION
# =============================================================================

SCRIPT_DIR   = Path(__file__).resolve().parent
META_VERSION = "v25.0"
GRAPH_URL    = "https://graph.facebook.com"

# Pfade zu den .env-Dateien relativ zum Skript-Ordner
ENV_FILES = [
    SCRIPT_DIR.parent / ".env",          # 01 Python Skript/.env
    SCRIPT_DIR.parent / "publisher" / ".env",  # 01 Python Skript/publisher/.env
]

# =============================================================================
# HILFSFUNKTIONEN
# =============================================================================

def ask(prompt: str, secret: bool = False) -> str:
    """Fragt den User nach einem Wert. Leer = Abbruch."""
    import getpass
    try:
        val = getpass.getpass(prompt) if secret else input(prompt)
    except (KeyboardInterrupt, EOFError):
        print("\nAbgebrochen.")
        sys.exit(0)
    val = val.strip()
    if not val:
        print("❌ Leere Eingabe – Abbruch.")
        sys.exit(1)
    return val


def get_long_lived_user_token(app_id: str, app_secret: str, short_token: str) -> str:
    """Tauscht Short-Lived User Token gegen Long-Lived User Token (60 Tage)."""
    print("\n🔄 Tausche Short-Lived → Long-Lived User Token...")
    resp = requests.get(
        f"{GRAPH_URL}/oauth/access_token",
        params={
            "grant_type":        "fb_exchange_token",
            "client_id":         app_id,
            "client_secret":     app_secret,
            "fb_exchange_token": short_token,
        },
        timeout=15,
    )
    if resp.status_code != 200:
        print(f"❌ Fehler {resp.status_code}: {resp.text[:300]}")
        sys.exit(1)

    data = resp.json()
    if "error" in data:
        print(f"❌ API-Fehler: {data['error'].get('message', data)}")
        sys.exit(1)

    token = data.get("access_token", "")
    expires = data.get("expires_in", "?")
    print(f"   ✅ Long-Lived User Token erhalten (läuft ab in {expires} Sekunden ≈ 60 Tage)")
    return token


def get_page_access_token(long_lived_user_token: str, page_id: str) -> str:
    """Holt den nie ablaufenden Page Access Token für eine bestimmte Page ID."""
    print(f"\n🔄 Hole Page Access Token für Page ID {page_id}...")
    resp = requests.get(
        f"{GRAPH_URL}/me/accounts",
        params={"access_token": long_lived_user_token},
        timeout=15,
    )
    if resp.status_code != 200:
        print(f"❌ Fehler {resp.status_code}: {resp.text[:300]}")
        sys.exit(1)

    data = resp.json()
    if "error" in data:
        print(f"❌ API-Fehler: {data['error'].get('message', data)}")
        sys.exit(1)

    pages = data.get("data", [])
    if not pages:
        print("❌ Keine Pages gefunden. Prüfe, ob der Token ausreichende Berechtigungen hat.")
        sys.exit(1)

    # Passende Page suchen
    for page in pages:
        if page.get("id") == page_id:
            token = page.get("access_token", "")
            name  = page.get("name", page_id)
            print(f"   ✅ Page Token erhalten für: {name}")
            return token

    # Page ID nicht gefunden → alle anzeigen und Auswahl
    print(f"\n⚠️  Page ID '{page_id}' nicht gefunden. Verfügbare Pages:")
    for i, page in enumerate(pages):
        print(f"   [{i}] {page.get('name', '?')}  (ID: {page.get('id', '?')})")
    try:
        choice = int(input("\nNummer wählen: ").strip())
        selected = pages[choice]
    except (ValueError, IndexError):
        print("❌ Ungültige Auswahl – Abbruch.")
        sys.exit(1)

    token = selected.get("access_token", "")
    print(f"   ✅ Page Token erhalten für: {selected.get('name', '?')}")
    return token


def update_env_file(env_path: Path, new_token: str) -> bool:
    """
    Ersetzt META_ACCESS_TOKEN in einer .env-Datei.
    Unterstützt YAML-Format (KEY: "value") und Standard-Format (KEY=value).
    Gibt True zurück wenn erfolgreich, False wenn Datei nicht existiert.
    """
    if not env_path.exists():
        print(f"   ⏭  Nicht gefunden, übersprungen: {env_path}")
        return False

    content = env_path.read_text(encoding="utf-8")

    # Ersetze YAML-Format: META_ACCESS_TOKEN: "..."
    pattern_yaml = r'(META_ACCESS_TOKEN\s*:\s*)["\']?[^"\'#\n]+["\']?'
    replacement_yaml = rf'\g<1>"{new_token}"'

    # Ersetze Standard-Format: META_ACCESS_TOKEN=...
    pattern_std = r'(META_ACCESS_TOKEN\s*=\s*)["\']?[^"\'#\n]+["\']?'
    replacement_std = rf'\g<1>"{new_token}"'

    new_content = re.sub(pattern_yaml, replacement_yaml, content)
    new_content = re.sub(pattern_std, replacement_std, new_content)

    if new_content == content:
        print(f"   ⚠️  META_ACCESS_TOKEN nicht gefunden in: {env_path.name}")
        return False

    env_path.write_text(new_content, encoding="utf-8")
    print(f"   ✅ Aktualisiert: {env_path}")
    return True


def set_windows_env_var(name: str, value: str) -> bool:
    """Setzt eine Windows-Umgebungsvariable permanent via setx (nur auf Windows)."""
    if os.name != "nt":
        return False
    try:
        subprocess.run(["setx", name, value], check=True, capture_output=True)
        print(f"   ✅ Windows-Umgebungsvariable '{name}' gesetzt (setx)")
        return True
    except subprocess.CalledProcessError as e:
        print(f"   ⚠️  setx fehlgeschlagen: {e}")
        return False
    except FileNotFoundError:
        print("   ⚠️  setx nicht gefunden – Windows-Umgebungsvariable nicht gesetzt")
        return False


# =============================================================================
# HAUPTLOGIK
# =============================================================================

def main():
    print("=" * 60)
    print("  Meta Token Refresh")
    print("=" * 60)
    print()
    print("Benötigt:")
    print("  • Meta App ID + App Secret (aus developers.facebook.com)")
    print("  • Kurzzeitiger User Token")
    print("    → https://developers.facebook.com/tools/accesstoken/")
    print()

    # ── Page ID aus .env lesen ────────────────────────────────────────────────
    page_id = os.environ.get("FB_PAGE_ID", "").strip()
    if not page_id:
        # Fallback: direkt aus .env-Datei lesen
        env_main = SCRIPT_DIR.parent / ".env"
        if env_main.exists():
            for line in env_main.read_text(encoding="utf-8").splitlines():
                line = line.strip()
                if re.match(r"meta_page_id\s*[:=]", line, re.IGNORECASE):
                    _, _, val = line.partition(":" if ":" in line else "=")
                    page_id = val.strip().strip('"\'')
                    break

    if page_id:
        print(f"📋 Page ID aus Konfiguration: {page_id}")
    else:
        page_id = ask("Facebook Page ID: ")

    # ── Credentials abfragen ──────────────────────────────────────────────────
    app_id     = os.environ.get("META_APP_ID",     "").strip() or ask("Meta App ID:     ")
    app_secret = os.environ.get("META_APP_SECRET", "").strip() or ask("Meta App Secret: ", secret=True)
    short_token = ask(
        "\nShort-Lived User Token\n"
        "(von https://developers.facebook.com/tools/accesstoken/ ): "
    )

    # ── Token-Exchange ────────────────────────────────────────────────────────
    long_token  = get_long_lived_user_token(app_id, app_secret, short_token)
    page_token  = get_page_access_token(long_token, page_id)

    print(f"\n🔑 Neuer Page Access Token (nie ablaufend):")
    print(f"   {page_token[:40]}...{page_token[-10:]}")

    # ── .env-Dateien aktualisieren ────────────────────────────────────────────
    print("\n📝 Aktualisiere .env-Dateien...")
    for env_path in ENV_FILES:
        update_env_file(env_path, page_token)

    # ── Windows-Umgebungsvariable setzen ─────────────────────────────────────
    print("\n💻 Setze Windows-Umgebungsvariable...")
    if not set_windows_env_var("META_ACCESS_TOKEN", page_token):
        print("   ℹ️  Manuell setzen (einmalig, als Admin):")
        print(f'   setx META_ACCESS_TOKEN "{page_token}"')
        print("   Danach Terminal/Python neu starten.")

    print()
    print("=" * 60)
    print("✅ Token-Refresh abgeschlossen.")
    print("   Neuer Token ist gültig – läuft nicht ab.")
    print("=" * 60)


if __name__ == "__main__":
    main()
