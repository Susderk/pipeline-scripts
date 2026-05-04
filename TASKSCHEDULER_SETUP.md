# Windows Task Scheduler Setup — DPS Pipeline Morgen-Block

Stand: 2026-04-26 (Pipeline-Split-Patch + Listings-Gate-Doku-Fix)

Der Morgen-Block der DPS-Pipeline (Step 07a → 11) läuft automatisch via
Windows Task Scheduler. Diese Anleitung beschreibt die Einrichtung.

## Voraussetzungen

- Vorabend-Block (Step 01–06) ist am Vorabend manuell gelaufen und sauber
  durch (Sichtkontrolle ✅, Cowork-Mockup-Task ✅, Lock-Datei gelöscht).
- Python 3.9+ installiert und im System-PATH (`python --version` muss in
  PowerShell funktionieren).
- Scheduler darf Aufgaben mit Vollzugriff ausführen (gleiche Rechte wie
  Ingo's interaktiver Account).

> **Hinweis:** Die `etsy-listing.csv` existiert noch NICHT, wenn der Morgen-Lauf
> startet — sie wird erst IM Morgen-Lauf vom Listings-Gate (zwischen Step_09
> und Step_10) geschrieben. Befüllung erfolgt während des Laufs an einer
> Pause; siehe Abschnitt „Ablauf Morgen-Lauf" weiter unten.

## Ablauf Morgen-Lauf (mit Listings-Gate-Pause)

Der Morgen-Lauf ist **monolithisch** — `Start_Scripts.py --morning` läuft
Step 07a → 09 → **Listings-Gate (Pause)** → Step 10 → 11. Reihenfolge:

1. **Pre-Flight-Check** verifiziert Voraussetzungen (a)–(e).
2. **Step 07a/07/08/09** laufen automatisch durch (Musik → Video → YouTube
   → Upscaling/GitHub).
3. **Listings-Gate (Pause):** Direkt nach Step_09 schreibt
   `Start_Scripts.py` die `etsy-listing.csv` aus `master-listings.json` in
   den Tagesordner und pausiert mit `input("ENTER drücken …")`. Das
   Console-Window bleibt offen und wartet auf den Operator.
4. **Operator-Eingriff:** Thomas öffnet `etsy-listing.csv` (Doppelklick im
   Tagesordner, Excel öffnet die Datei). Der Promo-Code ist automatisch mit
   `NEWCUST50` als Pre-Fill befüllt — kann überschrieben werden, falls
   gewünscht. Thomas trägt nur noch `product_link` aus Etsy in jede Zeile
   ein, speichert, schließt Excel.
5. **ENTER im Console-Window:** Pipeline liest die CSV zurück, synct
   `product_link` und `promo_code` per ID-Match nach
   `master-listings.json`.
6. **Step 10/11:** Pipeline läuft automatisch weiter (Etsy-Listing-API
   sofern API-Keys vorhanden; Meta Video Post FB+IG).
7. **End-of-Pipeline-Hook:** Öffnet `payhip-listing.csv` und
   `stockportal-listing.csv` in Excel (sofern `open_payhip_and_stockportal_at_end`
   aktiviert).

> **Console-Window muss während des gesamten Laufs offen bleiben.** Bei
> Windows-Update-Reboot mitten im Lauf bricht die Pipeline ab. Das ist
> bewusst akzeptiert, weil das Etsy-Approval noch aussteht; sobald Etsy
> live ist, wird das Listings-Gate komplett automatisiert (Promo-Code
> Pre-Fill bleibt, `product_link` kommt aus dem Etsy-API-Response). Bis
> dahin: Console offen lassen, Reboot-Risiko in Kauf nehmen. Separates
> Folge-Ticket nach Etsy-Approval.

> **Promo-Code-Pre-Fill (ab 2026-04-26-j):** Standardmäßig wird in der CSV
> `promo_code = NEWCUST50` vorbelegt. Operator kann den Wert in Excel
> überschreiben. Wenn `master-listings.json` für ein Item bereits einen
> anderen `promo_code` enthält (z.B. aus einem vorherigen Lauf), bleibt
> dieser Wert erhalten und wird NICHT durch den Pre-Fill überschrieben.

## Task-Scheduler-Eintrag anlegen

### Schritt 1: Aufgabenplanung öffnen

`Start` → `Aufgabenplanung` (oder `taskschd.msc`).

### Schritt 2: Neue Aufgabe erstellen

Rechtsklick auf `Aufgabenplanungsbibliothek` → `Aufgabe erstellen…`
(NICHT „Einfache Aufgabe erstellen" — wir brauchen die erweiterten Optionen).

### Schritt 3: Tab „Allgemein"

- **Name:** `DPS Pipeline Morgen-Block`
- **Beschreibung:** `Automatischer Morgen-Lauf Step 07a–11 der DPS-Pipeline`
- **Sicherheitsoptionen:**
  - „Nur ausführen, wenn der Benutzer angemeldet ist" aktivieren
    (so bleibt das Console-Window sichtbar — der Listings-Gate-Pause-Schritt
    benötigt eine sichtbare Konsole für die ENTER-Eingabe).
  - „Mit höchsten Privilegien ausführen" aktivieren.
- **Konfigurieren für:** `Windows 10` (oder höher).

### Schritt 4: Tab „Trigger"

→ `Neu…`

- **Aufgabe starten:** `Nach einem Zeitplan`
- **Einstellungen:** `Täglich`
- **Start:** Heute, **03:40:00** (Berlin-Lokalzeit)
- **Wiederholen alle:** `1 Tage`
- Häkchen `Aktiviert` ✅

→ `OK`

### Schritt 5: Tab „Aktionen"

→ `Neu…`

- **Aktion:** `Programm starten`
- **Programm/Skript:** `python` (oder voller Pfad zu `python.exe`, z.B.
  `C:\Users\ingos\AppData\Local\Programs\Python\Python314\python.exe`)
- **Argumente hinzufügen:** `Start_Scripts.py --morning`
- **Starten in (optional):** `C:\Companies\DPS\01 Python Skript\pipeline`

→ `OK`

### Schritt 6: Tab „Bedingungen"

- **Leerlauf:** alle Häkchen entfernen.
- **Energie:**
  - „Aufgabe nur starten, falls der Computer im Netzbetrieb ausgeführt wird"
    deaktivieren.
  - **„Computer zum Ausführen dieser Aufgabe reaktivieren"** ✅ aktivieren
    (das ist der „Computer aus Standby wecken"-Modus aus dem Auftrag).
- **Netzwerk:** Häkchen je nach VPN-Setup.

### Schritt 7: Tab „Einstellungen"

- „Ausführung der Aufgabe bei Bedarf zulassen" ✅
- „Aufgabe so schnell wie möglich nach einem verpassten Start ausführen" ✅
- „Falls die Aufgabe fehlschlägt, neu starten alle:" 15 Minuten, max. 2 Versuche.
- „Aufgabe beenden, falls Ausführung länger dauert als:" **8 Stunden**
  (Sicherheitsnetz; reale Laufzeit ~30–60 Minuten plus Listings-Gate-Pause —
  Operator hat genügend Zeit für den `product_link`-Eintrag, ohne dass der
  Task vorzeitig gekillt wird).
- „Falls die Aufgabe nicht beendet wird, beenden erzwingen" ✅

→ `OK`

### Schritt 8: Passwort eingeben (entfällt bei „Nur ausführen wenn angemeldet")

Wenn in Schritt 3 „Nur ausführen, wenn der Benutzer angemeldet ist"
gewählt wurde, fragt Windows nicht nach einem Passwort. Andernfalls
Benutzer-Passwort eingeben.

## Exit-Code-Handling

`Start_Scripts.py --morning` setzt im Pre-Flight-Check folgende Exit-Codes:

- `0` → Pipeline lief sauber durch.
- `1` → Regulärer Pipeline-Fehler in Step 07a/07/08/09/10/11.
- `2` → **Pre-Flight-Veto** — Voraussetzungen nicht erfüllt (z.B. Vorabend-
  Lauf nicht durchgelaufen, Lock-Datei noch da, keine Mockups, etc.). Konkrete
  Begründung steht in der Konsolenausgabe / im Eventlog.

Im Task Scheduler unter `Verlauf` → Spalte „Last Run Result" sieht Ingo
welcher Lauf sauber war. Code `0x2` (= 2) signalisiert immer „Vorabend
nicht gemacht / unvollständig".

## Logs einsehen

Stdout/Stderr des Tasks landet im Standard-Windows-Eventlog
(`Anwendungs- und Dienstprotokolle` → `Microsoft` → `Windows` →
`TaskScheduler`). Für detaillierte Pipeline-Logs Ingo bitte zusätzlich die
Konsolenausgabe in eine Datei umleiten — alternative Action-Konfiguration:

- **Programm/Skript:** `cmd.exe`
- **Argumente:** `/c python Start_Scripts.py --morning >> "logs\morning_%date:~6,4%-%date:~3,2%-%date:~0,2%.log" 2>&1`
- **Starten in:** `C:\Companies\DPS\01 Python Skript\pipeline`

(Logs landen dann unter `pipeline\logs\morning_YYYY-MM-DD.log`.)

## Manuelles Anstoßen

In der Aufgabenplanung Rechtsklick auf den Task → `Ausführen`. Der Task läuft
dann mit denselben Argumenten wie zur geplanten Zeit.

Alternativ direkt in PowerShell:

```powershell
cd "C:\Companies\DPS\01 Python Skript\pipeline"
python Start_Scripts.py --morning
# optional mit anderem Datum:
python Start_Scripts.py --morning --target-date=2026-05-01
```

## Kompletter Tagesablauf (Referenz)

1. **Vorabend (manuell, Ingo):** `Start_Evening.bat` doppelklicken →
   GUI öffnet sich → Datum „morgen" akzeptieren → „Vorabend-Lauf starten".
2. **Vorabend-Lauf:** Step 01 → 06 läuft, Step 06 öffnet das
   Bildkontroll-Tool und legt `REVIEW_PENDING.lock` an.
3. **Manuelle Sichtkontrolle (Ingo):** Bilder durchgehen, ggf. löschen,
   Tool schließen.
4. **Cowork-Task (Ingo):** „canva-mockup-pipeline-creation" manuell
   triggern → erzeugt Mockups in `<listing>/Mockups/`.
5. **ENTER + Lock löschen (Ingo):** Im Vorabend-Konsolenfenster ENTER
   drücken (löst nolist-Bildzählung aus) → `REVIEW_PENDING.lock` manuell
   löschen.
6. **03:40 Berlin (automatisch):** Task Scheduler weckt Computer aus
   Standby → ruft `python Start_Scripts.py --morning` → Pre-Flight-Check
   verifiziert (a)–(e) → läuft Step 07a → 09 automatisch.
7. **Listings-Gate-Pause (während des Morgen-Laufs):** Pipeline schreibt
   `etsy-listing.csv` (Promo-Code `NEWCUST50` vorbelegt) und pausiert. Das
   Console-Window bleibt offen — Operator (Thomas) öffnet die CSV in Excel,
   trägt `product_link` aus Etsy ein, speichert, schließt Excel, drückt
   ENTER im Console-Window.
8. **Step 10/11 (automatisch):** Pipeline läuft weiter → Etsy-Listing-API
   (sofern API-Keys vorhanden) → Meta Video Post FB+IG → öffnet
   `payhip-listing.csv` und `stockportal-listing.csv` in Excel am Ende.
9. **Morgens (Ingo wacht auf):** Excel-CSVs sind offen → Stockportal-Tags
   copy-paste, payhip-Items checken.

## Troubleshooting

- **Task läuft nicht:** Prüfe in der Aufgabenplanung den `Verlauf`-Tab →
  letzter Eintrag mit Fehlercode? Häufige Ursachen: Passwort geändert,
  „Unabhängig von Benutzeranmeldung" nicht gesetzt, Energieprofil verbietet
  Aufwecken (siehe Schritt 6).
- **Exit-Code 2 (Pre-Flight-Veto):** Manuell `python Start_Scripts.py --morning`
  in PowerShell ausführen → der Pre-Flight-Check druckt die konkrete Ursache
  (welcher der Punkte (a)–(e) failed, mit Listing-Name und Soll/Ist).
- **`zoneinfo`-Fehler in der GUI:** `pip install tzdata` (zoneinfo ist seit
  Python 3.9 stdlib, aber Windows hat keine IANA-Datenbank vorinstalliert).
- **Mockups fehlen, obwohl Cowork-Task lief:** Der Cowork-Task überspringt
  Listings ohne ausreichende GitHub-URLs (siehe Cowork-Task-Doku
  „weniger als 4 Bilder mit GitHub-URL"). Dann markiert Step 06 das Listing
  als `nolist` — der Pre-Flight-Check (e) ignoriert nolist-Listings.
- **Pipeline hängt nach Step_09 stundenlang:** Das ist normal — Listings-Gate
  wartet auf ENTER. Console-Window prüfen, `product_link` befüllen, ENTER.
  Falls der Task scheinbar „eingefroren" ist: prüfen, ob das Console-Window
  in der Taskleiste noch existiert (manchmal minimiert). Bei Reboot mitten
  in der Pause bricht der Lauf ab — manueller Re-Run nötig.
