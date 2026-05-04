# Pipeline – AI-generierte Bilder bis zum Shop

Dieses Verzeichnis enthält alle Skripte für die Workflow-Pipeline:
**KI-generierte Bilder (Leonardo) → Upscaling → Marketing → YouTube/Etsy/Meta**.

---

## Überblick

| Phase | Schritte | Inhalt |
|-------|----------|--------|
| **PHASE 1: Generierung** | Step 01–05 | Prompts, CSV, Bilder, Umbenennung |
| **PHASE 2: Sichtkontrolle & Media** | Step 06–08 | Review, Musik, Video, YouTube |
| **PHASE 3: Verarbeitung & Export** | Step 09–11 | Upscaling, Etsy, Meta |

---

## Config-Dateien

| Config | Zweck | Verwendung |
|--------|-------|-----------|
| **config.yaml** | Produktions-Lauf | Normale Workflow-Ausführung (alle API-Calls live) |
| **config.staging.yaml** | Test mit echtem Upscaling | Alle Steps aktiv, aber externe APIs simuliert (images, youtube, etsy, meta) — RealESRGAN läuft real |
| **config.dev.yaml** | Schneller Lokal-Test | Minimaler Batch (1 Wallpaper), alle Steps simuliert, keine realen APIs |

### Konfiguration Wechseln

**Staging-Modus (empfohlen zum Testen):**
```powershell
python Start_Scripts.py --staging
```

Oder manuell:
```powershell
$env:PIPELINE_CONFIG = "config.staging.yaml"
python Start_Scripts.py
```

**Dev-Modus (schneller Lokal-Test):**
```powershell
$env:PIPELINE_CONFIG = "config.dev.yaml"
python Start_Scripts.py
```

**Produktion (default):**
```powershell
python Start_Scripts.py
```

---

## Staging-Isolation — Dateisystem-Schutz

**Problem:** Im Staging-Modus schrieben die Steps ursprünglich direkt in den Produktions-Tagesordner.

**Lösung:** Staging-Isolation

- **Automatisch aktiviert** in `config.staging.yaml` und `config.dev.yaml`
- **Isolierter Temp-Ordner** wird bei Laufstart erstellt
- **Alle Steps** schreiben in diesen isolierten Ordner statt in Produktion
- **Nach Laufende** wird der Temp-Ordner automatisch gelöscht

**Wie es funktioniert:**

1. `config_loader.py` prüft `staging_isolation: true`
2. Erstellt einen Temp-Ordner (z.B. `C:\Users\...\AppData\Local\Temp\pipeline_staging_20260404_120000`)
3. Leitet `IMAGES_PATH` zu diesem Temp-Ordner um
4. Alle nachfolgenden Steps (Step 03, 05, 07, 07a) schreiben nur hierhin
5. Nach erfolgreicher Ausführung: Temp-Ordner automatisch gelöscht

**Konfiguration:**

```yaml
# config.staging.yaml / config.dev.yaml
staging_isolation: true
staging_temp_dir: null        # null = System-Temp verwenden
                              # oder: "C:/path/to/staging"
```

**Manuelle Cleanup (falls nötig):**

Falls der Temp-Ordner manuell bereinigt werden muss:
- Windows: `C:\Users\...\AppData\Local\Temp\pipeline_staging_*`
- Linux: `/tmp/pipeline_staging_*`

---

## Staging-Workflow — Schritt für Schritt

### 1. Test-Lauf starten
```powershell
cd C:\Companies\DPS\01 Python Skript\pipeline
python Start_Scripts.py --staging
```

Der Workflow läuft bis **Step 09** (Upscaling) und stoppt dann.

**Info:** Staging-Isolation ist aktiviert – alle Dateisystem-Operationen schreiben in einen isolierten Temp-Ordner, nicht in den Produktions-Ordner.

### 2. Genehmigungsdatei erstellen (Approval Gate)

Nach Step 09 stoppt der Workflow und gibt diese Meldung aus:

```
🔐 APPROVAL GATE: Staging-Testlauf vor Produktionscode
=============================================
Bilder wurden hochgeladen und gefiltert (Step 09).
Um fortzufahren (Step 10–11), benötige ich Freigabe:

  Windows:  approve_for_prod.bat
  Linux:    ./approve_for_prod.sh

Nach Freigabe: Diesen Workflow erneut starten.
=============================================
```

**Genehmigung erteilen:**
```powershell
cd C:\Companies\DPS\01 Python Skript\pipeline
.\approve_for_prod.bat
```

Dies erstellt die Datei `.approval` mit aktuellem Timestamp.

### 3. Workflow fortsetzten (Produktion-Code)

Starten Sie den Workflow erneut:
```powershell
python Start_Scripts.py --staging
```

Der Workflow erkennt `.approval`, überspringt das Gate und läuft bis zum Ende (Step 10–11).

### 4. Genehmigung zurücksetzen (optional)

Für den nächsten Test:
```powershell
.\clear_approval.bat
```

---

## Fixture-Daten

Die Staging- und Dev-Configs verwenden eine **Fixture**-Datei statt echte Prompts:

| Datei | Inhalt |
|-------|--------|
| `fixtures/prompts_pending_fixture.json` | 2 Test-Prompts (Morning Calm, Retro Family) |

Diese Fixture wird in `JSON Dateien/prompts_pending_fixture.json` erwartet.

**Info:** Bilder aus dem letzten echten Lauf (2026-04-04) werden referenziert, aber **NICHT** ins Repo committed (siehe `.gitignore`).

---

## Wichtige Pfade

```
pipeline/
├── Start_Scripts.py              (Launcher)
├── config.yaml                   (Produktion)
├── config.staging.yaml           (Staging — Approval Gate)
├── config.dev.yaml               (Dev — schneller Test)
├── .gitignore                    (Keine Bilder/JSON ins Repo)
├── approve_for_prod.bat          (Genehmigung erteilen)
├── clear_approval.bat            (Genehmigung zurücksetzen)
├── .approval                     (Wird von approve_for_prod.bat erstellt)
├── fixtures/
│   └── prompts_pending_fixture.json   (Test-Daten)
└── Step_01–11.py                (Workflow-Skripte)
```

---

## Was gehört NICHT ins Repo?

Siehe `.gitignore`:

- `../JSON Dateien/` — Produktionsdaten
- `../Generated pics/` — Bilder (zu groß)
- `../Claude Workspace/` — Artikelspeicher
- `.approval` — Approval-Gate (flüchtig)
- `CREDENTIALS.md`, `.env`, `credentials.json` — Secrets

---

## Abhängigkeiten

Siehe `../DEPENDENCIES.md` für vollständige Liste.

Minimal für lokale Tests:
```bash
pip install pyyaml
```

Für volle Funktionalität:
```bash
pip install pyyaml requests pillow moviepy transformers torch torchaudio
```

---

## Troubleshooting

### "config.staging.yaml fehlt"
→ Stelle sicher, dass Du im Verzeichnis `pipeline/` bist.

### ".approval nicht erkannt"
→ Führe `approve_for_prod.bat` aus (nicht einfach Datei erstellen).

### "PIPELINE_CONFIG Umgebungsvariable wird nicht erkannt"
→ Nutze das `--staging` Flag: `python Start_Scripts.py --staging`

### "Bilder nicht gefunden"
→ Prüfe, dass `../Generated pics/2026/2026 April/2026-04-04/` existiert.

---

## Changelog

### 2026-04-04 — Staging-Isolation implementiert
- **Neue Feature:** Automatische Isolation von Staging-Läufen
- `config_loader.py`: `staging_isolation` Flag unterstützen
- `config.staging.yaml` / `config.dev.yaml`: Isolation aktiviert
- `Start_Scripts.py`: Auto-Cleanup nach erfolgreichem Lauf
- README.md: Dokumentation der Staging-Isolation
- Fixture `prompts_pending_fixture.json` bleibt unverändert
- **Effekt:** Staging-Läufe berühren Produktions-Tagesordner nicht mehr

---

## Autor
Indi — technischer Mitarbeiter
Aktualisierung: 2026-04-04
