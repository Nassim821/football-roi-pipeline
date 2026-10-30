# Football ROI Pipeline — Guide d'installation Windows

Pipeline d'analyse du ROI sportif des clubs de football européens.
Stack : Airflow 3 + PySpark + scikit-learn + Elasticsearch + Kibana.

Ce guide est destiné à un utilisateur **Windows 10/11**.

---

## 1. Prérequis logiciels à installer

| Outil | Version | Pourquoi | Lien de téléchargement |
|---|---|---|---|
| **Docker Desktop** | 4.x+ | Orchestrate Airflow + Postgres + Redis | https://www.docker.com/products/docker-desktop |
| **Git for Windows** | 2.x+ | Cloner le repo | https://git-scm.com/download/win |
| **Python 3** | 3.9+ | Générer la FERNET_KEY (optionnel) | https://www.python.org/downloads/windows/ |
| **Elasticsearch 9.0.0** | 9.0.0 | Stockage analytique | (téléchargement détaillé §3) |
| **Kibana 9.0.0** | 9.0.0 | Dashboard | (téléchargement détaillé §3) |
| **7-Zip** ou WinRAR | n'importe | Décompresser les `.tar.gz` | https://www.7-zip.org/ |

### Configurer Docker Desktop

Après installation :
1. Lance Docker Desktop, attends qu'il finisse de démarrer
2. ⚙️ Settings → **Resources** → alloue au minimum **4 GB RAM** et **2 CPUs**
3. ⚙️ Settings → **General** → vérifie que **WSL 2 backend** est activé (recommandé)
4. ⚙️ Settings → **General** → coche **"Use Docker Compose V2"**

Vérifie que tout marche :

```powershell
docker --version
docker compose version
```

---

## 2. Créer un dossier de travail

Ouvre **PowerShell** (pas CMD) et exécute :

```powershell
mkdir C:\Users\$env:USERNAME\Projets
cd C:\Users\$env:USERNAME\Projets
```

Tous les chemins du guide partent de ce dossier.

---

## 3. Télécharger Elasticsearch + Kibana

### Téléchargement

Va dans ton navigateur sur :

- ES : https://www.elastic.co/downloads/past-releases/elasticsearch-9-0-0
- Kibana : https://www.elastic.co/downloads/past-releases/kibana-9-0-0

Choisis pour les deux : **WINDOWS** (fichier `.zip`).

Place les 2 zip dans `C:\Users\<TON_USER>\Projets\elastic\` (crée le dossier).

### Décompression

Avec 7-Zip ou l'explorateur Windows, décompresse les deux archives. Tu obtiens :

```
C:\Users\<TON_USER>\Projets\elastic\
├── elasticsearch-9.0.0\
└── kibana-9.0.0\
```

### Désactiver la sécurité ES (config locale uniquement)

Ouvre dans le **Bloc-notes** :

```
C:\Users\<TON_USER>\Projets\elastic\elasticsearch-9.0.0\config\elasticsearch.yml
```

Ajoute en fin de fichier :

```yaml
xpack.security.enabled: false
network.host: 0.0.0.0
discovery.type: single-node
```

Sauvegarde puis ouvre :

```
C:\Users\<TON_USER>\Projets\elastic\kibana-9.0.0\config\kibana.yml
```

Ajoute en fin de fichier :

```yaml
elasticsearch.hosts: ["http://localhost:9200"]
server.host: "0.0.0.0"
```

Sauvegarde.

---

## 4. Cloner le repo du projet

Dans PowerShell :

```powershell
cd C:\Users\$env:USERNAME\Projets
git clone https://github.com/Nassim821/football-roi-pipeline.git
cd football-roi-pipeline
```

---

## 5. Configurer le fichier `.env`

```powershell
copy .env.example .env
notepad .env
```

Remplis ces 3 valeurs :

```
API_FOOTBALL_KEY=<ta_cle_api_football>
FERNET_KEY=<une_cle_fernet_42_chars_base64>
AIRFLOW_UID=50000
```

### Obtenir une clé API-Football

1. Inscription gratuite : https://dashboard.api-football.com/register
2. Plan gratuit = 100 requêtes par jour (suffit pour 3 runs du DAG)
3. Copie ta clé depuis le dashboard

### Générer la FERNET_KEY

Génère ta propre clé (Python requis) :

```powershell
pip install cryptography
python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"
```

Copie la sortie dans `.env`.

---

## 6. Chemins dans `docker-compose.yaml`

Rien à modifier : les volumes utilisent des chemins relatifs au dossier du projet. Lance simplement `docker compose` depuis la racine du repo.

---

## 7. Démarrer la stack

Tu auras besoin de **3 fenêtres PowerShell** en parallèle.

### Fenêtre 1 — Elasticsearch

```powershell
cd C:\Users\$env:USERNAME\Projets\elastic\elasticsearch-9.0.0
.\bin\elasticsearch.bat
```

Garde cette fenêtre ouverte. ES démarre en 20-40s.

Quand tu vois `started` dans les logs, ouvre un test depuis une autre fenêtre :

```powershell
curl http://localhost:9200
```

Tu dois voir un JSON avec `"cluster_name"` et `"version" : { "number" : "9.0.0" ...`.

### Fenêtre 2 — Kibana

```powershell
cd C:\Users\$env:USERNAME\Projets\elastic\kibana-9.0.0
.\bin\kibana.bat
```

Garde cette fenêtre ouverte. Kibana démarre en 30-60s.

Quand tu vois `Server running at http://localhost:5601`, ouvre **http://localhost:5601** dans ton navigateur.

### Fenêtre 3 — Docker (Airflow)

```powershell
cd C:\Users\$env:USERNAME\Projets\football-roi-pipeline
docker compose up -d
```

Attends ~90s (Airflow met du temps à initialiser les healthchecks), puis vérifie :

```powershell
docker compose ps
```

Tu dois voir **7 services** tous en `Up X seconds (healthy)`.

---

## 8. Charger les données

Deux options selon que tu disposes déjà d'un export du data lake ou non.

### Option A — Tu as déjà un dossier `data/` exporté (raccourci, conseillé)

1. Place le dossier `data/` à la racine du projet :
   ```
   C:\Users\<TON_USER>\Projets\football-roi-pipeline\data\
   ```

2. Copie son contenu dans le volume Docker :

   ```powershell
   docker compose cp .\data\. airflow-worker:/opt/airflow/data/
   ```

3. Vérifie :

   ```powershell
   docker compose exec airflow-worker ls /opt/airflow/data/raw/
   ```

   Tu dois voir 3 dossiers : `football`, `transfermarkt`, `champions_league`.

4. Dans Airflow UI (http://localhost:8080), trigger uniquement les **tâches aval** (pas besoin de re-fetch les APIs) :
   - Clear les tâches `produce_usage`, `produce_usage_cl`, `ml_clustering`, `index_to_elastic`
   - Elles se relanceront automatiquement

### Option B — Tu pars de zéro (le DAG va re-fetch les APIs)

1. Va sur **http://localhost:8080** (login `airflow` / `airflow`)
2. Cherche le DAG `football_roi_pipeline`
3. **Active-le** (toggle à gauche du nom du DAG)
4. Bouton **▶ Trigger DAG** en haut à droite
5. Attends ~5 min que toutes les tâches passent vert

⚠️ L'option B consomme ~33 appels API-Football par run. Limite-toi à 2 runs par jour avec le plan gratuit.

---

## 9. Importer le dashboard Kibana

Va sur **http://localhost:5601** :

1. Menu burger ☰ en haut à gauche → **Stack Management** → **Saved Objects**
2. Bouton **Import** en haut à droite
3. Sélectionne le fichier `kibana\dashboard.ndjson` du repo
4. Coche **"Automatically overwrite all saved objects"**
5. Clique **Import**

Puis ☰ → **Dashboards** → ouvre le dashboard Football ROI.

> Tu peux aussi recréer le dashboard manuellement avec les 8 visualisations décrites dans `docs/RAPPORT.md`.

---

## 10. Vérification finale

```powershell
curl "http://localhost:9200/_cat/indices?v"
```

Tu dois voir au moins ces 3 indices dans la liste :

| index | docs |
|---|---|
| `football_roi` | 254 |
| `football_cl_roi` | 58 |
| `football_roi_clusters` | 254 |

Si OK → tu es opérationnel.

---

## URLs des services

| Service | URL | Login |
|---|---|---|
| Airflow | http://localhost:8080 | `airflow` / `airflow` |
| Kibana | http://localhost:5601 | (pas d'auth) |
| Elasticsearch | http://localhost:9200 | (pas d'auth) |

---

## Arrêter la stack

### Stop Airflow

```powershell
cd C:\Users\$env:USERNAME\Projets\football-roi-pipeline
docker compose down
```

(Les volumes nommés `football-data` et `postgres-db-volume` sont préservés.)

### Stop Kibana

Va dans la fenêtre PowerShell où tourne Kibana → **Ctrl+C**.

### Stop Elasticsearch

Va dans la fenêtre PowerShell où tourne ES → **Ctrl+C**.

Si ES ne répond pas à Ctrl+C, kill en force :

```powershell
Get-Process | Where-Object {$_.ProcessName -eq "java" -and $_.MainWindowTitle -like "*Elasticsearch*"} | Stop-Process -Force
```

Ou plus brutalement :

```powershell
taskkill /F /IM java.exe
```

---

## Troubleshooting

### Port 8080 déjà utilisé

```powershell
netstat -ano | findstr :8080
```

Identifie le PID coupable, puis :

```powershell
taskkill /F /PID <le_PID>
```

### Cluster ES en RED

```powershell
curl "http://localhost:9200/_cluster/health?pretty"
```

Si statut RED et message disque, relax temporairement le watermark :

```powershell
curl -X PUT "http://localhost:9200/_cluster/settings" -H "Content-Type: application/json" -d "{\"persistent\": {\"cluster.routing.allocation.disk.watermark.low\": \"95%\", \"cluster.routing.allocation.disk.watermark.high\": \"97%\", \"cluster.routing.allocation.disk.watermark.flood_stage\": \"98%\"}}"
```

### Erreur Docker "Cannot connect to Docker daemon"

Docker Desktop n'est pas démarré. Lance-le depuis le menu Démarrer, attends qu'il finisse de booter (l'icône baleine dans la barre des tâches devient stable), puis réessaie.

### Erreur "config is not a package" dans Airflow

Vérifie que `config\__init__.py` existe (fichier vide). S'il manque :

```powershell
New-Item -ItemType File config\__init__.py
docker compose restart airflow-worker airflow-scheduler airflow-dag-processor
```

### API-Football : quota dépassé

Le plan gratuit est limité à 100 requêtes/jour. Si tu vois `"You have reached the request limit"`, soit tu attends minuit UTC (reset), soit tu réduis dans `config\config.py` :

```python
SEASONS = [2024]   # au lieu de [2022, 2023, 2024]
```

Cela divise les appels API par 3.

### ES ne démarre pas (erreur "max file descriptors")

Sur Windows c'est rare, mais possible. Sors et relance PowerShell en mode admin (clic droit → "Exécuter en tant qu'administrateur"), puis relance ES.

### Kibana montre "Configure Elastic to get started"

C'est que Kibana ne trouve pas la config. Vérifie :

1. Que ES tourne bien : `curl http://localhost:9200`
2. Que `kibana.yml` contient bien `elasticsearch.hosts: ["http://localhost:9200"]`
3. Au pire, clique sur **"Configure manually"** → mets `http://localhost:9200` → Check address

---

## Architecture du projet (rapide)

```
football-roi-pipeline/
├── config/              # Config Python (API key, paths, leagues)
├── dags/                # DAG Airflow
├── jobs/                # Scripts métier
│   ├── ingestion/       # API fetch + scraping
│   ├── formatting/      # JSON → Parquet
│   ├── combination/     # JOIN Spark + ROI
│   ├── ml/              # K-Means clustering
│   └── indexing/        # Push vers ES
├── data/                # Datalake (gitignored)
│   ├── raw/             # JSON brut
│   ├── formatted/       # Parquet typé
│   └── usage/           # Métriques + ML
├── kibana/              # Export du dashboard
├── docker-compose.yaml  # Stack Airflow
├── Dockerfile           # Image Airflow custom
└── .env                 # Secrets (gitignored)
```

---

## Contact

Repo GitHub : https://github.com/Nassim821/football-roi-pipeline
