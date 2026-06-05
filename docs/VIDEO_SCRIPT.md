# 🎬 Script Vidéo — Football ROI Pipeline (10 min)

Document à utiliser en mode téléprompteur pendant l'enregistrement.
Chaque section indique : **temps**, **ce qu'il faut montrer à l'écran**, **texte exact à dire**, et les **commandes** à exécuter.

---

## ✅ Checklist avant de cliquer "Record"

```bash
# 1. Démarrer ES
cd /Users/nassimelkharroubi/elastic/elasticsearch-9.0.0
./bin/elasticsearch -d
sleep 25 && curl -s http://localhost:9200    # doit renvoyer le JSON cluster

# 2. Démarrer Kibana (autre terminal)
cd /Users/nassimelkharroubi/elastic/kibana-9.0.0
./bin/kibana
# Attendre "Server running at http://localhost:5601"

# 3. Démarrer la stack Airflow (autre terminal)
cd /Users/nassimelkharroubi/Documents/Projet_data
docker compose up -d
sleep 60 && docker compose ps                 # tous "healthy"

# 4. Vérifier que les 3 indices ES sont peuplés
curl -s "http://localhost:9200/_cat/indices?v" | grep football

# 5. Pause le DAG dans l'UI pour éviter un trigger auto pendant le tournage
```

### Onglets navigateur à pré-ouvrir
- http://localhost:8080 → Airflow, sur la vue Graph du DAG `football_roi_pipeline`
- http://localhost:5601 → Kibana, dashboard ouvert en plein écran
- https://github.com/Nassim821/football-roi-pipeline → page d'accueil du repo

### Setup capture
- **QuickTime Player** → Fichier → Nouvel enregistrement de l'écran (ou OBS pour plus pro)
- Micro testé : casque ou micro intégré, voix proche, pas de bruit de fond
- Résolution écran : passe en 1920×1080 si possible

---

## 🎬 0:00 — 0:45 — INTRO (45 secondes)

### 🖥️ À montrer
Une slide PowerPoint / Keynote simple avec :
- Titre **"Football ROI Pipeline"**
- Sous-titre **"Quels clubs investissent le mieux ?"**
- Ton nom + filière + date
- En bas : logos API-Football, Transfermarkt, Spark, Airflow, Elasticsearch, Kibana

### 🗣️ Texte à dire (mot à mot)

> "Bonjour, je vous présente mon projet Big Data : un pipeline d'analyse du retour sur investissement sportif des clubs de football européens.
>
> La question que je me suis posée : *quels clubs tirent réellement parti de l'argent qu'ils investissent en transferts ?*
>
> Pour y répondre, j'ai construit un pipeline complet qui ingère trois sources de données, calcule un ROI métier — c'est-à-dire le nombre de points obtenus en championnat par million d'euros investi —, enrichit le résultat avec un algorithme de machine learning, et restitue tout dans un dashboard interactif Kibana.
>
> La stack technique utilisée : Airflow pour l'orchestration, Spark pour les jointures, scikit-learn pour le ML, Elasticsearch et Kibana pour la visualisation, le tout containerisé avec Docker."

⏱️ **40 secondes**. Fais une pause de 3 secondes avant de basculer sur la suite.

---

## 🏗️ 0:45 — 2:00 — ARCHITECTURE (1 min 15)

### 🖥️ À montrer
1. **D'abord** : une slide avec le schéma d'architecture (voir bloc ASCII ci-dessous, à reproduire dans PowerPoint ou Excalidraw)
2. **Puis** : bascule sur le terminal pour montrer l'arborescence physique

### Schéma à reproduire dans la slide

```
   ┌──────────────┐  ┌──────────────┐  ┌──────────────┐
   │ API-Football │  │ Transfermarkt│  │ API-Football │
   │   standings  │  │  squad values│  │  CL fixtures │
   └──────┬───────┘  └──────┬───────┘  └──────┬───────┘
          │                 │                 │
          ▼                 ▼                 ▼
   ┌──────────────────────────────────────────────┐
   │  RAW     (data/raw/)        JSON brut         │
   ├──────────────────────────────────────────────┤
   │  FORMATTED  (data/formatted/)  Parquet typé   │
   ├──────────────────────────────────────────────┤
   │  USAGE  (data/usage/)    Métriques ROI + ML   │
   └──────────────────┬───────────────────────────┘
                      │
                      ▼
              ┌───────────────┐         ┌─────────┐
              │ Elasticsearch │ ──────► │ Kibana  │
              └───────────────┘         └─────────┘

   ╔══════════════════════════════════════════════╗
   ║ Orchestration : Airflow (Docker)              ║
   ║ Compute : PySpark + pandas + scikit-learn     ║
   ╚══════════════════════════════════════════════╝
```

### 🗣️ Texte à dire — pendant que la slide est affichée (40 sec)

> "L'architecture suit le pattern Medallion, très répandu en data engineering. Trois couches dans un data lake.
>
> La couche **RAW** contient les données brutes des trois sources sous forme de JSON — c'est ma source de vérité, je n'y touche jamais.
>
> La couche **FORMATTED** contient les mêmes données mais nettoyées, typées et converties en Parquet, qui est environ dix fois plus rapide à lire que du JSON.
>
> Enfin, la couche **USAGE** contient les datasets agrégés et enrichis avec le ROI calculé et les clusters de machine learning, prêts pour la visualisation.
>
> L'ensemble est orchestré par Apache Airflow tournant dans une stack Docker avec PostgreSQL pour les métadonnées et Redis pour le broker Celery. Elasticsearch et Kibana tournent en local sur le host pour la couche analytique."

### 💻 Bascule sur le terminal et exécute

```bash
cd /Users/nassimelkharroubi/Documents/Projet_data
tree data/ -L 3 -d
```

### 🗣️ Texte à dire — pendant que tu montres l'arborescence (25 sec)

> "Concrètement, voici l'arborescence physique du data lake : raw, formatted, usage, et chaque couche est organisée par source — football, transfermarkt, champions_league — puis par entité. Cette convention de nommage stricte est d'ailleurs un point bonus dans la grille d'évaluation du projet."

---

## 💻 2:00 — 3:30 — CODE WALKTHROUGH (1 min 30)

Tu vas montrer **3 fichiers** seulement. Ouvre VS Code en split avec le terminal, ou GitHub dans le navigateur.

### Fichier 1 — `jobs/ingestion/ingest_football.py` (25 sec)

🖥️ **Ouvre le fichier**, scroll doucement de haut en bas.

🗣️ **À dire** :
> "Premier exemple, l'ingestion API-Football. Je récupère les standings et les résultats pour cinq ligues majeures et trois saisons. Chaque appel est sauvegardé dans le data lake avec une date dans le path, ce qui me permet de tracer les ingestions et de relancer en cas d'erreur. Notez le `time.sleep` de 7 secondes entre les appels pour respecter le rate limit de l'API gratuite."

🎯 **Lignes à highlight** : la boucle `for season... for league_id...` vers la ligne 53.

### Fichier 2 — `jobs/combination/combine_roi.py` (45 sec)

🖥️ **Ouvre le fichier**.

🗣️ **À dire** :
> "Deuxième fichier clé : le calcul du ROI. C'est là que la valeur métier se crée. J'utilise PySpark pour faire un JOIN entre les standings et les valeurs marchandes des effectifs.
>
> Il y a une subtilité importante : les noms de clubs diffèrent entre l'API et Transfermarkt — par exemple 'Bayer 04 Leverkusen' versus 'Bayer Leverkusen'. J'ai donc un dictionnaire de normalisation d'environ 70 entrées qui fait correspondre les deux nomenclatures."

🎯 **Highlight** :
- Le dict `CLUB_NAME_MAPPING` (lignes 18-97) — scroll rapide
- Le JOIN Spark (ligne ~124)
- **La ligne du calcul ROI** (ligne 137) :
  ```python
  F.round(standings.points / F.greatest(transfers.squad_value_m, F.lit(0.1)), 4).alias("roi")
  ```

🗣️ **Suite** :
> "Et voilà la ligne clé : ROI égal points divisés par valeur d'effectif en millions, avec un `greatest` qui évite la division par zéro. J'ajoute ensuite un classement par ligue et par saison via une window function Spark."

### Fichier 3 — `jobs/ml/ml_clustering.py` (20 sec)

🗣️ **À dire** :
> "Troisième fichier, le bonus machine learning. J'applique un K-Means avec 4 clusters sur les features budget, ROI et points, après normalisation avec StandardScaler. J'assigne ensuite automatiquement des labels métiers — Efficaces, Dominants, Standards, Gaspilleurs — en calculant la distance de chaque centroïde aux 4 archétypes du plan budget × ROI."

🎯 **Highlight** :
- `KMeans(n_clusters=4, random_state=42)` (ligne ~45)
- Le dict `archetypes` (lignes ~62-67)

---

## ✈️ 3:30 — 5:00 — DAG AIRFLOW LIVE (1 min 30)

### 🖥️ À montrer
Le navigateur sur **http://localhost:8080**, page du DAG `football_roi_pipeline` en vue Graph.

### 🗣️ Texte à dire (30 sec d'intro)

> "Place à la démo live. Voici l'interface Airflow. Mon DAG `football_roi_pipeline` est composé de 10 tâches structurées en 5 étapes.
>
> À gauche, les trois sources sont ingérées **en parallèle** : standings, valeurs marchandes, fixtures Champions League. Ensuite, les trois jobs de formatting sont exécutés en **séquentiel** — j'expliquerai pourquoi à la fin de la vidéo, c'est lié à un bug Docker que j'ai contourné. Vient ensuite le calcul du ROI domestique et CL en parallèle, puis le clustering ML, et enfin l'indexation dans Elasticsearch en bout de chaîne."

### 🎬 Action : trigger le DAG

1. Si le DAG est pausé, **active-le** (toggle à gauche du nom)
2. Clique le bouton **▶ Trigger DAG** en haut à droite

🗣️ **À dire** (10 sec) :
> "Je déclenche le DAG manuellement. Vous voyez les tâches passer du gris au vert au fur et à mesure de leur exécution."

### ⏩ Pendant que ça tourne

Le DAG met ~4 min à se compléter. **Coupe au montage** : ne garde que :
- 5-10 secondes pour voir les premières tâches s'allumer
- Un fondu vers l'état final avec toutes les tâches vertes
- Tu peux accélérer ×4 ou ×8 en post-prod

### 🗣️ Texte à dire (20 sec, sur le rendu final vert)

> "Voilà : toutes les tâches sont vertes en environ 4 minutes 30. Le pipeline a tourné de bout en bout, automatiquement, sans intervention manuelle. C'est exactement le bonus 'Run all in once' de la grille d'évaluation."

---

## 🔍 5:00 — 6:00 — VÉRIFICATION ES (1 min)

### 🖥️ À montrer
Terminal en split-screen ou fenêtre en avant-plan.

### 🗣️ Texte à dire (intro, 5 sec)

> "Côté Elasticsearch, je vérifie maintenant que tout est bien indexé."

### 💻 Commande 1

```bash
curl -s "http://localhost:9200/_cat/indices?v" | grep football
```

🗣️ **À dire** (15 sec) :
> "J'ai bien mes trois indices : `football_roi` pour les championnats domestiques, `football_cl_roi` pour la Champions League, et `football_roi_clusters` pour les données enrichies par le ML."

### 💻 Commande 2

```bash
curl -s "http://localhost:9200/football_roi/_count"
curl -s "http://localhost:9200/football_cl_roi/_count"
curl -s "http://localhost:9200/football_roi_clusters/_count"
```

🗣️ **À dire** (20 sec) :
> "254 documents pour les championnats domestiques — ce qui correspond à 5 ligues fois environ 18 clubs fois 3 saisons. 58 documents pour la Champions League. Et 254 documents enrichis avec les clusters ML."

### 💻 Commande 3 — Répartition des clusters

```bash
curl -s "http://localhost:9200/football_roi_clusters/_search?size=0" -H "Content-Type: application/json" -d '{"aggs":{"by_label":{"terms":{"field":"cluster_label","size":10}}}}' | python3 -m json.tool
```

🗣️ **À dire** (20 sec) :
> "Et voici la répartition des 4 clusters détectés par le K-Means : 119 clubs en Efficaces, 104 en Gaspilleurs, 31 en Dominants, 26 en Standards. Les 4 labels sont bien représentés — ce qui n'a pas été trivial à obtenir, j'y reviens dans la section défis."

---

## 📊 6:00 — 8:30 — KIBANA DASHBOARD (2 min 30)

### 🖥️ À montrer
Kibana en plein écran (bouton "Full screen" en bas à droite du dashboard pour virer la sidebar), dashboard ouvert.

### 🗣️ Intro (10 sec)

> "Passons maintenant au dashboard Kibana, qui est la sortie finale du pipeline. Huit visualisations qui répondent à ma question initiale : quels clubs investissent le mieux ?"

### Viz 1 — Top clubs par ROI (15 sec)

🖥️ Zoom sur la viz n°1.

🗣️ :
> "Premier graphique : le top des clubs par ROI absolu. On voit immédiatement que ce sont des clubs comme Clermont, Heidenheim ou Rayo Vallecano qui dominent — des clubs à petit budget qui marquent beaucoup de points proportionnellement à ce qu'ils ont investi. C'est exactement l'inverse du palmarès classique des grands clubs."

### Viz 2 — ROI moyen par ligue (15 sec)

🖥️ Zoom sur la viz n°2.

🗣️ :
> "ROI moyen par championnat. La Bundesliga ressort comme la ligue la plus rentable sportivement, la Premier League comme la moins efficiente — ce qui s'explique par l'inflation des salaires en Angleterre, où l'argent investi se traduit beaucoup moins en points marqués."

### Viz 3 — Distribution ROI par saison (15 sec)

🖥️ Zoom sur la viz n°3.

🗣️ :
> "Évolution du ROI moyen sur trois saisons. On observe une tendance baissière : les budgets globaux croissent plus vite que les performances sportives, donc le rendement marginal de l'argent dans le foot décroît."

### Viz 4 — Budget vs Points par ligue (20 sec)

🖥️ Zoom sur la viz n°4.

🗣️ :
> "Corrélation budget-points par ligue. La relation est positive mais clairement non-linéaire. Au-delà d'un certain seuil, dépenser deux fois plus n'apporte pas deux fois plus de points. C'est un argument économique fort contre les très gros transferts."

### Viz 5 — CL Budget vs Stade atteint (20 sec)

🖥️ Zoom sur la viz n°5.

🗣️ :
> "Champions League : corrélation entre la valeur de l'effectif et le stade atteint dans la compétition. La relation est ici beaucoup plus marquée — la CL est trustée par les ultra-riches. Quasiment aucun club avec moins de 300 millions d'euros d'effectif ne dépasse les quarts de finale."

### Viz 6 — Évolution CL grands clubs (20 sec)

🖥️ Zoom sur la viz n°6.

🗣️ :
> "Évolution sur trois saisons des parcours en Champions League des grands clubs. PSG progresse, le Real Madrid reste constant à haut niveau, le Barça est en repli. C'est une analyse temporelle qui complète bien la photo statique."

### Viz 7 — Data Table ROI complet (15 sec)

🖥️ Zoom sur la viz n°7.

🗣️ :
> "Une table exhaustive pour le drill-down. Utile pour vérifier un club spécifique, filtrer par ligue ou par saison à la volée."

### Viz 8 — Bar chart Clusters ML (30 sec)

🖥️ Zoom sur la viz n°8.

🗣️ :
> "Et la visualisation phare, celle qui correspond au bonus ML : les quatre clusters détectés par K-Means.
>
> À gauche les Efficaces, avec un budget moyen de 85 M€ et un ROI de 0.53. À droite les Gaspilleurs, près d'un milliard d'euros de valeur d'effectif mais un ROI de seulement 0.08.
>
> C'est mécanique puisque ROI égal points sur budget, mais c'est précisément ce que l'algorithme révèle : **il existe un seuil au-delà duquel ajouter de l'argent ne se traduit plus en points**.
>
> L'apport du machine learning par rapport à une simple agrégation, c'est d'avoir trouvé ces seuils automatiquement, sans que je les définisse à la main."

---

## ⚙️ 8:30 — 9:30 — DÉFIS TECHNIQUES (1 min)

### 🖥️ À montrer
Soit 4 slides courtes (une par défi), soit rester sur GitHub avec les commits visibles dans l'historique. Je recommande les slides pour la clarté.

### 🗣️ Intro (5 sec)

> "Pour finir, quatre défis techniques non-triviaux que j'ai résolus pendant ce projet et qui m'ont fait progresser sur la stack."

### Défi 1 — Deadlock virtioFS macOS (15 sec)

🖥️ Slide titre : **"Defi 1 — EDEADLK virtioFS"**

🗣️ :
> "Premièrement : un deadlock système entre PyArrow et le bind mount Docker sur macOS. L'erreur 'Resource deadlock avoided' survenait à chaque écriture parquet. Diagnostic : la couche virtioFS de Docker Desktop sur macOS gère mal les locks fichiers. Solution : remplacer le bind mount par un volume Docker nommé, ce qui bypass complètement virtioFS."

### Défi 2 — Namespace package Airflow 3 (15 sec)

🖥️ Slide : **"Defi 2 — config not a package"**

🗣️ :
> "Deuxièmement : conflit de namespace package. Mon dossier `config` était monté à la fois dans `/opt/airflow/config` et dans `/opt/airflow/project_root/config`, ce qui faisait fusionner les paths en namespace package PEP 420, mal géré par le task runner d'Airflow 3. Le fix : ajouter un simple `__init__.py` vide pour forcer un package classique."

### Défi 3 — ES single-node defaults (15 sec)

🖥️ Slide : **"Defi 3 — ES cluster YELLOW"**

🗣️ :
> "Troisièmement : Elasticsearch crée par défaut des indices avec une réplique, mais sur un cluster single-node cette réplique reste impossible à allouer, le cluster passe en yellow, et la création d'index timeout côté client Python à 10 secondes. Fix : forcer `number_of_replicas: 0` dans tous les mappings."

### Défi 4 — Bug labelling ML (15 sec)

🖥️ Slide : **"Defi 4 — ML labels collapse"**

🗣️ :
> "Quatrièmement : ma première logique de labelling des clusters utilisait des seuils de rang qui se chevauchaient. Résultat : quatre clusters K-Means distincts mais seulement deux labels uniques attribués — Efficaces et Gaspilleurs. J'ai réécrit avec une assignation 1-pour-1 par distance euclidienne aux quatre archétypes du plan normalisé budget × ROI, ce qui garantit l'utilisation de tous les labels."

---

## 🎯 9:30 — 10:00 — CONCLUSION (30 sec)

### 🖥️ À montrer
La page d'accueil GitHub du repo en plein écran.

### 🗣️ Texte à dire

> "Le code est intégralement open source sur GitHub. Vous y trouverez le `docker-compose` pour reproduire l'infrastructure, les scripts Python, le DAG Airflow, le guide d'installation Windows pour mes collaborateurs, et l'export NDJSON du dashboard Kibana pour le réimporter directement.
>
> Le lien est en description. Merci de votre attention."

### 🖥️ Affichage final
URL en grand, bien visible :

```
github.com/Nassim821/football-roi-pipeline
```

---

## 🎬 POST-PRODUCTION

### Outils gratuits recommandés
- **iMovie** (Mac, intégré) — pour coupes et accélérations
- **DaVinci Resolve** (Mac/Win, gratuit) — pro mais courbe d'apprentissage
- **CapCut Desktop** (Mac/Win, gratuit) — entre les deux

### Actions à faire en montage
1. **Couper les blancs** : tout silence de plus d'1 seconde
2. **Accélérer ×4** les attentes du DAG (passage 3:30-5:00)
3. **Zoomer doucement** sur les viz Kibana (effet "Ken Burns") — donne du dynamisme
4. **Sous-titres** : optionnels mais bonus de pro (générables auto avec iMovie/CapCut)
5. **Musique de fond** : optionnelle, très douce, libre de droits (YouTube Audio Library)
6. **Intro/outro** : 2 secondes de fondu noir au début et à la fin

### Vérifs avant export final
- [ ] Durée totale ≤ 10 minutes
- [ ] Aucune info sensible visible (clé API, mot de passe `.env` flouté)
- [ ] Audio audible sans monter le volume à fond
- [ ] URL GitHub bien lisible à l'écran à la fin
- [ ] Export en **1080p MP4** (taille raisonnable et qualité OK)

---

## 📞 En cas de bug pendant le tournage

| Problème | Solution rapide |
|---|---|
| Docker plante | `docker compose restart` |
| ES inaccessible | `pkill -f Elasticsearch` puis `./bin/elasticsearch -d` |
| Kibana montre "Configure" | Vérifier que ES tourne, sinon Configure manually → `http://localhost:9200` |
| Le DAG plante sur une tâche | Clear la tâche dans l'UI et re-trigger, ou coupe au montage |

Bon tournage ! 🎬
