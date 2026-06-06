# Football ROI Pipeline
## Analyse du retour sur investissement sportif des clubs européens

**Nassim El Kharroubi** — 4ème année école d'ingénieur
Projet Apache Airflow & Big Data
Code source : https://github.com/Nassim821/football-roi-pipeline

---

## Sommaire

1. Introduction et problématique
2. Architecture technique
3. Sources de données et data lake
4. Le pipeline Airflow
5. Le machine learning
6. Résultats : analyse des 8 visualisations
7. Défis techniques surmontés
8. Conclusion

---

## 1. Introduction et problématique

### Contexte

Le football européen vit une période d'inflation historique. Les clubs dépensent chaque année des sommes record en transferts et en salaires, mais cet argent ne se traduit pas toujours en performances sportives. Certains clubs au budget modeste réussissent à finir devant des géants financiers, et d'autres dépensent des centaines de millions d'euros sans même gagner un trophée.

### Question business

Le projet répond à une question simple mais rarement posée dans les classements traditionnels :

> **Quels clubs investissent le mieux leur argent ?**

Pour y répondre, j'ai inventé une métrique métier : le **ROI sportif**, défini comme le nombre de points obtenus en championnat divisé par la valeur de l'effectif en millions d'euros.

```
ROI = points du championnat / valeur de l'effectif (M€)
```

Plus un club obtient de points avec un petit effectif, plus son ROI est élevé : il est "efficace". À l'inverse, un club avec un énorme effectif mais peu de points a un ROI faible : il "gaspille".

### Périmètre

Le projet couvre :

- **5 championnats majeurs** : Ligue 1, Premier League, La Liga, Bundesliga, Serie A
- **La Champions League** (avec un ROI adapté basé sur le stade atteint)
- **3 saisons** : 2022, 2023, 2024

### Apport du machine learning

En plus du ROI, j'utilise un algorithme de clustering (K-Means avec 4 groupes) pour identifier automatiquement des profils de clubs : les Efficaces, les Dominants, les Standards et les Gaspilleurs.

---

## 2. Architecture technique

### Vue d'ensemble

Le projet suit le pattern **Medallion**, très courant en data engineering. Il consiste à organiser le data lake en trois couches successives.

![Architecture](../screens/architecture.png)

### Stack utilisée

| Étape | Technologie | Rôle |
|---|---|---|
| Ingestion | Python `requests` + `cloudscraper` | Récupération des données API + scraping HTML |
| Stockage | JSON puis Parquet | Brut traçable / Parquet typé 10× plus rapide |
| Compute | PySpark + pandas | Spark pour les jointures, pandas pour le formatage |
| Orchestration | Apache Airflow 3.2.1 | Planification + retries + observabilité |
| ML | scikit-learn (K-Means) | Clustering non supervisé |
| Indexation | Elasticsearch 9.0.0 | Stockage analytique et recherche |
| Visualisation | Kibana 9.0.0 | Dashboard interactif |
| Conteneurisation | Docker Compose | Reproductibilité de l'environnement |

### Pourquoi ces choix ?

- **Airflow** plutôt qu'un simple cron : pour gérer automatiquement les dépendances entre tâches, relancer en cas d'erreur, et visualiser l'avancement.
- **Parquet** plutôt que CSV ou JSON : format colonnaire compressé, beaucoup plus rapide à lire et plus léger.
- **Spark** plutôt que pandas pour les JOINs : Spark gère mieux les volumes importants et reste lisible.
- **Elasticsearch + Kibana** plutôt que Tableau ou PowerBI : open source, performant pour la recherche, intégré nativement.

---

## 3. Sources de données et data lake

### Trois sources combinées

Pour calculer le ROI, j'ai eu besoin de deux types d'informations : les performances sportives (points, rang) et les valeurs marchandes (budget de l'effectif).

| Source | Type | Donnée récupérée | Volume / run |
|---|---|---|---|
| API-Football (standings) | API REST officielle | Classements + résultats des 5 ligues | 15 appels |
| Transfermarkt | Web scraping HTML | Valeurs marchandes des effectifs | 15 scrapes |
| API-Football (CL) | API REST officielle | Matchs Champions League | 3 appels |

### Organisation du data lake

Le data lake respecte une **convention de nommage stricte** :

```
data/<layer>/<group>/<dataEntity>/<dateVersion>/
```

Exemple concret :

```
data/
├── raw/                       # Couche 1 : JSON brut
│   ├── football/standings/2022/
│   ├── transfermarkt/squad_values/2022/
│   └── champions_league/fixtures/2022/
├── formatted/                 # Couche 2 : Parquet nettoyé
│   ├── football/standings/
│   ├── transfermarkt/squad_values/
│   └── champions_league/parcours/
└── usage/                     # Couche 3 : Métriques business
    ├── roi_sportif/
    ├── cl_roi/
    └── roi_clusters/
```

### Volumétrie finale

Après le pipeline complet, j'obtiens **312 lignes business** indexées dans Elasticsearch :

- 254 lignes pour les championnats domestiques (5 ligues × ~17 clubs × 3 saisons)
- 58 lignes pour la Champions League
- 254 lignes enrichies par le clustering ML

---

## 4. Le pipeline Airflow

### Vue d'ensemble du DAG

Le DAG `football_roi_pipeline` est composé de **10 tâches** organisées en 5 étapes successives. Il tourne automatiquement tous les jours à 6h UTC.

![DAG Airflow](../screens/airflow.png)

### Détail des tâches

| Tâche | Couche | Rôle |
|---|---|---|
| `source_to_raw_1` | source → raw | Appelle l'API-Football pour récupérer les classements |
| `source_to_raw_2` | source → raw | Scrape Transfermarkt pour les valeurs marchandes |
| `source_to_raw_3` | source → raw | Récupère les matchs de Champions League |
| `raw_to_formatted_1` | raw → formatted | Parse les JSON football → Parquet typé |
| `raw_to_formatted_2` | raw → formatted | Nettoie Transfermarkt (ex : "€45.20m" → 45.2) |
| `raw_to_formatted_3` | raw → formatted | Détermine le stade max atteint en CL par club |
| `produce_usage` | formatted → usage | JOIN Spark standings × valeurs marchandes, calcul du ROI |
| `produce_usage_cl` | formatted → usage | JOIN CL × valeurs marchandes, calcul du `cl_roi` |
| `ml_clustering` | usage → usage | K-Means 4 clusters + assignation des labels |
| `index_to_elastic` | usage → ES | Pousse les 3 datasets vers Elasticsearch |

### Pourquoi 3 couches ?

Cette structure est une best practice de data engineering :

- **Raw** est immuable. Si demain l'API change ou tombe, j'ai toujours mes données originales pour recalculer.
- **Formatted** est lisible techniquement (types corrects, dates UTC, doublons supprimés) mais ne contient pas encore de logique métier.
- **Usage** est spécifique à mon cas d'usage (calcul du ROI). Plusieurs cas d'usage différents pourraient cohabiter à ce niveau.

---

## 5. Le machine learning

### Choix de l'algorithme

J'ai choisi **K-Means** parce que :

- Le problème est **non supervisé** : je n'ai pas de labels au départ, je veux découvrir des groupes naturels dans les données.
- C'est un algorithme **simple à interpréter** : chaque cluster est défini par un centroïde, facile à expliquer.
- Il est **rapide et léger** : 254 lignes traitées en moins d'une seconde.

### Features utilisées

Trois variables servent au clustering :

- `squad_value_m` : valeur de l'effectif (budget)
- `roi` : retour sur investissement sportif
- `points` : points obtenus en championnat

Ces variables sont normalisées avec `StandardScaler` pour éviter qu'une variable à grande échelle (le budget en M€) ne domine l'algorithme.

### Choix de k = 4

J'ai fixé le nombre de clusters à 4 pour obtenir 4 archétypes métiers facilement interprétables, correspondant aux 4 quadrants du plan (budget, ROI) :

- **Petit budget + gros ROI** : Efficaces
- **Gros budget + gros ROI** : Dominants
- **Gros budget + petit ROI** : Gaspilleurs
- **Petit budget + petit ROI** : Standards

### Logique de labelling

Plutôt que d'attribuer les labels à la main, j'ai écrit une fonction qui calcule la **distance euclidienne** entre chaque centroïde et les 4 archétypes du plan normalisé. Une **assignation 1-pour-1** garantit que chaque label est utilisé exactement une fois (j'ai eu un bug sur cette partie, expliqué en section 7).

### Résultat

| Cluster | Budget moyen | ROI moyen | Points moyens | Nombre de clubs |
|---|---|---|---|---|
| Efficaces | 85 M€ | 0.53 | 43 | 119 |
| Dominants | 155 M€ | 0.27 | 40 | 31 |
| Standards | 423 M€ | 0.15 | 59 | 26 |
| Gaspilleurs | 980 M€ | 0.08 | 79 | 104 |

---

## 6. Résultats : analyse des 8 visualisations

Le dashboard Kibana regroupe 8 visualisations. Chacune répond à une question spécifique.

### Viz 1 — Top des clubs par ROI

![Top clubs par ROI](../screens/Top%20club%20par%20ROI.png)

**Lecture :** Clermont Foot domine largement avec un ROI proche de 0.82, suivi de Rayo Vallecano, Holstein Kiel et Cadiz.

**Insight :** Ce sont tous des clubs à petit budget. La logique du ROI inverse complètement le palmarès classique : les "petits" deviennent les plus efficaces, parce qu'ils marquent beaucoup de points par rapport à leur faible investissement.

### Viz 2 — ROI moyen par ligue

![ROI par ligue](../screens/top%20ligue%20par%20roi.png)

**Lecture :** La Liga et la Ligue 1 sont en tête avec un ROI moyen autour de 0.28-0.29. La Bundesliga et la Serie A suivent. La Premier League est largement dernière avec un ROI de seulement 0.10.

**Insight :** La Premier League est la ligue la plus riche du monde, mais c'est aussi la moins efficiente sportivement. L'inflation des salaires anglais ne se traduit pas par une supériorité proportionnelle en points.

### Viz 3 — Évolution du ROI (exemple Barça vs PSG)

![Évolution ROI](../screens/ROI%20moyen%20Barca%20vs%20PSG.png)

**Lecture :** Sur les 3 saisons, le ROI du Barça passe de 0.105 à 0.082. Celui du PSG passe de 0.095 à 0.073. La tendance est nettement baissière pour les deux.

**Insight :** Même pour les très grands clubs, dépenser plus n'apporte pas plus de points. Cette tendance baissière illustre l'inflation des budgets non compensée par une amélioration des performances.

### Viz 4 — Budget vs Points par ligue

![Budget vs Points](../screens/budget%20vs%20pts%20.png)

**Lecture :** Globalement, plus le budget monte, plus les points montent. Mais la courbe sature au-delà de ~600 M€. La Premier League (en rouge) est très éparpillée, ce qui montre qu'avoir un gros budget en Angleterre n'est pas une garantie.

**Insight :** La relation budget-performance est **non linéaire**. C'est un argument économique fort contre les transferts à plusieurs centaines de millions : le rendement marginal de chaque euro investi décroît rapidement.

### Viz 5 — Champions League : Budget vs Stade atteint

![CL Budget vs Stade](../screens/budget%20vs%20stage%20atteint.png)

**Lecture :** Manchester City, Arsenal, Barcelone et le Real Madrid ont les plus gros effectifs et atteignent les derniers stades (5 = demi, 6 = finale, 7 = vainqueur). Les clubs avec moins de 500 M€ d'effectif sont rares au-delà des quarts.

**Insight :** La Champions League est verrouillée par les clubs les plus riches. Le ticket d'entrée pour aller loin est élevé.

### Viz 6 — Évolution CL des grands clubs

![Évolution CL](../screens/évolution_CL.png)

**Lecture :** Sur 3 saisons, le Real Madrid a gagné 2 CL (2022 et 2024). Manchester City a gagné en 2023 puis chute à 2 en 2024. Le PSG progresse (3 → 5 → 7 en 2024 : finale). Le Barça remonte de 1 à 5.

**Insight :** Même chez les plus riches, les performances en CL sont volatiles. La preuve qu'à ce niveau, l'argent ne fait plus toute la différence : il faut aussi la chance et les bons choix sportifs.

### Viz 7 — Table ROI exhaustive

![Data Table](../screens/data%20table.png)

**Lecture :** Tableau trié par ROI décroissant. Clermont Foot (0.818, 42 points pour 54 M€ d'effectif), FC Heidenheim (0.632), Rayo Vallecano (0.611) sont en haut.

**Insight :** Cette table sert de référence pour vérifier n'importe quel club. C'est aussi la donnée brute qui alimente les autres visualisations.

### Viz 8 — Clusters ML : ROI moyen par profil

![Clusters ML](../screens/Roi%20par%20cluster.png)

**Lecture :** Les 4 clusters apparaissent clairement : Efficaces (ROI 0.53), Dominants (0.27), Standards (0.15), Gaspilleurs (0.08).

**Insight :** C'est la vraie valeur ajoutée du ML. L'algorithme a découvert tout seul qu'il existe des **seuils d'efficience** dans le foot. En particulier, il révèle qu'au-delà d'un certain budget, l'argent ne se transforme plus du tout en points. Ce résultat n'aurait pas pu être visible sur une simple moyenne globale.

---

## 7. Défis techniques surmontés

### Défi 1 : Deadlock virtioFS sur macOS

**Symptôme :** Erreur `OSError: [Errno 35] Resource deadlock avoided` à chaque écriture de fichier Parquet par PyArrow dans le conteneur Docker.

**Diagnostic :** Bug connu de la couche virtioFS de Docker Desktop sur macOS. Quand PyArrow utilise du memory-mapping sur un bind mount, les locks ne sont jamais libérés.

**Solution :** Remplacer le bind mount du dossier `data/` par un **volume Docker nommé**. Le volume nommé ne passe pas par virtioFS, le bug disparaît.

### Défi 2 : Conflit de namespace package dans Airflow 3

**Symptôme :** Erreur `ModuleNotFoundError: No module named 'config.config'; 'config' is not a package`.

**Diagnostic :** Le dossier `config/` du projet était monté à deux endroits différents dans le conteneur (`/opt/airflow/config` et `/opt/airflow/project_root/config`). Python le traitait comme un namespace package PEP 420, mal géré par le task runner d'Airflow 3.

**Solution :** Ajouter un simple `__init__.py` vide dans `config/`. Cela force Python à le considérer comme un package régulier, sans ambiguïté.

### Défi 3 : Timeout Elasticsearch sur la création d'index

**Symptôme :** La tâche `index_to_elastic` timeout au moment de créer l'index `football_roi_clusters`.

**Diagnostic :** Par défaut, Elasticsearch crée des indices avec 1 réplique. Sur un cluster single-node, la réplique ne peut pas être allouée, le cluster passe en YELLOW, et la création d'index attend en boucle jusqu'au timeout client (10 secondes).

**Solution :** Forcer `"number_of_replicas": 0` dans les mappings des indices. La création devient instantanée.

### Défi 4 : Labels du clustering qui s'effondrent

**Symptôme :** 4 clusters K-Means détectés, mais seulement 2 labels uniques attribués (Efficaces et Gaspilleurs). Les labels Standards et Dominants n'étaient jamais utilisés.

**Diagnostic :** Ma première logique de labelling utilisait des conditions de seuils de rangs qui se chevauchaient. Quand plusieurs clusters tombaient dans la même condition, ils recevaient le même label.

**Solution :** Réécrire avec une **assignation 1-pour-1** basée sur la distance euclidienne aux 4 archétypes dans le plan normalisé (budget, ROI). Ainsi, chaque label est attribué exactement une fois.

---

## 8. Conclusion

### Bilan technique

J'ai construit un pipeline data complet et fonctionnel de bout en bout :

- 3 sources hétérogènes ingérées automatiquement chaque jour
- Un data lake en 3 couches respectant les bonnes pratiques
- Une orchestration Airflow propre avec 10 tâches dépendantes
- Un calcul métier en PySpark
- Un enrichissement par machine learning
- Une indexation dans Elasticsearch et une visualisation dans Kibana

Le tout est containerisé avec Docker, reproductible par n'importe quel utilisateur grâce au repo GitHub et au guide d'installation.

### Bilan business

Le projet apporte trois insights principaux :

1. **Le ROI sportif inverse le palmarès classique.** Les "petits" clubs comme Clermont Foot ou Rayo Vallecano sont en réalité les plus efficaces. Cette métrique offre une lecture nouvelle du football.

2. **La relation budget-points est non linéaire.** Au-delà d'un certain seuil (autour de 600 M€), dépenser plus n'apporte plus de points proportionnels. C'est un argument économique fort.

3. **Le machine learning révèle des archétypes économiques.** Les 4 clusters montrent qu'il existe des profils de clubs structurellement différents, et que certains sont condamnés à un faible ROI par leur taille.

### Limites identifiées

- Le projet est en batch quotidien, pas en streaming temps réel.
- Le plan API gratuit est limité à 100 requêtes par jour.
- Elasticsearch est en single-node, sans haute disponibilité.

### Pistes futures

- Ajouter Kafka pour ingérer des données temps réel (matchs en direct).
- Monitorer le pipeline avec Prometheus et Grafana.
- Écrire des tests automatisés (pytest) pour la robustesse.
- Étendre à d'autres sports (basket, F1) pour valider la généralité de la métrique.

### Livrables

- **Code source** : https://github.com/Nassim821/football-roi-pipeline
- **Vidéo de démonstration** : (lien à compléter)
- **Dashboard Kibana** : exporté en `.ndjson` dans le dossier `kibana/` du repo
- **Guide d'installation Windows** : `docs/SETUP_WINDOWS.md`

---

*Merci de votre lecture.*
