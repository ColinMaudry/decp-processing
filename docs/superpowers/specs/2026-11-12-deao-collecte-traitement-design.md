# DEAO — séparer la collecte et le traitement

Date : 2026-11-12
Issue : aucune (refactor).
Statut : design validé, à planifier.
Base : branche `feature/178_boamp` après la source JOUE
(`2026-11-11-deao-processing-design.md`, `2026-11-12-deao-joue-source-design.md`).

## Intention

DEAO doit produire **deux datasets**, pas un :

- **dataset a** (public, existant, `DEAO_DATASET_ID` actuel) : les appels d'offres
  **Open Data** agrégés (BOAMP + JOUE), avec leur `schema.json` ;
- **dataset b** (privé, `6af5c52094447e949d27b452`) : un fichier par source collectée
  (Open Data et, plus tard, plateformes scrapées en Closed Data), et l'agrégat
  **Open + Closed** `deao.parquet`, lu par Colibre.

Aujourd'hui, un seul flow `deao_processing` fait tout, pour BOAMP + JOUE. Avec l'arrivée de
scrapers d'appels d'offres de plateformes (AWS, marches-securises.fr…), on distingue deux
familles de flows :

1. **la collecte** : récupérer, normaliser au schéma `reference/schema_deao.json`,
   consolider, enrichir et publier les données d'une source (ou d'une famille de sources) ;
2. **le traitement** (`deao_processing`, cousin simplifié de `decp_processing`) : les
   données sont déjà standardisées et enrichies par nos collectes ; il reste à les
   concaténer, dédoublonner entre sources, produire les statistiques et publier les
   agrégats.

Les appels d'offres sont sensibles au temps : les utilisateurs cherchent presque toujours
les derniers avis publiés. Collectes et traitement tournent donc plusieurs fois par jour.

## Objectifs

- Un flow de collecte `scrap_open` (l'actuel `deao_processing` sans sa publication
  agrégée) qui publie `boamp.parquet` et `joue.parquet` sur le dataset b, puis lance
  `deao_processing`.
- Un flow de collecte `scrap_closed` qui lance en parallèle tous les scrapers d'appels
  d'offres des plateformes, chacun publiant son fichier sur le dataset b, puis lance
  `deao_processing`. Créé dès maintenant, **sans aucun scraper** (le premier s'y branchera).
- Un nouveau `deao_processing` qui lit les fichiers par source **sur data.gouv.fr**
  (dataset b), les concatène, applique le dédoublonnage inter-sources, produit les
  statistiques par source et publie les agrégats sur les datasets a et b.
- Code rangé **par produit** : `src/tasks/scrap/decp/` (scrapers DECP existants),
  `src/tasks/scrap/deao/` (collectes DEAO).
- Déploiements DEAO sur le serveur principal (pool `local`) depuis GitHub, avec des tags
  Prefect ; tags ajoutés aussi aux déploiements DECP.

## Non-objectifs

- Écrire un scraper d'appels d'offres de plateforme (Closed Data) : hors périmètre, le
  flow `scrap_closed` est livré vide.
- Définir les règles de dédoublonnage entre Open Data et Closed Data : elles seront
  établies à partir des données des premières plateformes scrapées. En v1, l'étape existe
  (emplacement, interface) mais ne retire rien.
- Changer le contenu ou le schéma publié des appels d'offres BOAMP/JOUE.
- Modifier `decp_processing` au-delà du déplacement des scrapers DECP et des tags de
  déploiement.

## Anti-objectifs

- Faire dépendre `deao_processing` du disque d'une collecte (partitions, `dist`) : il ne
  lit que data.gouv.fr, pour que collectes et traitement restent découplés.
- Publier un agrégat à partir d'un fichier source manifestement incomplet (collecte
  cassée) : les garde-fous de nombre de lignes s'appliquent aux fichiers par source comme
  aux agrégats.
- Perdre le dédoublonnage BOAMP/JOUE ou son garde-fou (`RecouvrementIncoherent`) dans le
  déplacement.

## Décisions

| Sujet                         | Décision                                                                                                                                                                                                                                     |
| ----------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Collecte BOAMP/JOUE           | **Un seul** flow `scrap_open` ; il garde acquisition, consolidation, dédoublonnage BOAMP/JOUE et garde-fou, enrichissement ; publie **deux ressources** (`boamp.parquet`, `joue.parquet`)                                                    |
| Garde-fou de recouvrement     | Reste dans `scrap_open` (il porte sur les partitions locales BOAMP) ; inchangé                                                                                                                                                               |
| Lecture par `deao_processing` | Depuis les ressources du **dataset b** sur data.gouv.fr, avec la clé API (`DATAGOUVFR_API_KEY`) sur les GET comme sur les PUT (dataset privé)                                                                                                |
| Dédoublonnage inter-sources   | Étape présente, sans règle en v1 (aucune source Closed Data)                                                                                                                                                                                 |
| Statistiques                  | Celles du rapport de run actuel, **par source**                                                                                                                                                                                              |
| Publication dataset a         | `deao.parquet` + `deao.csv.gz` + `schema.json` = agrégat des sources Open Data, ressources actuelles (`DEAO_PARQUET_RESOURCE_ID`, `DEAO_CSV_RESOURCE_ID`, `DEAO_SCHEMA_RESOURCE_ID`)                                                         |
| Publication dataset b         | `boamp.parquet` (`9861dd3a-e4af-49c7-b8f9-8d20b0cfdd62`) et `joue.parquet` (`8fd238e4-d5ab-40d1-9b61-a07c2fdb020c`) par `scrap_open` ; `deao.parquet` (`b86dd1f4-68f5-4847-9a25-c6d234aa3d66`) = agrégat Open + Closed par `deao_processing` |
| Enchaînement                  | `scrap_open` puis `deao_processing` (sous-flow) ; `scrap_closed` (scrapers en parallèle) puis `deao_processing` (sous-flow)                                                                                                                  |
| Planification                 | `scrap_open` : tous les jours à 8h, 12h et 16h ; `scrap_closed` : tous les jours à 6h                                                                                                                                                        |
| Exécution                     | Serveur principal, pool `local`, déploiement depuis GitHub (`flow.from_source(GitRepository(...))`, comme avant la v2.17) ; plus de déploiement DEAO sur Scaleway                                                                            |
| Rangement du code             | Par produit : `src/tasks/scrap/decp/` et `src/tasks/scrap/deao/`                                                                                                                                                                             |
| Tags                          | Quatre familles combinables (voir « Tags ») ; déploiements DEAO et DECP tagués                                                                                                                                                               |

## Architecture

### Flows

```
src/flows/scrap_open.py   # collecte BOAMP + JOUE → boamp.parquet, joue.parquet (dataset b) → deao_processing
src/flows/scrap_closed.py       # scrapers de plateformes en parallèle (aucun en v1) → deao_processing
src/flows/deao_processing.py    # lecture dataset b → concaténation → dédoublonnage → stats → publication a et b
src/flows/scrap.py              # inchangé (scrapers DECP), imports mis à jour
```

`deao_processing` reste aussi lançable seul (`run_flow.py deao_processing`, déploiement
sans planification) pour republier sans recollecter.

### Rangement des tâches

```
src/tasks/scrap/decp/           # aws.py, dume.py, klekoon.py, marches_securises.py (déplacés tels quels)
src/tasks/scrap/deao/
  boamp_joue/                   # tout ce qui est propre à BOAMP/JOUE :
    boamp/  joue/               #   adaptateurs actuels (src/tasks/deao/sources/{boamp,joue})
    eforms.py  xml.py  codes.py  classement.py
  <plateforme>/                 # futurs scrapers Closed Data (aucun en v1)
src/tasks/deao/                 # tronc commun DEAO, utilisé par les collectes et le traitement :
  schema.py  enrich.py  geocode_cache.py  values.py  report.py
  acquire.py  archive.py        #   acquisition par jour, générique (Source)
  consolidate.py                #   chaînage des avis + recouvrement BOAMP/JOUE
  sources_publiees.py           #   (nouveau) lecture/écriture des fichiers par source sur data.gouv.fr (registre : reference/deao_sources.yaml)
```

_(Mon choix.)_ Règle de rangement : ce qui ne sert qu'à BOAMP/JOUE va sous
`scrap/deao/boamp_joue/` ; ce que d'autres collectes DEAO ou le traitement peuvent réutiliser
(schéma, enrichissement, rapport, acquisition générique par jour, chaînage d'avis) reste dans
`src/tasks/deao/`. Le recouvrement BOAMP/JOUE (`ecarter_recouvrement`) peut rester dans
`consolidate.py` ou partir sous `boamp_joue/` : laissé au builder, tant que le comportement est
inchangé.

### `scrap_open`

Déroulé = étapes 0 à 5 de l'actuel `deao_processing` (référentiels, acquisition BOAMP et
JOUE, résolution des identifiants JOUE, consolidation avec recouvrement et garde-fou,
enrichissement, rapport de collecte), puis :

1. écriture dans `DEAO_DIST_DIR` de `boamp.parquet` et `joue.parquet` : colonnes publiées
   (`schema_deao.json`), une ligne par appel d'offres × lot, filtrées par `sourceDataset` ;
2. publication des deux fichiers sur le dataset b (garde-fous ci-dessous) ;
3. appel de `deao_processing` en sous-flow.

`DEAO_SOURCES` garde son rôle : sources acquises par ce flow (`boamp`, `joue`). Une source
désactivée n'est ni collectée ni publiée ; son ancien fichier sur le dataset b reste en place
_(mon choix)_.

Le rapport de collecte actuel (compteurs d'acquisition, exclusions, recouvrement,
identifiants, statuts, taux de remplissage) reste l'artefact de ce flow.

Un jour en erreur fait toujours échouer le flow en fin de run, après publication des
fichiers sains. `deao_processing` est quand même lancé avant cet échec _(mon choix : les
fichiers publiés sont cohérents, les jours manquants sont retentés au run suivant)_.

### `scrap_closed`

- Liste des scrapers de plateformes (vide en v1), lancés en parallèle (`run_in_pool` ou
  tâches Prefect concurrentes, au choix du builder).
- Chaque scraper produit des lignes au schéma `schema_deao.json` (colonnes publiées) et
  publie son fichier sur la ressource de son entrée du registre. Le contrat d'un scraper
  (signature, retour, compteurs) est défini dans ce refactor, pour que le premier scraper
  n'ait qu'à s'y conformer.
- Scrapers exécutés : sources `open_data: false` non `skip` du registre, ou
  `SOLO_DEAO_SCRAPS` si renseignée.
- Un scraper en erreur n'empêche pas les autres ; le flow lance `deao_processing` puis
  échoue s'il y a eu une erreur.
- En v1, le flow ne fait que lancer `deao_processing`.

### `deao_processing`

1. **Lecture** : pour chaque source du **registre des sources publiées** (voir ci-dessous ;
   v1 : `boamp`, `joue`), téléchargement de sa ressource du dataset b (API data.gouv.fr, clé
   API). Une ressource absente ou illisible arrête le run avant publication _(mon choix :
   publier un agrégat sans une source ferait disparaître ses appels d'offres de Colibre
   jusqu'au run suivant)_.
2. **Contrôle** : colonnes conformes à `schema_deao.json` (types compris) ; nombre de lignes
   par source au moins égal à son minimum (voir Configuration).
3. **Concaténation** des sources.
4. **Dédoublonnage inter-sources** : fonction dédiée, sans règle en v1 (identité) ; elle
   reçoit toutes les lignes et renvoie les lignes conservées plus ses compteurs.
5. **Contrôle d'unicité de `uid`** sur l'agrégat (le préfixe de source rend une collision
   entre sources impossible ; un doublon signale un bug et arrête le run).
6. **Statistiques par source** (artefact Prefect) : nombre de lignes, répartition par
   `statut`, répartition de `acheteur_id_source`, taux de remplissage par colonne — les
   indicateurs du rapport actuel qui se calculent sur les colonnes publiées, ventilés par
   `sourceDataset`. Les compteurs propres à la collecte (avis lus, exclus, jours en erreur…)
   restent dans le rapport de la collecte.
7. **Écriture** dans `DEAO_DIST_DIR` : agrégat Open Data (`deao.parquet`, `deao.csv.gz`,
   `schema.json`) et agrégat complet (`deao.parquet` du dataset b, sous un autre nom de
   fichier local).
8. **Publication** : dataset a (agrégat Open Data + schéma), dataset b (agrégat complet).

### Registre des sources : `reference/deao_sources.yaml`

Fichier de référence YAML, analogue à `reference/source_datasets.json` (DECP), une entrée par
source publiée sur le dataset b :

```yaml
- code: boamp # = sourceDataset, préfixe d'uid
  name: Bulletin officiel des annonces des marchés publics (BOAMP, DILA)
  open_data: true
  resource_id: 9861dd3a-e4af-49c7-b8f9-8d20b0cfdd62
  min_rows: 50000
- code: joue
  name: Journal officiel de l'Union européenne (JOUE, via TED)
  open_data: true
  resource_id: 8fd238e4-d5ab-40d1-9b61-a07c2fdb020c
  min_rows: 10000
```

Champs : `code`, `name`, `open_data` (vrai : source collectée par `scrap_open` et
incluse dans l'agrégat du dataset a ; faux : source collectée par `scrap_closed`),
`resource_id` (ressource du dataset b), `min_rows` (garde-fou du fichier), et `skip: true`
facultatif comme dans `source_datasets.json`. Chargé et validé par `src/config.py` (champs
obligatoires, codes uniques) ; chemin surchargeable par une variable
(`DEAO_SOURCES_REFERENCE_FILEPATH`, sur le modèle de `DATASETS_REFERENCE_FILEPATH`) pour les
tests _(mon choix)_.

- `deao_processing` lit **toutes** les sources non `skip` du registre, indépendamment de
  `DEAO_SOURCES` (qui ne pilote que la collecte BOAMP/JOUE) : désactiver la collecte JOUE
  laisse `joue.parquet` en place et ses appels d'offres restent dans les agrégats.
- L'agrégat Open Data (dataset a) est l'ensemble des sources `open_data: true`.
- `scrap_closed` exécute les sources `open_data: false` non `skip` ; chacune a son
  scraper, retrouvé par son `code` (correspondance code → fonction de scraping dans le code
  de `scrap_closed`, comme le `target` du flow `scrap` de DECP).
- Ajouter une source Closed Data = écrire son scraper sous `src/tasks/scrap/deao/<code>/`,
  créer sa ressource sur le dataset b, l'inscrire au registre. Tant que son premier fichier
  n'est pas publié, elle reste `skip: true` (sinon `deao_processing` échouerait, ressource
  absente). Un scraper inscrit qui échoue ne bloque pas le traitement : son fichier précédent
  reste lu.
- `SOLO_DEAO_SCRAPS` (liste séparée par des virgules, vide par défaut), sur le modèle de
  `SOLO_DATASETS` : si renseignée, `scrap_closed` n'exécute que ces scrapers (y compris une
  source `skip`, pour la tester). Comme `SOLO_DATASETS` dans `decp_processing`, un run avec
  `SOLO_DEAO_SCRAPS` non vide **ne publie rien** (ni fichiers par source, ni agrégats) _(mon
  choix : c'est un mode de test)_.

En v1, les deux agrégats ont le même contenu.

## Publication et garde-fous

- Toute publication exige `DECP_PROCESSING_PUBLISH` (comme aujourd'hui).
- Pas de publication si `SOLO_DEAO_SCRAPS` est renseignée.
- Fichier par source : au moins le `min_rows` de son entrée du registre, sinon pas de
  publication de ce fichier (l'ancien reste) et le flow échoue en fin de run.
- Agrégat Open Data (dataset a) : au moins `DEAO_MIN_ROWS_TO_PUBLISH` (actuel, 50 000).
- Agrégat complet (dataset b) : au moins `DEAO_MIN_ROWS_TO_PUBLISH` également.
- Mise à jour des ressources par la fonction existante `update_resource(dataset_id, resource_id, file_path, api_key)` (`src/tasks/publish.py`).

## Exécution et déploiements

- Pool `local` (serveur principal), source GitHub
  (`GitRepository(url="https://github.com/ColinMaudry/decp-processing.git", branch=...)`),
  comme les déploiements DECP d'avant la v2.17.
- Déploiements prod (branche `main`) :
  - `scrap-open` : `cron="0 8,12,16 * * *"` ;
  - `scrap-closed` : `cron="0 6 * * *"` ;
  - `deao-processing` : sans planification (lancement manuel).
- Déploiements dev (branche `dev`), sans planification, `DECP_PROCESSING_PUBLISH` faux _(mon
  choix, comme `decp-processing-dev`)_.
- Concurrence _(mon choix)_ :
  - un seul run `scrap_open` à la fois (un rattrapage long ne doit pas chevaucher le
    run suivant) : `concurrency_limit=1` sur son déploiement (même chose pour
    `scrap-closed`) ;
  - un seul `deao_processing` à la fois (deux collectes qui se terminent ensemble ne
    publient pas en parallèle). Comme il tourne en **sous-flow**, une limite de déploiement
    ou de tag ne s'applique pas : le corps de `deao_processing` s'exécute sous une limite de
    concurrence **globale** Prefect (`with concurrency("deao-processing", occupy=1)`), qui
    fait attendre la seconde collecte. La limite `deao-processing` (1 slot) est créée sur le
    serveur Prefect à la mise en service ; hors serveur (tests, run local éphémère), son
    absence ne doit pas bloquer le run (comportement à vérifier, au besoin
    `strict=False`).
- Variables d'environnement des déploiements : `DEAO_DATA_DIR` et `DEAO_DIST_DIR` sur le
  serveur (sur le modèle de `DECP_DATA_DIR` / `DECP_DIST_DIR`), identifiants des datasets et
  ressources, `DECP_PROCESSING_PUBLISH`.
- Suppression des déploiements DEAO Scaleway (`deao-processing` lanceur, `-exec-prod`,
  `-exec-dev`) de `src/deployments.py`. Le lanceur Scaleway et ses autres usages (DECP)
  restent.
- SIRENE : déjà préparé sur le serveur principal par `sirene_preprocess` (mensuel) ; la
  collecte garde son repli (sous-flow si le mois est absent).
- Mémoire : non mesurée sur le serveur principal ; l'utilisateur juge le risque acceptable.

## Tags

Posés sur les déploiements (les runs en héritent). Quatre familles :

| Famille       | Valeurs                                                                                   |
| ------------- | ----------------------------------------------------------------------------------------- |
| produit       | `attributions` (DECP), `appels-offres` (DEAO)                                             |
| étape         | `scrap`, `traitement`, `referentiel`                                                      |
| source        | `boamp`, `joue`, `aws`, `marches-securises`, `dume`, `klekoon` (une par source collectée) |
| environnement | `prod`, `dev`                                                                             |

Exemples : `scrap-open` → `appels-offres`, `scrap`, `boamp`, `joue`, `prod` ;
`decp-processing` → `attributions`, `traitement`, `prod` ; `sirene-preprocess` →
`referentiel`, `prod`.

Un sous-flow n'hérite pas des tags du déploiement parent pour le filtrage par étape :
l'appel de `deao_processing` depuis une collecte se fait dans un bloc
`with tags("appels-offres", "traitement")`.

Tous les déploiements de `src/deployments.py` (DECP compris) reçoivent leurs tags.

## Configuration

| Variable                                                                      | Valeur / défaut                                                   | Rôle                                                                         |
| ----------------------------------------------------------------------------- | ----------------------------------------------------------------- | ---------------------------------------------------------------------------- |
| `DEAO_OPEN_DATASET_ID`                                                        | valeur actuelle de `DEAO_DATASET_ID` (`6a26929bee84b87c37602925`) | dataset a ; remplace `DEAO_DATASET_ID` _(mon choix, renommage)_              |
| `DEAO_PARQUET_RESOURCE_ID`, `DEAO_CSV_RESOURCE_ID`, `DEAO_SCHEMA_RESOURCE_ID` | inchangés                                                         | ressources du dataset a                                                      |
| `DEAO_CLOSED_DATASET_ID`                                                      | `6af5c52094447e949d27b452`                                        | dataset b                                                                    |
| `DEAO_CLOSED_PARQUET_RESOURCE_ID`                                             | `b86dd1f4-68f5-4847-9a25-c6d234aa3d66`                            | `deao.parquet` du dataset b                                                  |
| `DEAO_MIN_ROWS_TO_PUBLISH`                                                    | `50000` (inchangé)                                                | garde-fou des agrégats                                                       |
| `DEAO_SOURCES`                                                                | `boamp,joue` (inchangé)                                           | sources acquises par `scrap_open`                                            |
| `SOLO_DEAO_SCRAPS`                                                            | vide                                                              | scrapers de `scrap_closed` à exécuter (test) ; non vide = aucune publication |
| `DEAO_SOURCES_REFERENCE_FILEPATH`                                             | `reference/deao_sources.yaml`                                     | registre des sources (surchargé en test)                                     |

Les identifiants de ressource et les minimums par source sont dans `reference/deao_sources.yaml`, pas dans l'environnement.

Les valeurs ci-dessus sont reportées dans `template.env`.

## Tests

- Collecte : le test bout en bout actuel devient celui de `scrap_open`
  (HTTP, S3 et data.gouv.fr simulés) ; il vérifie l'écriture et la publication de
  `boamp.parquet` et `joue.parquet` (colonnes publiées, `uid` uniques, chaîne JOUE
  recoupant le BOAMP absente de `joue.parquet`) et l'appel de `deao_processing`.
- `deao_processing` : lecture simulée des ressources du dataset b (avec clé API) ;
  concaténation ; dédoublonnage identité ; unicité de `uid` (doublon → échec) ; source
  absente → échec avant publication ; fichier source sous son minimum → échec ; agrégat
  Open Data restreint aux sources Open Data du registre ; lecture indépendante de `DEAO_SOURCES` ; statistiques ventilées par source ;
  publication sur a et b derrière les garde-fous.
- `scrap_closed` : sans scraper, lance `deao_processing` ; avec deux faux scrapers dont un
  en erreur, l'autre publie, `deao_processing` est lancé, le flow échoue ; `SOLO_DEAO_SCRAPS`
  n'exécute que les scrapers listés (source `skip` comprise) et ne publie rien.
- Registre : chargement de `reference/deao_sources.yaml`, champ manquant ou code en double
  → erreur au chargement ; `skip` exclu de la lecture par `deao_processing`.
- Déploiements : `test_deao_deployments.py` adapté (pool `local`, crons, tags, plus de
  Scaleway pour DEAO) ; tags présents sur les déploiements DECP.
- Les tests existants des scrapers DECP passent après déplacement (imports seulement).

## Mise en service (hors code)

- Retirer du serveur Prefect les anciens déploiements DEAO Scaleway.
- Créer la limite de concurrence globale `deao-processing` (1 slot) sur le serveur Prefect.
- Créer `DEAO_DATA_DIR` et `DEAO_DIST_DIR` sur le serveur principal ; premier run de
  `scrap_open` avec `DEAO_REBUILD=true` (reconstruction depuis l'archive S3, sans
  requête DILA/TED pour les jours archivés).
- `.env` : `DEAO_DATASET_ID` → `DEAO_OPEN_DATASET_ID`, nouvelles variables du dataset b.

## Hors périmètre

- Scrapers d'appels d'offres de plateformes et règles de dédoublonnage Open/Closed Data.
- Statistiques publiées en fichier (au-delà de l'artefact Prefect).
- Retour éventuel de la collecte BOAMP/JOUE sur une instance éphémère si la mémoire du
  serveur principal ne suffit pas.
