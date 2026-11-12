# DEAO — séparation collecte / traitement Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Scinder l'actuel `deao_processing` en flows de collecte (`scrap_open` pour BOAMP/JOUE, `scrap_closed` pour les plateformes, vide en v1) qui publient un fichier par source sur le dataset b, et un nouveau `deao_processing` qui lit ces fichiers sur data.gouv.fr, les agrège et publie les datasets a et b.

**Architecture:** Un registre YAML (`reference/deao_sources.yaml`) décrit les sources publiées. Le code propre à BOAMP/JOUE part sous `src/tasks/scrap/deao/boamp_joue/`, les scrapers DECP sous `src/tasks/scrap/decp/`, le tronc commun reste dans `src/tasks/deao/`. Les collectes appellent `deao_processing` en sous-flow, qui tourne sous une limite de concurrence globale. Les déploiements DEAO passent sur le pool `local` depuis GitHub, avec des tags.

**Tech Stack:** Python 3, polars, Prefect 3, httpx, PyYAML, pytest.

**Spec:** `docs/superpowers/specs/2026-11-12-deao-collecte-traitement-design.md` (autorité ; lire la section de la tâche avant de commencer).

## Global Constraints

- Aucun changement de contenu ni de schéma des lignes BOAMP/JOUE publiées (`reference/schema_deao.json` inchangé).
- Le dédoublonnage BOAMP/JOUE et le garde-fou `RecouvrementIncoherent` restent dans la collecte `scrap_open`, inchangés.
- `deao_processing` ne lit que data.gouv.fr (dataset b), jamais le disque d'une collecte.
- Datasets : a = `DEAO_OPEN_DATASET_ID` (ex-`DEAO_DATASET_ID`, `6a26929bee84b87c37602925`) ; b = `DEAO_CLOSED_DATASET_ID` = `6af5c52094447e949d27b452`, `deao.parquet` du b = `DEAO_CLOSED_PARQUET_RESOURCE_ID` = `b86dd1f4-68f5-4847-9a25-c6d234aa3d66`.
- Registre : `boamp` → ressource `9861dd3a-e4af-49c7-b8f9-8d20b0cfdd62`, `min_rows: 50000` ; `joue` → `8fd238e4-d5ab-40d1-9b61-a07c2fdb020c`, `min_rows: 10000` ; tous deux `open_data: true`.
- Clé API `DATAGOUVFR_API_KEY` sur les GET et les PUT vers data.gouv.fr (dataset b privé).
- Toute publication exige `DECP_PROCESSING_PUBLISH` ; aucune publication si `SOLO_DEAO_SCRAPS` est non vide.
- Crons : `scrap-open` `0 8,12,16 * * *` ; `scrap-closed` `0 6 * * *` ; `deao-processing` sans planification. Pool `local`, source `GitRepository(url=REPO_URL, branch="main"|"dev")`.
- Tags : produit `attributions`/`appels-offres` ; étape `scrap`/`traitement`/`referentiel` ; source `boamp`, `joue`, `aws`, `marches-securises`, `dume`, `klekoon` ; environnement `prod`/`dev`.
- Imports absolus `src.` ; tests `uv run pytest` ; aucun réseau en test (HTTP, S3, data.gouv.fr simulés).
- Commits : français, préfixe `DEAO : ` (ou `DECP : ` pour la tâche 1), **sans numéro d'issue**, terminés par `Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>`. Staging par chemins explicites uniquement (jamais `git add -A`/`.`/`-u`/`commit -a` : fichiers non suivis de l'utilisateur dans le dépôt). Pas de push.

## Review Focus

- Ressource d'une source inscrite au registre absente ou illisible sur data.gouv.fr : `deao_processing` échoue **avant** toute publication (test tâche 5).
- Collecte BOAMP/JOUE qui produit un fichier sous son minimum : ce fichier n'est pas publié, l'ancien reste, `deao_processing` est lancé, le flow échoue en fin de run (test tâche 6).
- `SOLO_DEAO_SCRAPS` renseignée : aucun appel `update_resource`, ni par les scrapers ni par `deao_processing` (test tâche 7).
- Deux sources du registre dont les fichiers contiennent un même `uid` : `deao_processing` échoue au contrôle d'unicité, sans publier (test tâche 5).
- `DEAO_SOURCES=boamp` : `scrap_open` ne publie que `boamp.parquet`, et `deao_processing` lit quand même `joue.parquet` (test tâche 6).

---

### Task 1: Scrapers DECP sous `src/tasks/scrap/decp/`

**Files:**

- Move: `src/tasks/scrap/{aws,dume,klekoon,marches_securises}.py` → `src/tasks/scrap/decp/` (+ `__init__.py`)
- Modify: `src/flows/scrap.py`, tout autre import (grep `src.tasks.scrap.`), tests (`tests/test_scrap_marches_securises.py`…)

**Interfaces:**

- Produces: `src.tasks.scrap.decp.{aws,dume,klekoon,marches_securises}` (contenus inchangés).

- [ ] **Step 1:** `git mv` des quatre fichiers ; mise à jour des imports (`grep -rn "src.tasks.scrap\." src tests run_flow.py`).
- [ ] **Step 2:** Vérifier : `uv run pytest -q --ignore=tests/test_main.py` → tout passe ; `grep -rn "src.tasks.scrap.aws\|src.tasks.scrap.dume\|src.tasks.scrap.klekoon\|src.tasks.scrap.marches_securises" src tests` → aucun résultat.
- [ ] **Step 3:** Commit `DECP : scrapers DECP rangés sous src/tasks/scrap/decp`.

---

### Task 2: Code BOAMP/JOUE sous `src/tasks/scrap/deao/boamp_joue/`

**Files:**

- Move: `src/tasks/deao/sources/boamp/` → `src/tasks/scrap/deao/boamp_joue/boamp/` ; `src/tasks/deao/sources/joue/` → `src/tasks/scrap/deao/boamp_joue/joue/` ; `src/tasks/deao/{eforms,xml,codes,classement}.py` → `src/tasks/scrap/deao/boamp_joue/` (+ `__init__.py` à chaque niveau, `src/tasks/scrap/deao/__init__.py`)
- Delete: `src/tasks/deao/sources/`
- Modify: imports dans `src/`, `tests/` (`tests/deao_fakes.py` compris : chemins de monkeypatch)
- Restent dans `src/tasks/deao/` : `schema.py`, `enrich.py`, `geocode_cache.py`, `values.py`, `report.py`, `acquire.py`, `archive.py`, `consolidate.py`.

**Interfaces:**

- Produces: `src.tasks.scrap.deao.boamp_joue.boamp.source.BOAMP`, `src.tasks.scrap.deao.boamp_joue.joue.source.JOUE`, `src.tasks.scrap.deao.boamp_joue.joue.identifiants.resoudre_avis_precedents`, `src.tasks.scrap.deao.boamp_joue.boamp.parse.source_file_expr` (contenus inchangés).

- [ ] **Step 1:** `git mv` ; mise à jour des imports. `values.py` ne doit pas importer depuis `scrap/` (le tronc commun ne dépend pas d'un scraper) : vérifier `grep -rn "src.tasks.scrap" src/tasks/deao` → aucun résultat.
- [ ] **Step 2:** Vérifier : `uv run pytest tests -q -k "deao or boamp"` → 263 passed (même nombre qu'avant) ; `grep -rn "src.tasks.deao.sources\|src.tasks.deao.eforms\|src.tasks.deao.xml\|src.tasks.deao.codes\|src.tasks.deao.classement" src tests` → aucun résultat.
- [ ] **Step 3:** Commit `DEAO : code propre à BOAMP/JOUE rangé sous src/tasks/scrap/deao/boamp_joue`.

---

### Task 3: Registre `reference/deao_sources.yaml` et configuration

**Files:**

- Create: `reference/deao_sources.yaml`, `tests/data/deao_sources_test.yaml`
- Modify: `src/config.py`, `pyproject.toml` (dépendance `pyyaml`, + `DEAO_SOURCES_REFERENCE_FILEPATH=tests/data/deao_sources_test.yaml` dans `[tool.pytest.ini_options]` env), `template.env`, `src/tasks/publish.py` (`DEAO_DATASET_ID` → `DEAO_OPEN_DATASET_ID`)
- Test: `tests/test_deao_registre.py`

**Interfaces:**

- Produces (`src/config.py`) :
  ```python
  @dataclass(frozen=True)
  class SourceDeao:
      code: str
      name: str
      open_data: bool
      resource_id: str
      min_rows: int
      skip: bool = False
  def charger_registre_deao(path: Path) -> tuple[SourceDeao, ...]   # ValueError : champ manquant, code en double
  DEAO_REGISTRE: tuple[SourceDeao, ...]       # chargé depuis DEAO_SOURCES_REFERENCE_FILEPATH (défaut REFERENCE_DIR / "deao_sources.yaml")
  SOLO_DEAO_SCRAPS: list[str]                 # env, virgules, vide par défaut (même parsing que SOLO_DATASETS)
  DEAO_OPEN_DATASET_ID, DEAO_CLOSED_DATASET_ID, DEAO_CLOSED_PARQUET_RESOURCE_ID: str   # env, défaut ""
  ```
  `DEAO_DATASET_ID` disparaît (remplacé partout par `DEAO_OPEN_DATASET_ID`).
- Contenu de `reference/deao_sources.yaml` : exactement les deux entrées de la spec (section « Registre des sources ») avec les valeurs des Global Constraints. Le fichier de test reprend les mêmes codes avec `min_rows: 1`.
- `template.env` : `DEAO_OPEN_DATASET_ID=6a26929bee84b87c37602925`, `DEAO_CLOSED_DATASET_ID=6af5c52094447e949d27b452`, `DEAO_CLOSED_PARQUET_RESOURCE_ID=b86dd1f4-68f5-4847-9a25-c6d234aa3d66`, `# SOLO_DEAO_SCRAPS=`, `# DEAO_SOURCES_REFERENCE_FILEPATH=reference/deao_sources.yaml`.

- [ ] **Step 1: Tests**

```python
def test_registre_reference():
    r = {s.code: s for s in charger_registre_deao(REFERENCE_DIR / "deao_sources.yaml")}
    assert set(r) == {"boamp", "joue"}
    assert r["boamp"].resource_id == "9861dd3a-e4af-49c7-b8f9-8d20b0cfdd62" and r["boamp"].min_rows == 50000
    assert r["joue"].resource_id == "8fd238e4-d5ab-40d1-9b61-a07c2fdb020c" and r["joue"].min_rows == 10000
    assert all(s.open_data and not s.skip for s in r.values())

def test_registre_champ_manquant(tmp_path):      # entrée sans resource_id → ValueError
def test_registre_code_en_double(tmp_path):      # deux "boamp" → ValueError
def test_registre_skip_lu(tmp_path):             # skip: true → SourceDeao.skip is True
```

- [ ] **Step 2:** `uv run pytest tests/test_deao_registre.py -q` → FAIL (ImportError).
- [ ] **Step 3:** Implémenter ; `yaml.safe_load`.
- [ ] **Step 4:** `uv run pytest tests -q -k "deao or boamp"` → PASS.
- [ ] **Step 5:** Commit `DEAO : registre des sources reference/deao_sources.yaml, SOLO_DEAO_SCRAPS, datasets a et b`.

---

### Task 4: Fichiers par source sur data.gouv.fr (`src/tasks/deao/sources_publiees.py`)

**Files:**

- Create: `src/tasks/deao/sources_publiees.py`
- Modify: `tests/deao_fakes.py` (faux data.gouv.fr)
- Test: `tests/test_deao_sources_publiees.py`

**Interfaces:**

- Consumes: `SourceDeao`, `DEAO_CLOSED_DATASET_ID`, `DATAGOUVFR_API_KEY`, `update_resource` (`src/tasks/publish.py`).
- Produces :
  - `http_get(url: str, headers: dict) -> httpx.Response` : seul accès réseau en lecture (monkeypatché en test).
  - `telecharger_fichier_source(source: SourceDeao, dest_dir: Path) -> Path` : `GET https://www.data.gouv.fr/api/1/datasets/{DEAO_CLOSED_DATASET_ID}/resources/{source.resource_id}/` avec en-tête `X-API-KEY`, puis GET du champ `url` (même en-tête) ; écrit `dest_dir / f"{source.code}.parquet"`. Statut non 200 ou fichier non lisible en parquet → `FichierSourceIndisponible` (sous-classe de `RuntimeError`, nom de la source dans le message).
  - `publier_fichier(dataset_id: str, resource_id: str, path: Path) -> None` : `update_resource(..., DATAGOUVFR_API_KEY)` ; `success` différent de `True` → `RuntimeError`.
  - `publication_autorisee() -> bool` : `DECP_PROCESSING_PUBLISH` vrai et `SOLO_DEAO_SCRAPS` vide (lus via `src.config` à l'appel).
- `tests/deao_fakes.py` : fixture `fake_datagouv` (classe `FakeDataGouv` : `fichiers: dict[str, bytes]` par `resource_id`, `publies: list[tuple[dataset_id, resource_id, nom_fichier]]`, `en_erreur: set[str]` → 500 ; patche `sources_publiees.http_get` et `sources_publiees.update_resource`, vérifie la présence de `X-API-KEY` sur chaque GET).

- [ ] **Step 1: Tests**

```python
def test_telechargement_avec_cle(fake_datagouv, tmp_path):   # parquet valide servi → fichier écrit, X-API-KEY vu sur les 2 GET
def test_ressource_absente(fake_datagouv, tmp_path):        # 404 → FichierSourceIndisponible("joue" dans le message)
def test_fichier_illisible(fake_datagouv, tmp_path):        # b"pas un parquet" → FichierSourceIndisponible
def test_publication_autorisee(monkeypatch):                # PUBLISH vrai + SOLO vide → True ; SOLO ["x"] → False ; PUBLISH faux → False
def test_publier_fichier_echec(fake_datagouv, tmp_path):    # success False → RuntimeError
```

- [ ] **Step 2:** FAIL attendu ; **Step 3:** implémenter ; **Step 4:** `uv run pytest tests/test_deao_sources_publiees.py -q` → PASS.
- [ ] **Step 5:** Commit `DEAO : lecture et publication des fichiers par source sur data.gouv.fr`.

---

### Task 5: Nouveau `deao_processing` (traitement)

Le flow actuel `src/flows/deao_processing.py` (collecte) est déplacé en `src/flows/scrap_open.py` (fonction `scrap_open`) **dans cette tâche**, à l'identique (tests de flow existants retargetés sur `scrap_open`, publication encore celle d'aujourd'hui) ; la tâche 6 l'adapte. Puis `src/flows/deao_processing.py` est réécrit.

**Files:**

- Move: `src/flows/deao_processing.py` → `src/flows/scrap_open.py` (flow renommé `scrap_open`) ; `tests/test_deao_flow.py` → `tests/test_deao_scrap_open.py`
- Create: `src/flows/deao_processing.py` (nouveau), `tests/test_deao_processing.py`
- Modify: `src/tasks/deao/report.py`, `src/tasks/publish.py` (`publish_deao_to_datagouv` / `should_publish_deao` : dataset a via `DEAO_OPEN_DATASET_ID`), `run_flow.py` (`scrap_open`, `deao_processing`)

**Interfaces:**

- Consumes: `DEAO_REGISTRE`, `telecharger_fichier_source`, `publier_fichier`, `publication_autorisee`, `published_schema()`, `sink_to_files`, `write_table_schema`, `publish_deao_to_datagouv`, `taux_remplissage`, `publier_rapport`.
- Produces :

  - `dedoublonner_sources(df: pl.DataFrame) -> tuple[pl.DataFrame, Counter]` (`src/flows/deao_processing.py` ou `src/tasks/deao/dedoublonnage.py`) : identité en v1, compteur `dedoublonnage_retire = 0`.
  - `lignes_rapport_sources(df: pl.DataFrame) -> list[dict]` (`report.py`) : par `sourceDataset` : `lignes:<src>`, `statut:<src>:<valeur>`, `acheteur_id_source:<src>:<valeur|null>`, `remplissage:<colonne>:<src>` (via `taux_remplissage(df, par="sourceDataset")`).
  - `@flow deao_processing(run_date: str | None = None) -> None`, corps exécuté dans `with concurrency("deao-processing", occupy=1, strict=False)` (`prefect.concurrency.sync`) :
    1. sources = entrées non `skip` de `DEAO_REGISTRE` ; téléchargement de chacune dans `DEAO_DATA_DIR / "sources"` ;
    2. chaque fichier : colonnes et types == `published_schema()`, sinon `RuntimeError` ; lignes ≥ `min_rows`, sinon `RuntimeError` ;
    3. concaténation, `dedoublonner_sources`, unicité de `uid` (doublon → `RuntimeError`) ;
    4. rapport `publier_rapport(lignes_rapport_sources(df))` ;
    5. écriture : agrégat Open Data (sources `open_data`) en `DEAO_DIST_DIR / "deao.parquet"`, `deao.csv.gz` (via `sink_to_files(..., DEAO_DIST_DIR / "deao")`), `schema.json` ; agrégat complet en `DEAO_DIST_DIR / "closed" / "deao.parquet"` ;
    6. si `publication_autorisee()` : dataset a si ≥ `DEAO_MIN_ROWS_TO_PUBLISH` lignes (`publish_deao_to_datagouv`), dataset b si ≥ `DEAO_MIN_ROWS_TO_PUBLISH` (`publier_fichier(DEAO_CLOSED_DATASET_ID, DEAO_CLOSED_PARQUET_RESOURCE_ID, ...)`).
       Toute erreur des étapes 1 à 3 survient avant la moindre publication.

- [ ] **Step 1: Tests** (`tests/test_deao_processing.py`, faux data.gouv.fr servant deux parquets construits avec `selection_publiee`/`published_schema()`, registre de test, `DECP_PROCESSING_PUBLISH` vrai)

```python
def test_agregats_et_publication(fake_datagouv, env):
    deao_processing()
    assert {(d, r) for d, r, _ in fake_datagouv.publies} == {
        (OPEN, PARQUET_A), (OPEN, CSV_A), (OPEN, SCHEMA_A), (CLOSED, "b86dd1f4-68f5-4847-9a25-c6d234aa3d66")}
    assert set(pl.read_parquet(env.dist / "closed" / "deao.parquet")["sourceDataset"]) == {"boamp", "joue"}

def test_source_absente_rien_publie(fake_datagouv, env):         # joue en 404 → FichierSourceIndisponible, publies == []
def test_source_sous_minimum_rien_publie(...):                    # min_rows 5 et 3 lignes → RuntimeError, publies == []
def test_uid_en_double_rien_publie(...):                          # même uid dans boamp et joue → RuntimeError, publies == []
def test_schema_non_conforme_rien_publie(...):                    # colonne manquante → RuntimeError, publies == []
def test_source_skip_non_lue(...):                                # joue skip: true → aucune requête pour sa ressource
def test_agregat_open_data_restreint(...):                        # source open_data: false dans le registre → absente de deao.parquet (a), présente dans closed/deao.parquet
def test_solo_aucune_publication(...):                            # SOLO_DEAO_SCRAPS=["x"] → publies == [] mais fichiers écrits
def test_rapport_par_source():                                    # lignes_rapport_sources : "lignes:boamp", "statut:joue:ouvert", "remplissage:objet:boamp"
```

Les tests existants du flow de collecte (`tests/test_deao_scrap_open.py`) passent après le renommage.

- [ ] **Step 2:** FAIL attendu ; **Step 3:** implémenter ; **Step 4:** `uv run pytest tests -q -k "deao or boamp"` → PASS.
- [ ] **Step 5:** Commit `DEAO : collecte renommée scrap_open, nouveau deao_processing (agrégation et publication des datasets a et b)`.

---

### Task 6: `scrap_open` publie `boamp.parquet` et `joue.parquet`, puis lance `deao_processing`

**Files:**

- Modify: `src/flows/scrap_open.py`, `tests/test_deao_scrap_open.py`

**Interfaces:**

- Consumes: `DEAO_REGISTRE`, `publier_fichier`, `publication_autorisee`, `deao_processing`.
- Produces : `@flow scrap_open(run_date: str | None = None) -> None` : déroulé actuel jusqu'à l'enrichissement et au rapport de collecte (inchangés), puis :

  1. pour chaque `code` de `DEAO_SOURCES` : `selection_publiee(df.filter(sourceDataset == code))` (avec `source_file_expr` appliqué) écrit en `DEAO_DIST_DIR / f"{code}.parquet"` ;
  2. si `publication_autorisee()` et lignes ≥ `min_rows` de son entrée : `publier_fichier(DEAO_CLOSED_DATASET_ID, entree.resource_id, path)` ; sous le minimum : pas de publication, compteur `<code>:sous_minimum`, échec en fin de run ;
  3. `with tags("appels-offres", "traitement"): deao_processing(run_date=run_date)` ;
  4. puis seulement, échec éventuel (jours en erreur, fichier sous minimum), message listant les causes.
     L'ancienne écriture `deao.parquet`/`deao.csv.gz`/`schema.json` et l'appel `publish_deao_to_datagouv` sont retirés de ce flow (ils sont dans `deao_processing`).

- [ ] **Step 1: Tests** (faux DILA, faux TED, faux S3 existants + `fake_datagouv` ; `deao_processing` remplacé par un espion dans `scrap_open` pour la plupart des tests, un test bout en bout le garde réel)

```python
def test_scrap_open_publie_deux_fichiers(...):        # publies == {(CLOSED, boamp_rid), (CLOSED, joue_rid)} ; joue.parquet sans la chaîne 631605-2026 ; colonnes == published_schema()
def test_scrap_open_lance_deao_processing(...):      # espion appelé une fois, avec run_date
def test_scrap_open_sous_minimum(...):               # min_rows joue 10**6 → joue non publié, boamp publié, espion appelé, RuntimeError match "joue"
def test_scrap_open_jour_en_erreur_lance_traitement(...):  # recherche TED en 500 → espion appelé, puis RuntimeError
def test_scrap_open_sources_boamp_seul(...):         # DEAO_SOURCES=("boamp",) → seul boamp publié ; deao_processing réel lit encore joue (servi par fake_datagouv)
def test_bout_en_bout(...):                          # deao_processing réel : closed/deao.parquet contient boamp et joue, uid uniques
```

- [ ] **Step 2:** FAIL attendu ; **Step 3:** implémenter ; **Step 4:** `uv run pytest tests -q -k "deao or boamp"` → PASS.
- [ ] **Step 5:** Commit `DEAO : scrap_open publie boamp.parquet et joue.parquet sur le dataset b puis lance deao_processing`.

---

### Task 7: Flow `scrap_closed` et contrat des scrapers

**Files:**

- Create: `src/flows/scrap_closed.py`, `src/tasks/scrap/deao/contrat.py`, `tests/test_deao_scrap_closed.py`
- Modify: `run_flow.py`

**Interfaces:**

- Produces :

  - `src/tasks/scrap/deao/contrat.py` : `Scraper = Callable[[date], pl.DataFrame]` (renvoie les lignes au schéma `published_schema()`, `sourceDataset` = code de la source) ; `SCRAPERS: dict[str, Scraper] = {}` (vide en v1 ; un futur scraper s'y ajoute par son `code`).
  - `scrap_closed(run_date: str | None = None) -> None` :
    1. codes à exécuter = `SOLO_DEAO_SCRAPS` si non vide, sinon entrées `open_data: false` non `skip` du registre ; code sans scraper dans `SCRAPERS` → erreur pour ce code ;
    2. scrapers en parallèle (`run_in_pool`, `max_workers=MAX_PREFECT_WORKERS`) ; chaque résultat : colonnes == `published_schema()`, écrit en `DEAO_DIST_DIR / f"{code}.parquet"`, publié si `publication_autorisee()` et lignes ≥ `min_rows` (entrée du registre ; une source `SOLO` absente du registre → erreur) ;
    3. `with tags("appels-offres", "traitement"): deao_processing(run_date=run_date)` ;
    4. puis échec si au moins un scraper a échoué ou n'a pas été publié faute de lignes.

- [ ] **Step 1: Tests**

```python
def test_sans_scraper_lance_traitement(...):          # registre sans source fermée → espion deao_processing appelé, aucune publication
def test_deux_scrapers_dont_un_en_erreur(...):         # registre de test avec "plateforme_a" et "plateforme_b" (open_data: false), SCRAPERS patché : b lève → a publié, espion appelé, RuntimeError match "plateforme_b"
def test_solo_seulement_les_listes(...):               # SOLO_DEAO_SCRAPS=["plateforme_b"] (skip: true) → seul b exécuté, publies == []
def test_scraper_schema_non_conforme(...):             # colonne manquante → non publié, RuntimeError
```

- [ ] **Step 2:** FAIL attendu ; **Step 3:** implémenter ; **Step 4:** `uv run pytest tests/test_deao_scrap_closed.py -q` → PASS.
- [ ] **Step 5:** Commit `DEAO : flow scrap_closed (scrapers de plateformes en parallèle, SOLO_DEAO_SCRAPS)`.

---

### Task 8: Déploiements (pool local, GitHub, tags, concurrence)

**Files:**

- Modify: `src/deployments.py`, `tests/test_deao_deployments.py`
- Create: `tests/test_deployments_tags.py`

**Interfaces:**

- Produces :
  - `deao_deployment_specs() -> list[dict]` réécrit : `scrap-open` (`src/flows/scrap_open.py:scrap_open`, `main`, `cron="0 8,12,16 * * *"`, `concurrency_limit=1`, tags `appels-offres`, `scrap`, `boamp`, `joue`, `prod`), `scrap-closed` (`src/flows/scrap_closed.py:scrap_closed`, `cron="0 6 * * *"`, `concurrency_limit=1`, tags `appels-offres`, `scrap`, `prod`), `deao-processing` (`src/flows/deao_processing.py:deao_processing`, sans cron, tags `appels-offres`, `traitement`, `prod`), et leurs `-dev` (branche `dev`, sans cron, tag `dev` au lieu de `prod`, `DECP_PROCESSING_PUBLISH=False`). Tous : `work_pool_name="local"` ; `job_variables.env` prod : `DECP_PROCESSING_PUBLISH=True`, `DEAO_DATA_DIR=/home/yunohost.app/prefect/deao`, `DEAO_DIST_DIR=/srv/shared/deao/prod/dist` (dev : `/srv/shared/deao/dev/dist`). Plus aucune référence à `run_on_scaleway`, `scaleway-*` ni `DEAO_INSTANCE_TYPE` côté DEAO.
  - `decp_deployment_specs() -> list[dict]` : les déploiements DECP actuels (`decp-processing*`, `sirene-preprocess*`, `scrap*`, `get-cog` s'il existe), inchangés hormis `tags` : `decp-processing*` → `attributions`, `traitement` ; `sirene-preprocess*` (et COG) → `referentiel` ; `scrap-<cible>` → `attributions`, `scrap`, `<source>` (`marches-securises`, `aws`, `dume`, `klekoon` selon la cible) ; plus `prod`/`dev` selon la branche. Le bloc `__main__` déploie `decp_deployment_specs() + deao_deployment_specs()` avec la même boucle.
- Le commentaire d'en-tête de `deployments.py` mentionne que DEAO tourne sur le pool `local`.

- [ ] **Step 1: Tests**

```python
def test_deao_crons_et_pools():          # scrap-open "0 8,12,16 * * *", scrap-closed "0 6 * * *", deao-processing sans cron ; tous work_pool_name "local"
def test_deao_concurrence():             # scrap-open et scrap-closed concurrency_limit == 1
def test_deao_dev_sans_publication():    # tous les -dev : branch "dev", sans cron, DECP_PROCESSING_PUBLISH "False", tag "dev"
def test_tags_familles():                # pour chaque spec DECP + DEAO : un tag produit ou "referentiel", un tag étape, un tag env ; valeurs dans les familles de la spec
def test_plus_de_scaleway_pour_deao():   # aucune spec DEAO avec entrypoint launcher ni pool scaleway-*
```

- [ ] **Step 2:** FAIL attendu ; **Step 3:** implémenter ; **Step 4:** `uv run pytest tests/test_deao_deployments.py tests/test_deployments_tags.py -q` → PASS, puis `uv run pytest -q --ignore=tests/test_main.py` → PASS ; `pre-commit run --files <fichiers modifiés>` → Passed.
- [ ] **Step 5:** Commit `DEAO : déploiements scrap-open, scrap-closed, deao-processing sur le pool local, tags DECP et DEAO`.

---

## Après le plan (hors code, utilisateur)

Repris de la section « Mise en service » de la spec :

1. Retirer du serveur Prefect les anciens déploiements DEAO Scaleway ; créer la limite de concurrence globale `deao-processing` (1 slot).
2. Créer `DEAO_DATA_DIR` et `DEAO_DIST_DIR` sur le serveur principal ; `.env` : `DEAO_DATASET_ID` → `DEAO_OPEN_DATASET_ID`, `DEAO_CLOSED_*`.
3. Premier run `scrap-open` avec `DEAO_REBUILD=true`.
