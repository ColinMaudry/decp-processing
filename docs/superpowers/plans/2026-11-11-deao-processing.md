# Flow `deao_processing` (DEAO) — plan d'implémentation

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** publier sur data.gouv.fr le dataset DEAO : une ligne par appel d'offres × lot, construite à partir des avis BOAMP (eForms + legacy), consolidée, enrichie et dans un schéma calqué sur les DECP.

**Architecture:** un adaptateur de source BOAMP (acquisition DILA ⇄ archive brute S3, classement, mapping eForms/legacy) produit des lignes **avis-lot** au contrat commun, écrites en partitions parquet par jour. Un tronc commun indépendant de la source consolide (chaînage des avis → appel d'offres × lot, cycle de vie), enrichit l'acheteur (dictionnaire SIRET, SIRENE, géocodage, replis), écrit et publie.

**Tech Stack:** Python ≥ 3.11, Prefect 3, polars, lxml, httpx, boto3, `zstandard` (nouveau), Géoplateforme (géocodage batch CSV).

**Spec:** `docs/superpowers/specs/2026-11-11-deao-processing-design.md`

## Global Constraints

- Messages de commit : en français, terminés par `#178`, puis la ligne `Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>`.
- Tests : toujours `uv run pytest …` (jamais `pytest` nu). Pas de réseau dans les tests (HTTP, S3, géocodage simulés par monkeypatch).
- Imports absolus depuis la racine : `from src.tasks.deao… import …` (vérifié par pre-commit).
- Fichiers : flow `src/flows/deao_processing.py`, code dans `src/tasks/deao/`, fixtures dans `tests/data/deao/`, sorties `DEAO_DIST_DIR / "deao.{parquet,csv,csv.gz}"` + `DEAO_DIST_DIR / "schema.json"`, avec `DEAO_DIST_DIR = make_path_from_env("DEAO_DIST_DIR", DIST_DIR / "deao")` (ne pas la renseigner sur l'instance : le lanceur ne rapatrie que `/work/dist`).
- Préfixe des variables : `DEAO_*` (produit), `BOAMP_*` (acquisition propre à la source). `sourceDataset = "boamp"`.
- Valeurs fixées par la spec : `BOAMP_START_DATE=2024-01-01`, `BOAMP_RECRAWL_DAYS=3`, `DEAO_STATUT_DELAI_JOURS=90`, `DEAO_MIN_ROWS_TO_PUBLISH=50000`, archive S3 `DEAO_S3_PREFIX="boamp/raw"` + `/<AAAA>/<MM>/<JJ>.tar.zst`, instance `STANDARD3-X4C-16G`, `dist_dir` du lanceur `/srv/shared/deao/<env>/dist` (**jamais** celui des DECP).
- `uid = f"{sourceDataset}_{idAvisInitial}_{lotId}"`, ex. `boamp_26-10255_1`.
- `statut` ∈ {`ouvert`, `clos`, `annule`, `pre-information`}.
- Ordre et noms des 39 colonnes publiées : tableau « Schéma publié » de la spec, à l'identique.
- `dureeMois` arrondi à l'entier **supérieur** (jours / 30).
- polars en lazy quand c'est possible ; la consolidation (~800 000 lignes) peut matérialiser un DataFrame.

## Review Focus

1. **Crawl d'un jour interrompu** (DILA indisponible, un XML en erreur) : le jour ne doit être ni archivé ni marqué comme traité, sinon il serait perdu pour toujours. → Task 9, `test_process_day_echec_telechargement_n_ecrit_rien`.
2. **SIRET avec espaces dans eForms** (`323 841 353 00911`, avis 26-98580) : retirer les espaces avant validation, ne pas le jeter. → Task 6, `test_eforms_siret_avec_espaces`.
3. **Formats de date hétérogènes** (`…T12:00:00.000+02:00`, `…T16:00:00Z`, `…T22:00:00` naïf, eForms `2026-10-30+01:00` + `16:30:00+01:00`) : tous convertis en Europe/Paris, sans erreur. → Task 4, `test_parse_datetime_paris`.
4. **Synchronisation du lanceur** : un `dist_dir` partagé avec les DECP effacerait les fichiers DECP du serveur. → Task 15, test sur les paramètres de déploiement.
5. **XML illisible dans un jour** : compté et logué, le reste du jour est traité. → Task 7, `test_parse_day_xml_illisible`.

---

## Fixtures (référence pour toutes les tâches)

Fichiers DILA réels, à placer dans `tests/data/deao/` sous leur nom d'origine, **après suppression des éléments `<HTML>` et `<HTMLSYNTHESE>`** (blobs volumineux, ignorés par le parsing). Date de publication = dossier DILA `https://echanges.dila.gouv.fr/OPENDATA/BOAMP/<AAAA>/<MM>/<JJ>/<IDWEB>.xml`.

| Fichier        | Date       | Rôle                                                            |
| -------------- | ---------- | --------------------------------------------------------------- |
| `26-87868.xml` | 2026-09-12 | eForms initial, `LOT-0000`, chaîne → 26-99074                   |
| `26-99074.xml` | 2026-10-14 | eForms rectificatif de 26-87868 (date limite 30/10)             |
| `26-92574.xml` | 2026-09-24 | FNSimple initial, SIRET, 8 lots sans `numLot`                   |
| `26-99891.xml` | 2026-10-14 | FNSimple rectificatif de 26-92574 (texte libre)                 |
| `26-99177.xml` | 2026-10-13 | FNSimple initial, 11 lots, annulé par 26-99542                  |
| `26-99542.xml` | 2026-10-14 | `NATURE=ANNULATION` legacy de 26-99177                          |
| `26-99870.xml` | 2026-10-14 | MAPA sans SIRET, sans lot                                       |
| `26-98580.xml` | 2026-10-13 | eForms initial, `CompanyID` `323 841 353 00911`, 4 lots         |
| `26-98723.xml` | 2026-10-14 | eForms PIN sous-type 10, `MISE_EN_CONCURRENCE`, 4 lots          |
| `26-97189.xml` | 2026-10-09 | eForms PIN sous-type 4 (pré-information simple), lot `PAR-0001` |
| `26-94806.xml` | 2026-10-02 | eForms PIN sous-type 1 (`PROFIL_ACHETEUR`), sans lot            |
| `26-98989.xml` | 2026-10-14 | `NATURE=MODIFICATION` (sous-type 38) → exclu                    |
| `26-99072.xml` | 2026-10-14 | `NATURE=ATTRIBUTION` → exclu                                    |

Valeurs relevées dans ces fichiers (utilisées par les tests) : voir chaque tâche.

---

### Task 1: fixtures, configuration DEAO, `siret_valide`, client S3

**Files:**

- Create: `tests/data/deao/*.xml` (13 fichiers ci-dessus), `script/get_deao_fixtures.py`
- Modify: `src/config.py` (bloc BOAMP, lignes ~225-268), `src/tasks/utils.py`, `pyproject.toml`, `template.env`
- Test: `tests/test_deao_utils.py`

**Interfaces:**

- Produces (`src/config.py`) : `BOAMP_BASE_URL`, `BOAMP_START_DATE` (str), `BOAMP_RECRAWL_DAYS: int = 3`, `BOAMP_INCLUDED_NATURES = {"APPEL_OFFRE", "QUALIFICATION", "RECTIFICATIF", "ANNULATION"}`, `BOAMP_PRE_INFORMATION_NATURES = {"PRE-INFORMATION", "PERIODIQUE"}` (remplace `BOAMP_MISE_EN_CONCURRENCE_NATURES`), `BOAMP_AVIS_URL_TEMPLATE` ; `DEAO_DATA_DIR: Path` (`DATA_DIR / "deao"`), `DEAO_DIST_DIR: Path` (`DIST_DIR / "deao"`, surchargeable), `DEAO_PARTITIONS_DIR = DEAO_DATA_DIR / "avis"`, `DEAO_S3_PREFIX = "boamp/raw"`, `DEAO_GEOCODE_S3_PREFIX = "deao"`, `DEAO_REBUILD: bool`, `DEAO_STATUT_DELAI_JOURS: int = 90`, `DEAO_MIN_ROWS_TO_PUBLISH: int = 50000`, `DEAO_DATASET_ID`, `DEAO_PARQUET_RESOURCE_ID`, `DEAO_CSV_RESOURCE_ID`, `DEAO_SCHEMA_RESOURCE_ID` (str, défaut `""`). Supprimer `BOAMP_ARCHIVE_CUTOFF_YEAR`, `BOAMP_DATA_DIR`, `BOAMP_PARTITIONS_DIR`, `BOAMP_DATASET_ID`, `BOAMP_*_RESOURCE_ID` **à la Task 15** seulement (encore utilisés par l'ancien flow d'ici là).
- Produces (`src/tasks/utils.py`) : `siret_valide(s: str | None) -> bool`, `s3_client() -> botocore.client.S3` (même configuration que `get_from_s3`/`publish_to_s3`, lève `ValueError` si `check_s3_config()` renvoie des manquants).

- [ ] **Step 1: écrire `script/get_deao_fixtures.py`** — télécharge les 13 fichiers du tableau via `httpx`, supprime les éléments `HTML` et `HTMLSYNTHESE` (lxml, par nom local), écrit `tests/data/deao/<IDWEB>.xml`. Lancer `uv run python script/get_deao_fixtures.py` ; attendu : 13 fichiers, chacun < 130 Ko, aucun ne contient `<HTML>`.

- [ ] **Step 2: tests `siret_valide`**

```python
@pytest.mark.parametrize("s, attendu", [
    ("83942912300020", True),       # Luhn OK
    ("32384135300911", True),
    ("35600000000010", True),       # La Poste : Luhn KO mais somme % 5 == 0
    ("35600000000011", False),      # La Poste, somme % 5 != 0
    ("12345678900001", False),      # Luhn KO
    ("8394291230002", False),       # 13 chiffres
    ("8394291230002A", False),
    ("", False), (None, False),
])
def test_siret_valide(s, attendu):
    assert siret_valide(s) is attendu
```

- [ ] **Step 3: lancer** `uv run pytest tests/test_deao_utils.py -v` → FAIL (`ImportError: siret_valide`).
- [ ] **Step 4: implémenter** `siret_valide` et `s3_client` dans `src/tasks/utils.py` ; ajouter les variables de config ; ajouter `"zstandard"` aux dépendances de `pyproject.toml` puis `uv sync` ; ajouter les variables `DEAO_*` et `BOAMP_RECRAWL_DAYS=3` à `template.env` (les `DEAO_*_ID` reprennent les valeurs actuelles de `BOAMP_DATASET_ID`, `BOAMP_PARQUET_RESOURCE_ID`, `BOAMP_CSV_RESOURCE_ID` ; `DEAO_SCHEMA_RESOURCE_ID=` vide).
- [ ] **Step 5: lancer** `uv run pytest tests/test_deao_utils.py tests/test_boamp_flatten.py -v` → PASS.
- [ ] **Step 6: commit** `git add script/get_deao_fixtures.py tests/data/deao tests/test_deao_utils.py src/config.py src/tasks/utils.py pyproject.toml uv.lock template.env && git commit -m "DEAO : fixtures, configuration, siret_valide, client S3 #178 …"`

---

### Task 2: schéma (contrat avis-lot + schéma publié)

**Files:**

- Create: `src/tasks/deao/__init__.py`, `src/tasks/deao/schema.py`
- Test: `tests/test_deao_schema.py`

**Interfaces:**

- Produces :
  - `@dataclass(frozen=True) class Field: name: str; dtype: pl.DataType; title: str; description: str; enum: tuple[str, ...] | None = None`
  - `PUBLISHED_FIELDS: tuple[Field, ...]` — les 39 colonnes, dans l'ordre exact de la spec.
  - `AVIS_LOT_SCHEMA: dict[str, pl.DataType]` — contrat des adaptateurs.
  - `published_schema() -> dict[str, pl.DataType]`
  - `conform_avis(df: pl.DataFrame) -> pl.DataFrame` — ajoute les colonnes manquantes (null), caste, ordonne selon `AVIS_LOT_SCHEMA`, rejette les colonnes inconnues (`ValueError`).
  - `empty_avis_frame() -> pl.DataFrame`
  - `write_table_schema(path: Path) -> Path` — Table Schema JSON `{"fields": [{"name", "type", "title", "description", ("enum")}]}`.

Types : `dateLimiteReponse` = `pl.Datetime("us", "Europe/Paris")` ; `datePublicationInitiale`, `dateDerniereModification`, `datePublicationAvis` = `pl.Date` ; `montantEstime`, `acheteur_latitude`, `acheteur_longitude` = `pl.Float64` ; `dureeMois` = `pl.Int64` ; `rectifie`, `preInformation`, `avisComplet` = `pl.Boolean` ; `techniques`, `avisLies`, `infosRectificatifs` = `pl.List(pl.String)` ; tout le reste `pl.String`.

`AVIS_LOT_SCHEMA` = colonnes descriptives `referenceAcheteur, acheteur_id, acheteur_nom, objet, lotId, lotIntitule, description, montantEstime, type, codeCPV, procedure, techniques, dureeMois, dateLimiteReponse, lieuExecution_code, lieuExecution_typeCode, urlConsultation, urlAvis, sourceDataset, sourceFile` + internes `idAvis, avisPrecedent, typeAvis, preInformation, avisComplet, datePublicationAvis, infosRectificatif, formatSource, acheteur_adresse_voie, acheteur_adresse_cp, acheteur_adresse_ville, acheteur_adresse_nuts`.

Titres et descriptions des colonnes homonymes : reprendre `reference/schema_base.json` en remplaçant « marché public » par « appel d'offres » quand c'est pertinent ; `type` garde l'enum `["Fournitures", "Services", "Travaux"]` ; `statut` porte l'enum `["ouvert", "clos", "annule", "pre-information"]` et une description qui documente la règle et la limite des rectificatifs legacy (spec, « Cycle de vie »).

- [ ] **Step 1: tests**

```python
def test_ordre_des_colonnes_publiees():
    noms = [f.name for f in PUBLISHED_FIELDS]
    assert len(noms) == 39
    assert noms[:6] == ["uid", "referenceAcheteur", "acheteur_id", "acheteur_id_source", "acheteur_nom", "objet"]
    assert noms[29:32] == ["statut", "rectifie", "infosRectificatifs"]
    assert noms[-2:] == ["sourceDataset", "sourceFile"]

def test_conform_avis_complete_et_caste():
    df = conform_avis(pl.DataFrame({"idAvis": ["26-1"], "dureeMois": [3]}))
    assert list(df.schema) == list(AVIS_LOT_SCHEMA)
    assert df.schema["dureeMois"] == pl.Int64 and df["objet"][0] is None

def test_conform_avis_refuse_colonne_inconnue():
    with pytest.raises(ValueError):
        conform_avis(pl.DataFrame({"inconnue": [1]}))

def test_write_table_schema(tmp_path):
    data = json.loads(write_table_schema(tmp_path / "schema.json").read_text())
    statut = next(f for f in data["fields"] if f["name"] == "statut")
    assert statut["enum"] == ["ouvert", "clos", "annule", "pre-information"]
    assert next(f for f in data["fields"] if f["name"] == "dateLimiteReponse")["type"] == "datetime"
```

- [ ] **Step 2:** `uv run pytest tests/test_deao_schema.py -v` → FAIL.
- [ ] **Step 3:** implémenter `src/tasks/deao/schema.py` (mapping dtype polars → type Table Schema : String→string, Int64→integer, Float64→number, Boolean→boolean, Date→date, Datetime→datetime, List→array).
- [ ] **Step 4:** `uv run pytest tests/test_deao_schema.py -v` → PASS.
- [ ] **Step 5: commit** « DEAO : schéma publié et contrat avis-lot #178 ».

---

### Task 3: helpers XML et classement des avis

**Files:**

- Create: `src/tasks/deao/sources/__init__.py`, `src/tasks/deao/sources/boamp/__init__.py`, `src/tasks/deao/sources/boamp/xml.py`, `src/tasks/deao/sources/boamp/classify.py`
- Test: `tests/test_deao_classify.py`

**Interfaces:**

- Produces (`xml.py`, repris de `src/tasks/boamp/flatten.py`) : `localname(tag) -> str`, `find(elem, name) -> elem | None` (premier descendant ou soi-même), `direct_child(elem, name) -> elem | None`, `children(elem, name) -> list`, `text(elem) -> str | None` (strip, `""` → None), `first_child_localname(elem) -> str | None`, `gestion_text(root, name) -> str | None` (premier `name` sous `GESTION`), `indexation_choice(root, name) -> str | None` (nom local du premier enfant de `GESTION/INDEXATION/<name>`, ex. `TYPE_PROCEDURE` → `"OUVERT"`), `idweb(root) -> str | None` (`GESTION/REFERENCE/IDWEB`), `annonce_anterieure(root) -> str | None` (`GESTION/REFERENCE/ANNONCE_ANTERIEUR/IDWEB`), `detect_format(root) -> Literal["eforms", "legacy"]`.
- Produces (`classify.py`) : `@dataclass(frozen=True) class Classement: nature: str | None; retenu: bool; typeAvis: str | None; preInformation: bool | None; formatSource: str` et `classify(root) -> Classement`.

Règles (`nature`, `sous` = premier enfant de l'élément nature, `statut`) :

- `nature ∈ BOAMP_INCLUDED_NATURES ∪ BOAMP_PRE_INFORMATION_NATURES`, sinon `retenu=False` (`MODIFICATION`, `ATTRIBUTION`, … et `None`).
- `typeAvis` : `"annulation"` si `nature == "ANNULATION"` ou `statut == "ANNULATION"` ; `"rectificatif"` si `nature == "RECTIFICATIF"` ou `statut == "RECTIFICATIF"` ; sinon `"initial"`.
- `preInformation` : `None` si `typeAvis != "initial"` ; sinon `True` si `nature ∈ BOAMP_PRE_INFORMATION_NATURES and sous != "MISE_EN_CONCURRENCE"`, sinon `False`.

- [ ] **Step 1: tests** (fixtures chargées avec `etree.parse(FIXTURES / nom).getroot()`)

```python
@pytest.mark.parametrize("fichier, retenu, type_avis, pre, fmt", [
    ("26-87868.xml", True, "initial", False, "eforms"),
    ("26-99074.xml", True, "rectificatif", None, "eforms"),
    ("26-99891.xml", True, "rectificatif", None, "legacy"),
    ("26-99542.xml", True, "annulation", None, "legacy"),
    ("26-99870.xml", True, "initial", False, "legacy"),
    ("26-98723.xml", True, "initial", False, "eforms"),   # PIN valant mise en concurrence
    ("26-97189.xml", True, "initial", True, "eforms"),    # PIN simple
    ("26-94806.xml", True, "initial", True, "eforms"),    # PIN PROFIL_ACHETEUR
    ("26-98989.xml", False, None, None, "eforms"),        # MODIFICATION
    ("26-99072.xml", False, None, None, "eforms"),        # ATTRIBUTION
])
def test_classify(fichier, retenu, type_avis, pre, fmt): ...

def test_classify_nature_exclue_est_renseignee():
    assert classify(load("26-98989.xml")).nature == "MODIFICATION"

def test_helpers_gestion():
    root = load("26-99074.xml")
    assert idweb(root) == "26-99074" and annonce_anterieure(root) == "26-87868"
    assert indexation_choice(root, "TYPE_PROCEDURE") == "OUVERT"
```

- [ ] **Step 2:** `uv run pytest tests/test_deao_classify.py -v` → FAIL.
- [ ] **Step 3:** implémenter `xml.py` et `classify.py`.
- [ ] **Step 4:** → PASS.
- [ ] **Step 5: commit** « DEAO : classement des avis BOAMP (annulations incluses, modifications exclues, pré-informations) #178 ».

---

### Task 4: normalisation des valeurs

**Files:**

- Create: `src/tasks/deao/values.py`, `src/tasks/deao/sources/boamp/codes.py`, `reference/nuts3_departements.csv`
- Test: `tests/test_deao_values.py`

**Interfaces:**

- Produces (`values.py`, indépendant de la source) :
  - `normalize_lot_id(raw: str | None) -> str | None` : `"LOT-0001"` → `"1"`, `"LOT-0000"` → `"0"`, `"PAR-0001"` → `"1"`, `"03"` → `"3"`, `"A"` → `"A"`, `None` → `None`.
  - `duree_mois(valeur: str | float | None, unite: str) -> int | None` : unités `"MONTH"`, `"DAY"`, `"YEAR"` (eForms) et `"mois"`, `"jours"`, `"ans"` ; jours → `ceil(n / 30)`, ans → `n * 12`, mois → `ceil(n)` ; unité inconnue → `None`.
  - `parse_datetime_paris(valeur: str | None, heure: str | None = None) -> datetime | None` : renvoie un `datetime` aware Europe/Paris. Accepte ISO avec offset, `Z`, millisecondes, naïf (interprété Europe/Paris), date seule avec offset (`2026-10-30+01:00`) combinée à `heure` (`16:30:00+01:00`) ; date sans heure → 23:59:59 locale ; valeur illisible → `None`.
  - `departement_depuis_cp(cp: str | None) -> str | None` : 2 premiers chiffres ; `97x`/`98x` → 3 chiffres ; Corse : `int(cp[:3]) <= 201` → `"2A"`, sinon `"2B"`.
  - `departement_depuis_nuts(nuts: str | None) -> str | None` : via `reference/nuts3_departements.csv` (colonnes `nuts3,departement_code`, 101 départements, nomenclature NUTS 2021 d'Eurostat), chargé une fois.
- Produces (`codes.py`, propre au BOAMP) :

  - `PROCEDURE_INDEXATION = {"OUVERT": "Appel d'offres ouvert", "RESTREINT": "Appel d'offres restreint", "NEGOCIE": "Procédure négociée", "PROCEDURE_ADAPTE": "Procédure adaptée", "DIALOGUE_COMPETITIF": "Dialogue compétitif", "PARTENARIAT_INNOVATION": "Partenariat d'innovation", "CONCOURS_OUVERT": "Concours ouvert", "CONCOURS_RESTREINT": "Concours restreint", "DSP": "Concession", "AUTRE": "Autre procédure"}` (énumération de `TYPE_PROCEDURE` dans `jo_types_diffusion.xsd` v3.2.5).
  - `PROCEDURE_EFORMS = {"open": "Appel d'offres ouvert", "restricted": "Appel d'offres restreint", "neg-w-call": "Procédure négociée", "neg-wo-call": "Procédure négociée", "comp-dial": "Dialogue compétitif", "innovation": "Partenariat d'innovation", "oth-single": "Autre procédure", "oth-mult": "Autre procédure"}`.
  - `TYPE_INDEXATION = {"TRAVAUX": "Travaux", "FOURNITURES": "Fournitures", "SERVICES": "Services"}`, `TYPE_EFORMS = {"works": "Travaux", "supplies": "Fournitures", "services": "Services"}`.
  - `libelle(table: dict, code: str | None, compteur: Counter, nom: str) -> str | None` : valeur traduite ; code absent de la table → renvoie le code brut et incrémente `compteur[f"non_mappe:{nom}:{code}"]`.

- [ ] **Step 1: tests**

```python
PARIS = ZoneInfo("Europe/Paris")
@pytest.mark.parametrize("valeur, heure, attendu", [
    ("2026-10-22T12:00:00.000+02:00", None, datetime(2026, 10, 22, 12, 0, tzinfo=PARIS)),
    ("2026-11-16T16:00:00Z", None, datetime(2026, 11, 16, 17, 0, tzinfo=PARIS)),
    ("2026-03-30T22:00:00", None, datetime(2026, 3, 30, 22, 0, tzinfo=PARIS)),
    ("2026-10-30+01:00", "16:30:00+01:00", datetime(2026, 10, 30, 16, 30, tzinfo=PARIS)),
    ("2026-10-30+01:00", None, datetime(2026, 10, 30, 23, 59, 59, tzinfo=PARIS)),
    ("pas une date", None, None), (None, None, None),
])
def test_parse_datetime_paris(valeur, heure, attendu):
    assert parse_datetime_paris(valeur, heure) == attendu   # comparer aussi .utcoffset() quand non None

@pytest.mark.parametrize("v, u, attendu", [("45", "DAY", 2), ("30", "DAY", 1), ("2", "YEAR", 24), ("29", "mois", 29), ("1.5", "MONTH", 2), ("3", "WEEK", None)])
def test_duree_mois(v, u, attendu): ...

@pytest.mark.parametrize("cp, dep", [("75004", "75"), ("29260", "29"), ("20000", "2A"), ("20137", "2A"), ("20250", "2B"), ("20600", "2B"), ("97110", "971"), ("98800", "988"), (None, None)])
def test_departement_depuis_cp(cp, dep): ...

@pytest.mark.parametrize("nuts, dep", [("FRK27", "73"), ("FR101", "75"), ("FRM01", "2A"), ("FRY10", "971"), ("XX999", None)])
def test_departement_depuis_nuts(nuts, dep): ...

def test_libelle_non_mappe_compte():
    c = Counter()
    assert libelle(PROCEDURE_INDEXATION, "INCONNU", c, "procedure") == "INCONNU"
    assert c["non_mappe:procedure:INCONNU"] == 1
```

Plus `test_normalize_lot_id` avec les cas de l'interface.

- [ ] **Step 2:** → FAIL. **Step 3:** implémenter (créer `reference/nuts3_departements.csv` à partir de la liste NUTS 2021 FR d'Eurostat). **Step 4:** → PASS.
- [ ] **Step 5: commit** « DEAO : normalisation des valeurs (dates, durées, lots, départements, codes BOAMP) #178 ».

---

### Task 5: mapping legacy (BOAMP 3.2.5)

**Files:**

- Create: `src/tasks/deao/sources/boamp/legacy.py`
- Test: `tests/test_deao_legacy.py`

**Interfaces:**

- Consumes : `xml.py`, `values.py`, `codes.py`, `siret_valide`, `Classement`.
- Produces : `map_legacy(root, classement: Classement, compteur: Counter) -> list[dict]` — lignes avis-lot (clés ⊂ `AVIS_LOT_SCHEMA`, sans `sourceDataset`/`sourceFile`/`idAvis`/`avisPrecedent`/`typeAvis`/`preInformation`/`avisComplet`/`datePublicationAvis`/`formatSource`, que `parse.py` ajoute).

Règles (spec, « Mapping des champs », colonne legacy) :

- `INDEXATION` d'abord : `objet` = `RESUME_OBJET` (sinon `intitule`, puis `objet`) ; `type` = `libelle(TYPE_INDEXATION, indexation_choice("NATURE_MARCHE"))` ; `procedure` = `libelle(PROCEDURE_INDEXATION, indexation_choice("TYPE_PROCEDURE"))` ; `dateLimiteReponse` = `parse_datetime_paris(INDEXATION/DATE_LIMITE_REPONSE)`, sinon `procedure/dateReceptionOffres`, sinon `delais/receptionOffres`.
- Acheteur (`organisme`) : `acheteur_nom` = `nomOfficiel` sinon `acheteurPublic` ; `acheteur_id` = `codeIdentificationNational` sans espaces si `typeIdentificationNational` a l'enfant `siret` et `siret_valide`, sinon `None` ; adresse : `acheteur_adresse_voie` = `adr/voie/nomvoie` (ou `voie/nomvoie`), `_cp` = premier `cp` sous `organisme`, `_ville` = premier `ville` sous `organisme`, `_nuts` = `None`.
- `codeCPV` = premier `codeCPV/objetPrincipal/classPrincipale` ; `dureeMois` = `dureeMois` (mois), sinon `duree/nbMois`, sinon `duree/nbJours` (jours) ; `montantEstime` = `valeurEstimee/valeur` (float, nul si seule une `fourchette`) ; `description` = `natureMarche/description` sinon `description/objet` ; `referenceAcheteur` = premier `numeroReference` ; `urlConsultation` = `urlDocConsul`, sinon URL de `adressesComplt/document`, sinon `urlProfilAcheteur` ; `lieuExecution_code` = `cp` de `lieuExecution` ou `lieuExecutionLivraison`, `lieuExecution_typeCode` = `"Code postal"` si renseigné ; `techniques` = `["Accord-cadre"]` / `["Système d'acquisition dynamique"]` si les éléments correspondants sont présents, sinon liste vide ; `urlAvis` = `BOAMP_AVIS_URL_TEMPLATE.format(idweb=…)`.
- `infosRectificatif` = texte de `infosRectif` ou `infosAnnulation` (concaténation des textes descendants si structuré, séparateur `" | "`).
- Lots : éléments `lot` enfants de tout conteneur `lots`. Une ligne par lot, `lotId` = `normalize_lot_id(numLot)` ; si un `numLot` est absent ou en double dans l'avis, **tous** les lots de l'avis prennent leur rang (`"1"`, `"2"`, …) ; `lotIntitule` = `intitule` sinon `description` du lot ; CPV, durée et montant du lot priment sur ceux de l'avis. Aucun lot → une ligne `lotId="0"`. Rectificatif/annulation (`classement.typeAvis != "initial"`) → toujours **une** ligne `lotId="0"`, sans lots.

Les chemins exacts varient selon la famille : les vérifier dans `reference/boamp_field_definitions.csv` et sur les fixtures, et privilégier la recherche par nom local plutôt que par chemin complet.

- [ ] **Step 1: tests**

```python
def test_fnsimple_initial_lots_sans_numlot():
    rows = map_legacy(load("26-92574.xml"), classify(load("26-92574.xml")), Counter())
    assert [r["lotId"] for r in rows] == [str(i) for i in range(1, 9)]
    r = rows[0]
    assert r["lotIntitule"].startswith("Installation de chantier")
    assert r["acheteur_id"] == "83942912300020" and r["acheteur_nom"] == "SPL PARISEINE"
    assert r["acheteur_adresse_cp"] == "75004" and r["acheteur_adresse_ville"] == "Paris"
    assert (r["codeCPV"], r["dureeMois"], r["type"], r["procedure"]) == ("45000000", 30, "Travaux", "Appel d'offres ouvert")
    assert r["dateLimiteReponse"] == datetime(2026, 10, 22, 12, 0, tzinfo=PARIS)
    assert r["objet"].startswith("Travaux de réhabilitation de la tribune")

def test_mapa_sans_siret_sans_lot():
    rows = map_legacy(load("26-99870.xml"), classify(load("26-99870.xml")), Counter())
    assert len(rows) == 1 and rows[0]["lotId"] == "0"
    assert rows[0]["acheteur_id"] is None
    assert rows[0]["acheteur_nom"] == "Communauté Lesneven Côte des Légendes"
    assert rows[0]["acheteur_adresse_cp"] == "29260"
    assert rows[0]["procedure"] == "Procédure adaptée"
    assert rows[0]["dateLimiteReponse"] == datetime(2026, 11, 19, 11, 30, tzinfo=PARIS)

def test_rectificatif_une_ligne_avec_texte():
    rows = map_legacy(load("26-99891.xml"), classify(load("26-99891.xml")), Counter())
    assert len(rows) == 1 and rows[0]["lotId"] == "0"
    assert rows[0]["infosRectificatif"].startswith("La date limite de remise des plis est reportée")

def test_annulation_texte():
    rows = map_legacy(load("26-99542.xml"), classify(load("26-99542.xml")), Counter())
    assert rows[0]["infosRectificatif"].startswith("Publié via un mauvais canal")

def test_numlot_en_double_bascule_sur_le_rang():
    # XML minimal construit dans le test : deux <lot><numLot>1</numLot>… sous <lots>
    ...
    assert [r["lotId"] for r in rows] == ["1", "2"]
```

- [ ] **Step 2:** → FAIL. **Step 3:** implémenter. **Step 4:** → PASS.
- [ ] **Step 5: commit** « DEAO : mapping des avis BOAMP legacy #178 ».

---

### Task 6: mapping eForms

**Files:**

- Create: `src/tasks/deao/sources/boamp/eforms.py`
- Test: `tests/test_deao_eforms.py`

**Interfaces:**

- Consumes : `xml.py`, `values.py`, `codes.py`, `siret_valide` ; reprendre `_buyer_org_id` / `_find_company_by_id` de `src/tasks/boamp/flatten.py`.
- Produces : `map_eforms(root, classement: Classement, compteur: Counter) -> list[dict]` (mêmes clés que `map_legacy`).

Règles (spec, colonne eForms ; `notice` = premier enfant de `EFORMS`, `pp` = `direct_child(notice, "ProcurementProject")`) :

- Acheteur : organisation dont l'ID = `ContractingParty/Party/PartyIdentification/ID` (enfants **directs**, pas le `ServiceProviderParty`) → `acheteur_nom` = `PartyName/Name`, `acheteur_id` = `PartyLegalEntity/CompanyID` **sans espaces** si `siret_valide`, sinon `None` ; adresse `PostalAddress` : `StreetName`, `PostalZone`, `CityName`, `CountrySubentityCode` (→ `acheteur_adresse_nuts`).
- Niveau avis : `objet` = `pp/Name` ; `referenceAcheteur` = `pp/ID` ; `type` = `libelle(TYPE_INDEXATION, INDEXATION/NATURE_MARCHE)`, à défaut `libelle(TYPE_EFORMS, pp/ProcurementTypeCode)` ; `procedure` = `libelle(PROCEDURE_INDEXATION, INDEXATION/TYPE_PROCEDURE)`, à défaut `libelle(PROCEDURE_EFORMS, ProcedureCode)` ; `infosRectificatif` = textes `ChangeDescription` joints par `" | "`.
- Lots : `children(notice, "ProcurementProjectLot")`, `lotId` = `normalize_lot_id` de l'`ID` dont `schemeName ∈ {"Lot", "Part"}` ; `lpp` = `direct_child(lot, "ProcurementProject")` ; `lotIntitule` = `direct_child(lpp, "Name")` ; `description` = `lpp/Description` sinon `pp/Description` ; `codeCPV` = `lpp/MainCommodityClassification/ItemClassificationCode` sinon celui de `pp` ; `dureeMois` = `duree_mois(DurationMeasure, @unitCode)` sous `lpp/PlannedPeriod` ; `montantEstime` = `EstimatedOverallContractAmount` du lot sinon de `pp` ; `dateLimiteReponse` = `parse_datetime_paris(TenderSubmissionDeadlinePeriod/EndDate, EndTime)` du lot, à défaut `INDEXATION/DATE_LIMITE_REPONSE` ; `urlConsultation` = `CallForTendersDocumentReference//URI` du lot (entités `&amp;` décodées par lxml) ; `lieuExecution_code` = `departement_depuis_nuts(RealizedLocation//CountrySubentityCode)` du lot sinon de `pp`, `lieuExecution_typeCode = "Code département"` si renseigné ; `techniques` : `"Accord-cadre"` si `ContractingSystem/ContractingSystemTypeCode == "fa-*"` (BT-765), `"Système d'acquisition dynamique"` si `"dps-*"` (BT-766).
- Aucun lot → une ligne `lotId="0"` avec les champs de niveau avis.

- [ ] **Step 1: tests**

```python
def test_initial_lot0000():
    rows = map_eforms(load("26-87868.xml"), classify(load("26-87868.xml")), Counter())
    assert len(rows) == 1
    r = rows[0]
    assert r["lotId"] == "0" and r["referenceAcheteur"] == "2026/4/3/54/00/00/00"
    assert (r["codeCPV"], r["type"], r["procedure"]) == ("90520000", "Services", "Appel d'offres ouvert")
    assert r["dateLimiteReponse"] == datetime(2026, 10, 19, 16, 30, tzinfo=PARIS)
    assert r["urlConsultation"].startswith("https://nr.aws-achat.info") and "&amp;" not in r["urlConsultation"]

def test_rectificatif_date_limite_du_lot():
    r = map_eforms(load("26-99074.xml"), classify(load("26-99074.xml")), Counter())[0]
    assert r["dateLimiteReponse"] == datetime(2026, 10, 30, 16, 30, tzinfo=PARIS)
    assert "Remise des offres" in r["infosRectificatif"]

def test_eforms_siret_avec_espaces():
    rows = map_eforms(load("26-98580.xml"), classify(load("26-98580.xml")), Counter())
    assert {r["acheteur_id"] for r in rows} == {"32384135300911"}
    assert [r["lotId"] for r in rows] == ["1", "2", "3", "4"]
    assert rows[0]["acheteur_adresse_cp"] == "75019"

def test_pin_mise_en_concurrence():
    rows = map_eforms(load("26-98723.xml"), classify(load("26-98723.xml")), Counter())
    assert [r["lotId"] for r in rows] == ["1", "2", "3", "4"]
    assert rows[0]["codeCPV"] == "66515200" and rows[0]["procedure"] == "Autre procédure"
    assert rows[0]["dateLimiteReponse"] is None

def test_pin_simple_part_et_pin_sans_lot():
    assert [r["lotId"] for r in map_eforms(load("26-97189.xml"), classify(load("26-97189.xml")), Counter())] == ["1"]
    assert [r["lotId"] for r in map_eforms(load("26-94806.xml"), classify(load("26-94806.xml")), Counter())] == ["0"]

def test_acheteur_n_est_pas_le_service_provider():
    root = load("26-87868.xml")
    r = map_eforms(root, classify(root), Counter())[0]
    nom_org0000 = nom_organisation(root, "ORG-0000")   # helper de test : PartyName/Name de l'Organization d'ID ORG-0000
    assert r["acheteur_nom"] is not None and r["acheteur_nom"] != nom_org0000
```

- [ ] **Step 2:** → FAIL. **Step 3:** implémenter. **Step 4:** → PASS.
- [ ] **Step 5: commit** « DEAO : mapping des avis eForms #178 ».

---

### Task 7: parsing d'un avis et d'un jour

**Files:**

- Create: `src/tasks/deao/sources/boamp/parse.py`
- Test: `tests/test_deao_parse.py`

**Interfaces:**

- Consumes : `classify`, `map_eforms`, `map_legacy`, `conform_avis`, `empty_avis_frame`.
- Produces :

  - `parse_avis(xml: bytes, filename: str, compteur: Counter) -> list[dict]` : XML illisible → `compteur["erreur_xml"] += 1`, log `warning` avec `filename`, `[]` ; avis exclu → `compteur[f"exclu:{nature}"] += 1`, `[]` ; avis retenu → `compteur["retenu"] += 1` et lignes complétées de `sourceDataset="boamp"`, `idAvis`, `avisPrecedent`, `typeAvis`, `preInformation`, `avisComplet` (`formatSource == "eforms" or typeAvis == "initial"`), `datePublicationAvis` (`GESTION/…/DATE_PUBLICATION`, `date`), `formatSource`, `sourceFile` (URL DILA du fichier : `f"{BOAMP_BASE_URL}/{AAAA}/{MM}/{JJ}/{filename}"` d'après `datePublicationAvis`). Toujours `compteur["lu"] += 1`.
  - `parse_day(files: dict[str, bytes]) -> tuple[pl.DataFrame, Counter]` : frame conforme à `AVIS_LOT_SCHEMA` (vide si aucun avis retenu).

- [ ] **Step 1: tests**

```python
def test_parse_day_fixtures():
    files = {p.name: p.read_bytes() for p in FIXTURES.glob("*.xml")}
    df, c = parse_day(files)
    assert c["lu"] == 13 and c["retenu"] == 11
    assert c["exclu:MODIFICATION"] == 1 and c["exclu:ATTRIBUTION"] == 1
    assert df.schema == pl.Schema(AVIS_LOT_SCHEMA)
    r = df.filter(pl.col("idAvis") == "26-99074").row(0, named=True)
    assert (r["avisPrecedent"], r["typeAvis"], r["avisComplet"]) == ("26-87868", "rectificatif", True)
    r = df.filter(pl.col("idAvis") == "26-99891").row(0, named=True)
    assert r["avisComplet"] is False and r["datePublicationAvis"] == date(2026, 10, 14)
    assert r["sourceFile"].endswith("/2026/10/14/26-99891.xml")

def test_parse_day_xml_illisible():
    files = {"26-1.xml": b"<pas du xml", "26-99870.xml": (FIXTURES / "26-99870.xml").read_bytes()}
    df, c = parse_day(files)
    assert c["erreur_xml"] == 1 and df.height == 1

def test_parse_day_vide():
    df, c = parse_day({})
    assert df.height == 0 and df.schema == pl.Schema(AVIS_LOT_SCHEMA)
```

- [ ] **Step 2:** → FAIL. **Step 3:** implémenter. **Step 4:** → PASS.
- [ ] **Step 5: commit** « DEAO : parsing des avis et des jours BOAMP #178 ».

---

### Task 8: archive brute S3

**Files:**

- Create: `src/tasks/deao/sources/boamp/archive.py`
- Test: `tests/test_deao_archive.py`

**Interfaces:**

- Consumes : `s3_client()`, `DEAO_S3_PREFIX`, `S3_BUCKET`.
- Produces :

  - `pack(files: dict[str, bytes]) -> bytes` / `unpack(data: bytes) -> dict[str, bytes]` (tar en mémoire, compressé `zstandard`, entrées triées par nom pour un résultat déterministe).
  - `archive_key(d: date) -> str` → `f"{DEAO_S3_PREFIX}/{d:%Y}/{d:%m}/{d:%d}.tar.zst"`.
  - `list_archived_days() -> set[date]` (paginateur `list_objects_v2` sur le préfixe, clés non conformes ignorées).
  - `read_day_archive(d: date) -> dict[str, bytes]`, `write_day_archive(d: date, files: dict[str, bytes]) -> None`.

- [ ] **Step 1: tests** — `FakeS3` en mémoire (méthodes `put_object(Bucket, Key, Body)`, `get_object(Bucket, Key)` → `{"Body": io.BytesIO}`, `get_paginator("list_objects_v2").paginate(Bucket, Prefix)` → pages `{"Contents": [{"Key": …}]}`) injecté par `monkeypatch.setattr(archive, "s3_client", lambda: fake)`.

```python
def test_pack_unpack_roundtrip():
    files = {"26-2.xml": b"<b/>", "26-1.xml": b"<a/>"}
    assert unpack(pack(files)) == files
    assert pack(files) == pack(dict(reversed(list(files.items()))))   # déterministe

def test_pack_vide():
    assert unpack(pack({})) == {}

def test_write_read_list(fake_s3):
    write_day_archive(date(2026, 10, 14), {"26-1.xml": b"<a/>"})
    assert archive_key(date(2026, 10, 14)) == "boamp/raw/2026/10/14.tar.zst"
    assert read_day_archive(date(2026, 10, 14)) == {"26-1.xml": b"<a/>"}
    fake_s3.put_object(Bucket="b", Key="boamp/raw/readme.txt", Body=b"")
    assert list_archived_days() == {date(2026, 10, 14)}
```

- [ ] **Step 2:** → FAIL. **Step 3:** implémenter. **Step 4:** → PASS.
- [ ] **Step 5: commit** « DEAO : archive brute des XML DILA sur S3 (tar.zst par jour) #178 ».

---

### Task 9: acquisition (index DILA, plan des jours, traitement d'un jour)

**Files:**

- Create: `src/tasks/deao/sources/boamp/fetch.py`, `src/tasks/deao/sources/boamp/acquire.py`
- Test: `tests/test_deao_acquire.py`

**Interfaces:**

- Consumes : `HTTP_CLIENT`, `HTTP_HEADERS` (config), `BOAMP_BASE_URL`, archive (Task 8), `parse_day`, `DEAO_PARTITIONS_DIR`, `run_in_pool`.
- Produces (`fetch.py`) :
  - `day_index_urls(d: date) -> list[str]` → `[f"{BOAMP_BASE_URL}/{d:%Y/%m/%d}/", f"{BOAMP_BASE_URL}/FluxHistorique/Boamp_v240/{d:%Y/%m/%d}/"]`.
  - `http_get(url: str) -> httpx.Response` (seul point d'accès réseau, monkeypatché en test).
  - `list_day_files(d: date) -> tuple[str | None, list[str]]` : première URL qui répond 200 → `(url, noms .xml triés dédupliqués)` ; 404 sur les deux → `(None, [])` ; autre statut → `httpx.HTTPStatusError`.
  - `download_xml(base_url: str, filename: str) -> bytes` (statut ≠ 200 → exception).
- Produces (`acquire.py`) :

  - `class Action(StrEnum): FROM_ARCHIVE, CRAWL, RECRAWL`
  - `plan_days(start: date, end: date, recrawl_days: int, local_days: set[date], archived_days: set[date]) -> list[tuple[date, Action]]` : `d > end - timedelta(days=recrawl_days)` → `RECRAWL` ; sinon `d in local_days` → omis ; sinon `d in archived_days` → `FROM_ARCHIVE` ; sinon `CRAWL`.
  - `partition_path(d: date, source: str = "boamp") -> Path` → `DEAO_PARTITIONS_DIR / source / f"{d:%Y}" / f"{d:%m}" / f"{d:%d}.parquet"`.
  - `local_days(source: str = "boamp") -> set[date]` (partitions existantes).
  - `process_day(d: date, action: Action, archived: bool) -> Counter` : `FROM_ARCHIVE` → lit l'archive ; `CRAWL` → liste + télécharge tout + écrit l'archive ; `RECRAWL` → lit l'archive si `archived`, télécharge seulement les noms absents, réécrit l'archive seulement s'il y a du nouveau ou si `archived` est faux. Puis `parse_day` et écriture de la partition (frame vide comprise, c'est le marqueur). **Toute erreur de listing ou de téléchargement lève une exception avant toute écriture** (ni archive ni partition).
  - `acquire_boamp(start: date, end: date, rebuild: bool) -> Counter` : si `rebuild`, supprime `DEAO_PARTITIONS_DIR / "boamp"` ; `plan_days(..., local_days(), list_archived_days())` ; traite par lots de 30 jours dans une `@task` Prefect (`retries=2`) via `run_in_pool(max_workers=MAX_PREFECT_WORKERS)` ; additionne les compteurs ; compte `compteur["jour_en_erreur"]` pour chaque jour en échec (renvoyé absent par `run_in_pool`).

- [ ] **Step 1: tests** (monkeypatch de `fetch.http_get` avec des `httpx.Response(status, text=…)`, de l'archive avec le `FakeS3` de la Task 8, de `DEAO_PARTITIONS_DIR` vers `tmp_path`)

```python
def test_plan_days():
    end = date(2026, 10, 15)
    plan = dict(plan_days(date(2026, 10, 10), end, 3, local_days={date(2026, 10, 10), date(2026, 10, 14)}, archived_days={date(2026, 10, 11)}))
    assert date(2026, 10, 10) not in plan
    assert plan[date(2026, 10, 11)] == Action.FROM_ARCHIVE
    assert plan[date(2026, 10, 12)] == Action.CRAWL
    assert plan[date(2026, 10, 13)] == plan[date(2026, 10, 14)] == plan[date(2026, 10, 15)] == Action.RECRAWL

def test_list_day_files_repli_flux_historique(monkeypatch): # 1re URL 404, 2e 200 avec href="26-1.xml"
    assert list_day_files(date(2025, 3, 12)) == (".../FluxHistorique/Boamp_v240/2025/03/12/", ["26-1.xml"])

def test_list_day_files_absent(monkeypatch):            # 404 partout
    assert list_day_files(date(2026, 1, 1)) == (None, [])

def test_process_day_jour_sans_dossier_ecrit_marqueur(...):
    c = process_day(date(2026, 1, 1), Action.CRAWL, archived=False)
    assert partition_path(date(2026, 1, 1)).exists() and pl.read_parquet(partition_path(date(2026, 1, 1))).height == 0

def test_process_day_recrawl_differentiel(...):
    # archive existante {26-99870.xml}, index {26-99870.xml, 26-99891.xml}
    process_day(date(2026, 10, 14), Action.RECRAWL, archived=True)
    assert telechargements == ["26-99891.xml"]
    assert set(read_day_archive(date(2026, 10, 14))) == {"26-99870.xml", "26-99891.xml"}

def test_process_day_from_archive_sans_reseau(...):  # http_get lève si appelé
    ...

def test_process_day_echec_telechargement_n_ecrit_rien(...):
    # index liste 2 fichiers, le 2e répond 500
    with pytest.raises(Exception):
        process_day(date(2026, 10, 14), Action.CRAWL, archived=False)
    assert not partition_path(date(2026, 10, 14)).exists()
    assert date(2026, 10, 14) not in list_archived_days()

def test_acquire_rebuild_supprime_les_partitions(...):
    # partition existante d'un jour ancien + archive → rebuild=True la reparse depuis l'archive
```

- [ ] **Step 2:** → FAIL. **Step 3:** implémenter. **Step 4:** → PASS.
- [ ] **Step 5: commit** « DEAO : acquisition DILA (repli FluxHistorique, re-crawl différentiel, marqueurs, rebuild) #178 ».

---

### Task 10: consolidation

**Files:**

- Create: `src/tasks/deao/consolidate.py`
- Test: `tests/test_deao_consolidate.py`

**Interfaces:**

- Consumes : frame avis-lot (`AVIS_LOT_SCHEMA`).
- Produces :
  - `resolve_roots(avis: pl.DataFrame, max_iter: int = 20) -> pl.DataFrame` : colonnes `sourceDataset, idAvis, idAvisInitial`. Point fixe : `root = avisPrecedent ?? idAvis` puis, tant que des `root` ont eux-mêmes un `avisPrecedent` connu, `root ← avisPrecedent(root)`. Un `avisPrecedent` absent des données reste la racine. Au-delà de `max_iter`, les avis encore instables sont des cycles : ils gardent leur propre `idAvis` comme racine, sont comptés et logués.
  - `statut_expr(run_date: date, delai_jours: int) -> pl.Expr` : règle de la spec (« Cycle de vie », colonne `statut`), dans cet ordre : `annule` ; `pre-information` ; date limite renseignée → `clos` si `dateLimiteReponse.dt.date() < run_date` sinon `ouvert` ; date limite nulle → `ouvert` si `run_date - datePublicationInitiale < delai_jours` jours, sinon `clos`.
  - `@dataclass class Consolidation: df: pl.DataFrame; stats: Counter` et `consolidate(avis: pl.DataFrame, run_date: date, delai_jours: int) -> Consolidation`.

Algorithme de `consolidate` :

1. `avis ← avis.join(resolve_roots(avis), on=["sourceDataset", "idAvis"])`.
2. Agrégats par chaîne (`sourceDataset, idAvisInitial`), sur une ligne par avis (`unique(["sourceDataset", "idAvis"])`) : `avisLies` (IDWEB uniques triés), `idDernierAvis` / `urlAvis` (avis max selon `datePublicationAvis`, puis `idAvis`), `datePublicationInitiale` (date de l'avis `idAvis == idAvisInitial`, sinon min), `dateDerniereModification` (max des dates des avis non `initial`, sinon null), `rectifie` (au moins un `rectificatif`), `annule` (au moins une `annulation`), `infosRectificatifs` (textes non nuls triés par date), `preInformation` (celui de l'avis racine, sinon `False`), `seulement_annulations`.
3. Lignes descriptives : parmi les avis `avisComplet`, pour chaque `(sourceDataset, idAvisInitial, lotId)`, la ligne de l'avis le plus récent (`datePublicationAvis`, `idAvis`). Chaînes sans avis complet mais avec un rectificatif → ligne du rectificatif le plus récent. Chaînes `seulement_annulations` → écartées, `stats["chaine_annulation_seule"]`.
4. Jointure descriptif × agrégats ; `uid = sourceDataset + "_" + idAvisInitial + "_" + lotId` ; `statut = statut_expr(...)` ; colonnes internes conservées pour l'enrichissement et le rapport : `formatSource`, `acheteur_adresse_*` ; colonnes de travail (`annule`, `typeAvis`, `avisComplet`, `idAvis`, `avisPrecedent`, `datePublicationAvis`, `infosRectificatif`, `preInformation`) supprimées.
5. Doublons d'`uid` → `stats["uid_doublon"]` = nb de lignes supprimées, log `warning`, on garde la ligne dont `idDernierAvis` est le plus récent.

- [ ] **Step 1: tests** — frames construites à la main via un helper `avis(**kw)` qui remplit les défauts du contrat ; plus un test sur les fixtures.

```python
RUN = date(2026, 10, 15)

def test_resolve_roots_chaine_racine_absente_et_cycle():
    df = frame([avis(idAvis="B", avisPrecedent="A"), avis(idAvis="C", avisPrecedent="B"),
                avis(idAvis="E", avisPrecedent="Z"),                         # Z absent
                avis(idAvis="X", avisPrecedent="Y"), avis(idAvis="Y", avisPrecedent="X")])
    r = dict(resolve_roots(df).select("idAvis", "idAvisInitial").iter_rows())
    assert r["C"] == "A" and r["B"] == "A" and r["E"] == "Z"
    assert r["X"] == "X" and r["Y"] == "Y"

def test_rectificatif_legacy_s_applique_a_tous_les_lots():
    df = frame([avis(idAvis="I", lotId="1", dateLimiteReponse=dl(2026, 10, 20)), avis(idAvis="I", lotId="2", dateLimiteReponse=dl(2026, 10, 20)),
                avis(idAvis="R", avisPrecedent="I", typeAvis="rectificatif", avisComplet=False, lotId="0", infosRectificatif="report", datePublicationAvis=date(2026, 10, 14))])
    out = consolidate(df, RUN, 90).df.sort("uid")
    assert out["uid"].to_list() == ["boamp_I_1", "boamp_I_2"]
    assert out["rectifie"].all() and out["idDernierAvis"].to_list() == ["R", "R"]
    assert out["infosRectificatifs"][0].to_list() == ["report"]

def test_dernier_avis_complet_et_lot_absent_du_dernier():
    # I : lots 1 (objet "v1") et 2 ; R eForms complet : lot 1 seulement (objet "v2")
    # → lot 1 objet "v2", lot 2 garde sa version de I

@pytest.mark.parametrize("cas, attendu", [
    (dict(annule=True), "annule"),
    (dict(preInformation=True), "pre-information"),
    (dict(preInformation=True, annule=True), "annule"),
    (dict(dateLimiteReponse=dl(2026, 10, 14)), "clos"),
    (dict(dateLimiteReponse=dl(2026, 10, 15)), "ouvert"),
    (dict(dateLimiteReponse=None, datePublicationInitiale=date(2026, 7, 18)), "ouvert"),   # 89 jours
    (dict(dateLimiteReponse=None, datePublicationInitiale=date(2026, 7, 17)), "clos"),     # 90 jours
])
def test_statut(cas, attendu): ...   # évaluer statut_expr(RUN, 90) sur une frame d'une ligne

def test_chaine_annulation_seule_ecartee(): ...   # stats["chaine_annulation_seule"] == 1

def test_uid_doublon(): ...                        # deux lignes même uid → 1 ligne, stats["uid_doublon"] == 1

def test_consolidate_fixtures():
    df, _ = parse_day({p.name: p.read_bytes() for p in FIXTURES.glob("*.xml")})
    out = consolidate(df, RUN, 90).df
    assert out.height == 31 and out["uid"].is_unique().all()
    r = out.filter(pl.col("uid") == "boamp_26-87868_0").row(0, named=True)
    assert (r["idDernierAvis"], r["rectifie"], r["statut"]) == ("26-99074", True, "ouvert")
    assert r["dateLimiteReponse"] == datetime(2026, 10, 30, 16, 30, tzinfo=PARIS)
    assert (r["datePublicationInitiale"], r["dateDerniereModification"]) == (date(2026, 9, 12), date(2026, 10, 14))
    assert out.filter(pl.col("idAvisInitial") == "26-99177")["statut"].unique().to_list() == ["annule"]
    assert out.filter(pl.col("idAvisInitial") == "26-92574").height == 8
    assert set(out.filter(pl.col("idAvisInitial").is_in(["26-97189", "26-94806"]))["statut"]) == {"pre-information"}
    assert set(out.filter(pl.col("idAvisInitial") == "26-98723")["statut"]) == {"ouvert"}
```

- [ ] **Step 2:** → FAIL. **Step 3:** implémenter. **Step 4:** → PASS.
- [ ] **Step 5: commit** « DEAO : consolidation des avis en appels d'offres × lots #178 ».

---

### Task 11: géocodage d'adresses (généralisation + cache S3)

**Files:**

- Modify: `src/tasks/geocode.py`
- Create: `src/tasks/deao/geocode_cache.py`
- Test: `tests/test_geocode.py` (existant, doit rester vert), `tests/test_deao_geocode.py`

**Interfaces:**

- Modify (`geocode.py`, comportement SIRENE inchangé par défaut) : `parse_geocoding_results(csv_bytes, min_score, today, key_column: str = "siret", with_citycode: bool = False)` (avec `with_citycode`, lit aussi `result_citycode` → `commune_code`, nul si échec) ; `geocode_csv(addresses, chunk_size=…, min_score=…, key_column: str = "siret", with_citycode: bool = False, csv_builder: Callable[[pl.DataFrame], bytes] = build_geocoding_csv)` ; nouveau `build_address_geocoding_csv(addresses: pl.DataFrame) -> bytes` (colonnes d'entrée `adresse_cle, q, postcode` ; écrit `adresse_cle, q, postcode, citycode` avec `citycode` vide).
- Produces (`geocode_cache.py`) :

  - `ADRESSE_GEOCODE_SCHEMA = {"adresse_cle": String, "commune_code": String, "latitude": Float64, "longitude": Float64, "score": Float64, "status": String, "geocoded_at": Date}`
  - `adresse_cle_expr() -> pl.Expr` : `lower(strip(voie)) | cp | lower(strip(ville))` sur `acheteur_adresse_voie/_cp/_ville`, espaces multiples réduits, séparateur `"|"` ; null si `cp` et `ville` sont nuls.
  - `a_geocoder(cles: pl.DataFrame, cache: pl.DataFrame, today: date, retry_days: int) -> pl.DataFrame` : clés absentes du cache + clés en `failed` dont `geocoded_at` date d'au moins `retry_days` jours.
  - `geocoder_adresses(adresses: pl.DataFrame, today: date, geocode: Callable[[pl.DataFrame], pl.DataFrame] = …) -> pl.DataFrame` : charge le cache (`get_from_s3("adresse_geocode.parquet", prefix=DEAO_GEOCODE_S3_PREFIX)`, `None` → cache vide), géocode le delta, fusionne (la nouvelle valeur remplace l'ancienne), réécrit et publie le cache (`publish_to_s3(path, prefix=DEAO_GEOCODE_S3_PREFIX)`) **uniquement si le delta est non vide**, renvoie le cache complet. `adresses` : colonnes `adresse_cle, q, postcode`.

- [ ] **Step 1: tests**

```python
def test_parse_geocoding_results_citycode():
    csv = b"adresse_cle,result_score,latitude,longitude,result_citycode\nk1,0.9,48.8,2.3,75104\nk2,0.2,1,1,99999\n"
    df = parse_geocoding_results(csv, 0.5, date(2026, 11, 11), key_column="adresse_cle", with_citycode=True)
    assert df.filter(pl.col("adresse_cle") == "k1")["commune_code"][0] == "75104"
    assert df.filter(pl.col("adresse_cle") == "k2")["commune_code"][0] is None
    assert df.filter(pl.col("adresse_cle") == "k2")["status"][0] == "failed"

def test_a_geocoder_retry():
    cache = pl.DataFrame({"adresse_cle": ["ok", "vieux_echec", "echec_recent"], "status": ["success", "failed", "failed"],
                          "geocoded_at": [date(2026, 1, 1), date(2026, 10, 1), date(2026, 11, 10)], ...})
    cles = pl.DataFrame({"adresse_cle": ["ok", "vieux_echec", "echec_recent", "nouvelle"]})
    assert set(a_geocoder(cles, cache, date(2026, 11, 11), 30)["adresse_cle"]) == {"vieux_echec", "nouvelle"}

def test_geocoder_adresses_ne_republie_pas_sans_delta(monkeypatch): ...
def test_adresse_cle_expr(): ...   # (" 12 Bvd  X ", "29260", "Lesneven") → "12 bvd x|29260|lesneven"
```

- [ ] **Step 2:** → FAIL. **Step 3:** implémenter. **Step 4:** `uv run pytest tests/test_geocode.py tests/test_deao_geocode.py -v` → PASS.
- [ ] **Step 5: commit** « DEAO : géocodage des adresses d'acheteurs avec cache S3 #178 ».

---

### Task 12: enrichissement acheteur

**Files:**

- Create: `src/tasks/deao/enrich.py`
- Test: `tests/test_deao_enrich.py`

**Interfaces:**

- Consumes : `add_unite_legale_data`, `add_etablissement_data` (`src/tasks/enrich.py`), `adresse_cle_expr`, `departement_depuis_cp`, `departement_depuis_nuts`, COG (`DATA_DIR / "code_officiel_geographique.parquet"`, colonnes `commune_code, commune_nom, departement_code, departement_nom, region_code, region_nom`).
- Produces :

  - `normaliser_nom_expr(col: str) -> pl.Expr` : minuscules, `str.normalize("NFKD")` puis suppression de `\p{M}`, `[^a-z0-9]+` → espace, strip.
  - `appliquer_dictionnaire_siret(df: pl.DataFrame) -> pl.DataFrame` : ajoute `acheteur_id_source` (`"avis"` / `"dictionnaire"` / null) ; clé `(nom normalisé, acheteur_adresse_cp)` ; ne retient que les clés à un seul SIRET.
  - `enrichir_sirene(df: pl.DataFrame, lf_unites_legales: pl.LazyFrame, lf_etablissements: pl.LazyFrame) -> pl.DataFrame` : SIRET uniques → `add_unite_legale_data` → `add_etablissement_data` → jointure gauche ; `acheteur_nom` SIRENE prioritaire (`coalesce`), `acheteur_categorie` et colonnes géographiques `acheteur_*` ajoutées ; colonne interne `_sirene_trouve: bool`.
  - `enrichir_geographie(df: pl.DataFrame, df_cog: pl.DataFrame, geocoder: Callable[[pl.DataFrame], pl.DataFrame]) -> pl.DataFrame` : pour les lignes `~_sirene_trouve` avec une `adresse_cle` → `geocoder(adresses)` (renvoie le cache complet au format `ADRESSE_GEOCODE_SCHEMA`) → `commune_code`, `latitude`, `longitude` → COG → remplit les `acheteur_commune_*`, `acheteur_departement_*`, `acheteur_region_*`, `acheteur_latitude/longitude` encore nuls ; puis repli : `acheteur_departement_code` nul → `departement_depuis_cp(acheteur_adresse_cp)` → `departement_depuis_nuts(acheteur_adresse_nuts)`, avec nom de département et région complétés depuis le COG (table départementale dédoublonnée).
  - `enrich_acheteurs(df, lf_unites_legales, lf_etablissements, df_cog, geocoder) -> pl.DataFrame` : dictionnaire → SIRENE → géographie ; supprime `_sirene_trouve`.

- [ ] **Step 1: tests** (SIRENE de test : `tests/data/sirene/{unites_legales,etablissements}.json`, comme `tests/test_enrich.py` ; COG : petite frame écrite dans le test ; `geocoder` factice)

```python
def test_dictionnaire_cle_non_ambigue():
    df = pl.DataFrame({"acheteur_id": ["13002799800017", None, None], "acheteur_nom": ["CMAR Normandie", "C.M.A.R. normandie", "Autre"],
                       "acheteur_adresse_cp": ["14000", "14000", "14000"]})
    out = appliquer_dictionnaire_siret(df)
    assert out["acheteur_id"].to_list() == ["13002799800017", "13002799800017", None]
    assert out["acheteur_id_source"].to_list() == ["avis", "dictionnaire", None]

def test_dictionnaire_cle_ambigue_ignoree():
    # même (nom, cp) → deux SIRET différents ; la ligne sans SIRET reste sans SIRET
    ...

def test_enrichir_sirene_nom_et_categorie(): ...      # 20007695800011 → acheteur_categorie "Région", nom SIRENE

def test_geographie_par_geocodage_puis_replis():
    # ligne A : géocodée → commune 29160 → département 29, région 53
    # ligne B : géocodage en échec, cp 20137 → département 2A
    # ligne C : pas d'adresse, nuts FRK27 → département 73
    ...

def test_ligne_trouvee_dans_sirene_n_est_pas_geocodee(): ...   # le geocoder factice ne reçoit pas sa clé
```

- [ ] **Step 2:** → FAIL. **Step 3:** implémenter. **Step 4:** → PASS.
- [ ] **Step 5: commit** « DEAO : enrichissement des acheteurs (dictionnaire SIRET, SIRENE, géocodage, replis) #178 ».

---

### Task 13: rapport, écriture et publication

**Files:**

- Create: `src/tasks/deao/report.py`
- Modify: `src/tasks/output.py` (`_gzip_decp_csv`), `src/tasks/publish.py` (remplacer `publish_boamp_to_datagouv`)
- Test: `tests/test_deao_report.py`, `tests/test_deao_publish.py`

**Interfaces:**

- Produces (`report.py`) :
  - `taux_remplissage(df: pl.DataFrame, par: str = "formatSource") -> pl.DataFrame` : une ligne par (colonne publiée, valeur de `par`), colonne `taux` ∈ [0, 1] (liste vide = non rempli).
  - `lignes_rapport(stats: Counter, df: pl.DataFrame) -> list[dict]` : `{"indicateur": str, "valeur": str|int|float}` pour les compteurs d'avis (`lu`, `retenu`, `exclu:*`, `erreur_xml`, `jour_en_erreur`, `non_mappe:*`, `chaine_annulation_seule`, `uid_doublon`), les lignes par `statut`, la répartition `acheteur_id_source`, les taux de remplissage.
  - `publier_rapport(lignes: list[dict]) -> None` : `create_table_artifact(key="deao-processing-rapport", …)` ; log des mêmes lignes.
- Modify (`output.py`) : `_gzip_decp_csv` → `_gzip_csv`, appliqué pour `path.name in {"decp", "deao"}`.
- Produces (`publish.py`) : `should_publish_deao(n_lignes: int) -> bool` (`DECP_PROCESSING_PUBLISH and DEAO_DATASET_ID and n_lignes >= DEAO_MIN_ROWS_TO_PUBLISH`) et `publish_deao_to_datagouv(dist_dir: Path) -> None` (met à jour `deao.parquet`, `deao.csv.gz`, `schema.json` sur leurs ressources respectives ; ressource sans ID → warning, ignorée).

- [ ] **Step 1: tests**

```python
def test_taux_remplissage():
    df = pl.DataFrame({"formatSource": ["eforms", "eforms", "legacy"], "acheteur_id": ["1", None, None], "techniques": [["Accord-cadre"], [], []]})
    t = taux_remplissage(df.select("formatSource", "acheteur_id", "techniques"))
    assert t.filter((pl.col("colonne") == "acheteur_id") & (pl.col("formatSource") == "eforms"))["taux"][0] == 0.5
    assert t.filter((pl.col("colonne") == "techniques") & (pl.col("formatSource") == "legacy"))["taux"][0] == 0.0

@pytest.mark.parametrize("publish, dataset, n, attendu", [(True, "ds", 50000, True), (True, "ds", 49999, False), (False, "ds", 10**6, False), (True, "", 10**6, False)])
def test_should_publish_deao(monkeypatch, publish, dataset, n, attendu): ...

def test_publish_deao_appelle_update_resource(monkeypatch, tmp_path): ...   # 3 appels avec les bons fichiers/IDs

def test_sink_deao_produit_csv_gz(tmp_path):
    sink_to_files(pl.LazyFrame({"a": [1]}), tmp_path / "deao")
    assert (tmp_path / "deao.csv.gz").exists()
```

- [ ] **Step 2:** → FAIL. **Step 3:** implémenter. **Step 4:** `uv run pytest tests/test_deao_report.py tests/test_deao_publish.py tests/test_output.py -v` (ce dernier s'il existe) → PASS.
- [ ] **Step 5: commit** « DEAO : rapport de run, écriture CSV gzip et publication data.gouv.fr #178 ».

---

### Task 14: flow `deao_processing` (bout en bout)

**Files:**

- Create: `src/flows/deao_processing.py`
- Modify: `run_flow.py` (entrée `"deao_processing"`)
- Test: `tests/test_deao_flow.py`

**Interfaces:**

- Consumes : `acquire_boamp`, `consolidate`, `enrich_acheteurs`, `geocoder_adresses`, `PUBLISHED_FIELDS`, `write_table_schema`, `sink_to_files`, `lignes_rapport`, `publier_rapport`, `should_publish_deao`, `publish_deao_to_datagouv`.
- Produces :

  - `charger_referentiels() -> tuple[pl.LazyFrame, pl.LazyFrame, pl.DataFrame]` (unités légales, établissements depuis `SIRENE_DATA_DIR`, COG) — point de substitution en test.
  - `lire_partitions() -> pl.DataFrame` : `pl.scan_parquet(sorted(DEAO_PARTITIONS_DIR.rglob("*.parquet")))`, collecté ; frame vide conforme si aucune partition.
  - `selection_publiee(df: pl.DataFrame) -> pl.DataFrame` : colonnes de `PUBLISHED_FIELDS`, dans l'ordre, castées (colonne absente → null typé).
  - `@flow(log_prints=True) def deao_processing(run_date: str | None = None) -> None` : `run_date` ISO, défaut aujourd'hui ; acquisition `BOAMP_START_DATE` → `run_date` (`DEAO_REBUILD`) → `lire_partitions` → `consolidate(…, DEAO_STATUT_DELAI_JOURS)` → `enrich_acheteurs(…, geocoder=lambda a: geocoder_adresses(a, today))` → rapport (sur la frame enrichie, avant sélection) → `sink_to_files(selection_publiee(df).lazy(), DEAO_DIST_DIR / "deao")` → `write_table_schema(DEAO_DIST_DIR / "schema.json")` → publication si `should_publish_deao(df.height)`, sinon log de la raison.

- [ ] **Step 1: test bout en bout** — monkeypatch : `BOAMP_START_DATE="2026-09-12"` dans le module ; `fetch.http_get` sert l'index de chaque jour à partir de la `DATE_PUBLICATION` des fixtures (404 pour les jours sans fixture) et le contenu des fichiers ; archive S3 → `FakeS3` ; `charger_referentiels` → SIRENE de test + COG minimal ; `geocoder_adresses` → stub renvoyant `failed` ; `DEAO_PARTITIONS_DIR`, `DEAO_DIST_DIR` → `tmp_path` ; `DECP_PROCESSING_PUBLISH=False`.

```python
def test_deao_processing_bout_en_bout(...):
    deao_processing(run_date="2026-10-15")
    df = pl.read_parquet(dist / "deao.parquet")
    assert df.columns == [f.name for f in PUBLISHED_FIELDS]
    assert df.height == 31 and df["uid"].is_unique().all()
    assert (dist / "deao.csv.gz").exists() and (dist / "schema.json").exists()
    assert "avisLies" not in pl.read_csv(dist / "deao.csv").columns
    assert set(df["statut"]) >= {"ouvert", "annule", "pre-information"}

def test_deao_processing_second_run_ne_retelecharge_que_la_fenetre(...):
    # 2e appel avec le même run_date : http_get n'est appelé que pour les 3 derniers jours (index)
```

- [ ] **Step 2:** `uv run pytest tests/test_deao_flow.py -v` → FAIL. **Step 3:** implémenter. **Step 4:** → PASS.
- [ ] **Step 5:** essai réel court (réseau, sans publication) : `DEAO_DATA_DIR=/tmp/deao BOAMP_START_DATE=2026-10-12 DECP_PROCESSING_PUBLISH= uv run python run_flow.py deao_processing` ; attendu : fin sans erreur, `$DEAO_DIST_DIR/deao.parquet` non vide, artefact de rapport avec `erreur_xml == 0`. Lire le taux de remplissage par format : toute colonne descriptive à 0 % pour un format où la spec la mappe signale un chemin XML à corriger (Tasks 5-6).
- [ ] **Step 6: commit** « DEAO : flow deao_processing de bout en bout #178 ».

---

### Task 15: déploiement et retrait de `boamp_processing`

**Files:**

- Modify: `src/deployments.py`, `src/config.py`, `template.env`, `run_flow.py`
- Delete: `src/flows/boamp_processing.py`, `src/tasks/boamp/fetch.py`, `tests/test_boamp_flow.py`, `tests/test_boamp_fetch.py`
- Modify: `src/tasks/boamp/flatten.py` (ne garder que ce qu'utilisent `field_definitions.py` et `script/build_boamp_field_definitions.py` — vérifier avec `codegraph_callers`), `tests/test_boamp_flatten.py` en conséquence
- Les déploiements DEAO sont décrits par une fonction testable de `src/deployments.py`, `deao_deployment_specs() -> list[dict]` (kwargs de `.deploy()` + `entrypoint`), utilisée par le bloc `__main__`.
- Test: `tests/test_deao_deployments.py`

**Interfaces:**

- Produces : `deao_deployment_specs() -> list[dict]`, quatre entrées :

  - `deao-processing` : entrypoint `src/flows/launcher.py:run_on_scaleway`, pool `local`, `cron="0 9 * * *"`, `parameters={"env": "prod", "deployment": "deao-processing/deao-processing-exec-prod", "branch": "main", "dist_dir": "/srv/shared/deao/prod/dist", "instance_type": "STANDARD3-X4C-16G"}` ;
  - `deao-processing-exec-prod` : entrypoint `src/flows/deao_processing.py:deao_processing`, pool `scaleway-prod`, `job_variables={"env": {"DECP_PROCESSING_PUBLISH": "True"}}` ;
  - `deao-processing-dev` (sans cron, `env="dev"`, branche `dev`, `dist_dir="/srv/shared/deao/dev/dist"`, même `instance_type`) et `deao-processing-exec-dev` (pool `scaleway-dev`, `DECP_PROCESSING_PUBLISH="False"`).

- [ ] **Step 1: test**

```python
def test_deao_ne_partage_pas_le_dist_des_decp():
    specs = deao_deployment_specs()
    for s in specs:
        dist = s.get("parameters", {}).get("dist_dir")
        if dist is not None:
            assert dist.startswith("/srv/shared/deao/") and "/decp/" not in dist

def test_deao_lanceur_prod():
    s = next(s for s in deao_deployment_specs() if s["name"] == "deao-processing")
    assert s["parameters"]["instance_type"] == "STANDARD3-X4C-16G" and s["cron"] == "0 9 * * *"
```

- [ ] **Step 2:** → FAIL. **Step 3:** implémenter `deao_deployment_specs`, supprimer le déploiement `boamp-processing`, l'entrée `boamp_processing` de `run_flow.py`, les fichiers listés, et les variables `BOAMP_ARCHIVE_CUTOFF_YEAR`, `BOAMP_DATA_DIR`, `BOAMP_PARTITIONS_DIR`, `BOAMP_DATASET_ID`, `BOAMP_*_RESOURCE_ID`, `BOAMP_MISE_EN_CONCURRENCE_NATURES` de `src/config.py` et `template.env`.
- [ ] **Step 4:** `uv run pytest tests/test_deao_*.py tests/test_boamp_*.py tests/test_geocode.py tests/test_enrich.py -v` → PASS ; `pre-commit run --all-files` → PASS ; `grep -rn "boamp_processing\|BOAMP_PARTITIONS_DIR\|BOAMP_DATASET_ID" src tests run_flow.py` → aucun résultat.
- [ ] **Step 5: commit** « DEAO : déploiements Scaleway (dist_dir séparé) et retrait de boamp_processing #178 ».

---

## Après le plan (hors tâches, à faire avec l'utilisateur)

- Créer la ressource `schema.json` dans le dataset data.gouv.fr et renseigner `DEAO_SCHEMA_RESOURCE_ID`.
- Créer `/srv/shared/deao/{prod,dev}` sur le serveur.
- Mesurer le pic mémoire d'un rebuild complet sur l'instance (`/usr/bin/time -v`, sortie complète écrite).
- Pas de push ni de PR sans accord explicite.
