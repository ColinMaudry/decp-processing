# DEAO — source JOUE Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Ajouter au flow `deao_processing` les appels d'offres du JOUE (lus via l'API et le site TED) absents du BOAMP, sous le code de source `joue`.

**Architecture:** L'acquisition par jour (archive S3, partitions, re-crawl) devient générique et paramétrée par un objet `Source` ; le BOAMP et le JOUE en sont deux instances. Le mapping eForms est extrait dans un module commun utilisé par les deux adaptateurs. Le tronc commun gagne trois étapes : résolution des identifiants JOUE (avant consolidation), écartement des chaînes JOUE qui recoupent le BOAMP (dans la consolidation), neutralisation des adresses d'acheteurs étrangers (dans l'enrichissement).

**Tech Stack:** Python 3, polars, lxml, httpx, tenacity, boto3, Prefect 3, pytest.

**Spec:** `docs/superpowers/specs/2026-11-12-deao-joue-source-design.md` (à lire avant chaque tâche ; cette spec complète `2026-11-11-deao-processing-design.md`).

## Global Constraints

- Code de source : `joue` partout (`sourceDataset`, préfixe d'`uid`, module `src/tasks/deao/sources/joue/`, préfixe S3 `joue/raw`, partitions `DEAO_PARTITIONS_DIR/joue/`). « TED » ne nomme que le service : variables `TED_API_URL`, `TED_NOTICE_XML_URL`.
- Périmètre : codes pays `FRA ATF PYF NCL WLF SPM BLM MAF` (`JOUE_COUNTRY_CODES`), sur l'acheteur **ou** le lieu d'exécution.
- `uid` JOUE = `joue_<numéro de publication de l'avis initial>_<lotId>`, ex. `joue_707441-2026_1`. Les lignes et `uid` BOAMP ne changent pas.
- Les lignes BOAMP produites par les adaptateurs restent identiques (les tests BOAMP existants passent sans modification de leurs assertions, hors imports déplacés).
- Imports absolus depuis `src.` (`script/check_imports.py`).
- Tests : `uv run pytest` (jamais `pytest` nu).
- Messages de commit en français, préfixe `DEAO : `, suffixe ` #165`, terminés par `Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>`. Pas de push.
- Aucun accès réseau dans les tests : HTTP et S3 simulés (`tests/deao_fakes.py`).

## Review Focus

- Partitions BOAMP écrites avant ce changement (sans `idEforms`) lues avec un JOUE rempli : le run doit échouer avant publication, pas publier ~79 % de doublons (test dans la tâche 6).
- API TED qui répond 429 puis 200 : le jour doit aboutir, pas être compté en erreur (test dans la tâche 3).
- Rectificatif JOUE dont l'avis initial n'est ni dans les partitions ni résoluble (API en erreur) : la ligne existe, avec l'avis lui-même pour racine, et la résolution est retentée au run suivant (test dans la tâche 5).
- Acheteur étranger dont le code postal ressemble à un code postal français (`31-xxx` polonais, `1000` belge) : aucun département, aucun SIRET déduit, géocodeur non appelé pour lui (test dans la tâche 7).
- `DEAO_SOURCES=boamp` : le dataset ne contient aucune ligne `joue` même si des partitions `joue` existent sur le disque (test dans la tâche 8).

---

### Task 1: Acquisition et archive génériques, BOAMP comme première `Source`

Refactorisation sans changement de comportement.

**Files:**

- Create: `src/tasks/deao/archive.py` (contenu de `sources/boamp/archive.py`, préfixe en paramètre)
- Create: `src/tasks/deao/acquire.py` (partie générique de `sources/boamp/acquire.py`)
- Create: `src/tasks/deao/sources/boamp/source.py`
- Delete: `src/tasks/deao/sources/boamp/archive.py`, `src/tasks/deao/sources/boamp/acquire.py`
- Modify: `src/config.py` (`DEAO_S3_PREFIX` → `BOAMP_S3_PREFIX`), `src/flows/deao_processing.py` (appel), `template.env` si `DEAO_S3_PREFIX` y figure
- Test: `tests/test_deao_archive.py`, `tests/test_deao_acquire.py`, `tests/test_deao_flow.py`, `tests/deao_fakes.py` (imports et appels mis à jour)

**Interfaces:**

- Produces (`src/tasks/deao/archive.py`) : `pack(files) -> bytes`, `unpack(data) -> dict[str, bytes]` (inchangés), `archive_key(prefix: str, d: date) -> str`, `list_archived_days(prefix: str) -> set[date]`, `read_day_archive(prefix: str, d: date) -> dict[str, bytes]`, `write_day_archive(prefix: str, d: date, files: dict[str, bytes]) -> None`.
- Produces (`src/tasks/deao/acquire.py`) :
  ```python
  @dataclass(frozen=True)
  class Source:
      nom: str                                              # "boamp", "joue"
      s3_prefix: str
      list_day_files: Callable[[date], tuple[str | None, list[str]]]  # (base, noms de fichiers)
      download: Callable[[str | None, str], bytes]           # (base, nom) -> XML
      parse_day: Callable[[dict[str, bytes]], tuple[pl.DataFrame, Counter]]
      max_workers: int
  ```
  `Action` (inchangé), `plan_days(...)` (inchangé), `partition_path(source: str, d: date) -> Path`, `local_days(source: str) -> set[date]`, `process_day(source: Source, d: date, action: Action, archived: bool) -> Counter`, `process_batch(source: Source, items) -> Counter`, `acquire(source: Source, start: date, end: date, rebuild: bool) -> Counter`. `rebuild` supprime `DEAO_PARTITIONS_DIR / source.nom`.
- Produces (`sources/boamp/source.py`) : `BOAMP: Source` (`nom="boamp"`, `s3_prefix=BOAMP_S3_PREFIX`, `fetch.list_day_files`, `fetch.download_xml`, `parse.parse_day`, `max_workers=MAX_PREFECT_WORKERS`).
- `tests/deao_fakes.py::fake_s3` patche désormais `src.tasks.deao.archive.s3_client`.

- [ ] **Step 1: Mettre à jour les tests existants vers la nouvelle API**

`test_deao_archive.py` : `write_day_archive("boamp/raw", d, ...)`, `archive_key("boamp/raw", d) == "boamp/raw/2026/10/14.tar.zst"`, `list_archived_days("boamp/raw")`. `test_deao_acquire.py` et `test_deao_flow.py` : `acquire.process_day(BOAMP, ...)`, `acquire.DEAO_PARTITIONS_DIR` pointe vers `src.tasks.deao.acquire`, `BOAMP_RECRAWL_DAYS` patché dans ce module. Ajouter :

```python
def test_partition_path_par_source():
    assert acquire.partition_path("joue", date(2026, 10, 14)).parts[-4:] == ("joue", "2026", "10", "14.parquet")
```

- [ ] **Step 2: Vérifier l'échec**

Run: `uv run pytest tests/test_deao_archive.py tests/test_deao_acquire.py -q`
Expected: FAIL (ImportError sur `src.tasks.deao.archive` / `src.tasks.deao.acquire`).

- [ ] **Step 3: Implémenter les deux modules génériques, `BOAMP`, et brancher le flow**

Le flow appelle `acquire(BOAMP, date.fromisoformat(BOAMP_START_DATE), today, DEAO_REBUILD)`. `process_batch` passe `max_workers=source.max_workers` à `run_in_pool`, avec `label` = `f"{source.nom} {d} ({action})"`.

- [ ] **Step 4: Vérifier**

Run: `uv run pytest tests -q -k "deao or boamp"`
Expected: PASS (210 tests ou plus, aucune assertion BOAMP modifiée).

- [ ] **Step 5: Commit**

```bash
git add -A src/tasks/deao src/config.py src/flows/deao_processing.py tests template.env
git commit -m "DEAO : acquisition et archive génériques par source #165"
```

---

### Task 2: Mapping eForms commun, langue, nouvelles colonnes du contrat

**Files:**

- Create: `src/tasks/deao/xml.py` (helpers génériques déplacés depuis `sources/boamp/xml.py` : `localname`, `find`, `direct_child`, `children`, `text`, `first_child_localname`) + `child_fr`
- Create: `src/tasks/deao/eforms.py` (corps de `sources/boamp/eforms.py` sans rien de propre au BOAMP)
- Create: `src/tasks/deao/classement.py` (dataclass `Classement` déplacée depuis `sources/boamp/classify.py`)
- Modify: `src/tasks/deao/sources/boamp/xml.py` (ne garde que `indexation_choice`, `idweb`, `annonce_anterieure`, `detect_format`), `sources/boamp/eforms.py` (devient une enveloppe), `sources/boamp/classify.py`, `sources/boamp/parse.py`, `sources/boamp/legacy.py` (imports), `src/tasks/deao/schema.py`, `src/tasks/deao/consolidate.py`
- Test: `tests/test_deao_eforms_common.py` (nouveau), `tests/test_deao_eforms.py`, `tests/test_deao_parse.py`, `tests/test_deao_schema.py`

**Interfaces:**

- Produces (`src/tasks/deao/xml.py`) : `child_fr(elem, name: str)` → l'enfant direct `name` dont l'attribut `languageID == "FRA"`, sinon le premier enfant `name`, sinon `None`.
- Produces (`src/tasks/deao/eforms.py`) :
  - `map_notice(notice, compteur: Counter, *, type_marche: str | None = None, procedure: str | None = None) -> list[dict]` : lignes avis-lot de l'élément UBL `notice` (ContractNotice, PriorInformationNotice…), sans `urlAvis`, sans les champs communs ajoutés par les `parse.py`. `type_marche` / `procedure` non nuls l'emportent sur les codes eForms (c'est par là que le BOAMP passe son repli `INDEXATION`). Ajoute `acheteur_pays` (= `Company/PostalAddress/Country/IdentificationCode` de l'acheteur) et `infosRectificatif`.
  - `notice_id(notice) -> str | None` : `cbc:ID[@schemeName="notice-id"]` enfant direct, en minuscules.
  - `changed_notice_id(notice) -> str | None` : `efbc:ChangedNoticeIdentifier` (premier trouvé), sans le suffixe `-NN`, en minuscules.
  - `change_reason(notice) -> str | None` : `cbc:ReasonCode[@listName="change-corrig-justification"]`.
  - Les textes `Name`, `Description`, `PartyName/Name`, `ChangeDescription` sont lus via `child_fr`.
- Produces (`schema.py`) : `AVIS_LOT_SCHEMA` gagne `idEforms: String`, `idEformsPrecedent: String`, `acheteur_pays: String`. `consolidate.INTERNAL_COLUMNS` gagne `acheteur_pays`.
- `sources/boamp/eforms.map_eforms(root, classement, compteur)` garde sa signature et appelle `map_notice(notice, compteur, type_marche=..., procedure=...)`, puis ajoute `urlAvis`. Le repli `DATE_LIMITE_REPONSE` de `INDEXATION` (aujourd'hui dans `_date_limite`) reste côté BOAMP : appliqué par l'enveloppe aux lignes dont `dateLimiteReponse` est nul.
- `sources/boamp/parse.parse_avis` ajoute aux `communs` : `idEforms` / `idEformsPrecedent` (avis eForms : `notice_id` / `changed_notice_id` de l'élément sous `EFORMS` ; legacy : nuls) et `acheteur_pays = "FRA"`.

- [ ] **Step 1: Tests du module commun**

```python
# tests/test_deao_eforms_common.py
def test_child_fr_prefere_le_francais():
    e = etree.fromstring('<r xmlns:cbc="urn:c"><cbc:Name languageID="ENG">A</cbc:Name><cbc:Name languageID="FRA">B</cbc:Name></r>')
    assert text(child_fr(e, "Name")) == "B"

def test_child_fr_premier_sinon():
    e = etree.fromstring('<r><Name languageID="POL">A</Name><Name languageID="DEU">B</Name></r>')
    assert text(child_fr(e, "Name")) == "A"

def test_notice_id_et_changed_notice_id_boamp_26_99074():
    notice = _notice("26-99074.xml")      # élément sous EFORMS
    assert notice_id(notice) == "53d43f07-bbcd-4221-a838-ede3e3fe50b8"

def test_map_notice_identique_a_map_eforms_hors_url():
    # sur 26-87868.xml : map_eforms(...) == [ {**l, "urlAvis": ...} for l in map_notice(...) ] (aux overrides près)
```

Dans `test_deao_parse.py`, ajouter : `idEforms` de 26-87868 vaut `"17114689-cb66-44e1-b420-be7e501a33b0"`, `acheteur_pays == "FRA"` pour une fixture legacy (26-92574) et une eForms.

- [ ] **Step 2: Vérifier l'échec**

Run: `uv run pytest tests/test_deao_eforms_common.py tests/test_deao_parse.py -q`
Expected: FAIL (modules ou colonnes absents).

- [ ] **Step 3: Implémenter** (déplacements, `child_fr`, `map_notice`, enveloppe BOAMP, colonnes du contrat)

- [ ] **Step 4: Vérifier**

Run: `uv run pytest tests -q -k "deao or boamp"`
Expected: PASS, assertions BOAMP existantes inchangées.

- [ ] **Step 5: Commit**

```bash
git commit -am "DEAO : mapping eForms commun aux sources, idEforms et acheteur_pays #165"
```

---

### Task 3: Accès TED (liste du jour, XML, résolution d'identifiant)

**Files:**

- Create: `src/tasks/deao/sources/joue/__init__.py`, `src/tasks/deao/sources/joue/fetch.py`
- Modify: `src/config.py` (section JOUE)
- Test: `tests/test_deao_joue_fetch.py`

**Interfaces:**

- Config : `TED_API_URL = "https://api.ted.europa.eu/v3"`, `TED_NOTICE_XML_URL = "https://ted.europa.eu/en/notice/{publication_number}/xml"`, `JOUE_AVIS_URL_TEMPLATE = "https://ted.europa.eu/fr/notice/-/detail/{publication_number}"`, `JOUE_COUNTRY_CODES = ("FRA", "ATF", "PYF", "NCL", "WLF", "SPM", "BLM", "MAF")` (env : liste séparée par des espaces), `JOUE_S3_PREFIX = "joue/raw"`, `JOUE_MAX_WORKERS = 4`.
- Produces :

  - `http_post(url: str, payload: dict) -> httpx.Response` et `http_get(url: str) -> httpx.Response` : seuls points d'accès réseau (monkeypatchés en test), avec nouvel essai tenacity sur 429, 5xx et `httpx.TransportError` (5 tentatives, attente exponentielle plafonnée à 60 s) ; autre statut non 200 → `httpx.HTTPStatusError`.
  - `query_jour(d: date) -> str` : `(buyer-country IN (<codes>) OR place-of-performance IN (<codes>)) AND publication-date=<AAAAMMJJ>`.
  - `list_day_files(d: date) -> tuple[None, list[str]]` : `POST {TED_API_URL}/notices/search` avec `{"query": query_jour(d), "fields": ["publication-number"], "limit": 250, "scope": "ALL", "paginationMode": "ITERATION"}` en suivant `iterationNextToken` ; renvoie `(None, ["<pn>.xml", ...])` trié, dédupliqué.
  - `download_xml(base: str | None, name: str) -> bytes` : `GET TED_NOTICE_XML_URL` avec `publication_number = name.removesuffix(".xml")`.
  - `numero_publication(notice_id: str) -> str | None` : recherche `notice-identifier=<id>`, champ `publication-number` ; `None` si aucun résultat.

- [ ] **Step 1: Tests**

```python
def test_query_jour():
    q = query_jour(date(2026, 10, 14))
    assert "buyer-country IN (FRA ATF PYF NCL WLF SPM BLM MAF)" in q
    assert "place-of-performance IN (FRA ATF PYF NCL WLF SPM BLM MAF)" in q
    assert q.endswith("publication-date=20261014")

def test_list_day_files_pagination(fake_ted):   # 2 pages : 250 puis 3 avis, puis page vide
    base, noms = list_day_files(date(2026, 10, 14))
    assert base is None and len(noms) == 253 and noms == sorted(set(noms))

def test_list_day_files_jour_vide(fake_ted):
    assert list_day_files(date(2026, 10, 18)) == (None, [])

def test_429_puis_succes(fake_ted, no_sleep):
    fake_ted.reponses_xml["706679-2026"] = [429, 200]
    assert download_xml(None, "706679-2026.xml").startswith(b"<?xml")

def test_numero_publication(fake_ted):
    assert numero_publication("c2dbf0bf-bd32-4b44-9a85-c27a0f0c3e9b") == "677195-2026"
    assert numero_publication("00000000-0000-0000-0000-000000000000") is None
```

Le faux TED (`fake_ted`, dans `tests/deao_fakes.py`) remplace `fetch.http_post` / `fetch.http_get` ; `no_sleep` neutralise l'attente tenacity.

- [ ] **Step 2: Vérifier l'échec** — `uv run pytest tests/test_deao_joue_fetch.py -q` → FAIL (module absent).
- [ ] **Step 3: Implémenter `fetch.py` et la config.**
- [ ] **Step 4: Vérifier** — `uv run pytest tests/test_deao_joue_fetch.py -q` → PASS.
- [ ] **Step 5: Commit** — `git commit -m "DEAO : accès TED pour la source JOUE (liste du jour, XML, identifiants) #165"`

---

### Task 4: Adaptateur JOUE (fixtures, classement, lignes avis-lot, `Source`)

**Files:**

- Create: `tests/data/deao/joue/*.xml` (fixtures réelles, voir step 1)
- Create: `src/tasks/deao/sources/joue/classify.py`, `src/tasks/deao/sources/joue/parse.py`, `src/tasks/deao/sources/joue/source.py`
- Test: `tests/test_deao_joue_parse.py`

**Interfaces:**

- Consumes: `map_notice`, `notice_id`, `changed_notice_id`, `change_reason` (tâche 2) ; `Classement` ; `Source` (tâche 1) ; `fetch` (tâche 3).
- Produces :

  - `classify_joue(notice) -> Classement` : sur `cbc:NoticeTypeCode/@listName` (`formatSource="eforms"`, `nature` = listName) :
    - `planning` → retenu, `initial`, `preInformation=True` ;
    - `competition` → retenu ; avec `changed_notice_id` : `annulation` si `change_reason == "cancel"`, sinon `rectificatif` (`preInformation=None`) ; sans : `initial`, `preInformation=False` ;
    - toute autre valeur (`result`, `dir-awa-pre`, `cont-modif`, `completion`, …) → exclu. Un `planning` avec `changed_notice_id` suit la règle du `competition` (rectificatif / annulation).
  - `parse_avis(xml: bytes, filename: str, compteur: Counter) -> list[dict]` : mêmes compteurs que le BOAMP (`lu`, `erreur_xml`, `exclu:<listName>`, `erreur_mapping`, `retenu`, `sans_date_publication`). Champs communs : `sourceDataset="joue"`, `idAvis` = `efbc:NoticePublicationID[@schemeName="ojs-notice-id"]` sans zéros de tête (repli : `filename` sans `.xml`), `avisPrecedent=None`, `typeAvis`, `preInformation`, `avisComplet=True`, `datePublicationAvis` = `efbc:PublicationDate[:10]`, `formatSource="eforms"`, `idEforms`, `idEformsPrecedent`, `urlAvis` (`JOUE_AVIS_URL_TEMPLATE`), `sourceFile` (`TED_NOTICE_XML_URL`).
  - `parse_day(files: dict[str, bytes]) -> tuple[pl.DataFrame, Counter]` (même contrat que le BOAMP).
  - `JOUE: Source` (`nom="joue"`, `s3_prefix=JOUE_S3_PREFIX`, `fetch.list_day_files`, `fetch.download_xml`, `parse_day`, `max_workers=JOUE_MAX_WORKERS`).

- [ ] **Step 1: Télécharger les fixtures**

```bash
mkdir -p tests/data/deao/joue
for pn in 677195-2026 706679-2026 599757-2026 603380-2026 631605-2026 709755-2026 700680-2026 708669-2026 711056-2026 680725-2026; do
  curl -sfL "https://ted.europa.eu/en/notice/$pn/xml" -o tests/data/deao/joue/$pn.xml
done
```

Rôle de chaque fixture : SEMINOR initial 677195 → rectificatif `cor-buy` 706679 ; SEVADEC initial 599757 → annulation `cancel` 603380 ; 631605 → 709755 = chaîne BOAMP 26-87868 → 26-99074 (recouvrement) ; 700680 attribution (sous-type 30, exclue) ; 708669 acheteur polonais, exécution FRK26, textes en polonais ; 711056 acheteur irlandais, sous-type E3, exécution FRZZZ ; 680725 pré-information simple (= BOAMP 26-94806).

- [ ] **Step 2: Tests**

```python
def test_classement():
    assert _cl("677195-2026") == ("initial", False, True)       # (typeAvis, preInformation, retenu)
    assert _cl("706679-2026") == ("rectificatif", None, True)
    assert _cl("603380-2026") == ("annulation", None, True)
    assert _cl("680725-2026") == ("initial", True, True)
    assert _cl("700680-2026")[2] is False

def test_parse_rectificatif_seminor():
    lignes = _parse("706679-2026")
    assert {l["idAvis"] for l in lignes} == {"706679-2026"}
    assert lignes[0]["idEformsPrecedent"] == "c2dbf0bf-bd32-4b44-9a85-c27a0f0c3e9b"
    assert lignes[0]["avisPrecedent"] is None and lignes[0]["sourceDataset"] == "joue"
    assert lignes[0]["urlAvis"] == "https://ted.europa.eu/fr/notice/-/detail/706679-2026"

def test_parse_acheteur_etranger():
    l = _parse("708669-2026")[0]
    assert l["acheteur_pays"] == "POL" and l["lieuExecution_code"] == departement_depuis_nuts("FRK26")

def test_parse_day_compteurs():
    df, c = parse_day({p.name: p.read_bytes() for p in JOUE.glob("*.xml")})
    assert c["exclu:result"] == 1 and c["retenu"] == 9 and c["erreur_mapping"] == 0
    assert df.schema == AVIS_LOT_SCHEMA
```

- [ ] **Step 3: Vérifier l'échec** — `uv run pytest tests/test_deao_joue_parse.py -q` → FAIL.
- [ ] **Step 4: Implémenter** `classify.py`, `parse.py`, `source.py`.
- [ ] **Step 5: Vérifier** — `uv run pytest tests/test_deao_joue_parse.py -q` → PASS.
- [ ] **Step 6: Commit** — `git add tests/data/deao/joue src/tasks/deao/sources/joue tests/test_deao_joue_parse.py && git commit -m "DEAO : adaptateur JOUE (classement, lignes avis-lot, fixtures) #165"`

---

### Task 5: Résolution des identifiants JOUE (table interne, cache S3, API)

**Files:**

- Create: `src/tasks/deao/sources/joue/identifiants.py`
- Test: `tests/test_deao_joue_identifiants.py`

**Interfaces:**

- Consumes: `fetch.numero_publication` (tâche 3).
- Produces :

  - `IDENTIFIANTS_SCHEMA = {"idEforms": String, "idAvis": String}` ; cache S3 `f"{DEAO_GEOCODE_S3_PREFIX}/joue_identifiants.parquet"`, lu comme `geocode_cache.charger_cache` (clé absente → vide, autre erreur remonte), écrit via `publish_to_s3`. Seules les résolutions réussies y entrent.
  - `charger_cache_identifiants() -> pl.DataFrame`, `resoudre_par_api(ids: list[str]) -> dict[str, str]` (appelle `numero_publication`, ignore les `None`, une erreur HTTP sur un id → id non résolu, pas d'exception).
  - `resoudre_avis_precedents(avis: pl.DataFrame, stats: Counter, resolver: Callable[[list[str]], dict[str, str]] = resoudre_avec_cache) -> pl.DataFrame` : pour les lignes `sourceDataset == "joue"` et `idEformsPrecedent` non nul, remplit `avisPrecedent` : d'abord par la table `idEforms → idAvis` des lignes JOUE de `avis`, puis par `resolver` pour les ids restants. Compteurs : `joue:identifiant_interne`, `joue:identifiant_resolu` (cache ou API), `joue:identifiant_non_resolu` (en nombre d'ids distincts). Les lignes non JOUE sont inchangées ; le nombre de lignes est inchangé.
  - `resoudre_avec_cache(ids: list[str]) -> dict[str, str]` : cache d'abord, API pour le reste, réécriture du cache s'il a grandi.

- [ ] **Step 1: Tests**

```python
def test_resolution_interne():       # partitions contenant 677195 et 706679
    out = resoudre_avis_precedents(avis, stats, resolver=lambda ids: pytest.fail("pas d'API"))
    assert _prec(out, "706679-2026") == "677195-2026" and stats["joue:identifiant_interne"] == 1

def test_resolution_externe():       # 706679 seul
    out = resoudre_avis_precedents(avis, stats, resolver=lambda ids: {"c2dbf0bf-bd32-4b44-9a85-c27a0f0c3e9b": "677195-2026"})
    assert _prec(out, "706679-2026") == "677195-2026"

def test_echec_racine_elle_meme():
    out = resoudre_avis_precedents(avis, stats, resolver=lambda ids: {})
    assert _prec(out, "706679-2026") is None and stats["joue:identifiant_non_resolu"] == 1

def test_cache_ne_garde_que_les_succes(fake_s3, fake_ted):
    resoudre_avec_cache(["c2dbf0bf-bd32-4b44-9a85-c27a0f0c3e9b", "00000000-0000-0000-0000-000000000000"])
    assert charger_cache_identifiants().height == 1
    fake_ted.appels.clear()
    resoudre_avec_cache(["c2dbf0bf-bd32-4b44-9a85-c27a0f0c3e9b"])
    assert fake_ted.appels == []

def test_api_en_erreur_non_cachee(fake_s3, fake_ted):   # 500 persistant
    assert resoudre_avec_cache(["c2dbf0bf-bd32-4b44-9a85-c27a0f0c3e9b"]) == {}
    assert charger_cache_identifiants().height == 0
```

- [ ] **Step 2: Vérifier l'échec** — `uv run pytest tests/test_deao_joue_identifiants.py -q` → FAIL.
- [ ] **Step 3: Implémenter.**
- [ ] **Step 4: Vérifier** — PASS.
- [ ] **Step 5: Commit** — `git commit -m "DEAO : résolution des identifiants eForms JOUE (cache S3, API TED) #165"`

---

### Task 6: Recouvrement BOAMP/JOUE dans la consolidation, garde-fou

**Files:**

- Modify: `src/tasks/deao/consolidate.py`
- Test: `tests/test_deao_consolidate.py`

**Interfaces:**

- Produces :

  - `class RecouvrementIncoherent(RuntimeError)`.
  - `ecarter_recouvrement(avis: pl.DataFrame, stats: Counter) -> pl.DataFrame` : `avis` porte déjà `idAvisInitial`. Ensemble `E` = `idEforms` non nuls des lignes `boamp`. Une chaîne `joue` (clé `CHAIN_KEYS`) est écartée si l'un de ses avis a `idEforms ∈ E` ou `idEformsPrecedent ∈ E`. Compteurs : `joue:chaine_recouvrement`, `joue:chaine_conservee` (chaînes distinctes). Lève `RecouvrementIncoherent` si des lignes `boamp` ont `formatSource == "eforms"` et que (a) `E` est vide, ou (b) des lignes `joue` existent et aucune chaîne n'est écartée.
  - `consolidate(...)` appelle `ecarter_recouvrement` juste après la jointure avec `resolve_roots`, avant `_chain_aggregates`.

- [ ] **Step 1: Tests** (lignes avis-lot construites avec `conform_avis`)

```python
def test_chaine_joue_recoupant_boamp_ecartee():
    # boamp 26-87868 (idEforms=A) + joue 631605 (idEforms=A) → 709755 (idEforms=B, idEformsPrecedent=A)
    c = consolidate(avis, date(2026, 11, 12), 90)
    assert not c.df.filter(pl.col("sourceDataset") == "joue").height
    assert c.stats["joue:chaine_recouvrement"] == 1

def test_chaine_joue_seule_conservee():
    uids = consolidate(avis_seminor, d, 90).df["uid"].to_list()   # 677195 → 706679
    assert uids and all(u.startswith("joue_677195-2026_") for u in uids)

def test_garde_fou_partitions_boamp_anciennes():
    # boamp eForms avec idEforms nul partout + joue
    with pytest.raises(RecouvrementIncoherent):
        consolidate(avis, d, 90)

def test_garde_fou_aucun_recouvrement():
    with pytest.raises(RecouvrementIncoherent):
        consolidate(avis_boamp_eforms_et_joue_disjoints, d, 90)

def test_sans_joue_pas_de_garde_fou():
    consolidate(avis_boamp_seul, d, 90)   # ne lève pas
```

- [ ] **Step 2: Vérifier l'échec** — `uv run pytest tests/test_deao_consolidate.py -q` → FAIL.
- [ ] **Step 3: Implémenter.**
- [ ] **Step 4: Vérifier** — `uv run pytest tests/test_deao_consolidate.py -q` → PASS.
- [ ] **Step 5: Commit** — `git commit -m "DEAO : chaînes JOUE recoupant le BOAMP écartées, garde-fou #165"`

---

### Task 7: Acheteurs étrangers dans l'enrichissement

**Files:**

- Modify: `src/tasks/deao/enrich.py`
- Test: `tests/test_deao_enrich.py`

**Interfaces:**

- Produces : `neutraliser_acheteurs_etrangers(df: pl.DataFrame) -> pl.DataFrame` : pour les lignes dont `acheteur_pays` est non nul et hors `JOUE_COUNTRY_CODES`, met à nul `acheteur_id`, `acheteur_adresse_voie`, `acheteur_adresse_cp`, `acheteur_adresse_ville`, `acheteur_adresse_nuts`. Appelée en premier dans `enrich_acheteurs`. `acheteur_pays` nul (anciennes partitions) = français.

- [ ] **Step 1: Tests**

```python
def test_acheteur_etranger_sans_geo():
    # ligne POL, cp "31-000", nom identique à une ligne FRA avec SIRET et cp "31000"
    out = enrich_acheteurs(df, _lf_ul(), _lf_etab(), COG, geocoder=geocoder_espion)
    pol = out.filter(pl.col("acheteur_pays") == "POL").row(0, named=True)
    assert pol["acheteur_id"] is None and pol["acheteur_id_source"] is None
    assert pol["acheteur_departement_code"] is None
    assert all("31-000" not in q for q in geocoder_espion.postcodes)
```

- [ ] **Step 2: Vérifier l'échec** — `uv run pytest tests/test_deao_enrich.py -q` → FAIL.
- [ ] **Step 3: Implémenter.**
- [ ] **Step 4: Vérifier** — PASS.
- [ ] **Step 5: Commit** — `git commit -m "DEAO : pas de SIRET ni de géographie déduits pour les acheteurs étrangers #165"`

---

### Task 8: Flow, `DEAO_SOURCES`, rapport, schéma publié, bout en bout

**Files:**

- Modify: `src/config.py` (`DEAO_SOURCES`), `src/flows/deao_processing.py`, `src/tasks/deao/report.py`, `src/tasks/deao/schema.py` (descriptions), `template.env`
- Test: `tests/test_deao_flow.py`, `tests/test_deao_report.py`, `tests/test_deao_schema.py`

**Interfaces:**

- Consumes: `acquire`, `BOAMP`, `JOUE`, `resoudre_avis_precedents`, `RecouvrementIncoherent`.
- Produces :

  - `DEAO_SOURCES: tuple[str, ...]` (env `DEAO_SOURCES`, défaut `"boamp,joue"`, séparateur virgule) ; `SOURCES = {"boamp": BOAMP, "joue": JOUE}` dans le flow ; nom inconnu → `ValueError` en tête de flow.
  - Flow : pour chaque source de `DEAO_SOURCES`, `acquire(...)` ; compteurs BOAMP sans préfixe (compatibilité), compteurs des autres sources préfixés `"<nom>:"`. `lire_partitions(sources: tuple[str, ...])` ne lit que `DEAO_PARTITIONS_DIR / <nom>`. Puis `resoudre_avis_precedents`, `consolidate`, enrichissement, écriture, publication inchangés. Fin de flow : échec si `sum(stats[k] for k in stats if k == "jour_en_erreur" or k.endswith(":jour_en_erreur")) > 0`, message listant les sources.
  - `report.py` : une clé est publiée si, après retrait d'un éventuel préfixe `"<source>:"`, elle est dans `COMPTEURS` ou commence par `_PREFIXES_COMPTEURS`. `COMPTEURS` gagne `chaine_recouvrement`, `chaine_conservee`, `identifiant_interne`, `identifiant_resolu`, `identifiant_non_resolu`. Nouvelle répartition `sourceDataset:<valeur>`. Taux de remplissage par `sourceDataset` + `formatSource` (`"boamp/eforms"`, `"boamp/legacy"`, `"joue/eforms"`).
  - `schema.py` : descriptions de `uid`, `urlAvis`, `sourceFile`, `idAvisInitial`, `idDernierAvis`, `avisLies`, `sourceDataset` mentionnent les deux sources (`boamp` : IDWEB et boamp.fr ; `joue` : numéro de publication et ted.europa.eu).

- [ ] **Step 1: Tests**

`test_deao_flow.py` : `flow_env` fixe `DEAO_SOURCES = ("boamp",)` pour que les tests existants restent sans réseau TED. Les nouveaux tests utilisent un faux TED (fixture `fake_ted` de la tâche 3) qui sert les fixtures `tests/data/deao/joue/` par date de publication (`efbc:PublicationDate`) et une liste vide les autres jours, avec `DEAO_SOURCES = ("boamp", "joue")`, `BOAMP_START_DATE = "2026-09-01"` et `run_date` = lendemain de la date de publication la plus récente des fixtures JOUE (aucun avis futur).

```python
def test_flow_deux_sources(flow_env, fake_ted):
    flow_module.deao_processing(run_date=RUN_DATE)
    df = pl.read_parquet(flow_env.dist / "deao.parquet")
    assert df["uid"].is_unique().all()
    assert set(df["sourceDataset"]) == {"boamp", "joue"}
    joue = set(df.filter(pl.col("sourceDataset") == "joue")["idAvisInitial"])
    assert {"677195-2026", "599757-2026", "631605-2026"} & joue == {"677195-2026", "599757-2026"}
    assert df.filter(pl.col("idAvisInitial") == "599757-2026")["statut"].unique().to_list() == ["annule"]

def test_flow_sources_boamp_seul(flow_env, fake_ted, monkeypatch):
    flow_module.deao_processing(run_date=RUN_DATE)                   # remplit les partitions joue
    monkeypatch.setattr(flow_module, "DEAO_SOURCES", ("boamp",))
    flow_module.deao_processing(run_date=RUN_DATE)
    assert set(pl.read_parquet(flow_env.dist / "deao.parquet")["sourceDataset"]) == {"boamp"}

def test_flow_jour_joue_en_erreur_fait_echouer(flow_env, fake_ted):
    fake_ted.recherche_en_erreur = {date(2026, 10, 14)}              # 500 persistant
    with pytest.raises(RuntimeError, match="joue"):
        flow_module.deao_processing(run_date=RUN_DATE)
```

`test_deao_report.py` : `lignes_rapport(Counter({"joue:retenu": 3, "joue:exclu:result": 1}), df)` contient `{"indicateur": "joue:retenu", "valeur": 3}` et `{"indicateur": "joue:exclu:result", ...}`.

- [ ] **Step 2: Vérifier l'échec** — `uv run pytest tests/test_deao_flow.py tests/test_deao_report.py -q` → FAIL.
- [ ] **Step 3: Implémenter.**
- [ ] **Step 4: Vérifier toute la suite DEAO** — `uv run pytest tests -q -k "deao or boamp"` → PASS ; `pre-commit run --all-files` → Passed.
- [ ] **Step 5: Commit** — `git commit -am "DEAO : source JOUE branchée dans le flow, DEAO_SOURCES, rapport par source #165"`

---

## Après le plan (hors code, à faire par l'utilisateur)

Repris de la section « Mise en service » de la spec, à ajouter à la mémoire « Prérequis déploiement DEAO » :

1. Run **dev** (ou local avec les identifiants S3) avec `DEAO_SOURCES=boamp,joue` pour remplir l'archive `joue/raw/` (~250 000 XML) ; vérifier le rapport (`joue:jour_en_erreur`, `joue:chaine_recouvrement` ≈ 79 % des chaînes JOUE d'appels à la concurrence).
2. Premier run **prod** avec `DEAO_REBUILD=true` : partitions BOAMP reconstruites avec `idEforms`, partitions JOUE lues depuis S3.
3. `.env` local et de prod : `DEAO_S3_PREFIX` remplacé par `BOAMP_S3_PREFIX` s'il y était défini.
