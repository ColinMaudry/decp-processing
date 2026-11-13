# DEAO — scrapers Atexo (Mégalis Bretagne, Maximilien) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Ajouter à `scrap_closed` deux scrapers de profils d'acheteur Atexo (`megalis_bretagne`, `maximilien`) qui accumulent les consultations en cours, archivent les DCE dans S3 (accès public), retrouvent le SIRET des acheteurs, et faire dédoublonner `deao_processing` entre BOAMP et plateformes.

**Architecture:** Un client Atexo générique (`src/tasks/scrap/deao/atexo/`) rejoue les formulaires PRADO en httpx, parse le HTML en lxml, normalise au schéma DEAO, enrichit les acheteurs (dictionnaire SIRET appris sur `boamp.parquet` publié) et fusionne avec le fichier publié précédent. `deao_processing` gagne une règle de fusion BOAMP ↔ plateformes, une tolérance de transition de schéma, un dataset a restreint aux champs Open Data et l'isolement des sources fermées en échec.

**Tech Stack:** Python 3.13, polars, httpx, lxml, tenacity, boto3, Prefect 3, pytest (`uv run pytest`).

**Spec:** `docs/superpowers/specs/2026-11-13-deao-scraper-megalis-bretagne-design.md`

## Global Constraints

- Imports absolus depuis la racine du paquet : `from src.tasks.X import Y` (vérifié par `script/check_imports.py` en pre-commit).
- Tests : toujours `uv run pytest …`. Suite complète de référence : `uv run pytest -q --ignore=tests/test_main.py`.
- Code, noms, logs et messages en français, comme le reste de DEAO. Ne pas passer de logs existants en DEBUG.
- `config.*` lu à l'appel dans les flows (les tests le substituent) : suivre le motif existant (`from src import config` puis `config.X`).
- Commits locaux sur `dev`, sans numéro d'issue, terminés par la ligne `Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>`. **Aucun push.**
- `pre-commit` (ruff, ruff-format, prettier) doit passer ; s'il reformate, re-`git add` et recommiter.
- Clé S3 des DCE : `{DEAO_DCE_S3_PREFIX}/{code_plateforme}/{id_dce ou refCons}{extension}`, préfixe par défaut `dce`, même bucket `S3_BUCKET`.
- `uid` = `{sourceDataset}_{id}_{lotId}` ; `lotId` = `'0'` pour une consultation non allotie ; pour une plateforme `id` = `refCons`, `idDernierAvis` nul.
- Ressources data.gouv.fr (dataset b) : `megalis_bretagne` = `c5a67fe9-fe02-4c9e-b11a-ec6162ec2c76` (min_rows 300), `maximilien` = `e9a13bd3-b3d1-43e8-a79b-8e74e75e50d5` (min_rows 150).
- Bases URL : `https://marches.megalis.bretagne.bzh`, `https://marches.maximilien.fr`.
- En cas de doute, ne pas fusionner (dédoublonnage) et ne pas marquer `annule` (accumulation).
- Archivage S3 des DCE seulement si `publication_autorisee()` (désactivé en dev et en SOLO).

## Review Focus

1. Recherche paginée interrompue ou nombre de résultats incohérent → aucune consultation marquée `annule` à tort (test : Task 8, `test_recherche_incomplete_aucune_annulation`).
2. Fichier précédent de la plateforme indisponible sur data.gouv.fr → le scraper échoue au lieu de repartir de zéro et d'écraser l'historique, sauf `DEAO_SCRAP_PARTIR_DE_ZERO=true` (test : Task 9, `test_precedent_indisponible_echoue`).
3. Page détail ou DCE en échec pour une consultation → les autres sont publiées ; une consultation connue garde ses lignes, une nouvelle est retentée au passage suivant ; `urlDce` reste nul et le DCE est retenté (tests : Task 9, `test_detail_en_echec_*`, `test_dce_en_echec_retente`).
4. Dates françaises abrégées (« 15 Juil. 2026 », « 27 Août 2026 », « 10 Sept. 2026 », « 25 Janv. 2027 », « 31 Déc. 2027 ») → parsées sans erreur (test : Task 5, `test_date_fr` paramétré).
5. DCE volumineux (946 Mo observé) → écrit en streaming sur disque, jamais chargé en mémoire, fichier temporaire supprimé après dépôt (tests : Task 6, `test_dce_en_streaming` ; Task 7, `test_depot_supprime_rien_si_echec` ; Task 9 vérifie la suppression).

---

### Task 1: Schéma — `id`, `acheteur_codePostal`, champs propres au dataset b

**Files:**

- Modify: `reference/schema_deao.json`
- Modify: `src/tasks/deao/schema.py`
- Modify: `src/tasks/deao/consolidate.py`
- Modify: `src/tasks/deao/enrich.py:257-273` (`enrich_acheteurs`)
- Modify: tests qui citent `idAvisInitial` : `tests/test_deao_consolidate.py`, `tests/test_deao_schema.py`, `tests/test_deao_scrap_open.py`
- Test: `tests/test_deao_schema.py`, `tests/test_deao_enrich.py`

**Interfaces:**

- Produces: `Field.closed_only: bool` (défaut `False`, lu de la clé JSON `"closed_only"`) ; `open_schema() -> dict[str, pl.DataType]` (champs publiés sans `closed_only`) ; `write_table_schema(path)` écrit **uniquement** les champs Open Data ; colonne publiée `id` (ex-`idAvisInitial`) ; colonne publiée `acheteur_codePostal` (String) ; colonnes publiées `urlDce`, `urlReglementConsultation` (String, `closed_only: true`) ; `consolidate.CHAIN_KEYS == ["sourceDataset", "id"]`.

- [ ] **Step 1: Write the failing tests** (dans `tests/test_deao_schema.py` et `tests/test_deao_enrich.py`)

```python
def test_id_remplace_id_avis_initial():
    schema = published_schema()
    assert "id" in schema and "idAvisInitial" not in schema
    champ = next(f for f in PUBLISHED_FIELDS if f.name == "id")
    assert champ.title == "Identifiant appel d'offres"
    assert champ.short_title == "Identifiant AO"

def test_champs_propres_au_dataset_b():
    assert {"urlDce", "urlReglementConsultation", "acheteur_codePostal"} <= set(published_schema())
    ouvert = open_schema()
    assert "urlDce" not in ouvert and "urlReglementConsultation" not in ouvert
    assert "acheteur_codePostal" in ouvert

def test_table_schema_sans_champs_fermes(tmp_path):
    noms = [f["name"] for f in json.loads(write_table_schema(tmp_path / "s.json").read_text())["fields"]]
    assert "urlDce" not in noms and "id" in noms

def test_statut_decrit_annulation_plateforme():
    statut = next(f for f in PUBLISHED_FIELDS if f.name == "statut")
    assert "plateforme" in statut.description

# tests/test_deao_enrich.py
def test_code_postal_publie_et_neutralise_a_l_etranger(...):
    # deux lignes : une FR (acheteur_adresse_cp "35000"), une étrangère (acheteur_pays "DEU", cp "10115")
    # après enrich_acheteurs : acheteur_codePostal == ["35000", None]
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest tests/test_deao_schema.py tests/test_deao_enrich.py -q`
Expected: FAIL (`id` absent, `open_schema` non défini).

- [ ] **Step 3: Modifier `reference/schema_deao.json`**

- `idAvisInitial` → `name: "id"`, `title: "Identifiant appel d'offres"`, `short_title: "Identifiant AO"`, `description` : le texte de la spec (§ Renommage `idAvisInitial` → `id`), mot pour mot.
- Mettre à jour les descriptions de `uid` (`<sourceDataset>_<id>_<lotId>`, sources boamp, joue ou une plateforme), `idDernierAvis` (nul pour une plateforme), `sourceDataset` (boamp, joue, megalis_bretagne, maximilien), `statut` (ajouter : « pour une plateforme de profil d'acheteur, 'annule' si la consultation a disparu de la plateforme avant sa date limite »), `acheteur_id_source` inchangé.
- Ajouter après `acheteur_nom` : `acheteur_codePostal` (string, title « Code postal de l'acheteur », description « Code postal de l'adresse de l'acheteur indiquée dans l'avis ou sur la plateforme ; nul pour un acheteur étranger. »).
- Ajouter après `urlConsultation` : `urlDce` (string, title « URL du DCE archivé », `"closed_only": true`, description « Adresse de téléchargement public du dossier de consultation des entreprises archivé (dernière version connue), pour les consultations des plateformes de profil d'acheteur. ») et `urlReglementConsultation` (string, title « URL du règlement de consultation », `"closed_only": true`, description « Adresse du règlement de consultation sur la plateforme de profil d'acheteur. »).

- [ ] **Step 4: Implémenter `schema.py`, `consolidate.py`, `enrich.py`**

- `schema.py` : `Field.closed_only`, `open_schema()`, `write_table_schema` filtre `closed_only`.
- `consolidate.py` : renommer partout `idAvisInitial` → `id` (colonne interne et `CHAIN_KEYS`), valeurs inchangées. Ajouter `"acheteur_codePostal"` à `_ENRICHMENT_PREFIXES` (calculé à l'enrichissement).
- `enrich.py` : à la fin de `enrich_acheteurs`, `acheteur_codePostal = acheteur_adresse_cp` (après `neutraliser_acheteurs_etrangers`, donc nul pour un acheteur étranger).
- Remplacer `idAvisInitial` par `id` dans les tests existants.

- [ ] **Step 5: Run tests**

Run: `uv run pytest -q --ignore=tests/test_main.py`
Expected: PASS (toute la suite).

- [ ] **Step 6: Commit**

```bash
git add reference/schema_deao.json src/tasks/deao/schema.py src/tasks/deao/consolidate.py src/tasks/deao/enrich.py tests/
git commit -m "DEAO : schéma — idAvisInitial devient id, acheteur_codePostal, urlDce et urlReglementConsultation (dataset b)"
```

---

### Task 2: `deao_processing` — transition de schéma, dataset a Open Data, sources fermées non bloquantes

**Files:**

- Modify: `src/flows/deao_processing.py`
- Test: `tests/test_deao_processing.py`

**Interfaces:**

- Consumes: `open_schema()`, `published_schema()` (Task 1).
- Produces: `COLONNES_TRANSITOIRES = ("acheteur_codePostal", "urlDce", "urlReglementConsultation")` ; `_verifier_fichier(code, chemin, min_rows) -> pl.DataFrame` qui renomme `idAvisInitial` → `id` et complète par des nuls les `COLONNES_TRANSITOIRES` manquantes (toute autre différence reste une erreur) ; une source `open_data: false` en échec (téléchargement, schéma, minimum, `sourceDataset`) est écartée, journalisée en `logger.error` et signalée dans le rapport par l'indicateur `source_ecartee:{code}` = 1, sans lever ; une source Open Data en échec garde le comportement actuel (exception avant toute publication).

- [ ] **Step 1: Write the failing tests**

```python
def test_ancien_fichier_accepte_et_renomme(fake_datagouv, env):
    # fichier joue publié avant le déploiement : idAvisInitial au lieu de id, sans les 3 nouvelles colonnes
    df = pl.read_parquet(io.BytesIO(fake_datagouv.fichiers[_rid(env, "joue")]))
    df = df.rename({"id": "idAvisInitial"}).drop("acheteur_codePostal", "urlDce", "urlReglementConsultation")
    ... (réécrire dans fake_datagouv.fichiers)
    deao_processing()
    closed = pl.read_parquet(env.dist / "closed" / "deao.parquet")
    assert dict(closed.schema) == published_schema()

def test_dataset_a_sans_champs_fermes(fake_datagouv, env):
    deao_processing()
    ouvert = pl.read_parquet(env.dist / "deao.parquet")
    assert "urlDce" not in ouvert.columns and "id" in ouvert.columns

def test_source_fermee_absente_non_bloquante(fake_datagouv, env):
    _registre(env, joue={"open_data": False})
    del fake_datagouv.fichiers[_rid(env, "joue")]
    deao_processing()
    assert (OPEN, PARQUET_A) in {(d, r) for d, r, _ in fake_datagouv.publies}
    closed = pl.read_parquet(env.dist / "closed" / "deao.parquet")
    assert set(closed["sourceDataset"]) == {"boamp"}

def test_source_fermee_sous_minimum_non_bloquante(fake_datagouv, env):
    _registre(env, joue={"open_data": False, "min_rows": 99})
    deao_processing()
    assert ("ds-b", PARQUET_B) in {(d, r) for d, r, _ in fake_datagouv.publies}
```

Les tests existants `test_source_absente_rien_publie`, `test_source_sous_minimum_rien_publie`, `test_schema_non_conforme_rien_publie` (source Open Data) restent verts tels quels.

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest tests/test_deao_processing.py -q`
Expected: FAIL sur les 4 nouveaux tests.

- [ ] **Step 3: Implémenter dans `src/flows/deao_processing.py`**

Lecture : `pl.read_parquet` puis renommage/complétion **avant** la comparaison stricte des schémas. Dataset a : `df_open.select(list(open_schema()))` avant `sink_to_files`. Rapport : les lignes `source_ecartee:*` s'ajoutent à `lignes_rapport_sources(df)` dans l'appel à `publier_rapport`.

- [ ] **Step 4: Run tests**

Run: `uv run pytest tests/test_deao_processing.py tests/test_deao_scrap_open.py tests/test_deao_scrap_closed.py -q`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add src/flows/deao_processing.py tests/test_deao_processing.py
git commit -m "DEAO : deao_processing tolère l'ancien schéma, publie l'Open Data sans champs fermés, écarte une source fermée en échec"
```

---

### Task 3: Registre (`url_base`) et dédoublonnage BOAMP ↔ plateformes

**Files:**

- Modify: `src/config.py` (`SourceDeao`, `charger_registre_deao`)
- Modify: `reference/deao_sources.yaml`
- Create: `src/tasks/deao/dedoublonnage.py`
- Modify: `src/flows/deao_processing.py` (`dedoublonner_sources` déplacé et appelé avec le registre)
- Test: `tests/test_deao_registre.py`, `tests/test_deao_dedoublonnage.py`

**Interfaces:**

- Produces: `SourceDeao.url_base: str | None = None` (clé YAML `url_base`, doit commencer par `https://` sinon `ValueError` au chargement) ; `dedoublonner_sources(df: pl.DataFrame, registre: tuple[SourceDeao, ...]) -> tuple[pl.DataFrame, Counter]` dans `src/tasks/deao/dedoublonnage.py` ; `id_consultation_expr(url_base: str) -> pl.Expr` (identifiant de consultation extrait de `urlConsultation` si l'hôte est celui de `url_base`, par `(?:[?&]id=|/consultation/)(\d+)`, sinon nul). Compteurs : `fusion_exacte:{code}`, `fusion_stricte:{code}`, `seule:{code}`, `dedoublonnage_retire`.

Règle (spec, § Dédoublonnage) — à l'échelle de la **consultation** (couple `sourceDataset`, `id`) :

1. Exacte : une consultation `boamp` dont `id_consultation_expr(url_base)` vaut l'`id` d'une consultation plateforme (même plateforme). Plusieurs chaînes BOAMP peuvent pointer vers la même consultation : toutes sont complétées.
2. Stricte, parmi les consultations non appariées : `normaliser_nom_expr("referenceAcheteur")` égal, `acheteur_id` égal et non nul, `dateLimiteReponse.dt.date()` égale, **et** une seule consultation candidate de chaque côté pour cette clé.
3. Fusion : sur les lignes BOAMP appariées, `urlConsultation` ← celle de la plateforme ; `urlDce`, `urlReglementConsultation`, `acheteur_codePostal`, `referenceAcheteur` complétés s'ils sont nuls (valeur de la consultation plateforme, identique pour tous ses lots). Les lignes plateforme appariées sont retirées. `sourceDataset`, `uid`, `id` restent ceux du BOAMP.

- [ ] **Step 1: Write the failing tests**

```python
# tests/test_deao_registre.py
def test_url_base_lue(tmp_path): ...  # entrée avec url_base: https://exemple.fr → SourceDeao.url_base == "https://exemple.fr"
def test_url_base_non_https_refusee(tmp_path): ...  # url_base: http://exemple.fr → ValueError

# tests/test_deao_dedoublonnage.py — registre : boamp (open), megalis_bretagne (fermé, url_base https://marches.megalis.bretagne.bzh)
def test_fusion_exacte_par_url():
    # boamp id "26-1", 2 lots, urlConsultation ".../?page=Entreprise.EntrepriseAdvancedSearch&AllCons&id=242752&orgAcronyme=a4h", urlDce nul
    # megalis_bretagne id "242752", lotId "0", urlDce "https://s3/x.zip"
    df, stats = dedoublonner_sources(df, registre)
    assert set(df["sourceDataset"]) == {"boamp"} and df.height == 2
    assert df["urlDce"].to_list() == ["https://s3/x.zip"] * 2
    assert df["urlConsultation"][0] == "https://marches.megalis.bretagne.bzh/entreprise/consultation/242752?orgAcronyme=a4h"
    assert stats["fusion_exacte:megalis_bretagne"] == 1 and stats["dedoublonnage_retire"] == 1

def test_url_autre_hote_pas_de_fusion(): ...  # même id dans une URL maximilien → 2 consultations conservées
def test_fusion_stricte(): ...  # url racine, même ref "2026-21", même SIRET, même jour limite → fusion, fusion_stricte == 1
def test_stricte_ambigue_pas_de_fusion(): ...  # deux consultations BOAMP candidates → rien fusionné, seule:megalis_bretagne == 1
def test_stricte_siret_nul_pas_de_fusion(): ...
def test_valeur_boamp_non_ecrasee(): ...  # boamp referenceAcheteur "A", plateforme "B" (fusion exacte) → reste "A"
def test_sans_plateforme_identite(): ...  # df boamp + joue seulement → inchangé, dedoublonnage_retire == 0
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest tests/test_deao_registre.py tests/test_deao_dedoublonnage.py -q`
Expected: FAIL (`url_base`, module absents).

- [ ] **Step 3: Implémenter** `SourceDeao.url_base`, le module `dedoublonnage.py`, l'appel `dedoublonner_sources(pl.concat(...), config.DEAO_REGISTRE)` dans `deao_processing`, et les compteurs ajoutés au rapport (`{"indicateur": k, "valeur": v}`). Ajouter au registre :

```yaml
- code: megalis_bretagne
  name: Mégalis Bretagne (profil d'acheteur Atexo)
  open_data: false
  resource_id: c5a67fe9-fe02-4c9e-b11a-ec6162ec2c76
  min_rows: 300
  url_base: https://marches.megalis.bretagne.bzh
- code: maximilien
  name: Maximilien (profil d'acheteur Atexo, Île-de-France)
  open_data: false
  resource_id: e9a13bd3-b3d1-43e8-a79b-8e74e75e50d5
  min_rows: 150
  url_base: https://marches.maximilien.fr
```

`tests/data/deao_sources_test.yaml` ne change pas (les tests de flow supposent `{boamp, joue}`).

- [ ] **Step 4: Run tests**

Run: `uv run pytest -q --ignore=tests/test_main.py`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add src/config.py reference/deao_sources.yaml src/tasks/deao/dedoublonnage.py src/flows/deao_processing.py tests/
git commit -m "DEAO : registre des plateformes Atexo et dédoublonnage BOAMP ↔ plateformes"
```

---

### Task 4: Enrichissement réutilisable (dictionnaire SIRET externe, référentiels partagés)

**Files:**

- Modify: `src/tasks/deao/enrich.py`
- Create: `src/tasks/deao/referentiels.py`
- Modify: `src/flows/scrap_open.py` (délègue à `referentiels.py`, comportement inchangé)
- Test: `tests/test_deao_enrich.py`

**Interfaces:**

- Produces:

  - `dictionnaire_siret(df: pl.DataFrame) -> pl.DataFrame` : colonnes `_nom_norm`, `acheteur_adresse_cp`, `_siret_dico` ; appris sur les lignes à `acheteur_id`, nom et code postal non nuls, clé à SIRET unique (logique actuelle de `appliquer_dictionnaire_siret`, extraite).
  - `appliquer_dictionnaire_siret(df, dictionnaire: pl.DataFrame | None = None) -> pl.DataFrame` (sans dictionnaire : appris sur `df`, comportement actuel).
  - `dictionnaire_depuis_publie(df_publie: pl.DataFrame) -> pl.DataFrame` : à partir d'un fichier publié (`acheteur_nom`, `acheteur_codePostal`, `acheteur_id`) ; dictionnaire vide (bon schéma) si `acheteur_codePostal` est absent.
  - `enrich_acheteurs(df, lf_unites_legales, lf_etablissements, df_cog, geocoder, dictionnaire: pl.DataFrame | None = None) -> pl.DataFrame`.
  - `referentiels.charger_referentiels(sirene_dir: Path, data_dir: Path) -> tuple[pl.LazyFrame, pl.LazyFrame, pl.DataFrame]` et `referentiels.geocoder_tolerant(today: date, stats: Counter) -> Callable[[pl.DataFrame], pl.DataFrame]` (code actuel de `scrap_open`, paramétré). `scrap_open.charger_referentiels()` et `scrap_open._geocoder_tolerant` restent des noms du module `scrap_open` (les tests existants les substituent) et appellent ces fonctions.

- [ ] **Step 1: Write the failing tests**

```python
def test_dictionnaire_externe_applique():
    publie = pl.DataFrame({"acheteur_nom": ["Rennes Métropole"], "acheteur_codePostal": ["35000"], "acheteur_id": ["24350013900189"]})
    lignes = pl.DataFrame({"acheteur_nom": ["RENNES METROPOLE"], "acheteur_adresse_cp": ["35000"], "acheteur_id": [None]}, schema_overrides={"acheteur_id": pl.String})
    out = appliquer_dictionnaire_siret(lignes, dictionnaire_depuis_publie(publie))
    assert out["acheteur_id"].to_list() == ["24350013900189"]
    assert out["acheteur_id_source"].to_list() == ["dictionnaire"]

def test_dictionnaire_ambigu_ignore(): ...  # même nom+CP → deux SIRET différents dans le publié → acheteur_id reste nul
def test_dictionnaire_depuis_publie_sans_code_postal(): ...  # colonne absente → 0 ligne, colonnes _nom_norm/acheteur_adresse_cp/_siret_dico
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest tests/test_deao_enrich.py -q`
Expected: FAIL (fonctions absentes).

- [ ] **Step 3: Implémenter** (extraction sans changement de comportement pour `scrap_open`).

- [ ] **Step 4: Run tests**

Run: `uv run pytest -q --ignore=tests/test_main.py`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add src/tasks/deao/enrich.py src/tasks/deao/referentiels.py src/flows/scrap_open.py tests/test_deao_enrich.py
git commit -m "DEAO : dictionnaire SIRET applicable depuis un fichier publié, référentiels partagés"
```

---

### Task 5: Parsing HTML Atexo (fonctions pures)

**Files:**

- Create: `src/tasks/scrap/deao/atexo/__init__.py`, `src/tasks/scrap/deao/atexo/parse.py`
- Create: fixtures `tests/data/atexo/` copiées depuis `data/temp/megalis/` (repérage du 2026-11-13) : `megalis_resultats.html` (← `megalis_res2.html`), `megalis_aucun_resultat.html` (← `r_0203.html`), `megalis_detail_242752.html` (← `detail_242752.html`), `megalis_lots_242752.html` (← `lots_242752.html`), `maximilien_resultats.html` (← `max_16.html`)
- Test: `tests/test_deao_atexo_parse.py`

**Interfaces:**

- Produces (dataclasses figées dans `parse.py`) :
  - `ConsultationListe(ref_cons: str, org_cons: str, procedure_code: str, categorie: str, date_mise_en_ligne: date, reference: str, intitule: str, objet: str, organisme: str, code_postal: str | None, ville: str | None, nombre_lots: int, lieux_execution: list[str], date_limite: datetime)` — `nombre_lots` = 0 si non alloti ; `lieux_execution` = codes département (`"35"`) ; `date_limite` avec fuseau `Europe/Paris`.
  - `DetailConsultation(code_cpv: str | None, entite_publique: str | None, service: str | None, taille_dce: str | None)` — `taille_dce` tel qu'affiché (`"72,09 Mo"`).
  - `Lot(numero: str, intitule: str, categorie: str | None, description: str | None, code_cpv: str | None)`.
  - `champs_formulaire(html: str) -> dict[str, str]` (inputs, selects, textareas nommés ; cases et radios cochées seulement ; option sélectionnée ou première).
  - `nombre_resultats(html: str) -> int` (0 si « Aucun résultat »).
  - `parse_resultats(html: str) -> list[ConsultationListe]`, `parse_detail(html: str) -> DetailConsultation`, `parse_lots(html: str) -> list[Lot]`.
  - `date_fr(texte: str) -> date` (« 19 Oct. 2026 ») et `date_heure_fr(texte: str) -> datetime` (« 13 Nov. 2026 16:45 »).

Repères HTML (repérage) : bloc `div.item_consultation` ; champs cachés `…$refCons` / `…$orgCons` dans l'ordre des blocs ; compteur « Nombre de résultats : N » ; organisme « RENNES METROPOLE (35000 - Rennes ) » ; page détail « Code CPV : 45232410 (Code principal) », « Dossier de consultation - 72,09 Mo » ; popup lots « Lot 1 : ASSAINISSEMENT Catégorie : Travaux Description : ASSAINISSEMENT CPV: 45232410 (Code principal) ».

- [ ] **Step 1: Copier les fixtures** (tronquer la valeur de `PRADO_PAGESTATE` à 100 caractères pour réduire leur taille ; aucune donnée personnelle n'y figure, ce sont des avis publics).

- [ ] **Step 2: Write the failing tests**

```python
def test_resultats_megalis():
    html = (FIX / "megalis_resultats.html").read_text()
    assert nombre_resultats(html) == 45
    cs = parse_resultats(html)
    assert len(cs) == 10
    c = cs[0]
    assert (c.ref_cons, c.org_cons, c.procedure_code, c.categorie) == ("242752", "a4h", "MAPA", "Travaux")
    assert c.reference == "26190042" and c.date_mise_en_ligne == date(2026, 10, 19)
    assert (c.organisme, c.code_postal, c.ville) == ("RENNES METROPOLE", "35000", "Rennes")
    assert c.nombre_lots == 2 and c.lieux_execution == ["35"]
    assert c.date_limite == datetime(2026, 11, 13, 16, 45, tzinfo=ZoneInfo("Europe/Paris"))

def test_resultats_maximilien():
    cs = parse_resultats((FIX / "maximilien_resultats.html").read_text())
    assert nombre_resultats(...) == 24 and len(cs) == 10 and all(c.ref_cons.isdigit() for c in cs)

def test_aucun_resultat():
    html = (FIX / "megalis_aucun_resultat.html").read_text()
    assert nombre_resultats(html) == 0 and parse_resultats(html) == []

def test_detail():
    d = parse_detail((FIX / "megalis_detail_242752.html").read_text())
    assert d == DetailConsultation("45232410", "RENNES METROPOLE", d.service, "72,09 Mo")
    assert "Service Marchés publics" in d.service

def test_lots():
    lots = parse_lots((FIX / "megalis_lots_242752.html").read_text())
    assert [(l.numero, l.intitule, l.code_cpv) for l in lots] == [("1", "ASSAINISSEMENT", "45232410"), ("2", "VOIRIE", "45233140")]

@pytest.mark.parametrize("texte,attendu", [
    ("19 Oct. 2026", date(2026, 10, 19)), ("15 Juil. 2026", date(2026, 7, 15)),
    ("27 Août 2026", date(2026, 8, 27)), ("10 Sept. 2026", date(2026, 9, 10)),
    ("25 Janv. 2027", date(2027, 1, 25)), ("31 Déc. 2027", date(2027, 12, 31)),
    ("1 Févr. 2027", date(2027, 2, 1)), ("3 Mai 2027", date(2027, 5, 3)),
])
def test_date_fr(texte, attendu):
    assert date_fr(texte) == attendu

def test_champs_formulaire_recherche():
    champs = champs_formulaire((FIX / "megalis_resultats.html").read_text())
    assert "PRADO_PAGESTATE" in champs and "ctl0$CONTENU_PAGE$resultSearch$numPageTop" in champs
```

- [ ] **Step 3: Run tests to verify they fail**

Run: `uv run pytest tests/test_deao_atexo_parse.py -q`
Expected: FAIL (module absent).

- [ ] **Step 4: Implémenter `parse.py`** (lxml ; texte normalisé par `" ".join(t.split())`). Une valeur inattendue (mois inconnu) lève `ValueError` avec le texte fautif.

- [ ] **Step 5: Run tests**

Run: `uv run pytest tests/test_deao_atexo_parse.py -q`
Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add src/tasks/scrap/deao/atexo/ tests/data/atexo/ tests/test_deao_atexo_parse.py
git commit -m "DEAO : parsing HTML des plateformes Atexo (résultats, détail, lots)"
```

---

### Task 6: Client HTTP Atexo (recherche paginée, détail, lots, DCE anonyme)

**Files:**

- Create: `src/tasks/scrap/deao/limiteur.py` (déplacement de `Limiteur` depuis `joue/fetch.py`, qui le réimporte : `fetch.Limiteur` reste valide pour les tests existants)
- Create: `src/tasks/scrap/deao/atexo/client.py`
- Modify: `src/config.py`, `template.env` (variables ci-dessous)
- Create: fixtures complémentaires `tests/data/atexo/` enregistrées à l'étape 1
- Test: `tests/test_deao_atexo_client.py`

**Interfaces:**

- Consumes: `parse.*` (Task 5).
- Produces:
  - `class ClientAtexo(url_base: str, http: httpx.Client | None = None, limiteur: Limiteur | None = None)` ;
  - `ClientAtexo.rechercher() -> list[ConsultationListe]` — toutes les consultations en cours (plage de dates limites `01/01/2000` → aujourd'hui + 10 ans, format `jj/mm/aaaa`), 20 par page ; lève `RechercheIncomplete(RuntimeError)` si le nombre de consultations lues diffère de `nombre_resultats` ou si une page échoue après les nouvelles tentatives ;
  - `ClientAtexo.detail(ref_cons: str, org_cons: str) -> DetailConsultation` (GET `{url_base}/entreprise/consultation/{ref}?orgAcronyme={org}`) ;
  - `ClientAtexo.lots(ref_cons: str, org_cons: str) -> list[Lot]` (GET `{url_base}/index.php?page=Entreprise.PopUpDetailLots&orgAccronyme={org}&id={ref}&lang=`) ;
  - `ClientAtexo.telecharger_dce(ref_cons: str, org_cons: str, dest_dir: Path) -> DceTelecharge` avec `DceTelecharge(chemin: Path, nom_fichier: str, content_type: str, id_dce: str | None)` — écrit en streaming dans `dest_dir`, jamais en mémoire ;
  - config : `CLOSED_INTERVALLE_REQUETES_S` (float, défaut `0.2`, décision de l'utilisateur : intervalle minimal entre deux requêtes vers une même plateforme ; chaque `ClientAtexo` a son propre `Limiteur`, les deux plateformes tournant en parallèle), `CLOSED_USER_AGENT` (défaut : un User-Agent de navigateur générique), `DEAO_DCE_S3_PREFIX` (défaut `dce`), `DEAO_SCRAP_PARTIR_DE_ZERO` (bool, défaut `false`, utilisé Task 9). Documentées dans `template.env` (section DEAO, commentées avec leur défaut).
- Nouvelles tentatives : tenacity sur 429, 5xx et erreurs de transport, 5 essais, attente exponentielle de 5 s à 60 s ; 4xx autres que 429 : pas de nouvel essai.

- [ ] **Step 1: Repérage en direct des postbacks (pas de code produit)**

Sur Mégalis, avec un script jetable dans `data/temp/megalis/` (requêtes espacées d'au moins 1 s) :

1. Passage à 20 résultats par page : POST avec `PRADO_POSTBACK_TARGET` = `ctl0$CONTENU_PAGE$resultSearch$listePageSizeTop` et ce champ à `20` → la page renvoie 20 blocs.
2. Page suivante : POST avec `ctl0$CONTENU_PAGE$resultSearch$numPageTop` = `2` et la cible du bouton `DefaultButtonTop` (ou le lien suivant de `PagerTop`) → blocs différents de la page 1.
3. DCE anonyme sur une petite consultation (≤ 2 Mo, ex. `242039`/`a1w`, 1,18 Mo) : GET de `EntrepriseDemandeTelechargementDce`, POST avec le radio `…$RadioGroup` = `…$choixAnonyme` et `validateButton`, puis la (ou les) requête(s) qui renvoient le fichier ; noter l'en-tête `Content-Disposition` (nom de fichier) et tout identifiant propre au DCE (paramètre d'URL ou nom de fichier).

Enregistrer dans `tests/data/atexo/` les réponses nécessaires aux tests (page de recherche GET, page 2 de résultats, page du formulaire DCE, réponse de validation ; **pas** le zip) et consigner la séquence exacte en docstring de `client.py`. Si une étape ne se comporte pas comme décrit, s'arrêter et le signaler (BLOCKED) : c'est une hypothèse du design.

- [ ] **Step 2: Write the failing tests** (avec `httpx.MockTransport` servant les fixtures ; `Limiteur(0)`)

```python
def test_recherche_toutes_pages(): ...  # total annoncé 25, page 1 (20) + page 2 (5) → 25 consultations distinctes
def test_recherche_incomplete_leve(): ...  # total annoncé 45, page 2 vide → RechercheIncomplete
def test_recherche_sans_resultat(): ...  # → []
def test_dce_en_streaming(tmp_path): ...  # réponse fichier de 5 Mo en morceaux → fichier écrit, taille exacte, nom tiré de Content-Disposition
def test_429_puis_succes(no_sleep): ...  # détail : 429 puis 200 → DetailConsultation
def test_404_sans_nouvel_essai(): ...  # détail : 404 → httpx.HTTPStatusError, 1 appel
def test_limiteur_appele_a_chaque_requete(): ...
```

- [ ] **Step 3: Run tests to verify they fail**

Run: `uv run pytest tests/test_deao_atexo_client.py tests/test_deao_joue_fetch.py -q`
Expected: FAIL sur le nouveau fichier ; `test_deao_joue_fetch.py` vert.

- [ ] **Step 4: Implémenter `limiteur.py`, `client.py`, les variables de config et `template.env`.**

- [ ] **Step 5: Run tests**

Run: `uv run pytest -q --ignore=tests/test_main.py`
Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add src/tasks/scrap/deao/limiteur.py src/tasks/scrap/deao/boamp_joue/joue/fetch.py src/tasks/scrap/deao/atexo/client.py src/config.py template.env tests/
git commit -m "DEAO : client HTTP Atexo (recherche paginée, détail, lots, DCE anonyme)"
```

---

### Task 7: Archivage des DCE dans S3

**Files:**

- Create: `src/tasks/scrap/deao/atexo/dce.py`
- Modify: `tests/deao_fakes.py` (`FakeS3.upload_file`, `FakeS3.head_object`, mémorisation des `ExtraArgs`)
- Test: `tests/test_deao_atexo_dce.py`

**Interfaces:**

- Consumes: `DceTelecharge` (Task 6), `config.DEAO_DCE_S3_PREFIX`, `S3_ENDPOINT_URL`, `S3_BUCKET`, `src.tasks.utils.s3_client`.
- Produces:

  - `cle_dce(code: str, identifiant: str, nom_fichier: str) -> str` → `f"{prefixe}/{code}/{identifiant}{suffixe}"`, suffixe = extension de `nom_fichier` en minuscules, `.zip` à défaut ;
  - `url_publique(cle: str) -> str` → `f"{S3_ENDPOINT_URL.rstrip('/')}/{S3_BUCKET}/{cle}"` (adressage par chemin) ;
  - `taille_archivee(cle: str) -> str | None` → métadonnée `taille-affichee` de l'objet (`head_object`), `None` si l'objet n'existe pas ;
  - `deposer_dce(dce: DceTelecharge, cle: str, taille_affichee: str | None) -> str` → `upload_file` avec `ExtraArgs={"ACL": "public-read", "ContentType": dce.content_type, "ContentDisposition": f'attachment; filename="{dce.nom_fichier}"', "Metadata": {"taille-affichee": taille_affichee or ""}}`, supprime `dce.chemin` (même en cas d'échec du dépôt, puis relève), renvoie `url_publique(cle)`.

- [ ] **Step 1: Write the failing tests**

```python
def test_cle_dce(): assert cle_dce("megalis_bretagne", "242752", "DCE_26190042.ZIP") == "dce/megalis_bretagne/242752.zip"
def test_cle_dce_sans_extension(): assert cle_dce("maximilien", "9", "dce").endswith("/9.zip")
def test_url_publique(monkeypatch): ...  # endpoint "https://s3.example.com/", bucket "b" → "https://s3.example.com/b/dce/x/1.zip"
def test_depot_public_et_metadonnees(fake_s3, tmp_path): ...  # ExtraArgs ACL public-read, ContentDisposition, Metadata ; fichier local supprimé ; taille_archivee(cle) == "72,09 Mo"
def test_taille_archivee_absente(fake_s3): assert taille_archivee("dce/x/404.zip") is None
def test_depot_supprime_rien_si_echec(fake_s3, tmp_path): ...  # upload_file lève → exception propagée, fichier local supprimé
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest tests/test_deao_atexo_dce.py -q`
Expected: FAIL.

- [ ] **Step 3: Implémenter `dce.py` et les compléments de `FakeS3`** (`head_object` lève `ClientError` code `404` si absent, comme boto3).

- [ ] **Step 4: Run tests**

Run: `uv run pytest -q --ignore=tests/test_main.py`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add src/tasks/scrap/deao/atexo/dce.py tests/deao_fakes.py tests/test_deao_atexo_dce.py
git commit -m "DEAO : archivage public des DCE Atexo dans S3"
```

---

### Task 8: Lignes au schéma et accumulation

**Files:**

- Create: `src/tasks/scrap/deao/atexo/lignes.py`, `src/tasks/scrap/deao/atexo/accumulation.py`
- Test: `tests/test_deao_atexo_lignes.py`, `tests/test_deao_atexo_accumulation.py`

**Interfaces:**

- Consumes: `ConsultationListe`, `DetailConsultation`, `Lot` (Task 5) ; `published_schema()` ; `consolidate.statut_expr` n'est **pas** réutilisé (pas de chaîne d'avis).
- Produces:

  - `PROCEDURES: dict[str, str]` — `MAPA` → « Procédure adaptée », `AOO` → « Appel d'offres ouvert », `AOR` → « Appel d'offres restreint », `PN`/`PCN`/`PNSM` → « Procédure négociée », `DC` → « Dialogue compétitif », `CONC` → « Concession », tout autre code (dont `AUT`, `SAD-A`) → « Autre procédure » avec compteur `procedure_inconnue:{code}`. Compléter la table d'après les codes réellement présents sur les deux plateformes (relever la liste lors de l'étape 1 de Task 6).
  - `lignes_consultation(code: str, url_base: str, c: ConsultationListe, detail: DetailConsultation | None, lots: list[Lot], url_dce: str | None) -> list[dict]` — une ligne par lot (ou une ligne `lotId="0"`), colonnes publiées hors enrichissement plus `acheteur_adresse_cp`, `acheteur_adresse_ville`, `acheteur_pays` (`"FRA"`) : `uid`, `id`=`ref_cons`, `idDernierAvis` nul, `referenceAcheteur`, `acheteur_nom`=`organisme`, `objet`=`intitule`, `description`=`objet`, `lotId`, `lotIntitule`, `type`=`categorie`, `codeCPV` (lot puis détail), `procedure`, `datePublicationInitiale`, `dateLimiteReponse`, `lieuExecution_code`=premier département, `lieuExecution_typeCode`=« Code département », `urlConsultation`=`{url_base}/entreprise/consultation/{ref}?orgAcronyme={org}`, `urlReglementConsultation`=`{url_base}/index.php?page=Entreprise.EntrepriseDownloadReglement&id={base64(ref)}&orgAcronyme={org}`, `urlDce`, `rectifie`=False, `infosRectificatifs`=[], `avisLies`=[], `sourceDataset`=`code`, `sourceFile` nul.
  - `lots_a_relire(liste: list[ConsultationListe], precedent: pl.DataFrame) -> set[str]` — `ref_cons` dont la fenêtre des lots doit être (re)lue : nouveaux, ou dont la date limite diffère de `dateLimiteReponse` du précédent, ou dont `nombre_lots` diffère du nombre de lots du précédent (lignes `lotId != "0"`). Les autres reprennent les lots (numéro, intitulé, CPV) du précédent.
  - `dce_a_telecharger(taille_affichee: str | None, taille_archivee: str | None, url_dce_precedente: str | None) -> bool` — vrai si aucun DCE n'est archivé (`taille_archivee` nul ou `url_dce_precedente` nulle) ou si la taille affichée diffère de la taille archivée ; faux si `taille_affichee` est nulle (pas de DCE en ligne).
  - `recherche_complete(nb_lus: int, nb_annonces: int, precedent: pl.DataFrame, today: date) -> bool` — vrai si `nb_lus == nb_annonces` et `nb_lus >= 0.5 × (consultations précédentes encore ouvertes à today)`.
  - `fusionner(precedent: pl.DataFrame, nouvelles: pl.DataFrame, ids_vus: set[str], today: date, complete: bool) -> pl.DataFrame` — `nouvelles` remplace toutes les lignes précédentes de même `id` ; les autres lignes précédentes sont gardées ; statut : `annule` si (`id` ∉ `ids_vus`, `complete`, date limite ≥ today, et statut précédent ≠ `clos`) ou si déjà `annule` et toujours absent ; sinon `clos` si date limite < today, `ouvert` sinon (une consultation `annule` revue redevient `ouvert`/`clos`). Résultat au `published_schema()`, trié par `uid`.

- [ ] **Step 1: Write the failing tests**

```python
# lignes
def test_consultation_allotie_une_ligne_par_lot(): ...  # 2 lots → uids ["megalis_bretagne_242752_1", "megalis_bretagne_242752_2"], codeCPV par lot
def test_consultation_non_allotie(): ...  # → 1 ligne, lotId "0", uid "..._0", codeCPV du détail
def test_urls(): ...  # urlConsultation et urlReglementConsultation (id "MjQyNzUy" pour 242752)
def test_procedure_mapa(): ...  # "MAPA" → "Procédure adaptée"

# accumulation
def test_nouvelle_ajoutee_ancienne_conservee(): ...
def test_disparue_avant_date_limite_annulee(): ...  # complete=True, date limite future, id absent → "annule"
def test_disparue_apres_date_limite_close(): ...  # → "clos"
def test_recherche_incomplete_aucune_annulation(): ...  # complete=False → reste "ouvert"
def test_annulee_revue_redevient_ouverte(): ...
def test_recherche_complete_seuil(): ...  # 620 ouvertes précédemment, 200 lues == 200 annoncées → False
def test_lots_a_relire(): ...  # nouvelle, date limite changée, nombre de lots changé → dans l'ensemble ; inchangée → hors
@pytest.mark.parametrize("affichee,archivee,url,attendu", [
    ("72,09 Mo", None, None, True), ("72,09 Mo", "72,09 Mo", "https://s3/x.zip", False),
    ("73,10 Mo", "72,09 Mo", "https://s3/x.zip", True), ("72,09 Mo", "72,09 Mo", None, True),
    (None, None, None, False),
])
def test_dce_a_telecharger(affichee, archivee, url, attendu): ...
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest tests/test_deao_atexo_lignes.py tests/test_deao_atexo_accumulation.py -q`
Expected: FAIL.

- [ ] **Step 3: Implémenter `lignes.py` et `accumulation.py`.**

- [ ] **Step 4: Run tests**

Run: `uv run pytest tests/test_deao_atexo_lignes.py tests/test_deao_atexo_accumulation.py -q`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add src/tasks/scrap/deao/atexo/lignes.py src/tasks/scrap/deao/atexo/accumulation.py tests/test_deao_atexo_lignes.py tests/test_deao_atexo_accumulation.py
git commit -m "DEAO : lignes au schéma et accumulation des consultations Atexo"
```

---

### Task 9: Scraper assemblé et enregistrement dans `scrap_closed`

**Files:**

- Create: `src/tasks/scrap/deao/atexo/scraper.py`
- Modify: `src/tasks/scrap/deao/contrat.py` (`SCRAPERS`)
- Test: `tests/test_deao_atexo_scraper.py`, `tests/test_deao_scrap_closed.py`

**Interfaces:**

- Consumes: Tasks 4 à 8 ; `telecharger_fichier_source`, `FichierSourceIndisponible`, `publication_autorisee` ; `referentiels.charger_referentiels`, `referentiels.geocoder_tolerant` ; `enrich_acheteurs(..., dictionnaire=)` ; `selection_publiee` (déplacer de `scrap_open` vers `src/tasks/deao/schema.py` si l'import croisé pose problème, en gardant le nom réexporté par `scrap_open`).
- Produces: `scraper_atexo(code: str, client_factory: Callable[[str], ClientAtexo] = ClientAtexo) -> Scraper` ; `SCRAPERS = {"megalis_bretagne": scraper_atexo("megalis_bretagne"), "maximilien": scraper_atexo("maximilien")}`.

Déroulé d'un passage (`scraper(today)`) :

1. `entree` = registre[`code`] (`url_base` obligatoire, sinon `ValueError`).
2. Précédent : `telecharger_fichier_source(entree, DEAO_DATA_DIR / "sources")` ; `FichierSourceIndisponible` → si `config.DEAO_SCRAP_PARTIR_DE_ZERO`, précédent vide au schéma publié (avec un `logger.warning`) ; sinon l'exception remonte (le scraper échoue, rien n'est écrasé).
3. Dictionnaire SIRET : `boamp.parquet` téléchargé de la même façon → `dictionnaire_depuis_publie` ; en échec, `logger.warning` et pas de dictionnaire.
4. `client.rechercher()` ; `RechercheIncomplete` → on poursuit avec les consultations lues mais `complete=False`.
5. Pour **chaque** consultation lue (décision de l'utilisateur : surveiller les DCE à chaque passage) : `detail` ; `lots` si `nombre_lots > 0` et `ref_cons ∈ lots_a_relire(...)`, sinon lots repris du précédent ; DCE si `publication_autorisee()` et `S3_BUCKET` défini : `cle = cle_dce(code, id_dce or ref_cons, nom)` (pour une consultation déjà archivée, la clé est retrouvée à partir de `urlDce` du précédent) ; télécharger puis déposer si `dce_a_telecharger(detail.taille_dce, taille_archivee(cle), urlDce précédente)` ; sinon réutiliser `urlDce` du précédent. Dossier temporaire : `DEAO_DATA_DIR / "dce_tmp" / code`, vidé en fin de passage. Toute erreur sur une consultation est attrapée et comptée (`detail_en_erreur`, `dce_en_erreur`) : consultation nouvelle sans détail → ignorée ce passage ; consultation connue → lignes précédentes conservées ; DCE en échec → `urlDce` nul.
6. Lignes (`lignes_consultation`) → `enrich_acheteurs(..., dictionnaire=…)` → `selection_publiee` → `fusionner(precedent, nouvelles, ids_vus, today, complete)`.
7. Log de synthèse : consultations lues / annoncées, rafraîchies, DCE déposés et volume (Mo), compteurs d'erreur.

- [ ] **Step 1: Write the failing tests** (client factice en mémoire, `FakeS3`, `fake_datagouv`, référentiels minimaux comme `tests/test_deao_scrap_open.py`)

```python
def test_premier_passage_partir_de_zero(...): ...  # DEAO_SCRAP_PARTIR_DE_ZERO=True, précédent absent → lignes au published_schema, sourceDataset == code
def test_precedent_indisponible_echoue(...): ...  # flag False, précédent absent → FichierSourceIndisponible, aucun appel au client
def test_siret_par_dictionnaire_boamp(...): ...  # boamp.parquet publié avec acheteur_codePostal → acheteur_id_source == "dictionnaire"
def test_detail_en_echec_nouvelle_ignoree(...): ...
def test_detail_en_echec_connue_conservee(...): ...
def test_dce_depose_si_publication(...): ...  # publication autorisée → objet "dce/megalis_bretagne/242752.zip", urlDce = url_publique ; dossier temporaire vide après le passage
def test_dce_non_depose_en_dev(...): ...  # DECP_PROCESSING_PUBLISH faux → aucun objet S3, urlDce nul
def test_dce_en_echec_retente(...): ...  # 1er passage : dépôt lève → urlDce nul ; 2e passage : déposé
def test_dce_inchange_non_retelecharge(...): ...  # taille_archivee == taille affichée et urlDce présent → pas de téléchargement
def test_dce_modifie_meme_date_limite_retelecharge(...): ...  # 2e passage, date limite inchangée, taille affichée différente → nouveau dépôt, même clé
def test_detail_lu_a_chaque_passage_lots_non_relus(...): ...  # 2e passage inchangé → client.detail appelé pour chaque consultation, client.lots jamais

# tests/test_deao_scrap_closed.py
def test_scrapers_atexo_enregistres():
    assert {"megalis_bretagne", "maximilien"} <= set(SCRAPERS)
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest tests/test_deao_atexo_scraper.py tests/test_deao_scrap_closed.py -q`
Expected: FAIL.

- [ ] **Step 3: Implémenter `scraper.py` et `SCRAPERS`.**

- [ ] **Step 4: Run the full suite**

Run: `uv run pytest -q --ignore=tests/test_main.py`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add src/tasks/scrap/deao/atexo/scraper.py src/tasks/scrap/deao/contrat.py tests/
git commit -m "DEAO : scrapers megalis_bretagne et maximilien branchés sur scrap_closed"
```

- [ ] **Step 6: Essai réel sans publication** (vérification, pas de commit)

Run : `DEAO_SCRAP_PARTIR_DE_ZERO=true SOLO_DEAO_SCRAPS=maximilien DECP_PROCESSING_PUBLISH=False uv run python run_flow.py scrap_closed` (Maximilien, ~370 consultations : une page détail par consultation plus les lots, quelques minutes à 0,2 s entre requêtes).
Expected : `dist/deao/maximilien.parquet` écrit, nombre de consultations distinctes égal au nombre annoncé, `acheteur_id` renseigné pour une part non nulle des lignes, aucun objet S3 déposé ; le `deao_processing` final s'exécute (la source `maximilien` peut y être écartée faute de fichier publié : attendu en SOLO). Reporter les chiffres dans le rapport de tâche.

---

## Mise en service (à la charge de l'utilisateur, hors plan)

- Vérifier que le fournisseur S3 accepte l'ACL `public-read` par objet et sert l'objet en HTTPS sans authentification (un objet de test sous `dce/`) ; sinon poser une politique de bucket publique limitée au préfixe `dce/`.
- Déployer, puis lancer un `scrap-open` (republication de `boamp.parquet`/`joue.parquet` avec `id` et `acheteur_codePostal` ; vérifier si `DEAO_REBUILD=true` est nécessaire), puis le premier `scrap-closed` avec `DEAO_SCRAP_PARTIR_DE_ZERO=true`, à retirer ensuite.
- Surveiller le volume S3 (≈ 65 Go pour Mégalis et ≈ 40 Go pour Maximilien au premier passage).
