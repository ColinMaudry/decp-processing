# Géocodage quotidien des SIRETs Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Étendre `siret_latlong.parquet` chaque jour avec les SIRETs nouvellement apparus dans les DECP en interrogeant l'API Géoplateforme `/search/csv`, avec cache positif et négatif pour éviter les appels redondants.

**Architecture:** Une nouvelle module `src/tasks/geocode.py` encapsule l'API Géoplateforme (construction du CSV, appel HTTP par chunks, parsing). Un orchestrateur dans `src/tasks/enrich.py` sélectionne les SIRETs à géocoder (negative caching via `status` + `geocoded_at`), récupère leurs adresses dans le SIRENE preprocessing, lance le géocodage, et met à jour `siret_latlong.parquet`. Le flow `decp_processing` ajoute l'étape entre `enrich_from_sirene` et la génération des sorties, puis pousse le fichier mis à jour sur S3.

**Tech Stack:** Python 3, Polars (LazyFrame), httpx, tenacity, Prefect, boto3 (existant), pytest.

---

## File Structure

- `src/config.py` (modifier) — ajout des variables d'environnement et de la constante de schéma cible
- `src/tasks/get.py` (modifier) — extension de `get_etablissements` et `bootstrap_siret_latlong`
- `src/tasks/geocode.py` (créer) — client API Géoplateforme : construction CSV, appel chunked + retry, parsing
- `src/tasks/enrich.py` (modifier) — fonctions de sélection et orchestrateur `geocode_missing_sirets`
- `src/flows/decp_processing.py` (modifier) — insertion de l'étape géocodage + push S3
- `src/flows/sirene_preprocess.py` (modifier) — chargement adapté au nouveau schéma de `siret_latlong.parquet`
- `tests/test_geocode.py` (créer) — tests unitaires du module geocode
- `tests/test_enrich.py` (modifier) — tests de la sélection et de l'orchestrateur
- `tests/test_get.py` (créer si absent) — tests d'extension du bootstrap

---

## Conventions

- Branche : `bugfix/175_sirene` (déjà active).
- Messages de commit : style français court, terminant par `#175`. Exemple : `Ajout du module geocode #175`.
- Avant chaque commit : `pre-commit run --all-files`.
- Tests dépendent de l'env `[tool.pytest.ini_options]` (déjà configuré).

---

## Task 1 : Variables d'environnement et schéma cible

**Files:**

- Modify: `src/config.py`
- Modify: `template.env`

- [ ] **Step 1: Lire `src/config.py` pour repérer la zone des variables d'environnement et le bloc des constantes**

Identifier le bloc proche des autres `os.getenv(...)`. Repérer où placer une constante de schéma (à proximité de `BASE_DF_COLUMNS` par exemple).

- [ ] **Step 2: Ajouter les variables d'environnement dans `src/config.py`**

À placer dans la zone des `os.getenv` :

```python
GEOCODING_API_URL = os.getenv("GEOCODING_API_URL", "https://data.geopf.fr/geocodage")
GEOCODING_MIN_SCORE = float(os.getenv("GEOCODING_MIN_SCORE", "0.5"))
GEOCODING_RETRY_DAYS = int(os.getenv("GEOCODING_RETRY_DAYS", "30"))
GEOCODING_CHUNK_SIZE = int(os.getenv("GEOCODING_CHUNK_SIZE", "5000"))
```

- [ ] **Step 3: Ajouter la constante de schéma `SIRET_LATLONG_SCHEMA` dans `src/config.py`**

À ajouter (vers la fin du fichier, après les autres constantes de schéma) :

```python
import polars as pl  # déjà importé, sinon ajouter

SIRET_LATLONG_SCHEMA = {
    "siret": pl.String,
    "latitude": pl.Float64,
    "longitude": pl.Float64,
    "source": pl.String,
    "score": pl.Float64,
    "geocoded_at": pl.Date,
    "status": pl.String,
}
```

- [ ] **Step 4: Mettre à jour `template.env`**

Ajouter à la fin du fichier :

```env
# API de géocodage Géoplateforme
GEOCODING_API_URL=https://data.geopf.fr/geocodage
GEOCODING_MIN_SCORE=0.5
GEOCODING_RETRY_DAYS=30
GEOCODING_CHUNK_SIZE=5000
```

- [ ] **Step 5: Vérifier que `python -c "from src.config import GEOCODING_API_URL, SIRET_LATLONG_SCHEMA; print(GEOCODING_API_URL, SIRET_LATLONG_SCHEMA)"` fonctionne**

Run: `python -c "from src.config import GEOCODING_API_URL, SIRET_LATLONG_SCHEMA; print(GEOCODING_API_URL, SIRET_LATLONG_SCHEMA)"`
Expected: affiche l'URL et le dict de schéma sans erreur.

- [ ] **Step 6: Commit**

```bash
pre-commit run --all-files
git add src/config.py template.env
git commit -m "Ajout des variables d'environnement pour le géocodage #175"
```

---

## Task 2 : Extension des colonnes SIRENE pour le géocodage

**Files:**

- Modify: `src/tasks/get.py` (fonction `get_etablissements`)
- Test: `tests/test_get.py` (créer si nécessaire)

- [ ] **Step 1: Écrire le test qui vérifie que `get_etablissements` renvoie un LazyFrame avec les nouvelles colonnes**

Créer ou compléter `tests/test_get.py` :

```python
import polars as pl

from src.tasks.get import get_etablissements


REQUIRED_GEO_COLUMNS = {
    "numeroVoieEtablissement",
    "indiceRepetitionEtablissement",
    "codePostalEtablissement",
    "libelleCommuneEtablissement",
}


def test_get_etablissements_includes_geocoding_columns():
    lf = get_etablissements()
    assert isinstance(lf, pl.LazyFrame)
    columns = set(lf.collect_schema().names())
    missing = REQUIRED_GEO_COLUMNS - columns
    assert not missing, f"Colonnes manquantes : {missing}"
```

- [ ] **Step 2: Lancer le test pour confirmer qu'il échoue**

Run: `pytest tests/test_get.py::test_get_etablissements_includes_geocoding_columns -v`
Expected: FAIL — `Colonnes manquantes : {'numeroVoieEtablissement', ...}`.

(Si le test ne tourne pas parce que `SIRENE_ETABLISSEMENTS_URL` doit être disponible, ajouter un mock parquet en fixture — voir Task 2 bis ci-dessous. Sinon poursuivre.)

- [ ] **Step 3: Modifier la liste `columns` dans `get_etablissements` (`src/tasks/get.py`)**

Remplacer la liste `columns` par :

```python
columns = [
    "siret",
    "codeCommuneEtablissement",
    "activitePrincipaleEtablissement",
    "nomenclatureActivitePrincipaleEtablissement",
    "enseigne1Etablissement",
    "denominationUsuelleEtablissement",
    "libelleVoieEtablissement",
    "typeVoieEtablissement",
    "numeroVoieEtablissement",
    "indiceRepetitionEtablissement",
    "codePostalEtablissement",
    "libelleCommuneEtablissement",
]
```

- [ ] **Step 4: Lancer le test à nouveau pour vérifier qu'il passe**

Run: `pytest tests/test_get.py::test_get_etablissements_includes_geocoding_columns -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
pre-commit run --all-files
git add src/tasks/get.py tests/test_get.py
git commit -m "Ajout des colonnes adresse pour le géocodage #175"
```

---

## Task 3 : Extension du schéma de `bootstrap_siret_latlong`

**Files:**

- Modify: `src/tasks/get.py` (fonction `bootstrap_siret_latlong`)
- Test: `tests/test_get.py`

- [ ] **Step 1: Écrire le test qui vérifie le nouveau schéma**

Ajouter dans `tests/test_get.py` :

```python
from pathlib import Path
from unittest.mock import patch

import polars as pl
import pytest

from src.config import SIRET_LATLONG_SCHEMA
from src.tasks.get import bootstrap_siret_latlong


def test_bootstrap_siret_latlong_produces_extended_schema(tmp_path, monkeypatch):
    # Fabrique un decp.parquet minimal en local
    fake_decp = tmp_path / "decp_fake.parquet"
    pl.DataFrame({
        "acheteur_id": ["12345678901234", None, "00000000000000"],
        "acheteur_latitude": [48.85, None, 0.0],
        "acheteur_longitude": [2.35, None, 0.0],
        "titulaire_id": ["98765432109876", "98765432109876", None],
        "titulaire_latitude": [45.75, 45.75, None],
        "titulaire_longitude": [4.85, 4.85, None],
    }).write_parquet(fake_decp)

    # Redirige l'URL data.gouv.fr vers le fichier local
    def fake_scan(url):
        return pl.scan_parquet(fake_decp)

    monkeypatch.setattr("src.tasks.get.pl.scan_parquet", fake_scan)
    # Désactive l'upload S3
    monkeypatch.setattr("src.tasks.get.publish_to_s3", lambda *a, **kw: None)
    # Redirige DATA_DIR vers tmp_path
    monkeypatch.setattr("src.tasks.get.DATA_DIR", tmp_path)

    lf = bootstrap_siret_latlong()
    df = lf.collect()

    # Vérifie les colonnes du schéma cible
    assert set(df.columns) == set(SIRET_LATLONG_SCHEMA.keys())
    # Vérifie les valeurs par défaut
    assert df["source"].unique().to_list() == ["decp"]
    assert df["status"].unique().to_list() == ["success"]
    assert df["score"].is_null().all()
    assert df["geocoded_at"].is_null().all()
    # Vérifie le dédoublonnage par SIRET
    assert df["siret"].n_unique() == df.height
```

- [ ] **Step 2: Lancer le test pour confirmer qu'il échoue**

Run: `pytest tests/test_get.py::test_bootstrap_siret_latlong_produces_extended_schema -v`
Expected: FAIL — `set(df.columns)` n'inclut pas `source`, `score`, etc.

- [ ] **Step 3: Modifier `bootstrap_siret_latlong` dans `src/tasks/get.py`**

Remplacer le bloc `pl.concat([acheteurs, titulaires]).filter(...).unique(...).sink_parquet(output_path)` par :

```python
(
    pl.concat([acheteurs, titulaires])
    .filter(
        pl.col("siret").is_not_null()
        & (pl.col("siret").str.len_chars() == 14)
        & pl.col("latitude").is_not_null()
        & pl.col("longitude").is_not_null()
    )
    .unique(subset=["siret"], keep="first")
    .with_columns(
        pl.lit("decp").alias("source"),
        pl.lit(None, dtype=pl.Float64).alias("score"),
        pl.lit(None, dtype=pl.Date).alias("geocoded_at"),
        pl.lit("success").alias("status"),
    )
    .sink_parquet(output_path)
)
```

- [ ] **Step 4: Lancer le test pour vérifier qu'il passe**

Run: `pytest tests/test_get.py::test_bootstrap_siret_latlong_produces_extended_schema -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
pre-commit run --all-files
git add src/tasks/get.py tests/test_get.py
git commit -m "Extension du schéma de bootstrap_siret_latlong #175"
```

---

## Task 4 : Padding du schéma legacy au chargement S3

**Files:**

- Create: `src/tasks/geocode.py`
- Test: `tests/test_geocode.py`

Le fichier S3 actuel n'a que `(siret, latitude, longitude)`. Pour ne pas casser la prod, on ajoute les colonnes manquantes au chargement.

- [ ] **Step 1: Écrire le test qui vérifie le padding du schéma**

Créer `tests/test_geocode.py` :

```python
import polars as pl

from src.config import SIRET_LATLONG_SCHEMA
from src.tasks.geocode import pad_siret_latlong_schema


def test_pad_siret_latlong_adds_missing_columns():
    legacy = pl.LazyFrame({
        "siret": ["12345678901234"],
        "latitude": [48.85],
        "longitude": [2.35],
    })
    padded = pad_siret_latlong_schema(legacy)
    df = padded.collect()
    assert set(df.columns) == set(SIRET_LATLONG_SCHEMA.keys())
    assert df["source"].to_list() == ["decp"]
    assert df["status"].to_list() == ["success"]
    assert df["score"].is_null().all()
    assert df["geocoded_at"].is_null().all()


def test_pad_siret_latlong_preserves_existing_columns():
    current = pl.LazyFrame({
        "siret": ["12345678901234"],
        "latitude": [48.85],
        "longitude": [2.35],
        "source": ["geoplateforme"],
        "score": [0.92],
        "geocoded_at": [None],
        "status": ["success"],
    }, schema_overrides={"geocoded_at": pl.Date})
    padded = pad_siret_latlong_schema(current).collect()
    assert padded["source"].to_list() == ["geoplateforme"]
    assert padded["score"].to_list() == [0.92]
```

- [ ] **Step 2: Lancer pour confirmer l'échec**

Run: `pytest tests/test_geocode.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'src.tasks.geocode'`.

- [ ] **Step 3: Créer `src/tasks/geocode.py` avec `pad_siret_latlong_schema`**

```python
import polars as pl

from src.config import SIRET_LATLONG_SCHEMA


_DEFAULTS = {
    "source": pl.lit("decp"),
    "score": pl.lit(None, dtype=pl.Float64),
    "geocoded_at": pl.lit(None, dtype=pl.Date),
    "status": pl.lit("success"),
}


def pad_siret_latlong_schema(lf: pl.LazyFrame) -> pl.LazyFrame:
    """Ajoute les colonnes manquantes du schéma cible avec valeurs par défaut.

    Permet de lire un siret_latlong.parquet legacy (siret, latitude, longitude)
    et de le faire ressembler au schéma étendu.
    """
    existing = set(lf.collect_schema().names())
    to_add = [
        expr.alias(name)
        for name, expr in _DEFAULTS.items()
        if name not in existing
    ]
    if to_add:
        lf = lf.with_columns(to_add)
    return lf.select(list(SIRET_LATLONG_SCHEMA.keys()))
```

- [ ] **Step 4: Lancer les tests**

Run: `pytest tests/test_geocode.py -v`
Expected: PASS sur les deux tests.

- [ ] **Step 5: Brancher le padding dans `src/flows/sirene_preprocess.py`**

Modifier le bloc de chargement (lignes 34-37 environ) :

```python
from src.tasks.geocode import pad_siret_latlong_schema  # nouvel import en haut du fichier

# ...
lf_siret_latlong = get_from_s3(key="siret_latlong.parquet", prefix="")

if not isinstance(lf_siret_latlong, pl.LazyFrame):
    lf_siret_latlong = bootstrap_siret_latlong()

lf_siret_latlong = pad_siret_latlong_schema(lf_siret_latlong)
```

- [ ] **Step 6: Commit**

```bash
pre-commit run --all-files
git add src/tasks/geocode.py tests/test_geocode.py src/flows/sirene_preprocess.py
git commit -m "Ajout de pad_siret_latlong_schema pour compatibilité legacy #175"
```

---

## Task 5 : Construction du CSV d'entrée pour Géoplateforme

**Files:**

- Modify: `src/tasks/geocode.py`
- Test: `tests/test_geocode.py`

- [ ] **Step 1: Écrire le test de construction CSV**

Ajouter dans `tests/test_geocode.py` :

```python
import io


def test_build_geocoding_csv_concatenates_address():
    addresses = pl.DataFrame({
        "siret": ["12345678901234"],
        "numeroVoieEtablissement": ["10"],
        "indiceRepetitionEtablissement": ["bis"],
        "typeVoieEtablissement": ["RUE"],
        "libelleVoieEtablissement": ["DE LA PAIX"],
        "codePostalEtablissement": ["75001"],
        "codeCommuneEtablissement": ["75101"],
    })
    from src.tasks.geocode import build_geocoding_csv
    csv_bytes = build_geocoding_csv(addresses)
    df = pl.read_csv(io.BytesIO(csv_bytes))
    assert df.columns == ["siret", "q", "postcode", "citycode"]
    assert df["q"].to_list() == ["10 bis RUE DE LA PAIX"]
    assert df["postcode"].to_list() == ["75001"]
    assert df["citycode"].to_list() == ["75101"]


def test_build_geocoding_csv_handles_null_address_fields():
    addresses = pl.DataFrame({
        "siret": ["12345678901234"],
        "numeroVoieEtablissement": [None],
        "indiceRepetitionEtablissement": [None],
        "typeVoieEtablissement": ["AVENUE"],
        "libelleVoieEtablissement": ["DES CHAMPS"],
        "codePostalEtablissement": ["75008"],
        "codeCommuneEtablissement": ["75108"],
    })
    from src.tasks.geocode import build_geocoding_csv
    df = pl.read_csv(io.BytesIO(build_geocoding_csv(addresses)))
    assert df["q"].to_list() == ["AVENUE DES CHAMPS"]
```

- [ ] **Step 2: Lancer pour confirmer l'échec**

Run: `pytest tests/test_geocode.py::test_build_geocoding_csv_concatenates_address -v`
Expected: FAIL — `ImportError: cannot import name 'build_geocoding_csv'`.

- [ ] **Step 3: Implémenter `build_geocoding_csv` dans `src/tasks/geocode.py`**

Ajouter à `src/tasks/geocode.py` :

```python
import io


def build_geocoding_csv(addresses: pl.DataFrame) -> bytes:
    """Construit le CSV à envoyer à /search/csv.

    Colonnes attendues : siret, numeroVoieEtablissement,
    indiceRepetitionEtablissement, typeVoieEtablissement,
    libelleVoieEtablissement, codePostalEtablissement,
    codeCommuneEtablissement.
    """
    df = addresses.select(
        pl.col("siret"),
        pl.concat_str(
            [
                pl.col("numeroVoieEtablissement").fill_null(""),
                pl.col("indiceRepetitionEtablissement").fill_null(""),
                pl.col("typeVoieEtablissement").fill_null(""),
                pl.col("libelleVoieEtablissement").fill_null(""),
            ],
            separator=" ",
        )
        .str.replace_all(r"\s+", " ")
        .str.strip_chars()
        .alias("q"),
        pl.col("codePostalEtablissement").alias("postcode"),
        pl.col("codeCommuneEtablissement").alias("citycode"),
    )
    buf = io.BytesIO()
    df.write_csv(buf)
    return buf.getvalue()
```

- [ ] **Step 4: Lancer les tests**

Run: `pytest tests/test_geocode.py::test_build_geocoding_csv_concatenates_address tests/test_geocode.py::test_build_geocoding_csv_handles_null_address_fields -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
pre-commit run --all-files
git add src/tasks/geocode.py tests/test_geocode.py
git commit -m "Ajout de build_geocoding_csv #175"
```

---

## Task 6 : Parsing des résultats de l'API

**Files:**

- Modify: `src/tasks/geocode.py`
- Test: `tests/test_geocode.py`

- [ ] **Step 1: Écrire les tests du parser**

Ajouter dans `tests/test_geocode.py` :

```python
from datetime import date


def test_parse_results_marks_high_score_as_success():
    csv = (
        "siret,q,postcode,citycode,result_score,latitude,longitude\n"
        "12345678901234,10 RUE DE LA PAIX,75001,75101,0.92,48.869,2.331\n"
    ).encode()
    from src.tasks.geocode import parse_geocoding_results
    df = parse_geocoding_results(csv, min_score=0.5, today=date(2026, 5, 15))
    row = df.row(0, named=True)
    assert row["status"] == "success"
    assert row["latitude"] == 48.869
    assert row["longitude"] == 2.331
    assert row["score"] == 0.92
    assert row["source"] == "geoplateforme"
    assert row["geocoded_at"] == date(2026, 5, 15)


def test_parse_results_marks_low_score_as_failed():
    csv = (
        "siret,q,postcode,citycode,result_score,latitude,longitude\n"
        "12345678901234,FOO BAR,75001,75101,0.2,48.0,2.0\n"
    ).encode()
    from src.tasks.geocode import parse_geocoding_results
    df = parse_geocoding_results(csv, min_score=0.5, today=date(2026, 5, 15))
    row = df.row(0, named=True)
    assert row["status"] == "failed"
    assert row["latitude"] is None
    assert row["longitude"] is None
    assert row["score"] == 0.2


def test_parse_results_handles_no_match():
    csv = (
        "siret,q,postcode,citycode,result_score,latitude,longitude\n"
        "12345678901234,FOO BAR,75001,75101,,,\n"
    ).encode()
    from src.tasks.geocode import parse_geocoding_results
    df = parse_geocoding_results(csv, min_score=0.5, today=date(2026, 5, 15))
    row = df.row(0, named=True)
    assert row["status"] == "failed"
    assert row["latitude"] is None
```

- [ ] **Step 2: Lancer pour confirmer l'échec**

Run: `pytest tests/test_geocode.py::test_parse_results_marks_high_score_as_success -v`
Expected: FAIL — `cannot import name 'parse_geocoding_results'`.

- [ ] **Step 3: Implémenter `parse_geocoding_results`**

Ajouter à `src/tasks/geocode.py` :

```python
from datetime import date


def parse_geocoding_results(
    csv_bytes: bytes,
    min_score: float,
    today: date,
) -> pl.DataFrame:
    """Parse la réponse CSV de l'API Géoplateforme.

    Retourne un DataFrame conforme à SIRET_LATLONG_SCHEMA.
    Les lignes avec score < min_score ou sans match obtiennent status=failed
    et coords null.
    """
    df = pl.read_csv(
        io.BytesIO(csv_bytes),
        schema_overrides={
            "siret": pl.String,
            "result_score": pl.Float64,
            "latitude": pl.Float64,
            "longitude": pl.Float64,
        },
    )
    is_success = (
        pl.col("result_score").is_not_null()
        & (pl.col("result_score") >= min_score)
    )
    return df.select(
        pl.col("siret"),
        pl.when(is_success).then(pl.col("latitude")).otherwise(None).alias("latitude"),
        pl.when(is_success).then(pl.col("longitude")).otherwise(None).alias("longitude"),
        pl.lit("geoplateforme").alias("source"),
        pl.col("result_score").alias("score"),
        pl.lit(today).alias("geocoded_at"),
        pl.when(is_success)
        .then(pl.lit("success"))
        .otherwise(pl.lit("failed"))
        .alias("status"),
    )
```

- [ ] **Step 4: Lancer tous les tests du module**

Run: `pytest tests/test_geocode.py -v`
Expected: PASS (5 tests).

- [ ] **Step 5: Commit**

```bash
pre-commit run --all-files
git add src/tasks/geocode.py tests/test_geocode.py
git commit -m "Ajout de parse_geocoding_results #175"
```

---

## Task 7 : Client HTTP Géoplateforme avec chunking et retry

**Files:**

- Modify: `src/tasks/geocode.py`
- Test: `tests/test_geocode.py`

- [ ] **Step 1: Écrire le test du client (mock httpx)**

Ajouter dans `tests/test_geocode.py` :

```python
import httpx


def test_geocode_csv_calls_api_and_returns_dataframe(monkeypatch):
    addresses = pl.DataFrame({
        "siret": ["12345678901234"],
        "numeroVoieEtablissement": ["10"],
        "indiceRepetitionEtablissement": [None],
        "typeVoieEtablissement": ["RUE"],
        "libelleVoieEtablissement": ["DE LA PAIX"],
        "codePostalEtablissement": ["75001"],
        "codeCommuneEtablissement": ["75101"],
    })

    def handler(request: httpx.Request) -> httpx.Response:
        assert request.url.path.endswith("/search/csv/")
        # Réponse simulée de l'API
        body = (
            "siret,q,postcode,citycode,result_score,latitude,longitude\n"
            "12345678901234,10 RUE DE LA PAIX,75001,75101,0.92,48.869,2.331\n"
        )
        return httpx.Response(200, content=body)

    transport = httpx.MockTransport(handler)
    monkeypatch.setattr(
        "src.tasks.geocode.httpx.Client",
        lambda **kw: httpx.Client(transport=transport, **{k: v for k, v in kw.items() if k != "transport"}),
    )

    from src.tasks.geocode import geocode_csv
    result = geocode_csv(addresses)
    assert result.height == 1
    assert result.row(0, named=True)["status"] == "success"


def test_geocode_csv_chunks_large_input(monkeypatch):
    # 12 lignes, chunk_size=5 => 3 appels
    n = 12
    addresses = pl.DataFrame({
        "siret": [f"{i:014d}" for i in range(n)],
        "numeroVoieEtablissement": ["1"] * n,
        "indiceRepetitionEtablissement": [None] * n,
        "typeVoieEtablissement": ["RUE"] * n,
        "libelleVoieEtablissement": ["FOO"] * n,
        "codePostalEtablissement": ["75001"] * n,
        "codeCommuneEtablissement": ["75101"] * n,
    })
    call_count = {"n": 0}

    def handler(request: httpx.Request) -> httpx.Response:
        call_count["n"] += 1
        # Lit le CSV multipart envoyé pour échouer joliment si vide
        body_in = request.read()
        assert b"siret" in body_in
        # Renvoie une réponse minimale valide
        return httpx.Response(
            200,
            content=b"siret,q,postcode,citycode,result_score,latitude,longitude\n",
        )

    transport = httpx.MockTransport(handler)
    monkeypatch.setattr(
        "src.tasks.geocode.httpx.Client",
        lambda **kw: httpx.Client(transport=transport, **{k: v for k, v in kw.items() if k != "transport"}),
    )

    from src.tasks.geocode import geocode_csv
    geocode_csv(addresses, chunk_size=5)
    assert call_count["n"] == 3
```

- [ ] **Step 2: Lancer pour confirmer l'échec**

Run: `pytest tests/test_geocode.py::test_geocode_csv_calls_api_and_returns_dataframe -v`
Expected: FAIL — `cannot import name 'geocode_csv'`.

- [ ] **Step 3: Implémenter `post_geocoding_chunk` et `geocode_csv`**

Ajouter à `src/tasks/geocode.py` (en haut, mettre les imports si absents) :

```python
import httpx
from tenacity import retry, stop_after_attempt, wait_exponential

from src.config import (
    GEOCODING_API_URL,
    GEOCODING_CHUNK_SIZE,
    GEOCODING_MIN_SCORE,
    SIRET_LATLONG_SCHEMA,
)
```

Puis ajouter en fin de fichier :

```python
@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=1, max=20))
def post_geocoding_chunk(csv_bytes: bytes) -> bytes:
    """Envoie un chunk CSV à l'API Géoplateforme et retourne le CSV résultat."""
    with httpx.Client(timeout=120.0) as client:
        response = client.post(
            f"{GEOCODING_API_URL}/search/csv/",
            files={"data": ("input.csv", csv_bytes, "text/csv")},
            data={
                "columns": "q",
                "postcode": "postcode",
                "citycode": "citycode",
            },
        )
        response.raise_for_status()
        return response.content


def geocode_csv(
    addresses: pl.DataFrame,
    chunk_size: int = GEOCODING_CHUNK_SIZE,
    min_score: float = GEOCODING_MIN_SCORE,
) -> pl.DataFrame:
    """Géocode toutes les adresses en chunks. Retourne un DataFrame conforme
    au schéma SIRET_LATLONG_SCHEMA (sauf que `geocoded_at` est positionné à
    la date du jour pour chaque chunk)."""
    today = date.today()
    results = []
    for i in range(0, addresses.height, chunk_size):
        chunk = addresses.slice(i, chunk_size)
        csv_bytes = build_geocoding_csv(chunk)
        response_csv = post_geocoding_chunk(csv_bytes)
        results.append(parse_geocoding_results(response_csv, min_score, today))
    if not results:
        return pl.DataFrame(schema=SIRET_LATLONG_SCHEMA)
    return pl.concat(results)
```

- [ ] **Step 4: Lancer les tests du module**

Run: `pytest tests/test_geocode.py -v`
Expected: PASS sur tous les tests.

- [ ] **Step 5: Commit**

```bash
pre-commit run --all-files
git add src/tasks/geocode.py tests/test_geocode.py
git commit -m "Ajout du client HTTP Géoplateforme avec chunking #175"
```

---

## Task 8 : Sélection des SIRETs à géocoder (negative caching)

**Files:**

- Modify: `src/tasks/enrich.py`
- Test: `tests/test_enrich.py`

- [ ] **Step 1: Écrire le test**

Ajouter dans `tests/test_enrich.py` :

```python
from datetime import date, timedelta

import polars as pl

from src.tasks.enrich import select_sirets_to_geocode


def test_select_sirets_excludes_success_and_recent_failures_and_not_in_sirene():
    today = date(2026, 5, 15)
    retry_days = 30

    lf_decp = pl.LazyFrame({
        "acheteur_id": ["11111111111111", "22222222222222", "44444444444444"],
        "titulaire_id": ["33333333333333", "22222222222222", "55555555555555"],
    })
    lf_siret_latlong = pl.LazyFrame({
        "siret": [
            "11111111111111",  # success → exclu
            "22222222222222",  # failed récent (5j) → exclu
            "33333333333333",  # failed ancien (60j) → INCLUS (à retenter)
            "44444444444444",  # not_in_sirene → exclu
        ],
        "latitude": [48.0, None, None, None],
        "longitude": [2.0, None, None, None],
        "source": ["decp", "geoplateforme", "geoplateforme", "geoplateforme"],
        "score": [None, 0.2, 0.1, None],
        "geocoded_at": [None, today - timedelta(days=5), today - timedelta(days=60), today - timedelta(days=10)],
        "status": ["success", "failed", "failed", "not_in_sirene"],
    }, schema_overrides={"geocoded_at": pl.Date})

    result = select_sirets_to_geocode(lf_decp, lf_siret_latlong, today, retry_days).collect()
    sirets = set(result["siret"].to_list())
    assert sirets == {"33333333333333", "55555555555555"}
```

- [ ] **Step 2: Lancer pour confirmer l'échec**

Run: `pytest tests/test_enrich.py::test_select_sirets_excludes_success_and_recent_failures_and_not_in_sirene -v`
Expected: FAIL — `cannot import name 'select_sirets_to_geocode'`.

- [ ] **Step 3: Implémenter `select_sirets_to_geocode` dans `src/tasks/enrich.py`**

Ajouter (avec les imports nécessaires en haut du fichier : `from datetime import date, timedelta`) :

```python
def select_sirets_to_geocode(
    lf_decp: pl.LazyFrame,
    lf_siret_latlong: pl.LazyFrame,
    today: date,
    retry_days: int,
) -> pl.LazyFrame:
    """Retourne les SIRETs (de DECP) à géocoder, après exclusion de ceux déjà
    traités (success, not_in_sirene, ou failed récent)."""
    sirets_decp = (
        pl.concat([
            lf_decp.select(pl.col("acheteur_id").alias("siret")),
            lf_decp.select(pl.col("titulaire_id").alias("siret")),
        ])
        .filter(
            pl.col("siret").is_not_null()
            & (pl.col("siret").str.len_chars() == 14)
        )
        .unique()
    )

    cutoff = today - timedelta(days=retry_days)
    excluded = lf_siret_latlong.filter(
        (pl.col("status") == "success")
        | (pl.col("status") == "not_in_sirene")
        | (
            (pl.col("status") == "failed")
            & (pl.col("geocoded_at") >= cutoff)
        )
    ).select("siret")

    return sirets_decp.join(excluded, on="siret", how="anti")
```

- [ ] **Step 4: Lancer le test**

Run: `pytest tests/test_enrich.py::test_select_sirets_excludes_success_and_recent_failures_and_not_in_sirene -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
pre-commit run --all-files
git add src/tasks/enrich.py tests/test_enrich.py
git commit -m "Ajout de select_sirets_to_geocode avec negative caching #175"
```

---

## Task 9 : Orchestrateur `geocode_missing_sirets`

**Files:**

- Modify: `src/tasks/enrich.py`
- Test: `tests/test_enrich.py`

- [ ] **Step 1: Écrire le test bout en bout (API mockée)**

Ajouter dans `tests/test_enrich.py` :

```python
import httpx
import polars as pl
import pytest


def test_geocode_missing_sirets_appends_new_entries_and_marks_not_in_sirene(monkeypatch):
    today = date(2026, 5, 15)

    lf_decp = pl.LazyFrame({
        "acheteur_id": ["11111111111111", "99999999999999"],
        "titulaire_id": ["22222222222222", "99999999999999"],
    })
    lf_siret_latlong = pl.LazyFrame({
        "siret": ["00000000000000"],
        "latitude": [48.0],
        "longitude": [2.0],
        "source": ["decp"],
        "score": [None],
        "geocoded_at": [None],
        "status": ["success"],
    }, schema_overrides={"geocoded_at": pl.Date, "score": pl.Float64})
    # SIRENE n'a que 1111... et 2222... (pas 9999...)
    lf_etab = pl.LazyFrame({
        "siret": ["11111111111111", "22222222222222"],
        "numeroVoieEtablissement": ["10", "5"],
        "indiceRepetitionEtablissement": [None, None],
        "typeVoieEtablissement": ["RUE", "AVENUE"],
        "libelleVoieEtablissement": ["DE LA PAIX", "DE LYON"],
        "codePostalEtablissement": ["75001", "69001"],
        "codeCommuneEtablissement": ["75101", "69381"],
    })

    def handler(request: httpx.Request) -> httpx.Response:
        body = (
            "siret,q,postcode,citycode,result_score,latitude,longitude\n"
            "11111111111111,10 RUE DE LA PAIX,75001,75101,0.92,48.869,2.331\n"
            "22222222222222,5 AVENUE DE LYON,69001,69381,0.3,45.7,4.8\n"
        )
        return httpx.Response(200, content=body)

    transport = httpx.MockTransport(handler)
    monkeypatch.setattr(
        "src.tasks.geocode.httpx.Client",
        lambda **kw: httpx.Client(transport=transport, **{k: v for k, v in kw.items() if k != "transport"}),
    )
    monkeypatch.setattr("src.tasks.enrich.date", _FrozenDate(today))

    from src.tasks.enrich import geocode_missing_sirets
    updated = geocode_missing_sirets(lf_decp, lf_siret_latlong, lf_etab).collect()
    by_siret = {row["siret"]: row for row in updated.iter_rows(named=True)}

    # L'existant est conservé
    assert by_siret["00000000000000"]["status"] == "success"
    # 1111... → géocodé avec succès
    assert by_siret["11111111111111"]["status"] == "success"
    assert by_siret["11111111111111"]["latitude"] == 48.869
    # 2222... → score trop bas → failed
    assert by_siret["22222222222222"]["status"] == "failed"
    assert by_siret["22222222222222"]["latitude"] is None
    # 9999... → absent de SIRENE → not_in_sirene
    assert by_siret["99999999999999"]["status"] == "not_in_sirene"


class _FrozenDate:
    """Petit helper pour figer date.today() dans le test."""
    def __init__(self, value):
        self._value = value

    def today(self):
        return self._value

    def __call__(self, *args, **kwargs):
        # Pour permettre date(2026, 5, 15) si nécessaire
        return date(*args, **kwargs)


def test_geocode_missing_sirets_handles_api_failure_gracefully(monkeypatch):
    today = date(2026, 5, 15)
    lf_decp = pl.LazyFrame({
        "acheteur_id": ["11111111111111"],
        "titulaire_id": [None],
    })
    lf_siret_latlong = pl.LazyFrame(schema={
        "siret": pl.String, "latitude": pl.Float64, "longitude": pl.Float64,
        "source": pl.String, "score": pl.Float64, "geocoded_at": pl.Date,
        "status": pl.String,
    }).lazy() if False else pl.LazyFrame({
        "siret": [], "latitude": [], "longitude": [],
        "source": [], "score": [], "geocoded_at": [], "status": [],
    }, schema_overrides={
        "siret": pl.String, "latitude": pl.Float64, "longitude": pl.Float64,
        "source": pl.String, "score": pl.Float64, "geocoded_at": pl.Date,
        "status": pl.String,
    })
    lf_etab = pl.LazyFrame({
        "siret": ["11111111111111"],
        "numeroVoieEtablissement": ["10"],
        "indiceRepetitionEtablissement": [None],
        "typeVoieEtablissement": ["RUE"],
        "libelleVoieEtablissement": ["FOO"],
        "codePostalEtablissement": ["75001"],
        "codeCommuneEtablissement": ["75101"],
    })

    def handler(request):
        return httpx.Response(500)

    transport = httpx.MockTransport(handler)
    monkeypatch.setattr(
        "src.tasks.geocode.httpx.Client",
        lambda **kw: httpx.Client(transport=transport, **{k: v for k, v in kw.items() if k != "transport"}),
    )
    monkeypatch.setattr("src.tasks.enrich.date", _FrozenDate(today))

    from src.tasks.enrich import geocode_missing_sirets
    # Le flow ne doit pas planter même si l'API échoue
    updated = geocode_missing_sirets(lf_decp, lf_siret_latlong, lf_etab).collect()
    # Pas d'entrée géocodée ajoutée, mais le flow a survécu
    assert "11111111111111" not in set(updated["siret"].to_list())
```

- [ ] **Step 2: Lancer pour confirmer l'échec**

Run: `pytest tests/test_enrich.py::test_geocode_missing_sirets_appends_new_entries_and_marks_not_in_sirene -v`
Expected: FAIL — `cannot import name 'geocode_missing_sirets'`.

- [ ] **Step 3: Implémenter `geocode_missing_sirets` dans `src/tasks/enrich.py`**

Ajouter ces imports en haut si pas déjà présents :

```python
import httpx
from datetime import date
from src.config import (
    GEOCODING_RETRY_DAYS,
    LOG_LEVEL,
    SIRET_LATLONG_SCHEMA,
)
from src.tasks.geocode import geocode_csv
from src.tasks.utils import get_logger
```

Puis ajouter à la fin du fichier :

```python
def geocode_missing_sirets(
    lf_decp: pl.LazyFrame,
    lf_siret_latlong: pl.LazyFrame,
    lf_etablissements: pl.LazyFrame,
) -> pl.LazyFrame:
    """Géocode les SIRETs présents dans lf_decp mais pas encore dans siret_latlong
    (ou en échec récent), et retourne le siret_latlong mis à jour.

    Tolère un échec de l'API : dans ce cas, aucune entrée n'est ajoutée et le
    flow continue. Les SIRETs cibles seront retentés lors d'un prochain run.
    """
    logger = get_logger(level=LOG_LEVEL)
    today = date.today()

    to_geocode = select_sirets_to_geocode(
        lf_decp, lf_siret_latlong, today, GEOCODING_RETRY_DAYS
    )

    with_address = to_geocode.join(lf_etablissements, on="siret", how="inner")
    not_in_sirene = to_geocode.join(
        lf_etablissements.select("siret"), on="siret", how="anti"
    )

    not_in_sirene_entries = not_in_sirene.with_columns(
        pl.lit(None, dtype=pl.Float64).alias("latitude"),
        pl.lit(None, dtype=pl.Float64).alias("longitude"),
        pl.lit("geoplateforme").alias("source"),
        pl.lit(None, dtype=pl.Float64).alias("score"),
        pl.lit(today).alias("geocoded_at"),
        pl.lit("not_in_sirene").alias("status"),
    ).select(list(SIRET_LATLONG_SCHEMA.keys()))

    try:
        df_addresses = with_address.collect()
        if df_addresses.height > 0:
            geocoded = geocode_csv(df_addresses)
        else:
            geocoded = pl.DataFrame(schema=SIRET_LATLONG_SCHEMA)
    except httpx.HTTPError as exc:
        logger.warning(
            f"⚠️  API Géoplateforme indisponible ({exc}) — skip géocodage du jour"
        )
        geocoded = pl.DataFrame(schema=SIRET_LATLONG_SCHEMA)

    updated = pl.concat(
        [
            lf_siret_latlong.select(list(SIRET_LATLONG_SCHEMA.keys())),
            not_in_sirene_entries,
            geocoded.lazy(),
        ],
        how="vertical",
    ).unique(subset=["siret"], keep="last")

    return updated
```

- [ ] **Step 4: Lancer les tests de l'orchestrateur**

Run: `pytest tests/test_enrich.py::test_geocode_missing_sirets_appends_new_entries_and_marks_not_in_sirene tests/test_enrich.py::test_geocode_missing_sirets_handles_api_failure_gracefully -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
pre-commit run --all-files
git add src/tasks/enrich.py tests/test_enrich.py
git commit -m "Ajout de l'orchestrateur geocode_missing_sirets #175"
```

---

## Task 10 : Intégration dans le flow `decp_processing`

**Files:**

- Modify: `src/flows/decp_processing.py`
- Modify: `tests/test_main.py` (vérification que le test d'intégration tourne toujours)

- [ ] **Step 1: Repérer le point d'insertion dans `decp_processing`**

Ouvrir `src/flows/decp_processing.py` et localiser la ligne `lf: pl.LazyFrame = enrich_from_sirene(lf)` (ligne 113 environ). Le géocodage s'insère juste après cette ligne.

- [ ] **Step 2: Modifier les imports en tête de fichier**

```python
from src.tasks.enrich import (
    add_duree_restante,
    add_type_marche,
    enrich_from_sirene,
    geocode_missing_sirets,
)
from src.tasks.geocode import pad_siret_latlong_schema
from src.tasks.get import (
    bootstrap_siret_latlong,
    get_clean,
    get_from_s3,
)
from src.config import (
    BASE_DF_COLUMNS,
    DATA_DIR,
    DATE_NOW,
    DECP_PROCESSING_PUBLISH,
    DIST_DIR,
    LOG_LEVEL,
    MAX_PREFECT_WORKERS,
    PREFECT_API_URL,
    RESOURCE_CACHE_DIR,
    SIRENE_DATA_DIR,
    SOLO_DATASETS,
    TRACKED_DATASETS,
)
```

(N'ajouter `get_from_s3` et `bootstrap_siret_latlong` que s'ils ne sont pas déjà importés.)

- [ ] **Step 3: Insérer le bloc de géocodage après `enrich_from_sirene`**

Remplacer le bloc :

```python
lf: pl.LazyFrame = enrich_from_sirene(lf)

sink_to_files(lf, DIST_DIR / "decp", file_format="parquet")
```

par :

```python
lf: pl.LazyFrame = enrich_from_sirene(lf)

logger.info("Géocodage des SIRETs manquants...")
lf_siret_latlong = get_from_s3(key="siret_latlong.parquet", prefix="")
if not isinstance(lf_siret_latlong, pl.LazyFrame):
    lf_siret_latlong = bootstrap_siret_latlong()
lf_siret_latlong = pad_siret_latlong_schema(lf_siret_latlong)

lf_etab = pl.scan_parquet(SIRENE_DATA_DIR / "etablissements.parquet")
lf_siret_latlong_updated = geocode_missing_sirets(lf, lf_siret_latlong, lf_etab)

# Persistance et publication du cache mis à jour
siret_latlong_path = DATA_DIR / "siret_latlong.parquet"
lf_siret_latlong_updated.sink_parquet(siret_latlong_path)
if DECP_PROCESSING_PUBLISH:
    publish_to_s3(file=siret_latlong_path, prefix="")

sink_to_files(lf, DIST_DIR / "decp", file_format="parquet")
```

Note : `DECP_PROCESSING_PUBLISH` est nommé `decp_publish` localement dans la fonction (voir la fin du flow). Vérifier le nom de variable utilisé à ce point du flow et l'aligner.

- [ ] **Step 4: Lancer le test d'intégration principal**

Run: `pytest tests/test_main.py -v`
Expected: PASS. Si le test pioche dans un mini-jeu de données qui ne contient pas les colonnes SIRENE étendues, il faudra peut-être patcher `tests/data/source_datasets_test.json` ou mocker `get_from_s3` pour qu'il retourne None et donc déclenche un bootstrap factice. Sinon, monkeypatcher `geocode_missing_sirets` pour le faire devenir un no-op pendant ce test.

- [ ] **Step 5: Si le test d'intégration patine, ajouter un fixture de mock pour `geocode_missing_sirets`**

Si le test échoue à cause de l'appel HTTP réel (le mock httpx.MockTransport n'est branché que dans les tests unitaires), patcher dans `tests/test_main.py` :

```python
@pytest.fixture(autouse=True)
def _disable_geocoding(monkeypatch):
    """Skip le géocodage réel dans le test d'intégration principal."""
    import polars as pl
    from src.config import SIRET_LATLONG_SCHEMA

    def _noop(lf_decp, lf_siret_latlong, lf_etab):
        return lf_siret_latlong.select(list(SIRET_LATLONG_SCHEMA.keys()))

    monkeypatch.setattr("src.flows.decp_processing.geocode_missing_sirets", _noop)
```

Puis relancer.

- [ ] **Step 6: Commit**

```bash
pre-commit run --all-files
git add src/flows/decp_processing.py tests/test_main.py
git commit -m "Intégration du géocodage quotidien dans decp_processing #175"
```

---

## Atomicité / concurrence

`siret_latlong.parquet` est lu en début de flow et réécrit en fin. Si deux runs `decp_processing` se chevauchent, le dernier qui pousse écrase les entrées du premier. Avec le cron quotidien à 5h ce cas est très improbable. **Pas de lock S3** pour l'instant ; à revisiter si la fréquence augmente.

## TODO non couverts par ce plan

- Migration en prod du `siret_latlong.parquet` existant (la fonction `pad_siret_latlong_schema` traite ça à la volée au chargement, donc aucune action manuelle requise, mais on pourrait vouloir reuploader une version "officielle" du fichier au nouveau schéma dès le premier run en prod).
- Métriques : nombre de SIRETs géocodés / échoués / not_in_sirene par run, à exposer dans un artifact Prefect (peut suivre dans une seconde itération).
- Revalidation périodique des entrées `status="success"` issues du bootstrap (`source="decp"`) pour lesquelles on n'a pas de score — utile si on suspecte de mauvaises coords héritées d'Etalab.
- Backfill massif : si après un premier déploiement on veut géocoder en masse tous les SIRETs DECP absents de `siret_latlong`, l'orchestrateur le fera automatiquement mais lentement (un seul `decp_processing` par jour avec chunking). À envisager : tâche `geocode_backfill` indépendante avec rate-limit explicite.
