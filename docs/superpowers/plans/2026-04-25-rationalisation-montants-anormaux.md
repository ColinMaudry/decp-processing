# Rationalisation des montants anormaux — Plan d'implémentation

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Ajouter à la sortie du pipeline `decp-processing` trois colonnes (`montant_rationalise`, `montant_anomalie`, `montant_anomalie_raison`) qui détectent les montants saisis erronément (typiquement des forfaitaires/maximums surdimensionnés) et fournissent une valeur agrégeable sans biais, tout en conservant le `montant` déclaré.

**Architecture:** Une nouvelle task Prefect `detect_montant_anomalies` est insérée entre l'enrichissement SIRENE/type et le calcul des stats. Elle joint un référentiel population (CSV externe), calcule des statistiques par groupe de pairs (CPV à 2 chiffres × type × catégorie acheteur, stratifié par tranche de population pour les communes), et applique deux signaux d'anomalie (écart aux pairs sur log + ratio €/habitant) plus un modulateur sur la cohérence titulaire/montant. Tout reste en `LazyFrame` streaming.

**Tech Stack:** Python, Polars (LazyFrame), Prefect, pytest. Spec de référence : `docs/superpowers/specs/2026-04-25-montants-anormaux-rationalisation-design.md`.

---

## Cartographie des fichiers

**Nouveaux fichiers** :

- `src/tasks/anomaly.py` — module de détection (helpers + orchestrateur `detect_montant_anomalies`)
- `tests/test_anomaly.py` — tests unitaires
- `tests/data/identifiants-communes-test.csv` — petit CSV pour les tests (5-10 communes)
- `script/calibrate_anomaly_thresholds.py` — script one-shot de calibration

**Fichiers modifiés** :

- `reference/schema_base.json` — 3 nouveaux champs frictionless
- `src/config.py` — constantes de seuils, tranches, chemin du CSV population
- `src/flows/decp_processing.py` — branchement de la task entre `add_type_marche` et `generate_stats`
- `tests/test_main.py` — vérification que les nouvelles colonnes sont présentes en sortie

**Fichier référentiel attendu (fourni hors de cette implémentation)** :

- `data/identifiants-communes.csv` — colonnes `SIREN` (clé) et `population`. Si absent en local, l'engineer doit récupérer une copie minimale pour ses tests manuels.

---

## Task 1 : Schéma de sortie et constantes de config

**Files:**

- Modify: `reference/schema_base.json`
- Modify: `src/config.py`
- Test: lancement de la suite existante pour vérifier la non-régression

- [ ] **Step 1 : Ajouter les 3 champs à `schema_base.json`**

Insérer ces 3 entrées à la fin du tableau `fields` (juste avant `sourceDataset` pour rester proche des champs dérivés) :

```json
{
  "type": "number",
  "name": "montant_rationalise",
  "title": "Montant rationalisé",
  "description": "Égal à 'montant' sauf pour les marchés classés 'aberrant' (montant_anomalie='aberrant'), où il prend la valeur de la médiane des marchés similaires. À utiliser pour les agrégations (sommes, moyennes) afin d'éviter le biais des montants saisis erronément.",
  "short_title": "Montant rationalisé"
},
{
  "type": "string",
  "name": "montant_anomalie",
  "title": "Anomalie de montant",
  "description": "Indique si le montant déclaré est jugé anormal par comparaison avec les marchés similaires. 'suspect' = anomalie possible, montant conservé ; 'aberrant' = anomalie forte, montant remplacé dans 'montant_rationalise'.",
  "short_title": "Anomalie",
  "enum": ["suspect", "aberrant"]
},
{
  "type": "string",
  "name": "montant_anomalie_raison",
  "title": "Raison de l'anomalie",
  "description": "Code lisible expliquant le critère ayant déclenché la classification : montant_par_habitant_aberrant, montant_par_habitant_suspect, montant_vs_pairs_aberrant, montant_vs_pairs_suspect, titulaire_incoherent_pme_gros_marche, ou groupe_insuffisant.",
  "short_title": "Raison anomalie"
}
```

- [ ] **Step 2 : Ajouter les constantes de config dans `src/config.py`**

À la fin de la section "Données de référence" (juste après `BASE_DF_COLUMNS`) :

```python
# === Détection des anomalies de montant ===
# Voir docs/superpowers/specs/2026-04-25-montants-anormaux-rationalisation-design.md

# Chemin du CSV population des communes (fourni hors pipeline)
POPULATION_COMMUNES_CSV = BASE_DIR / "data" / "identifiants-communes.csv"

# Seuils Signal A (écart MAD sur log du montant normalisé)
ANOMALY_PAIRS_SUSPECT_THRESHOLD = 4.0
ANOMALY_PAIRS_ABERRANT_THRESHOLD = 6.0

# Seuils Signal B (€/habitant, par type de marché)
ANOMALY_HABITANT_THRESHOLDS = {
    "Travaux":     {"suspect":  5_000, "aberrant": 20_000},
    "Services":    {"suspect":  1_000, "aberrant":  5_000},
    "Fournitures": {"suspect":    500, "aberrant":  2_000},
}

# Seuil Signal C (modulateur titulaire PME)
ANOMALY_TITULAIRE_PME_MONTANT_SEUIL = 50_000_000

# Tranches de population (bornes inférieures) pour la stratification du Signal A
POPULATION_TRANCHES = [
    (0,       "très petite"),
    (2_000,   "petite"),
    (10_000,  "moyenne"),
    (50_000,  "grande"),
    (200_000, "très grande"),
]

# Taille minimale d'un groupe de pairs pour calculer une statistique fiable
ANOMALY_GROUPE_MIN_SIZE = 30
```

- [ ] **Step 3 : Vérifier que la suite de tests passe encore**

Run: `pytest tests/test_clean.py tests/test_enrich.py tests/test_transform.py tests/test_stats.py -q`
Expected: tous les tests passent (les nouvelles colonnes ne sont pas encore produites mais BASE_DF_COLUMNS les contient désormais via le schéma — c'est OK car aucun test ne valide l'absence d'une colonne).

- [ ] **Step 4 : Commit**

```bash
rtk pre-commit run --files reference/schema_base.json src/config.py
rtk git add reference/schema_base.json src/config.py
rtk git commit -m "Schéma : 3 nouveaux champs anomalie de montant + constantes config #174

Co-Authored-By: Claude Opus 4.7 <noreply@anthropic.com>"
```

---

## Task 2 : Helper `compute_tranche_population_expr`

**Files:**

- Create: `src/tasks/anomaly.py`
- Create: `tests/test_anomaly.py`

- [ ] **Step 1 : Écrire le test d'échec**

Créer `tests/test_anomaly.py` :

```python
import polars as pl
from polars.testing import assert_frame_equal

from src.tasks.anomaly import compute_tranche_population_expr


class TestTranchePopulation:
    def test_tranches_bornes(self):
        lf_in = pl.LazyFrame({
            "population": [None, 0, 1_999, 2_000, 9_999, 10_000, 49_999, 50_000, 199_999, 200_000, 1_000_000],
        })
        lf_out = lf_in.with_columns(compute_tranche_population_expr())
        result = lf_out.collect()["tranche_population"].to_list()
        assert result == [
            None,
            "très petite", "très petite",
            "petite", "petite",
            "moyenne", "moyenne",
            "grande", "grande",
            "très grande", "très grande",
        ]
```

- [ ] **Step 2 : Lancer le test pour vérifier l'échec**

Run: `pytest tests/test_anomaly.py::TestTranchePopulation -v`
Expected: ImportError ou ModuleNotFoundError sur `src.tasks.anomaly`.

- [ ] **Step 3 : Créer `src/tasks/anomaly.py` avec `compute_tranche_population_expr`**

```python
import polars as pl


def compute_tranche_population_expr(col: str = "population") -> pl.Expr:
    """Renvoie une expression polars qui calcule la tranche de population.

    Renvoie null si la population est null. Les bornes sont exclusives à droite :
    [0, 2000) → 'très petite', [2000, 10000) → 'petite', etc.
    """
    return (
        pl.when(pl.col(col).is_null()).then(None)
        .when(pl.col(col) < 2_000).then(pl.lit("très petite"))
        .when(pl.col(col) < 10_000).then(pl.lit("petite"))
        .when(pl.col(col) < 50_000).then(pl.lit("moyenne"))
        .when(pl.col(col) < 200_000).then(pl.lit("grande"))
        .otherwise(pl.lit("très grande"))
        .alias("tranche_population")
    )
```

- [ ] **Step 4 : Lancer le test pour vérifier le succès**

Run: `pytest tests/test_anomaly.py::TestTranchePopulation -v`
Expected: PASS.

- [ ] **Step 5 : Commit**

```bash
rtk pre-commit run --files src/tasks/anomaly.py tests/test_anomaly.py
rtk git add src/tasks/anomaly.py tests/test_anomaly.py
rtk git commit -m "Helper tranche_population pour la stratification des communes #174

Co-Authored-By: Claude Opus 4.7 <noreply@anthropic.com>"
```

---

## Task 3 : Helper `montant_normalise_expr`

**Files:**

- Modify: `src/tasks/anomaly.py`
- Modify: `tests/test_anomaly.py`

- [ ] **Step 1 : Écrire le test d'échec**

Ajouter dans `tests/test_anomaly.py` :

```python
from src.tasks.anomaly import compute_tranche_population_expr, montant_normalise_expr


class TestMontantNormalise:
    def test_normalisation_par_duree_pour_services_non_forfaitaire(self):
        lf = pl.LazyFrame({
            "montant": [120_000.0, 120_000.0, 120_000.0, 120_000.0, 120_000.0],
            "dureeMois": [12, 12, 12, 0, 1],
            "type": ["Services", "Services", "Travaux", "Services", "Services"],
            "formePrix": ["Unitaire", "Forfaitaire", "Unitaire", "Unitaire", "Unitaire"],
        })
        result = lf.with_columns(montant_normalise_expr()).collect()["montant_normalise"].to_list()
        # 1: Services + Unitaire + 12 mois → 120000/12 = 10000
        # 2: Services + Forfaitaire → pas de division → 120000
        # 3: Travaux → pas de division → 120000
        # 4: dureeMois=0 → pas de division → 120000
        # 5: dureeMois=1 → pas de division (condition > 1) → 120000
        assert result == [10_000.0, 120_000.0, 120_000.0, 120_000.0, 120_000.0]

    def test_normalisation_avec_montant_null(self):
        lf = pl.LazyFrame({
            "montant": [None],
            "dureeMois": [12],
            "type": ["Services"],
            "formePrix": ["Unitaire"],
        }, schema={"montant": pl.Float64, "dureeMois": pl.Int64, "type": pl.Utf8, "formePrix": pl.Utf8})
        result = lf.with_columns(montant_normalise_expr()).collect()["montant_normalise"].to_list()
        assert result == [None]
```

- [ ] **Step 2 : Lancer le test pour vérifier l'échec**

Run: `pytest tests/test_anomaly.py::TestMontantNormalise -v`
Expected: ImportError sur `montant_normalise_expr`.

- [ ] **Step 3 : Ajouter `montant_normalise_expr` dans `src/tasks/anomaly.py`**

```python
def montant_normalise_expr() -> pl.Expr:
    """Normalise le montant par mois pour Services/Fournitures non forfaitaires.

    Pour les Travaux ou les marchés Forfaitaires, le montant est ponctuel et n'est
    pas divisé. Pour Services/Fournitures avec dureeMois > 1 et formePrix != 'Forfaitaire',
    on divise par la durée pour comparer des €/mois aux pairs.
    """
    return (
        pl.when(
            pl.col("type").is_in(["Services", "Fournitures"])
            & (pl.col("dureeMois") > 1)
            & (pl.col("formePrix") != "Forfaitaire")
        )
        .then(pl.col("montant") / pl.col("dureeMois"))
        .otherwise(pl.col("montant"))
        .alias("montant_normalise")
    )
```

- [ ] **Step 4 : Lancer le test pour vérifier le succès**

Run: `pytest tests/test_anomaly.py::TestMontantNormalise -v`
Expected: PASS (les 2 tests).

- [ ] **Step 5 : Commit**

```bash
rtk pre-commit run --files src/tasks/anomaly.py tests/test_anomaly.py
rtk git add src/tasks/anomaly.py tests/test_anomaly.py
rtk git commit -m "Helper montant_normalise pour la comparaison aux pairs #174

Co-Authored-By: Claude Opus 4.7 <noreply@anthropic.com>"
```

---

## Task 4 : Jointure population via SIREN

**Files:**

- Modify: `src/tasks/anomaly.py`
- Modify: `tests/test_anomaly.py`
- Create: `tests/data/identifiants-communes-test.csv`

- [ ] **Step 1 : Créer un mini-CSV de test**

Créer `tests/data/identifiants-communes-test.csv` :

```csv
SIREN,nom,population
210054002,Lyon,520000
210130557,Marseille,870000
210590350,Lille,235000
217500016,Petite-Commune,1500
```

- [ ] **Step 2 : Écrire le test d'échec**

Ajouter dans `tests/test_anomaly.py` :

```python
from src.config import BASE_DIR
from src.tasks.anomaly import join_population


class TestJoinPopulation:
    def test_join_population_via_siren(self):
        # acheteur_id = SIRET (14 chiffres). SIREN = 9 premiers chiffres.
        lf = pl.LazyFrame({
            "acheteur_id": [21005400200012, 21013055700019, 12345678900012, None],
            "uid": ["A", "B", "C", "D"],
        }, schema={"acheteur_id": pl.Int64, "uid": pl.Utf8})

        csv_path = BASE_DIR / "tests/data/identifiants-communes-test.csv"
        result = join_population(lf, csv_path).collect()

        # Vérifier l'ordre par uid (le join peut le perturber)
        result = result.sort("uid")
        assert result["population"].to_list() == [520000, 870000, None, None]
```

- [ ] **Step 3 : Lancer le test pour vérifier l'échec**

Run: `pytest tests/test_anomaly.py::TestJoinPopulation -v`
Expected: ImportError sur `join_population`.

- [ ] **Step 4 : Ajouter `join_population` dans `src/tasks/anomaly.py`**

```python
from pathlib import Path


def join_population(lf: pl.LazyFrame, population_csv_path: Path) -> pl.LazyFrame:
    """Joint le LazyFrame avec le CSV des communes via le SIREN extrait de acheteur_id.

    SIREN = 9 premiers chiffres du SIRET = acheteur_id // 100_000.
    Renvoie le LazyFrame avec une nouvelle colonne 'population' (null si non trouvé).
    """
    population_lf = pl.scan_csv(population_csv_path).select(
        pl.col("SIREN").cast(pl.Int64),
        pl.col("population").cast(pl.Int64),
    )
    return (
        lf.with_columns(
            (pl.col("acheteur_id") // 100_000).alias("_siren_acheteur")
        )
        .join(population_lf, left_on="_siren_acheteur", right_on="SIREN", how="left")
        .drop("_siren_acheteur")
    )
```

- [ ] **Step 5 : Lancer le test pour vérifier le succès**

Run: `pytest tests/test_anomaly.py::TestJoinPopulation -v`
Expected: PASS.

- [ ] **Step 6 : Commit**

```bash
rtk pre-commit run --files src/tasks/anomaly.py tests/test_anomaly.py tests/data/identifiants-communes-test.csv
rtk git add src/tasks/anomaly.py tests/test_anomaly.py tests/data/identifiants-communes-test.csv
rtk git commit -m "Jointure population des communes via SIREN #174

Co-Authored-By: Claude Opus 4.7 <noreply@anthropic.com>"
```

---

## Task 5 : Statistiques par groupe de pairs avec fallback

**Files:**

- Modify: `src/tasks/anomaly.py`
- Modify: `tests/test_anomaly.py`

Cette task calcule, pour chaque marché, la médiane et la MAD du log du montant normalisé sur le groupe de pairs le plus spécifique avec ≥ 30 marchés. Quatre niveaux possibles :

| Niveau | Clé                                                     |
| ------ | ------------------------------------------------------- |
| L4     | `(cpv_2, type, acheteur_categorie, tranche_population)` |
| L3     | `(cpv_2, type, acheteur_categorie)`                     |
| L2     | `(type, acheteur_categorie)`                            |
| L1     | `(type,)`                                               |

Pour les marchés sans population, `tranche_population` est null donc le niveau L4 ne matche jamais → on commence à L3.

- [ ] **Step 1 : Écrire le test d'échec**

Ajouter dans `tests/test_anomaly.py` :

```python
from src.tasks.anomaly import compute_peer_group_stats


class TestPeerGroupStats:
    def test_stats_l3_groupe_suffisant(self):
        # Créer 35 marchés Travaux/Commune avec montants log-uniformes autour de 100_000
        # Plus 1 outlier à 100_000_000 → forte MAD attendue
        import math
        normaux = [100_000.0 * math.exp((i % 10 - 5) * 0.1) for i in range(35)]
        montants = normaux + [100_000_000.0]

        lf = pl.LazyFrame({
            "uid": [f"U{i}" for i in range(36)],
            "montant_normalise": montants,
            "log_montant_normalise": [math.log10(m + 1) for m in montants],
            "codeCPV_2": ["45"] * 36,  # Construction
            "type": ["Travaux"] * 36,
            "acheteur_categorie": ["Commune"] * 36,
            "tranche_population": [None] * 36,
        })

        result = compute_peer_group_stats(lf, min_size=30).collect()

        # n_groupe doit être 36 pour tous (groupe L3 suffisant)
        assert (result["n_groupe"] == 36).all()
        # niveau_groupe doit être "L3" pour tous
        assert (result["niveau_groupe"] == "L3").all()
        # mediane_log doit être proche de log10(100_001) ≈ 5.0
        median_log = result["mediane_log"].to_list()[0]
        assert 4.9 < median_log < 5.1

    def test_stats_groupe_insuffisant(self):
        # 5 marchés seulement → aucun niveau ne suffit → niveau_groupe = null
        lf = pl.LazyFrame({
            "uid": [f"U{i}" for i in range(5)],
            "montant_normalise": [100.0] * 5,
            "log_montant_normalise": [2.0] * 5,
            "codeCPV_2": ["99"] * 5,
            "type": ["Fournitures"] * 5,
            "acheteur_categorie": ["État"] * 5,
            "tranche_population": [None] * 5,
        })
        result = compute_peer_group_stats(lf, min_size=30).collect()
        assert result["niveau_groupe"].to_list() == [None] * 5
        assert result["mediane_log"].to_list() == [None] * 5
```

- [ ] **Step 2 : Lancer le test pour vérifier l'échec**

Run: `pytest tests/test_anomaly.py::TestPeerGroupStats -v`
Expected: ImportError sur `compute_peer_group_stats`.

- [ ] **Step 3 : Implémenter `compute_peer_group_stats`**

Ajouter dans `src/tasks/anomaly.py` :

```python
def compute_peer_group_stats(lf: pl.LazyFrame, min_size: int) -> pl.LazyFrame:
    """Calcule les statistiques médiane/MAD du log du montant normalisé par groupe de pairs.

    Quatre niveaux de granularité, du plus spécifique au plus large :
    - L4 : (cpv_2, type, acheteur_categorie, tranche_population)
    - L3 : (cpv_2, type, acheteur_categorie)
    - L2 : (type, acheteur_categorie)
    - L1 : (type,)

    Pour chaque marché, on retient les stats du niveau le plus spécifique dont le groupe
    a au moins `min_size` marchés. Si aucun niveau ne suffit, mediane_log/mad_log/median_montant_norm
    restent null et niveau_groupe est null.

    Le LazyFrame en entrée doit contenir les colonnes : codeCPV_2, type, acheteur_categorie,
    tranche_population, montant_normalise, log_montant_normalise.

    Renvoie le LazyFrame enrichi avec : n_groupe, niveau_groupe, mediane_log, mad_log,
    median_montant_norm.
    """
    levels = [
        ("L4", ["codeCPV_2", "type", "acheteur_categorie", "tranche_population"]),
        ("L3", ["codeCPV_2", "type", "acheteur_categorie"]),
        ("L2", ["type", "acheteur_categorie"]),
        ("L1", ["type"]),
    ]

    for name, keys in levels:
        stats = (
            lf.group_by(keys)
            .agg(
                pl.len().alias(f"n_{name}"),
                pl.col("log_montant_normalise").median().alias(f"mediane_log_{name}"),
                (pl.col("log_montant_normalise") - pl.col("log_montant_normalise").median())
                    .abs().median().alias(f"mad_log_{name}"),
                pl.col("montant_normalise").median().alias(f"median_montant_norm_{name}"),
            )
        )
        lf = lf.join(stats, on=keys, how="left")

    # Pour chaque niveau, ne garder les stats que si n >= min_size
    # Puis coalesce du plus spécifique au plus large
    lf = lf.with_columns(
        pl.coalesce([
            pl.when(pl.col(f"n_{name}") >= min_size).then(pl.col(f"n_{name}"))
            for name, _ in levels
        ]).alias("n_groupe"),
        pl.coalesce([
            pl.when(pl.col(f"n_{name}") >= min_size).then(pl.lit(name))
            for name, _ in levels
        ]).alias("niveau_groupe"),
        pl.coalesce([
            pl.when(pl.col(f"n_{name}") >= min_size).then(pl.col(f"mediane_log_{name}"))
            for name, _ in levels
        ]).alias("mediane_log"),
        pl.coalesce([
            pl.when(pl.col(f"n_{name}") >= min_size).then(pl.col(f"mad_log_{name}"))
            for name, _ in levels
        ]).alias("mad_log"),
        pl.coalesce([
            pl.when(pl.col(f"n_{name}") >= min_size).then(pl.col(f"median_montant_norm_{name}"))
            for name, _ in levels
        ]).alias("median_montant_norm"),
    )

    # Drop les colonnes intermédiaires de niveau
    cols_to_drop = []
    for name, _ in levels:
        cols_to_drop += [f"n_{name}", f"mediane_log_{name}", f"mad_log_{name}", f"median_montant_norm_{name}"]
    return lf.drop(cols_to_drop)
```

- [ ] **Step 4 : Lancer les tests pour vérifier le succès**

Run: `pytest tests/test_anomaly.py::TestPeerGroupStats -v`
Expected: PASS (les 2 tests).

- [ ] **Step 5 : Commit**

```bash
rtk pre-commit run --files src/tasks/anomaly.py tests/test_anomaly.py
rtk git add src/tasks/anomaly.py tests/test_anomaly.py
rtk git commit -m "Statistiques par groupe de pairs avec fallback 4 niveaux #174

Co-Authored-By: Claude Opus 4.7 <noreply@anthropic.com>"
```

---

## Task 6 : Calcul des signaux A et B

**Files:**

- Modify: `src/tasks/anomaly.py`
- Modify: `tests/test_anomaly.py`

- [ ] **Step 1 : Écrire le test d'échec**

Ajouter dans `tests/test_anomaly.py` :

```python
from src.tasks.anomaly import compute_signals


class TestSignals:
    def test_ecart_pairs_calcul(self):
        # Marché à montant_normalise = 100_000 (log10 = 5.0), groupe avec mediane_log=5.0, mad_log=0.5
        # ecart_pairs = (5.0 - 5.0) / 0.5 = 0
        # Pour un montant_normalise = 100_000_000 (log10 = 8.0) : ecart = (8.0 - 5.0) / 0.5 = 6
        lf = pl.LazyFrame({
            "montant_normalise": [100_000.0, 100_000_000.0, 100_000.0],
            "log_montant_normalise": [5.0, 8.0, 5.0],
            "mediane_log": [5.0, 5.0, None],  # Dernier : groupe insuffisant
            "mad_log": [0.5, 0.5, None],
            "montant": [100_000.0, 100_000_000.0, 100_000.0],
            "population": [10_000, 10_000, None],
            "type": ["Services", "Services", "Services"],
        })
        result = compute_signals(lf).collect()
        assert result["ecart_pairs"].to_list() == [0.0, 6.0, None]
        # Signal B : montant_par_habitant = 10 € pour ligne 1, 10000 € pour ligne 2, null pour ligne 3
        assert result["montant_par_habitant"].to_list() == [10.0, 10_000.0, None]

    def test_ecart_pairs_mad_zero_renvoie_null(self):
        # MAD nulle (groupe homogène) → division par zéro évitée → ecart_pairs null
        lf = pl.LazyFrame({
            "montant_normalise": [100_000.0],
            "log_montant_normalise": [5.0],
            "mediane_log": [5.0],
            "mad_log": [0.0],
            "montant": [100_000.0],
            "population": [None],
            "type": ["Services"],
        }, schema_overrides={"population": pl.Int64})
        result = compute_signals(lf).collect()
        assert result["ecart_pairs"].to_list() == [None]
```

- [ ] **Step 2 : Lancer le test pour vérifier l'échec**

Run: `pytest tests/test_anomaly.py::TestSignals -v`
Expected: ImportError sur `compute_signals`.

- [ ] **Step 3 : Implémenter `compute_signals`**

Ajouter dans `src/tasks/anomaly.py` :

```python
def compute_signals(lf: pl.LazyFrame) -> pl.LazyFrame:
    """Calcule ecart_pairs (Signal A) et montant_par_habitant (Signal B).

    ecart_pairs = (log_montant_normalise - mediane_log) / mad_log, null si mad_log nulle ou null.
    montant_par_habitant = montant / population, null si population null ou nulle.
    """
    return lf.with_columns(
        pl.when(pl.col("mad_log").is_null() | (pl.col("mad_log") == 0))
        .then(None)
        .otherwise(
            (pl.col("log_montant_normalise") - pl.col("mediane_log")) / pl.col("mad_log")
        )
        .alias("ecart_pairs"),
        pl.when(pl.col("population").is_null() | (pl.col("population") == 0))
        .then(None)
        .otherwise(pl.col("montant") / pl.col("population"))
        .alias("montant_par_habitant"),
    )
```

- [ ] **Step 4 : Lancer le test pour vérifier le succès**

Run: `pytest tests/test_anomaly.py::TestSignals -v`
Expected: PASS (les 2 tests).

- [ ] **Step 5 : Commit**

```bash
rtk pre-commit run --files src/tasks/anomaly.py tests/test_anomaly.py
rtk git add src/tasks/anomaly.py tests/test_anomaly.py
rtk git commit -m "Calcul des signaux ecart_pairs et montant_par_habitant #174

Co-Authored-By: Claude Opus 4.7 <noreply@anthropic.com>"
```

---

## Task 7 : Classification (suspect/aberrant + raisons + modulateur titulaire)

**Files:**

- Modify: `src/tasks/anomaly.py`
- Modify: `tests/test_anomaly.py`

- [ ] **Step 1 : Écrire les tests d'échec**

Ajouter dans `tests/test_anomaly.py` :

```python
from src.config import (
    ANOMALY_HABITANT_THRESHOLDS,
    ANOMALY_PAIRS_ABERRANT_THRESHOLD,
    ANOMALY_PAIRS_SUSPECT_THRESHOLD,
    ANOMALY_TITULAIRE_PME_MONTANT_SEUIL,
)
from src.tasks.anomaly import classify_anomalies


class TestClassification:
    def test_pairs_suspect(self):
        # ecart_pairs entre 4 et 6 → suspect, raison montant_vs_pairs_suspect
        lf = pl.LazyFrame({
            "ecart_pairs": [4.5],
            "montant_par_habitant": [None],
            "type": ["Services"],
            "titulaire_categorie": [None],
            "montant": [100_000.0],
        }, schema_overrides={"montant_par_habitant": pl.Float64})
        result = classify_anomalies(lf).collect()
        assert result["montant_anomalie"].to_list() == ["suspect"]
        assert result["montant_anomalie_raison"].to_list() == ["montant_vs_pairs_suspect"]

    def test_pairs_aberrant(self):
        lf = pl.LazyFrame({
            "ecart_pairs": [10.0],
            "montant_par_habitant": [None],
            "type": ["Services"],
            "titulaire_categorie": [None],
            "montant": [100_000.0],
        }, schema_overrides={"montant_par_habitant": pl.Float64})
        result = classify_anomalies(lf).collect()
        assert result["montant_anomalie"].to_list() == ["aberrant"]
        assert result["montant_anomalie_raison"].to_list() == ["montant_vs_pairs_aberrant"]

    def test_habitant_aberrant_prime_sur_pairs(self):
        # Les deux signaux déclenchent ; habitant a la priorité dans la raison
        lf = pl.LazyFrame({
            "ecart_pairs": [10.0],
            "montant_par_habitant": [25_000.0],  # Travaux aberrant > 20 000
            "type": ["Travaux"],
            "titulaire_categorie": [None],
            "montant": [100_000_000.0],
        })
        result = classify_anomalies(lf).collect()
        assert result["montant_anomalie"].to_list() == ["aberrant"]
        assert result["montant_anomalie_raison"].to_list() == ["montant_par_habitant_aberrant"]

    def test_modulateur_pme_escalade_suspect_en_aberrant(self):
        # Suspect via Signal A + PME + montant > 50 M€ → escalade en aberrant
        lf = pl.LazyFrame({
            "ecart_pairs": [4.5],
            "montant_par_habitant": [None],
            "type": ["Services"],
            "titulaire_categorie": ["PME"],
            "montant": [60_000_000.0],
        }, schema_overrides={"montant_par_habitant": pl.Float64})
        result = classify_anomalies(lf).collect()
        assert result["montant_anomalie"].to_list() == ["aberrant"]
        assert result["montant_anomalie_raison"].to_list() == ["titulaire_incoherent_pme_gros_marche"]

    def test_modulateur_pme_inactif_si_montant_sous_seuil(self):
        lf = pl.LazyFrame({
            "ecart_pairs": [4.5],
            "montant_par_habitant": [None],
            "type": ["Services"],
            "titulaire_categorie": ["PME"],
            "montant": [10_000_000.0],  # < 50 M€
        }, schema_overrides={"montant_par_habitant": pl.Float64})
        result = classify_anomalies(lf).collect()
        assert result["montant_anomalie"].to_list() == ["suspect"]
        assert result["montant_anomalie_raison"].to_list() == ["montant_vs_pairs_suspect"]

    def test_pas_d_anomalie(self):
        lf = pl.LazyFrame({
            "ecart_pairs": [2.0],
            "montant_par_habitant": [100.0],
            "type": ["Services"],
            "titulaire_categorie": ["GE"],
            "montant": [50_000.0],
        })
        result = classify_anomalies(lf).collect()
        assert result["montant_anomalie"].to_list() == [None]
        assert result["montant_anomalie_raison"].to_list() == [None]
```

- [ ] **Step 2 : Lancer les tests pour vérifier l'échec**

Run: `pytest tests/test_anomaly.py::TestClassification -v`
Expected: ImportError sur `classify_anomalies`.

- [ ] **Step 3 : Implémenter `classify_anomalies`**

Ajouter dans `src/tasks/anomaly.py` :

```python
from src.config import (
    ANOMALY_HABITANT_THRESHOLDS,
    ANOMALY_PAIRS_ABERRANT_THRESHOLD,
    ANOMALY_PAIRS_SUSPECT_THRESHOLD,
    ANOMALY_TITULAIRE_PME_MONTANT_SEUIL,
)


def classify_anomalies(lf: pl.LazyFrame) -> pl.LazyFrame:
    """Applique la logique de classification suspect/aberrant et choisit la raison principale.

    Le LazyFrame en entrée doit contenir : ecart_pairs, montant_par_habitant, type,
    titulaire_categorie, montant.

    Renvoie le LazyFrame enrichi avec montant_anomalie (null/suspect/aberrant) et
    montant_anomalie_raison (code parmi montant_par_habitant_*, montant_vs_pairs_*,
    titulaire_incoherent_pme_gros_marche).
    """
    # Construire les seuils habitant en colonnes (par type)
    suspect_habitant_expr = (
        pl.when(pl.col("type") == "Travaux").then(pl.lit(ANOMALY_HABITANT_THRESHOLDS["Travaux"]["suspect"]))
        .when(pl.col("type") == "Services").then(pl.lit(ANOMALY_HABITANT_THRESHOLDS["Services"]["suspect"]))
        .when(pl.col("type") == "Fournitures").then(pl.lit(ANOMALY_HABITANT_THRESHOLDS["Fournitures"]["suspect"]))
        .otherwise(None)
    )
    aberrant_habitant_expr = (
        pl.when(pl.col("type") == "Travaux").then(pl.lit(ANOMALY_HABITANT_THRESHOLDS["Travaux"]["aberrant"]))
        .when(pl.col("type") == "Services").then(pl.lit(ANOMALY_HABITANT_THRESHOLDS["Services"]["aberrant"]))
        .when(pl.col("type") == "Fournitures").then(pl.lit(ANOMALY_HABITANT_THRESHOLDS["Fournitures"]["aberrant"]))
        .otherwise(None)
    )

    # Flags par signal
    lf = lf.with_columns(
        # Signal A
        pairs_aberrant=pl.col("ecart_pairs") > ANOMALY_PAIRS_ABERRANT_THRESHOLD,
        pairs_suspect=(pl.col("ecart_pairs") > ANOMALY_PAIRS_SUSPECT_THRESHOLD)
                      & (pl.col("ecart_pairs") <= ANOMALY_PAIRS_ABERRANT_THRESHOLD),
        # Signal B
        habitant_aberrant=pl.col("montant_par_habitant").is_not_null()
                          & (pl.col("montant_par_habitant") > aberrant_habitant_expr),
        habitant_suspect=pl.col("montant_par_habitant").is_not_null()
                         & (pl.col("montant_par_habitant") > suspect_habitant_expr)
                         & ~(pl.col("montant_par_habitant") > aberrant_habitant_expr),
    )

    # Classification initiale
    lf = lf.with_columns(
        classification_initiale=pl.when(
            pl.col("pairs_aberrant") | pl.col("habitant_aberrant")
        ).then(pl.lit("aberrant"))
        .when(pl.col("pairs_suspect") | pl.col("habitant_suspect"))
        .then(pl.lit("suspect"))
        .otherwise(None),
    )

    # Modulateur titulaire : suspect + PME + montant > seuil → aberrant
    lf = lf.with_columns(
        modulateur_titulaire=(pl.col("classification_initiale") == "suspect")
                             & (pl.col("titulaire_categorie") == "PME")
                             & (pl.col("montant") > ANOMALY_TITULAIRE_PME_MONTANT_SEUIL),
    )

    lf = lf.with_columns(
        montant_anomalie=pl.when(pl.col("modulateur_titulaire"))
                         .then(pl.lit("aberrant"))
                         .otherwise(pl.col("classification_initiale")),
    )

    # Sélection de la raison (priorité : habitant > pairs > titulaire)
    # Format : <signal>_<niveau> où niveau correspond à la classification finale
    lf = lf.with_columns(
        montant_anomalie_raison=pl.when(pl.col("montant_anomalie").is_null())
        .then(None)
        .when(pl.col("habitant_aberrant"))
        .then(pl.lit("montant_par_habitant_aberrant"))
        .when(pl.col("habitant_suspect") & (pl.col("montant_anomalie") == "suspect"))
        .then(pl.lit("montant_par_habitant_suspect"))
        .when(pl.col("pairs_aberrant"))
        .then(pl.lit("montant_vs_pairs_aberrant"))
        .when(pl.col("pairs_suspect") & (pl.col("montant_anomalie") == "suspect"))
        .then(pl.lit("montant_vs_pairs_suspect"))
        .when(pl.col("modulateur_titulaire"))
        .then(pl.lit("titulaire_incoherent_pme_gros_marche"))
        .otherwise(None),
    )

    # Drop les colonnes intermédiaires
    return lf.drop([
        "pairs_aberrant", "pairs_suspect",
        "habitant_aberrant", "habitant_suspect",
        "classification_initiale", "modulateur_titulaire",
    ])
```

- [ ] **Step 4 : Lancer les tests pour vérifier le succès**

Run: `pytest tests/test_anomaly.py::TestClassification -v`
Expected: PASS (les 6 tests).

- [ ] **Step 5 : Commit**

```bash
rtk pre-commit run --files src/tasks/anomaly.py tests/test_anomaly.py
rtk git add src/tasks/anomaly.py tests/test_anomaly.py
rtk git commit -m "Classification suspect/aberrant + modulateur titulaire + raisons #174

Co-Authored-By: Claude Opus 4.7 <noreply@anthropic.com>"
```

---

## Task 8 : Calcul de `montant_rationalise`

**Files:**

- Modify: `src/tasks/anomaly.py`
- Modify: `tests/test_anomaly.py`

- [ ] **Step 1 : Écrire les tests d'échec**

Ajouter dans `tests/test_anomaly.py` :

```python
from src.tasks.anomaly import compute_montant_rationalise


class TestMontantRationalise:
    def test_aberrant_remplace_par_mediane_x_duree(self):
        # Marché aberrant Services, dureeMois=12, formePrix=Unitaire → normalisation appliquée
        # mediane montant_normalise du groupe = 8000 €/mois → rationalise = 8000*12 = 96000
        lf = pl.LazyFrame({
            "montant": [10_000_000.0],
            "montant_anomalie": ["aberrant"],
            "median_montant_norm": [8_000.0],
            "type": ["Services"],
            "dureeMois": [12],
            "formePrix": ["Unitaire"],
        })
        result = compute_montant_rationalise(lf).collect()
        assert result["montant_rationalise"].to_list() == [96_000.0]

    def test_aberrant_travaux_pas_de_normalisation(self):
        # Travaux : pas de normalisation → median_montant_norm est en € totaux
        lf = pl.LazyFrame({
            "montant": [10_000_000.0],
            "montant_anomalie": ["aberrant"],
            "median_montant_norm": [500_000.0],
            "type": ["Travaux"],
            "dureeMois": [24],
            "formePrix": ["Forfaitaire"],
        })
        result = compute_montant_rationalise(lf).collect()
        assert result["montant_rationalise"].to_list() == [500_000.0]

    def test_suspect_garde_montant_origine(self):
        lf = pl.LazyFrame({
            "montant": [10_000_000.0],
            "montant_anomalie": ["suspect"],
            "median_montant_norm": [8_000.0],
            "type": ["Services"],
            "dureeMois": [12],
            "formePrix": ["Unitaire"],
        })
        result = compute_montant_rationalise(lf).collect()
        assert result["montant_rationalise"].to_list() == [10_000_000.0]

    def test_pas_d_anomalie_garde_montant(self):
        lf = pl.LazyFrame({
            "montant": [50_000.0],
            "montant_anomalie": [None],
            "median_montant_norm": [None],
            "type": ["Services"],
            "dureeMois": [None],
            "formePrix": [None],
        }, schema_overrides={
            "montant_anomalie": pl.Utf8, "median_montant_norm": pl.Float64,
            "dureeMois": pl.Int64, "formePrix": pl.Utf8,
        })
        result = compute_montant_rationalise(lf).collect()
        assert result["montant_rationalise"].to_list() == [50_000.0]

    def test_aberrant_groupe_insuffisant_donne_null(self):
        # median_montant_norm null (groupe insuffisant) → montant_rationalise null
        lf = pl.LazyFrame({
            "montant": [10_000_000.0],
            "montant_anomalie": ["aberrant"],
            "median_montant_norm": [None],
            "type": ["Services"],
            "dureeMois": [12],
            "formePrix": ["Unitaire"],
        }, schema_overrides={"median_montant_norm": pl.Float64})
        result = compute_montant_rationalise(lf).collect()
        assert result["montant_rationalise"].to_list() == [None]
```

- [ ] **Step 2 : Lancer les tests pour vérifier l'échec**

Run: `pytest tests/test_anomaly.py::TestMontantRationalise -v`
Expected: ImportError sur `compute_montant_rationalise`.

- [ ] **Step 3 : Implémenter `compute_montant_rationalise`**

Ajouter dans `src/tasks/anomaly.py` :

```python
def compute_montant_rationalise(lf: pl.LazyFrame) -> pl.LazyFrame:
    """Calcule montant_rationalise.

    - Si montant_anomalie != 'aberrant' → montant_rationalise = montant
    - Si montant_anomalie == 'aberrant' :
        - Si normalisation par durée appliquée (Services/Fournitures + dureeMois>1 + non-Forfaitaire) :
            montant_rationalise = median_montant_norm × dureeMois
        - Sinon : montant_rationalise = median_montant_norm (déjà en € totaux)
        - Si median_montant_norm null (groupe insuffisant) : montant_rationalise = null

    Le LazyFrame en entrée doit contenir : montant, montant_anomalie, median_montant_norm,
    type, dureeMois, formePrix.
    """
    normalisation_appliquee = (
        pl.col("type").is_in(["Services", "Fournitures"])
        & (pl.col("dureeMois") > 1)
        & (pl.col("formePrix") != "Forfaitaire")
    )

    return lf.with_columns(
        montant_rationalise=pl.when(pl.col("montant_anomalie") != "aberrant")
        .then(pl.col("montant"))
        .when(pl.col("median_montant_norm").is_null())
        .then(None)
        .when(normalisation_appliquee)
        .then(pl.col("median_montant_norm") * pl.col("dureeMois"))
        .otherwise(pl.col("median_montant_norm")),
    )
```

- [ ] **Step 4 : Lancer les tests pour vérifier le succès**

Run: `pytest tests/test_anomaly.py::TestMontantRationalise -v`
Expected: PASS (les 5 tests).

- [ ] **Step 5 : Commit**

```bash
rtk pre-commit run --files src/tasks/anomaly.py tests/test_anomaly.py
rtk git add src/tasks/anomaly.py tests/test_anomaly.py
rtk git commit -m "Calcul de montant_rationalise pour les marchés aberrants #174

Co-Authored-By: Claude Opus 4.7 <noreply@anthropic.com>"
```

---

## Task 9 : Orchestrateur `detect_montant_anomalies` + nettoyage des colonnes

**Files:**

- Modify: `src/tasks/anomaly.py`
- Modify: `tests/test_anomaly.py`

- [ ] **Step 1 : Écrire le test d'intégration**

Ajouter dans `tests/test_anomaly.py` :

```python
from src.tasks.anomaly import detect_montant_anomalies


class TestDetectMontantAnomalies:
    def test_pipeline_complet_sur_petit_dataset(self):
        # Construire un mini-dataset avec :
        # - 35 marchés Services normaux (~100k €) pour Lyon (commune dans le CSV de test)
        # - 1 outlier à 10 milliards €
        import math
        n_normaux = 35
        normaux = [{
            "uid": f"N{i}",
            "acheteur_id": 21005400200012,  # Lyon, dans le CSV de test
            "montant": 100_000.0 * math.exp((i % 5 - 2) * 0.1),
            "type": "Services",
            "codeCPV": "72000000",  # Services informatiques (CPV 72)
            "dureeMois": 12,
            "formePrix": "Unitaire",
            "acheteur_categorie": "Commune",
            "titulaire_categorie": "GE",
        } for i in range(n_normaux)]

        outlier = [{
            "uid": "OUTLIER",
            "acheteur_id": 21005400200012,
            "montant": 10_000_000_000.0,  # 10 G€
            "type": "Services",
            "codeCPV": "72000000",
            "dureeMois": 12,
            "formePrix": "Unitaire",
            "acheteur_categorie": "Commune",
            "titulaire_categorie": "GE",
        }]

        lf = pl.LazyFrame(normaux + outlier)

        csv_path = BASE_DIR / "tests/data/identifiants-communes-test.csv"
        result = detect_montant_anomalies(lf, csv_path).collect()

        # Le pipeline doit avoir ajouté exactement 3 colonnes : montant_rationalise, montant_anomalie, montant_anomalie_raison
        added = set(result.columns) - set(lf.collect().columns)
        assert added == {"montant_rationalise", "montant_anomalie", "montant_anomalie_raison"}

        # L'outlier doit être classé aberrant
        outlier_row = result.filter(pl.col("uid") == "OUTLIER").to_dicts()[0]
        assert outlier_row["montant_anomalie"] == "aberrant"
        # montant_rationalise doit être très inférieur à 10 G€ (proche de 100k * 12)
        assert outlier_row["montant_rationalise"] is not None
        assert outlier_row["montant_rationalise"] < 1_000_000

        # Les marchés normaux ne doivent pas être flaggés
        normaux_anomalies = result.filter(pl.col("uid") != "OUTLIER")["montant_anomalie"].to_list()
        assert all(a is None for a in normaux_anomalies)
        # Pour les non-aberrants, montant_rationalise == montant
        normaux_check = result.filter(pl.col("uid") != "OUTLIER")
        assert (normaux_check["montant"] == normaux_check["montant_rationalise"]).all()
```

- [ ] **Step 2 : Lancer le test pour vérifier l'échec**

Run: `pytest tests/test_anomaly.py::TestDetectMontantAnomalies -v`
Expected: ImportError sur `detect_montant_anomalies`.

- [ ] **Step 3 : Implémenter l'orchestrateur**

Ajouter dans `src/tasks/anomaly.py` (en début de fichier, après les imports) :

```python
from prefect import task

from src.config import ANOMALY_GROUPE_MIN_SIZE, POPULATION_COMMUNES_CSV
```

Et à la fin du fichier :

```python
@task
def detect_montant_anomalies(
    lf: pl.LazyFrame,
    population_csv_path: Path = POPULATION_COMMUNES_CSV,
) -> pl.LazyFrame:
    """Task Prefect : détecte les anomalies de montant et calcule montant_rationalise.

    Ajoute trois colonnes au LazyFrame en entrée :
    - montant_rationalise : montant utilisable pour les agrégations
    - montant_anomalie : null, 'suspect', ou 'aberrant'
    - montant_anomalie_raison : code lisible du critère déclenché
    """
    # 1. Joindre la population
    lf = join_population(lf, population_csv_path)

    # 2. Préparer les colonnes intermédiaires
    lf = lf.with_columns(
        compute_tranche_population_expr(),
        montant_normalise_expr(),
        # CPV à 2 chiffres
        pl.col("codeCPV").str.slice(0, 2).alias("codeCPV_2"),
    )
    lf = lf.with_columns(
        log_montant_normalise=(pl.col("montant_normalise") + 1).log10(),
    )

    # 3. Calcul des stats par groupe avec fallback
    lf = compute_peer_group_stats(lf, min_size=ANOMALY_GROUPE_MIN_SIZE)

    # 4. Calcul des deux signaux
    lf = compute_signals(lf)

    # 5. Classification suspect/aberrant + raison
    lf = classify_anomalies(lf)

    # 6. Calcul de montant_rationalise
    lf = compute_montant_rationalise(lf)

    # 7. Drop des colonnes intermédiaires (ne garder que les 3 nouvelles)
    intermediaire = [
        "population", "tranche_population",
        "montant_normalise", "log_montant_normalise", "codeCPV_2",
        "n_groupe", "niveau_groupe", "mediane_log", "mad_log", "median_montant_norm",
        "ecart_pairs", "montant_par_habitant",
    ]
    return lf.drop([c for c in intermediaire if c in lf.collect_schema().names()])
```

- [ ] **Step 4 : Lancer le test pour vérifier le succès**

Run: `pytest tests/test_anomaly.py::TestDetectMontantAnomalies -v`
Expected: PASS.

- [ ] **Step 5 : Lancer toute la suite anomaly**

Run: `pytest tests/test_anomaly.py -v`
Expected: tous les tests PASS.

- [ ] **Step 6 : Commit**

```bash
rtk pre-commit run --files src/tasks/anomaly.py tests/test_anomaly.py
rtk git add src/tasks/anomaly.py tests/test_anomaly.py
rtk git commit -m "Orchestrateur Prefect detect_montant_anomalies #174

Co-Authored-By: Claude Opus 4.7 <noreply@anthropic.com>"
```

---

## Task 10 : Branchement dans le flow + extension de `test_main`

**Files:**

- Modify: `src/flows/decp_processing.py`
- Modify: `tests/test_main.py`

- [ ] **Step 1 : Lire le test_main actuel pour comprendre son fonctionnement**

Run: `cat tests/test_main.py`

L'engineer doit comprendre comment le flow est lancé en test et comment vérifier les colonnes de sortie. Le test attendu : après le run, les 3 colonnes existent dans le parquet de sortie.

- [ ] **Step 2 : Étendre `tests/test_main.py` avec un assert sur les nouvelles colonnes**

Ajouter dans `tests/test_main.py` (après l'assert existant qui vérifie le run du flow) :

```python
# Vérifier que les 3 nouvelles colonnes liées à l'anomalie de montant sont présentes
import polars as pl
from src.config import DIST_DIR

lf_out = pl.scan_parquet(DIST_DIR / "decp.parquet")
cols = lf_out.collect_schema().names()
assert "montant_rationalise" in cols
assert "montant_anomalie" in cols
assert "montant_anomalie_raison" in cols
```

(Adapter selon la structure exacte du test existant ; la suite peut nécessiter d'utiliser les fixtures du test.)

- [ ] **Step 3 : Lancer le test pour vérifier l'échec**

Run: `pytest tests/test_main.py -v`
Expected: AssertionError sur les colonnes manquantes (le flow ne produit pas encore ces colonnes).

- [ ] **Step 4 : Brancher la task dans le flow**

Dans `src/flows/decp_processing.py`, ajouter l'import :

```python
from src.tasks.anomaly import detect_montant_anomalies
```

Insérer l'appel entre `add_type_marche(lf)` (ligne ~122) et `calculate_naf_cpv_matching(lf)` (ligne ~125) :

```python
    logger.info("Détection des anomalies de montant...")
    lf = detect_montant_anomalies(lf)
```

- [ ] **Step 5 : Lancer le test pour vérifier le succès**

Run: `pytest tests/test_main.py -v`
Expected: PASS.

- [ ] **Step 6 : Lancer toute la suite**

Run: `pytest -q`
Expected: tous les tests PASS.

- [ ] **Step 7 : Commit**

```bash
rtk pre-commit run --files src/flows/decp_processing.py tests/test_main.py
rtk git add src/flows/decp_processing.py tests/test_main.py
rtk git commit -m "Branchement detect_montant_anomalies dans le flow principal #174

Co-Authored-By: Claude Opus 4.7 <noreply@anthropic.com>"
```

---

## Task 11 : Artifact Prefect d'observabilité

**Files:**

- Modify: `src/tasks/anomaly.py`
- Modify: `tests/test_anomaly.py`

- [ ] **Step 1 : Écrire le test d'échec**

Ajouter dans `tests/test_anomaly.py` :

```python
from src.tasks.anomaly import build_anomaly_summary


class TestAnomalySummary:
    def test_summary_compte_aberrants_et_suspects(self):
        lf = pl.LazyFrame({
            "montant": [100.0, 200.0, 300.0, 400.0, 500.0],
            "montant_rationalise": [100.0, 200.0, 50.0, 400.0, 500.0],
            "montant_anomalie": ["suspect", None, "aberrant", "aberrant", None],
            "montant_anomalie_raison": [
                "montant_vs_pairs_suspect",
                None,
                "montant_par_habitant_aberrant",
                "montant_par_habitant_aberrant",
                None,
            ],
        })
        summary = build_anomaly_summary(lf)
        assert summary["nb_suspects"] == 1
        assert summary["nb_aberrants"] == 2
        assert summary["sum_montant"] == 1500.0
        assert summary["sum_montant_rationalise"] == 1250.0
        assert summary["top_raisons"][0]["raison"] == "montant_par_habitant_aberrant"
        assert summary["top_raisons"][0]["count"] == 2
```

- [ ] **Step 2 : Lancer le test pour vérifier l'échec**

Run: `pytest tests/test_anomaly.py::TestAnomalySummary -v`
Expected: ImportError sur `build_anomaly_summary`.

- [ ] **Step 3 : Implémenter `build_anomaly_summary` et intégrer l'artifact dans la task**

Ajouter dans `src/tasks/anomaly.py` :

```python
from prefect.artifacts import create_markdown_artifact


def build_anomaly_summary(lf: pl.LazyFrame) -> dict:
    """Construit un résumé des anomalies pour observabilité.

    Implémentation lazy : on ne matérialise que les agrégats, pas le LazyFrame entier.
    Renvoie un dict avec : nb_suspects, nb_aberrants, sum_montant, sum_montant_rationalise,
    top_raisons (liste de dicts {raison, count}).
    """
    counts = lf.select(
        nb_suspects=(pl.col("montant_anomalie") == "suspect").sum(),
        nb_aberrants=(pl.col("montant_anomalie") == "aberrant").sum(),
        sum_montant=pl.col("montant").sum().fill_null(0),
        sum_montant_rationalise=pl.col("montant_rationalise").sum().fill_null(0),
    ).collect().to_dicts()[0]

    top_raisons_df = (
        lf.filter(pl.col("montant_anomalie_raison").is_not_null())
        .group_by("montant_anomalie_raison")
        .agg(pl.len().alias("count"))
        .sort("count", descending=True)
        .head(20)
        .collect()
    )

    return {
        "nb_suspects": int(counts["nb_suspects"]),
        "nb_aberrants": int(counts["nb_aberrants"]),
        "sum_montant": float(counts["sum_montant"]),
        "sum_montant_rationalise": float(counts["sum_montant_rationalise"]),
        "top_raisons": [
            {"raison": row["montant_anomalie_raison"], "count": int(row["count"])}
            for row in top_raisons_df.to_dicts()
        ],
    }


def _create_anomaly_artifact(summary: dict) -> None:
    """Crée un artifact markdown Prefect avec le résumé des anomalies.

    Enveloppé dans try/except pour ne pas casser les tests unitaires lancés
    hors d'un run Prefect (où create_markdown_artifact lève une RuntimeError).
    """
    diff_pct = (
        100.0 * (summary["sum_montant"] - summary["sum_montant_rationalise"]) / summary["sum_montant"]
        if summary["sum_montant"] > 0 else 0.0
    )
    raisons_md = "\n".join(
        f"- `{r['raison']}` : {r['count']}" for r in summary["top_raisons"]
    )
    md = f"""# Détection des anomalies de montant

## Comptage

- Marchés `suspect` : **{summary['nb_suspects']}**
- Marchés `aberrant` : **{summary['nb_aberrants']}**

## Impact sur les agrégations

- Somme des montants déclarés : **{summary['sum_montant']:,.0f} €**
- Somme des montants rationalisés : **{summary['sum_montant_rationalise']:,.0f} €**
- Différence : **{diff_pct:.2f} %**

## Top 20 raisons

{raisons_md}
"""
    try:
        create_markdown_artifact(markdown=md, key="montant-anomalies-summary")
    except Exception:
        # Hors run Prefect (tests unitaires) : on log seulement
        pass
```

Modifier l'orchestrateur `detect_montant_anomalies` pour appeler l'artifact (juste avant le `return`) :

```python
    # Forcer un collect pour produire l'artifact (matérialise une seule fois)
    summary = build_anomaly_summary(lf.select(
        "montant", "montant_rationalise", "montant_anomalie", "montant_anomalie_raison"
    ))
    _create_anomaly_artifact(summary)

    return lf.drop([c for c in intermediaire if c in lf.collect_schema().names()])
```

- [ ] **Step 4 : Lancer les tests pour vérifier le succès**

Run: `pytest tests/test_anomaly.py -v`
Expected: tous les tests PASS, y compris l'intégration (`TestDetectMontantAnomalies`).

> Note : `create_markdown_artifact` ne fonctionne réellement que sous un run Prefect. En test unitaire, il peut soit échouer silencieusement, soit lever une erreur. Si le test d'intégration `TestDetectMontantAnomalies` échoue à cause de ça, l'engineer doit envelopper l'appel d'artifact dans un `try/except` ou utiliser `prefect_test_harness`.

- [ ] **Step 5 : Commit**

```bash
rtk pre-commit run --files src/tasks/anomaly.py tests/test_anomaly.py
rtk git add src/tasks/anomaly.py tests/test_anomaly.py
rtk git commit -m "Artifact Prefect de résumé des anomalies #174

Co-Authored-By: Claude Opus 4.7 <noreply@anthropic.com>"
```

---

## Task 12 : Script de calibration des seuils

**Files:**

- Create: `script/calibrate_anomaly_thresholds.py`

- [ ] **Step 1 : Créer le script**

Créer `script/calibrate_anomaly_thresholds.py` :

```python
"""Script one-shot de calibration des seuils d'anomalie de montant.

Lit le parquet de sortie le plus récent et produit un CSV listant les marchés
flaggés par configuration de seuils. Permet de valider visuellement avant de
figer les valeurs définitives dans src/config.py.

Usage : python script/calibrate_anomaly_thresholds.py [--parquet PATH]
"""
import argparse
import math
from pathlib import Path

import polars as pl

from src.config import ANOMALY_HABITANT_THRESHOLDS, BASE_DIR, DIST_DIR
from src.tasks.anomaly import (
    classify_anomalies,
    compute_montant_rationalise,
    compute_peer_group_stats,
    compute_signals,
    compute_tranche_population_expr,
    join_population,
    montant_normalise_expr,
)

OUT_DIR = BASE_DIR / "data" / "calibration"


def calibrate(parquet_path: Path, pairs_grid: list[float]) -> None:
    OUT_DIR.mkdir(parents=True, exist_ok=True)

    lf = pl.scan_parquet(parquet_path)

    # Préparation des colonnes intermédiaires (mêmes étapes que detect_montant_anomalies sans la classification)
    lf = join_population(lf, BASE_DIR / "data" / "identifiants-communes.csv")
    lf = lf.with_columns(
        compute_tranche_population_expr(),
        montant_normalise_expr(),
        pl.col("codeCPV").str.slice(0, 2).alias("codeCPV_2"),
    )
    lf = lf.with_columns(log_montant_normalise=(pl.col("montant_normalise") + 1).log10())
    lf = compute_peer_group_stats(lf, min_size=30)
    lf = compute_signals(lf)

    df = lf.collect()
    print(f"Marchés analysés : {len(df):,}")

    # Pour chaque seuil de la grille, compter les flags
    rows = []
    for suspect_thr in pairs_grid:
        for aberrant_thr in pairs_grid:
            if aberrant_thr <= suspect_thr:
                continue
            n_suspect = int(((df["ecart_pairs"] > suspect_thr) & (df["ecart_pairs"] <= aberrant_thr)).sum())
            n_aberrant = int((df["ecart_pairs"] > aberrant_thr).sum())
            rows.append({
                "pairs_suspect": suspect_thr,
                "pairs_aberrant": aberrant_thr,
                "n_suspect": n_suspect,
                "n_aberrant": n_aberrant,
            })

    out_df = pl.DataFrame(rows)
    out_path = OUT_DIR / "grille_seuils_pairs.csv"
    out_df.write_csv(out_path)
    print(f"Grille écrite : {out_path}")

    # Top 50 marchés aberrants par montant déclaré (pour validation visuelle)
    top_aberrants = (
        df.filter(pl.col("ecart_pairs") > 6.0)
        .sort("montant", descending=True)
        .head(50)
        .select(["uid", "acheteur_nom", "titulaire_nom", "objet", "montant", "type", "codeCPV", "ecart_pairs"])
    )
    top_path = OUT_DIR / "top_aberrants.csv"
    top_aberrants.write_csv(top_path)
    print(f"Top aberrants écrit : {top_path}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--parquet", type=Path, default=DIST_DIR / "decp.parquet")
    args = parser.parse_args()

    pairs_grid = [3.0, 3.5, 4.0, 4.5, 5.0, 5.5, 6.0, 6.5, 7.0]
    calibrate(args.parquet, pairs_grid)
```

- [ ] **Step 2 : Vérifier que le script est syntaxiquement correct (sans le lancer, le parquet n'existe pas forcément)**

Run: `python -c "import ast; ast.parse(open('script/calibrate_anomaly_thresholds.py').read()); print('OK')"`
Expected: `OK`.

- [ ] **Step 3 : Commit**

```bash
rtk pre-commit run --files script/calibrate_anomaly_thresholds.py
rtk git add script/calibrate_anomaly_thresholds.py
rtk git commit -m "Script de calibration des seuils d'anomalie #174

Co-Authored-By: Claude Opus 4.7 <noreply@anthropic.com>"
```

---

## Vérifications finales

- [ ] **Lancer toute la suite de tests**

Run: `pytest -q`
Expected: tous les tests passent.

- [ ] **Vérifier les imports absolus**

Run: `python script/check_imports.py src/tasks/anomaly.py`
Expected: aucun warning.

- [ ] **Lancer pre-commit sur tous les fichiers modifiés**

Run: `pre-commit run --all-files`
Expected: tout passe.

- [ ] **Smoke test : lancer un sirene_preprocess (si pas déjà fait) puis decp_processing en local sur un mini dataset**

Run :

```bash
python run_flow.py sirene_preprocess  # une seule fois par mois
DATASETS_REFERENCE_FILEPATH=tests/data/source_datasets_test.json python run_flow.py decp_processing
```

Expected : le run se termine, le parquet `dist/decp.parquet` contient les 3 nouvelles colonnes, et l'artifact Prefect "montant-anomalies-summary" est visible dans l'UI Prefect (ou les logs).

- [ ] **Préparer la doc utilisateur (à inclure dans la PR ou dans une issue de suivi)**

Une fois la calibration finalisée par le mainteneur sur des données réelles, mettre à jour le README et les métadonnées du dataset publié sur data.gouv.fr avec une section "Rationalisation des montants" expliquant : pourquoi cette colonne existe, comment elle est calculée, quand utiliser `montant` vs `montant_rationalise`, et comment interpréter `montant_anomalie_raison`. (Voir spec section "Documentation utilisateur".)
