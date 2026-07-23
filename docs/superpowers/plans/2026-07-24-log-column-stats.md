# Log des statistiques par colonne (valeurs distinctes / % null) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Logger, pour chaque colonne du jeu de données final, le nombre de valeurs distinctes (hors null) et le pourcentage de valeurs nulles, au moment où les statistiques de publication sont déjà générées dans le flow `decp_processing`.

**Architecture:** Une nouvelle fonction pure `log_column_stats(lf, nb_lignes)` dans `src/tasks/utils.py`, à côté de `generate_stats`. Elle calcule `null_count` et `n_unique` pour toutes les colonnes en un seul `.collect()` (une seule passe sur les données), corrige le comptage `n_unique` pour ne pas compter `null` comme une valeur distincte, puis émet un unique `logger.info(...)` avec un tableau texte aligné. Appelée depuis `generate_stats()`, juste après le calcul de `nb_lignes` existant (valeur réutilisée, pas de collect supplémentaire pour le compte de lignes).

**Tech Stack:** Python 3, Polars (LazyFrame), pytest (avec `unittest.mock.patch.object`, pas `caplog` — voir Global Constraints).

## Global Constraints

- Lancer les tests avec `uv run pytest ...` (jamais `.venv/bin/pytest` ni `pytest` nu).
- `caplog` ne capture PAS les logs émis via le logger Prefect utilisé dans `src/tasks/utils.py` (`from src.config import logger`, qui vient de `prefect.logging.get_logger`) : Prefect écrit directement sur stderr via son propre handler, en dehors de l'arbre `logging` standard que `caplog` intercepte. Vérifié empiriquement : un test utilisant `caplog.at_level(...)` + `caplog.text` échoue silencieusement (le message apparaît dans "Captured stderr" mais pas dans `caplog.text`). **Utiliser `unittest.mock.patch.object(utils, "logger")` pour intercepter les appels à `logger.info` dans les tests.**
- `n_unique()` sur une colonne Polars compte `null` comme une valeur distincte à part entière (ex. `[100, 200, None]` → `n_unique() == 3`). Le nombre de valeurs distinctes _non-null_ rapporté doit être `n_unique - 1` si `null_count > 0`, sinon `n_unique` tel quel — même convention que `nb_acheteurs_uniques = df_uid["acheteur_id"].n_unique() - 1` déjà présent dans `generate_stats`.
- Pas de nouvel artefact Prefect (`create_table_artifact`) pour cette donnée — uniquement un log texte (hors scope, cf. spec).
- Spec de référence : `docs/superpowers/specs/2026-07-24-log-column-stats-design.md`.

---

## File Structure

- `src/tasks/utils.py` (modifier) — nouvelle fonction `log_column_stats`, appel ajouté dans `generate_stats`
- `tests/test_stats.py` (modifier) — tests unitaires de `log_column_stats`

---

## Task 1: Implémenter `log_column_stats`

**Files:**

- Modify: `src/tasks/utils.py`
- Test: `tests/test_stats.py`

**Interfaces:**

- Produces: `log_column_stats(lf: pl.LazyFrame, nb_lignes: int) -> None` dans `src/tasks/utils.py`, ne retourne rien, émet un seul `logger.info(...)`.

- [ ] **Step 1: Ouvrir `tests/test_stats.py` et ajouter les imports nécessaires en tête de fichier**

Le fichier commence actuellement par :

```python
import polars as pl
import pytest

from src.tasks.utils import calculate_duplicates_across_source
```

Remplacer ces lignes par :

```python
from unittest.mock import patch

import polars as pl
import pytest

from src.tasks import utils
from src.tasks.utils import calculate_duplicates_across_source, log_column_stats
```

- [ ] **Step 2: Écrire le test qui échoue, à la fin de `tests/test_stats.py`**

Ajouter :

```python
def test_log_column_stats_reports_distinct_and_null_percentage():
    titulaire_schema = pl.Struct({"a": pl.String, "b": pl.Int64})
    lf = pl.LazyFrame(
        {
            "montant": [100, 200, None],
            "nom": ["a", "b", "a"],
            "titulaires": [[{"a": "x", "b": 1}], [{"a": "y", "b": 2}], None],
        },
        schema={
            "montant": pl.Int64,
            "nom": pl.String,
            "titulaires": pl.List(titulaire_schema),
        },
    )

    with patch.object(utils, "logger") as mock_logger:
        log_column_stats(lf, nb_lignes=3)

    mock_logger.info.assert_called_once()
    message = mock_logger.info.call_args[0][0]

    # montant : 2 valeurs distinctes hors null (100, 200), 1/3 null
    assert "montant" in message
    lines = {line.split()[0]: line for line in message.splitlines() if line.strip()}
    assert lines["montant"].split()[1] == "2"
    assert "33.33%" in lines["montant"]

    # nom : 2 valeurs distinctes (a, b), 0% null
    assert lines["nom"].split()[1] == "2"
    assert "0.00%" in lines["nom"]

    # titulaires (List(Struct)) : 2 valeurs distinctes hors null, 1/3 null
    assert lines["titulaires"].split()[1] == "2"
    assert "33.33%" in lines["titulaires"]


def test_log_column_stats_handles_column_without_nulls():
    lf = pl.LazyFrame({"id": [1, 2, 3, 4]}, schema={"id": pl.Int64})

    with patch.object(utils, "logger") as mock_logger:
        log_column_stats(lf, nb_lignes=4)

    message = mock_logger.info.call_args[0][0]
    line = next(line for line in message.splitlines() if line.startswith("id"))
    assert line.split()[1] == "4"
    assert "0.00%" in line
```

- [ ] **Step 3: Lancer les tests pour vérifier qu'ils échouent (fonction pas encore définie)**

Run: `uv run pytest tests/test_stats.py -v`
Expected: `ImportError: cannot import name 'log_column_stats' from 'src.tasks.utils'`

- [ ] **Step 4: Repérer l'emplacement d'insertion dans `src/tasks/utils.py`**

`generate_stats` est définie section `# STATS` (juste après le commentaire `# Statistiques pour toutes les données collectées ce jour`, ligne ~122). `log_column_stats` doit être ajoutée juste avant `generate_stats`, dans la même section.

- [ ] **Step 5: Écrire l'implémentation minimale de `log_column_stats`**

Insérer dans `src/tasks/utils.py`, juste avant la ligne `# Statistiques pour toutes les données collectées ce jour` / `def generate_stats(lf: pl.LazyFrame):` :

```python
def log_column_stats(lf: pl.LazyFrame, nb_lignes: int) -> None:
    """Log, pour chaque colonne, son nombre de valeurs distinctes (hors null) et son % de valeurs nulles."""
    columns = lf.collect_schema().names()

    exprs = []
    for col in columns:
        exprs.append(pl.col(col).null_count().alias(f"{col}__null_count"))
        exprs.append(pl.col(col).n_unique().alias(f"{col}__n_unique"))

    stats_row = lf.select(exprs).collect().row(0, named=True)

    header = f"{'colonne':<40}{'valeurs distinctes':>20}{'% null':>10}"
    lines = [header, "-" * len(header)]

    for col in sorted(columns):
        null_count = stats_row[f"{col}__null_count"]
        n_unique = stats_row[f"{col}__n_unique"]
        n_unique_non_null = n_unique - 1 if null_count > 0 else n_unique
        pct_null = (null_count / nb_lignes * 100) if nb_lignes > 0 else 0.0
        lines.append(f"{col:<40}{n_unique_non_null:>20}{pct_null:>9.2f}%")

    logger.info(
        "Statistiques par colonne (valeurs distinctes / % null) :\n" + "\n".join(lines)
    )
```

- [ ] **Step 6: Lancer les tests pour vérifier qu'ils passent**

Run: `uv run pytest tests/test_stats.py -v`
Expected: `test_log_column_stats_reports_distinct_and_null_percentage` et `test_log_column_stats_handles_column_without_nulls` passent, ainsi que `test_calculate_duplicates_across_source` (non régressé).

- [ ] **Step 7: Commit**

```bash
git add src/tasks/utils.py tests/test_stats.py
git commit -m "$(cat <<'EOF'
Ajoute log_column_stats (valeurs distinctes / % null par colonne)

Co-Authored-By: Claude Sonnet 5 <noreply@anthropic.com>
EOF
)"
```

---

## Task 2: Brancher `log_column_stats` dans `generate_stats`

**Files:**

- Modify: `src/tasks/utils.py`

**Interfaces:**

- Consumes: `log_column_stats(lf: pl.LazyFrame, nb_lignes: int) -> None` (Task 1)

- [ ] **Step 1: Repérer le point d'appel dans `generate_stats`**

Dans `src/tasks/utils.py`, la ligne à modifier est :

```python
    # 2. Counts
    nb_lignes = lf.select(pl.len()).collect().item()
    nb_marches = len(df_uid)
```

- [ ] **Step 2: Ajouter l'appel juste après le calcul de `nb_lignes`**

Remplacer par :

```python
    # 2. Counts
    nb_lignes = lf.select(pl.len()).collect().item()
    log_column_stats(lf, nb_lignes)
    nb_marches = len(df_uid)
```

- [ ] **Step 3: Vérifier que le fichier reste syntaxiquement valide et que les imports sont inchangés**

Run: `uv run python -c "from src.tasks.utils import generate_stats, log_column_stats; print('ok')"`
Expected: `ok`

- [ ] **Step 4: Lancer la suite de tests concernée**

Run: `uv run pytest tests/test_stats.py -v`
Expected: tous les tests passent (aucun test existant n'appelle `generate_stats` directement, donc pas de nouveau test requis pour ce câblage — la couverture complète de `generate_stats` est déjà assurée par `tests/test_main.py` qui exécute le flow entier).

- [ ] **Step 5: Commit**

```bash
git add src/tasks/utils.py
git commit -m "$(cat <<'EOF'
Branche log_column_stats dans generate_stats, juste après le calcul de nb_lignes

Co-Authored-By: Claude Sonnet 5 <noreply@anthropic.com>
EOF
)"
```
