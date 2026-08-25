# Labels des entreprises — plan d'implémentation

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Ajouter deux champs `titulaire_labels` et `acheteur_labels` aux DECP publiées, alimentés par quatre sources (Bio, RGE, ESS, Association).

**Architecture:** Chaque label est porté par le fichier de prétraitement dont il partage la clé naturelle — ESS et Association (niveau SIREN) dans `unites_legales.parquet`, Bio et RGE (niveau SIRET) dans un nouveau `labels_entreprises.parquet`. Les labels circulent en interne sous forme de quatre colonnes booléennes, et la chaîne publiée n'est composée qu'une seule fois, à la fin de l'enrichissement.

**Tech Stack:** Python 3.13+, Polars 1.43.0 (LazyFrame), Prefect, tenacity, pytest.

**Spec:** `docs/superpowers/specs/2026-08-25-labels-entreprises-design.md`

## Global Constraints

- Les tests se lancent **toujours** avec `uv run pytest`, jamais `.venv/bin/pytest` ni `pytest` nu.
- Polars 1.43.0. `list.join()` sur une liste vide retourne `""` et **non** `null` — la conversion en `null` doit être explicite.
- L'ordre des labels dans la chaîne publiée est **`Bio, RGE, ESS, Association`**, séparés par `", "`. Cet ordre n'est défini qu'à un seul endroit du code (`add_labels`).
- Valeur en l'absence de tout label : `null`, jamais `""`.
- Messages de commit : ajouter la référence `#190`.
- `tests/test_main.py` lance le flow complet et écrit dans un `data/temp` partagé — un worktree ne protège pas de cette écriture, le `.env` parent imposant ses chemins.
- Ne jamais committer, pousser ou ouvrir de PR sans demander à Colin au préalable. Les étapes « Commit » de ce plan sont donc **soumises à validation** à chaque fois.

---

### Task 1 : ESS et Association dans les unités légales

**Files:**

- Modify: `src/tasks/transform.py:349-399` (`prepare_unites_legales`)
- Test: `tests/test_transform.py:17` (`TestPrepareUnitesLegales::test_prepare_unites_legales`)

**Interfaces:**

- Consumes: rien (première tâche).
- Produces: `prepare_unites_legales(lf: pl.LazyFrame) -> pl.LazyFrame` retourne désormais deux colonnes supplémentaires, `label_ess: pl.Boolean` et `label_association: pl.Boolean`, jamais nulles. Ces colonnes traversent `add_unite_legale_data` sans modification et sont consommées par `add_labels` (Task 5).

**Contexte pour l'implémenteur :** `prepare_unites_legales` prépare le stock des unités légales SIRENE (~29,9 M de lignes). Elle fait un `select` d'une liste blanche de colonnes, dérive une dénomination lisible, puis supprime les colonnes de travail. Les deux colonnes sources sont confirmées présentes dans le stock : `economieSocialeSolidaireUniteLegale` (String, valeurs `"O"` / `"N"` / `null`) et `identifiantAssociationUniteLegale` (String, non nul pour 1,1 M d'unités).

**Piège** : `pl.col("economieSocialeSolidaireUniteLegale") == "O"` retourne `null` — et non `False` — pour les 23,3 M de lignes où la colonne est nulle. Un `fill_null(False)` est indispensable, sinon `add_labels` produira des `null` en cascade.

- [ ] **Step 1 : Écrire le test qui échoue**

Le test existant `test_prepare_unites_legales` compare un DataFrame complet ; ajouter des colonnes le fera échouer. Il faut donc le mettre à jour **en même temps** qu'on ajoute les cas de test. Remplacer intégralement la classe `TestPrepareUnitesLegales` dans `tests/test_transform.py` par :

```python
class TestPrepareUnitesLegales:
    def test_prepare_unites_legales(self):
        lf = pl.LazyFrame(
            [
                # Cas 1: Personne morale, ESS, non-association
                {
                    "siren": "111111111",
                    "denominationUniteLegale": "Org 1",
                    "prenomUsuelUniteLegale": None,
                    "nomUniteLegale": None,
                    "nomUsageUniteLegale": None,
                    "statutDiffusionUniteLegale": "O",
                    "categorieEntreprise": "ETI",
                    "categorieJuridiqueUniteLegale": "1234",
                    "economieSocialeSolidaireUniteLegale": "O",
                    "identifiantAssociationUniteLegale": None,
                },
                # Cas 2: Personne physique avec nom d'usage, non-ESS, association
                {
                    "siren": "222222222",
                    "denominationUniteLegale": None,
                    "prenomUsuelUniteLegale": "Ambroise",
                    "nomUniteLegale": "Croizat",
                    "nomUsageUniteLegale": "Zacroit",  # a la priorité
                    "statutDiffusionUniteLegale": "O",
                    "categorieEntreprise": "PME",
                    "categorieJuridiqueUniteLegale": "1234",
                    "economieSocialeSolidaireUniteLegale": "N",
                    "identifiantAssociationUniteLegale": "W123456789",
                },
                # Cas 3: Personne physique sans nom d'usage, colonne ESS nulle
                {
                    "siren": "333333333",
                    "denominationUniteLegale": None,
                    "prenomUsuelUniteLegale": "Ambroise",
                    "nomUniteLegale": "Croizat",
                    "nomUsageUniteLegale": None,
                    "statutDiffusionUniteLegale": "O",
                    "categorieEntreprise": "PME",
                    "categorieJuridiqueUniteLegale": None,
                    "economieSocialeSolidaireUniteLegale": None,
                    "identifiantAssociationUniteLegale": None,
                },
                # Cas 4: Nom non-diffusible, ESS et association
                {
                    "siren": "44444444",
                    "denominationUniteLegale": None,
                    "prenomUsuelUniteLegale": "Ambroise",
                    "nomUniteLegale": "Croizat",
                    "nomUsageUniteLegale": None,
                    "statutDiffusionUniteLegale": "P",
                    "categorieEntreprise": "PME",
                    "categorieJuridiqueUniteLegale": None,
                    "economieSocialeSolidaireUniteLegale": "O",
                    "identifiantAssociationUniteLegale": "W987654321",
                },
            ]
        )

        expected_df = pl.DataFrame(
            [
                {
                    "siren": "111111111",
                    "denominationUniteLegale": "Org 1",
                    "categorieEntreprise": "ETI",
                    "categorieJuridiqueUniteLegale": "1234",
                    "label_ess": True,
                    "label_association": False,
                },
                {
                    "siren": "222222222",
                    "denominationUniteLegale": "Ambroise Zacroit",
                    "categorieEntreprise": "PME",
                    "categorieJuridiqueUniteLegale": "1234",
                    "label_ess": False,
                    "label_association": True,
                },
                {
                    "siren": "333333333",
                    "denominationUniteLegale": "Ambroise Croizat",
                    "categorieEntreprise": "PME",
                    "categorieJuridiqueUniteLegale": None,
                    # Colonne source nulle => False, surtout pas null
                    "label_ess": False,
                    "label_association": False,
                },
                {
                    "siren": "44444444",
                    "denominationUniteLegale": "[Données personnelles non-diffusibles]",
                    "categorieEntreprise": "PME",
                    "categorieJuridiqueUniteLegale": None,
                    "label_ess": True,
                    "label_association": True,
                },
            ]
        )

        result_df = prepare_unites_legales(lf).collect()

        result_df = result_df.sort("siren")
        expected_df = expected_df.sort("siren")

        assert_frame_equal(result_df, expected_df, check_column_order=False)

    def test_label_ess_et_association_jamais_nulls(self):
        """Régression : `col == "O"` propage le null. Sans fill_null(False),
        les 23,3 M d'unités légales à ESS nulle produiraient des labels nulls."""
        lf = pl.LazyFrame(
            [
                {
                    "siren": "555555555",
                    "denominationUniteLegale": "Org 5",
                    "prenomUsuelUniteLegale": None,
                    "nomUniteLegale": None,
                    "nomUsageUniteLegale": None,
                    "statutDiffusionUniteLegale": "O",
                    "categorieEntreprise": None,
                    "categorieJuridiqueUniteLegale": None,
                    "economieSocialeSolidaireUniteLegale": None,
                    "identifiantAssociationUniteLegale": None,
                }
            ]
        )

        result = prepare_unites_legales(lf).collect()

        assert result["label_ess"].null_count() == 0
        assert result["label_association"].null_count() == 0
        assert result["label_ess"].dtype == pl.Boolean
        assert result["label_association"].dtype == pl.Boolean
```

- [ ] **Step 2 : Lancer le test pour vérifier qu'il échoue**

Run : `uv run pytest tests/test_transform.py::TestPrepareUnitesLegales -v`

Expected : les deux tests ÉCHOUENT. `test_prepare_unites_legales` avec une erreur de colonnes manquantes (`label_ess`, `label_association` absentes du résultat), `test_label_ess_et_association_jamais_nulls` avec un `ColumnNotFoundError`.

- [ ] **Step 3 : Implémenter**

Dans `src/tasks/transform.py`, fonction `prepare_unites_legales`. Trois modifications.

Ajouter les deux colonnes sources à la liste du `select` (après `"categorieJuridiqueUniteLegale"`) :

```python
                "categorieJuridiqueUniteLegale",  # 1000, etc.
                "economieSocialeSolidaireUniteLegale",  # O = ESS
                "identifiantAssociationUniteLegale",  # non nul = association
```

Ajouter un `with_columns` dérivant les booléens, juste avant le `.drop(...)` final :

```python
        .with_columns(
            label_ess=(pl.col("economieSocialeSolidaireUniteLegale") == "O").fill_null(
                False
            ),
            label_association=pl.col("identifiantAssociationUniteLegale").is_not_null(),
        )
```

Ajouter les deux colonnes sources à la liste du `.drop(...)` existant :

```python
        .drop(
            [
                "prenomUsuelUniteLegale",
                "statutDiffusionUniteLegale",
                "nomUniteLegale",
                "nomUsageUniteLegale",
                "economieSocialeSolidaireUniteLegale",
                "identifiantAssociationUniteLegale",
            ]
        )
```

- [ ] **Step 4 : Lancer les tests pour vérifier qu'ils passent**

Run : `uv run pytest tests/test_transform.py::TestPrepareUnitesLegales -v`
Expected : PASS (2 tests).

Puis vérifier qu'aucun test voisin n'a régressé — `add_unite_legale_data` laisse passer les nouvelles colonnes sans les nommer, son test utilise une fixture JSON qui ne les contient pas et doit donc rester vert :

Run : `uv run pytest tests/test_transform.py tests/test_enrich.py -v`
Expected : PASS.

- [ ] **Step 5 : Commit** (demander validation à Colin avant d'exécuter)

```bash
git add src/tasks/transform.py tests/test_transform.py
git commit -m "feat: labels ESS et association depuis les unités légales SIRENE #190"
```

---

### Task 2 : Préparation des labels Bio et RGE

**Files:**

- Modify: `src/config.py` (section des URL de données externes, après `SIRENE_ETABLISSEMENTS_URL`)
- Modify: `template.env`
- Modify: `src/tasks/transform.py` (nouvelles fonctions, à placer après `prepare_unites_legales`)
- Test: `tests/test_transform.py` (nouvelle classe `TestPrepareLabels`)

**Interfaces:**

- Consumes: rien de la Task 1.
- Produces :
  - `prepare_labels_bio(lf: pl.LazyFrame) -> pl.LazyFrame` — colonnes `siret: pl.String`, `label_bio: pl.Boolean`
  - `prepare_labels_rge(lf: pl.LazyFrame, reference_date: date) -> pl.LazyFrame` — colonnes `siret: pl.String`, `label_rge: pl.Boolean`
  - `prepare_labels_entreprises(lf_bio: pl.LazyFrame, lf_rge: pl.LazyFrame, reference_date: date) -> pl.LazyFrame` — colonnes `siret: pl.String`, `label_bio: pl.Boolean`, `label_rge: pl.Boolean`, aucune valeur nulle dans les booléens
  - `LABELS_BIO_URL: str` et `LABELS_RGE_URL: str` dans `src/config.py`

**Contexte pour l'implémenteur :**

_Source Bio_ — CSV à séparateur `;`, 88 336 lignes, colonne SIRET nommée `SIRET` (majuscules). Deux anomalies réelles dans le fichier :

1. 742 valeurs sont la **chaîne littérale `"None"`**, pas une valeur vide — elles traversent donc les filtres de nullité.
2. Au moins une valeur porte un caractère de formatage invisible `U+202C` en fin (`"41812208100023‬"`, 15 caractères).

D'où le nettoyage par `str.replace_all(r"\D", "")` — il neutralise les deux d'un coup — suivi d'un filtre sur 14 chiffres. Un simple `strip_chars()` ne suffirait pas.

_Source RGE_ — Parquet, 30 000 lignes pour seulement 13 597 SIRET distincts : une ligne par qualification, donc plusieurs lignes par entreprise. Colonne `siret` (minuscules), propre (tous à 14 caractères). Colonnes `lien_date_debut` et `lien_date_fin` de type `Datetime(us)`.

_Pourquoi `reference_date` est un paramètre et non `date.today()` en dur_ : un test qui dépendrait de la date du jour deviendrait faux avec le temps, à mesure que les qualifications du jeu de test expirent. La valeur par défaut est fournie par l'appelant (Task 4).

- [ ] **Step 1 : Écrire les tests qui échouent**

Ajouter en tête de `tests/test_transform.py` l'import de `date` :

```python
from datetime import date
```

et compléter l'import depuis `src.tasks.transform` avec `prepare_labels_bio`, `prepare_labels_entreprises`, `prepare_labels_rge`.

Ajouter la classe suivante à `tests/test_transform.py` :

```python
class TestPrepareLabels:
    def test_prepare_labels_bio_nettoie_les_sirets(self):
        """Le fichier bio contient la chaîne littérale "None" (742 occurrences)
        et des SIRET suffixés d'un caractère invisible U+202C."""
        lf = pl.LazyFrame(
            {
                "SIRET": [
                    "38459550000024",  # valide
                    "None",  # chaîne littérale, pas un null
                    "41812208100023‬",  # caractère de formatage invisible
                    "38459550000024",  # doublon
                    None,  # vrai null
                    "1234",  # trop court
                ]
            }
        )

        result = prepare_labels_bio(lf).collect().sort("siret")

        assert result["siret"].to_list() == ["38459550000024", "41812208100023"]
        assert result["label_bio"].to_list() == [True, True]
        assert result["label_bio"].dtype == pl.Boolean

    def test_prepare_labels_rge_filtre_sur_la_validite(self):
        lf = pl.LazyFrame(
            {
                "siret": [
                    "11111111111111",
                    "22222222222222",
                    "33333333333333",
                    "44444444444444",
                ],
                "lien_date_debut": [
                    datetime(2020, 1, 1),  # en cours
                    datetime(2020, 1, 1),  # expirée
                    datetime(2027, 1, 1),  # pas encore commencée
                    None,  # date manquante
                ],
                "lien_date_fin": [
                    datetime(2099, 1, 1),
                    datetime(2025, 1, 1),
                    datetime(2099, 1, 1),
                    datetime(2099, 1, 1),
                ],
            }
        )

        result = prepare_labels_rge(lf, reference_date=date(2026, 8, 25)).collect()

        assert result["siret"].to_list() == ["11111111111111"]
        assert result["label_rge"].to_list() == [True]

    def test_prepare_labels_rge_deduplique_les_qualifications(self):
        """30 000 qualifications pour 13 597 SIRET : une entreprise porte
        plusieurs qualifications et ne doit produire qu'une ligne."""
        lf = pl.LazyFrame(
            {
                "siret": ["11111111111111"] * 3,
                "lien_date_debut": [datetime(2020, 1, 1)] * 3,
                "lien_date_fin": [datetime(2099, 1, 1)] * 3,
            }
        )

        result = prepare_labels_rge(lf, reference_date=date(2026, 8, 25)).collect()

        assert result.height == 1

    def test_prepare_labels_entreprises_combine_sans_nulls(self):
        lf_bio = pl.LazyFrame({"SIRET": ["11111111111111", "22222222222222"]})
        lf_rge = pl.LazyFrame(
            {
                "siret": ["22222222222222", "33333333333333"],
                "lien_date_debut": [datetime(2020, 1, 1)] * 2,
                "lien_date_fin": [datetime(2099, 1, 1)] * 2,
            }
        )

        result = (
            prepare_labels_entreprises(
                lf_bio, lf_rge, reference_date=date(2026, 8, 25)
            )
            .collect()
            .sort("siret")
        )

        assert result["siret"].to_list() == [
            "11111111111111",
            "22222222222222",
            "33333333333333",
        ]
        assert result["label_bio"].to_list() == [True, True, False]
        assert result["label_rge"].to_list() == [False, True, True]
        # Aucun null : un SIRET présent d'un seul côté doit valoir False de l'autre
        assert result["label_bio"].null_count() == 0
        assert result["label_rge"].null_count() == 0
```

Ajouter aussi `datetime` à l'import de `datetime` en tête de fichier :

```python
from datetime import date, datetime
```

- [ ] **Step 2 : Lancer les tests pour vérifier qu'ils échouent**

Run : `uv run pytest tests/test_transform.py::TestPrepareLabels -v`
Expected : ÉCHEC à la collecte, `ImportError: cannot import name 'prepare_labels_bio' from 'src.tasks.transform'`.

- [ ] **Step 3 : Ajouter la configuration**

Dans `src/config.py`, juste après la ligne `SIRENE_ETABLISSEMENTS_URL = os.getenv("SIRENE_ETABLISSEMENTS_URL", "")` :

```python
# Labels des entreprises (issue #190)
# Contrairement aux URL SIRENE, une valeur par défaut est fournie : ces permaliens
# sont stables et ne dépendent pas d'un millésime mensuel.
LABELS_BIO_URL = os.getenv(
    "LABELS_BIO_URL",
    "https://www.data.gouv.fr/api/1/datasets/r/657789db-d349-4554-aef6-eabde4bd1c57",
)
ALL_CONFIG["LABELS_BIO_URL"] = LABELS_BIO_URL

LABELS_RGE_URL = os.getenv(
    "LABELS_RGE_URL",
    "https://www.data.gouv.fr/api/1/datasets/r/b614571a-78f6-4177-a7ad-56b93233c997",
)
ALL_CONFIG["LABELS_RGE_URL"] = LABELS_RGE_URL
```

Dans `template.env`, à côté des variables `SIRENE_*_URL` :

```
# Liste des entreprises certifiées agriculture biologique (CSV, séparateur ";")
LABELS_BIO_URL=https://www.data.gouv.fr/api/1/datasets/r/657789db-d349-4554-aef6-eabde4bd1c57
# Liste des entreprises RGE (Parquet). Attention : l'export ADEME est plafonné
# à 30 000 lignes, la couverture du label RGE est donc partielle.
LABELS_RGE_URL=https://www.data.gouv.fr/api/1/datasets/r/b614571a-78f6-4177-a7ad-56b93233c997
```

- [ ] **Step 4 : Implémenter les trois fonctions**

Dans `src/tasks/transform.py`, après `prepare_unites_legales`. Vérifier que `from datetime import date` figure dans les imports du module et l'ajouter sinon.

```python
def prepare_labels_bio(lf: pl.LazyFrame) -> pl.LazyFrame:
    """SIRET des entreprises certifiées agriculture biologique.

    Le fichier source contient la chaîne littérale "None" (742 occurrences,
    qui passe donc les filtres de nullité) et des SIRET suffixés d'un caractère
    de formatage invisible U+202C. Le retrait de tout ce qui n'est pas un
    chiffre neutralise les deux cas.
    """
    return (
        lf.select(pl.col("SIRET").str.replace_all(r"\D", "").alias("siret"))
        .filter(pl.col("siret").str.len_chars() == 14)
        .unique()
        .with_columns(label_bio=pl.lit(True))
    )


def prepare_labels_rge(lf: pl.LazyFrame, reference_date: date) -> pl.LazyFrame:
    """SIRET des entreprises dont une qualification RGE est valide à `reference_date`.

    Le fichier source compte une ligne par qualification (30 000 lignes pour
    13 597 SIRET), d'où la déduplication. Les lignes dont une date est nulle
    sont écartées par la comparaison.
    """
    return (
        lf.filter(
            (pl.col("lien_date_debut").cast(pl.Date) <= reference_date)
            & (pl.col("lien_date_fin").cast(pl.Date) >= reference_date)
        )
        .select("siret")
        .unique()
        .with_columns(label_rge=pl.lit(True))
    )


def prepare_labels_entreprises(
    lf_bio: pl.LazyFrame, lf_rge: pl.LazyFrame, reference_date: date
) -> pl.LazyFrame:
    """Table SIRET -> labels externes (Bio, RGE), ~90 000 lignes.

    Les booléens ne sont jamais nuls : un SIRET présent d'un seul côté vaut
    False de l'autre.
    """
    return (
        prepare_labels_bio(lf_bio)
        .join(
            prepare_labels_rge(lf_rge, reference_date),
            on="siret",
            how="full",
            coalesce=True,
        )
        .with_columns(
            pl.col("label_bio").fill_null(False),
            pl.col("label_rge").fill_null(False),
        )
    )
```

- [ ] **Step 5 : Lancer les tests pour vérifier qu'ils passent**

Run : `uv run pytest tests/test_transform.py::TestPrepareLabels -v`
Expected : PASS (4 tests).

- [ ] **Step 6 : Commit** (demander validation à Colin avant d'exécuter)

```bash
git add src/config.py template.env src/tasks/transform.py tests/test_transform.py
git commit -m "feat: préparation des labels bio et RGE #190"
```

---

### Task 3 : Résilience réseau du prétraitement SIRENE

**Files:**

- Modify: `src/tasks/get.py:444-464` (`get_etablissements`) et `src/tasks/get.py:623-628` (`get_unite_legales`)
- Modify: `src/flows/sirene_preprocess.py:48-56`
- Test: `tests/test_get.py:19-24`

**Interfaces:**

- Consumes: rien des Tasks 1 et 2. Cette tâche est indépendante et peut être relue isolément.
- Produces :
  - `scan_etablissements() -> pl.LazyFrame` — le scan et le `select` des colonnes, non décoré
  - `get_etablissements(processed_parquet_path: Path, lf_siret_latlong: pl.LazyFrame) -> None` — décoré, enchaîne scan, `prepare_etablissements`, jointure géo et `sink_parquet`
  - `get_unite_legales(processed_parquet_path: Path) -> None` — inchangée dans sa signature, décorée

**Contexte pour l'implémenteur :**

Les fonctions de récupération SIRENE passent par `pl.scan_parquet(URL)` : c'est **Polars** qui effectue la requête HTTP, pas `httpx`. Les décorateurs tenacity déjà présents dans `get.py` (`json_stream_to_parquet:166`, `xml_stream_to_parquet:277`, `_download_file:565`) filtrent sur `httpx.TransportError` ou `BotoConnectionError` et ne couvrent donc pas ce chemin. Aujourd'hui, `get_unite_legales` et `get_etablissements` n'ont **aucun retry**.

Deux pièges à ne pas reproduire :

1. **Le décorateur doit envelopper `scan` _et_ `sink` ensemble.** Le scan est paresseux : le réseau n'est sollicité qu'à l'exécution du `sink_parquet`. Décorer une fonction qui se contente de retourner un `LazyFrame` produirait un retry qui ne se déclenche jamais. C'est exactement le cas de `get_etablissements` aujourd'hui — elle s'arrête au `select` et laisse `prepare_etablissements`, la jointure géo et le `sink_parquet` au flow. D'où la scission.
2. **Le type d'exception n'est pas `httpx.TransportError`.** Reprendre ce filtre par mimétisme avec les décorateurs voisins donnerait un décorateur inerte. Polars remonte `ComputeError` ou `OSError` selon la couche qui casse.

`get_unite_legales`, déjà structurée en scan → prepare → sink dans son corps, ne reçoit que le décorateur, sans refactor.

**Note de périmètre :** cette tâche déborde de l'issue #190. Décision prise sciemment lors du design : ces deux fonctions sont sur le chemin critique du prétraitement mensuel, et corriger le trou uniquement sur la nouvelle fonction aurait laissé le pipeline incohérent.

- [ ] **Step 1 : Écrire le test qui échoue**

Dans `tests/test_get.py`, remplacer `test_get_etablissements_includes_geocoding_columns` (qui appelle `get_etablissements()`, laquelle ne retournera plus de `LazyFrame`) et ajouter un test sur la présence du retry. Remplacer l'import :

```python
from src.tasks.get import (
    bootstrap_siret_latlong,
    get_etablissements,
    get_unite_legales,
    json_stream_to_parquet,
    scan_etablissements,
    xml_stream_to_parquet,
)
```

et le test :

```python
def test_scan_etablissements_includes_geocoding_columns():
    lf = scan_etablissements()
    assert isinstance(lf, pl.LazyFrame)
    columns = set(lf.collect_schema().names())
    missing = REQUIRED_GEO_COLUMNS - columns
    assert not missing, f"Colonnes manquantes : {missing}"


def test_les_recuperations_sirene_ont_un_retry():
    """Régression : le réseau n'est sollicité qu'au sink_parquet, le décorateur
    doit donc envelopper scan ET sink. Une fonction qui retourne un LazyFrame
    ne peut pas être utilement décorée."""
    for func in (get_unite_legales, get_etablissements):
        assert hasattr(func, "retry"), (
            f"{func.__name__} n'est pas décorée par tenacity"
        )
```

- [ ] **Step 2 : Lancer les tests pour vérifier qu'ils échouent**

Run : `uv run pytest tests/test_get.py -v -k "etablissements or retry"`
Expected : ÉCHEC à la collecte, `ImportError: cannot import name 'scan_etablissements'`.

- [ ] **Step 3 : Scinder `get_etablissements` et décorer**

Dans `src/tasks/get.py`, remplacer la fonction `get_etablissements` (lignes 444-464) par :

```python
def scan_etablissements() -> pl.LazyFrame:
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

    return pl.scan_parquet(SIRENE_ETABLISSEMENTS_URL).select(columns)


@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=2, max=30),
    retry=retry_if_exception_type((pl.exceptions.ComputeError, OSError)),
)
def get_etablissements(
    processed_parquet_path: Path, lf_siret_latlong: pl.LazyFrame
) -> None:
    """Le décorateur doit envelopper le scan ET le sink : le scan étant paresseux,
    le réseau n'est sollicité qu'à l'exécution du sink_parquet."""
    (
        scan_etablissements()
        .pipe(prepare_etablissements)
        .join(lf_siret_latlong, on="siret", how="left")
        .sink_parquet(processed_parquet_path)
    )
```

Ajouter `prepare_etablissements` à l'import depuis `src.tasks.transform` en tête de `get.py` s'il n'y figure pas déjà.

Décorer `get_unite_legales` (aucun autre changement) :

```python
@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=2, max=30),
    retry=retry_if_exception_type((pl.exceptions.ComputeError, OSError)),
)
def get_unite_legales(processed_parquet_path):
    (
        pl.scan_parquet(SIRENE_UNITES_LEGALES_URL)
        .pipe(prepare_unites_legales)
        .sink_parquet(processed_parquet_path)
    )
```

- [ ] **Step 4 : Simplifier le flow**

Dans `src/flows/sirene_preprocess.py`, remplacer le bloc « établissements » :

```python
        # préparer les données établissements
        processed_etab_parquet_path = SIRENE_DATA_DIR / "etablissements.parquet"
        if not processed_etab_parquet_path.exists():
            logger.info("Téléchargement et préparation des établissements...")
            get_etablissements(processed_etab_parquet_path, lf_siret_latlong)
        else:
            logger.info(str(processed_etab_parquet_path) + " existe, skipping.")
```

Supprimer l'import devenu inutile `from src.tasks.transform import prepare_etablissements` en tête du fichier — c'était son unique usage dans ce module.

- [ ] **Step 5 : Lancer les tests pour vérifier qu'ils passent**

Run : `uv run pytest tests/test_get.py -v`
Expected : PASS.

Vérifier ensuite qu'aucun import cassé ne subsiste :

Run : `uv run python -c "from src.flows.sirene_preprocess import sirene_preprocess; print('ok')"`
Expected : `ok`

- [ ] **Step 6 : Commit** (demander validation à Colin avant d'exécuter)

```bash
git add src/tasks/get.py src/flows/sirene_preprocess.py tests/test_get.py
git commit -m "fix: retry tenacity sur les récupérations SIRENE (scan+sink) #190"
```

---

### Task 4 : Production de `labels_entreprises.parquet`

**Files:**

- Modify: `src/tasks/get.py` (nouvelle fonction, à placer après `get_unite_legales`)
- Modify: `src/flows/sirene_preprocess.py`

**Interfaces:**

- Consumes: `prepare_labels_entreprises` (Task 2), le motif de décoration tenacity (Task 3).
- Produces: `get_labels_entreprises(processed_parquet_path: Path, reference_date: date | None = None) -> None`, qui écrit un parquet de colonnes `siret`, `label_bio`, `label_rge`. Le fichier `SIRENE_DATA_DIR / "labels_entreprises.parquet"` est consommé par la Task 5.

**Contexte pour l'implémenteur :** le fichier bio se lit avec `pl.scan_csv(separator=";")`, le fichier RGE avec `pl.scan_parquet`. `reference_date` vaut `date.today()` lorsqu'elle n'est pas fournie — la valeur par défaut est résolue **dans le corps** de la fonction et non dans la signature, un `date.today()` en argument par défaut étant évalué une seule fois à l'import du module.

- [ ] **Step 1 : Implémenter la récupération**

Dans `src/tasks/get.py`, après `get_unite_legales`. Ajouter `LABELS_BIO_URL` et `LABELS_RGE_URL` à l'import depuis `src.config`, `prepare_labels_entreprises` à l'import depuis `src.tasks.transform`, et `from datetime import date` aux imports du module s'il n'y figure pas.

```python
@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=2, max=30),
    retry=retry_if_exception_type((pl.exceptions.ComputeError, OSError)),
)
def get_labels_entreprises(
    processed_parquet_path: Path, reference_date: date | None = None
) -> None:
    """Table SIRET -> labels externes (Bio, RGE).

    reference_date est résolue ici et non dans la signature : un date.today()
    en argument par défaut serait évalué une seule fois, à l'import du module.
    """
    if reference_date is None:
        reference_date = date.today()

    prepare_labels_entreprises(
        pl.scan_csv(LABELS_BIO_URL, separator=";", infer_schema_length=0),
        pl.scan_parquet(LABELS_RGE_URL),
        reference_date,
    ).sink_parquet(processed_parquet_path)
```

`infer_schema_length=0` force la lecture du CSV bio en String : le SIRET doit rester textuel, une inférence numérique perdrait les zéros de tête.

- [ ] **Step 2 : Brancher le flow**

Dans `src/flows/sirene_preprocess.py`, ajouter `get_labels_entreprises` à l'import depuis `src.tasks.get`, puis insérer un troisième bloc à l'intérieur de la `transaction()`, après le bloc « établissements » :

```python
        # préparer les labels d'entreprises (bio, RGE)
        processed_labels_parquet_path = SIRENE_DATA_DIR / "labels_entreprises.parquet"
        if not processed_labels_parquet_path.exists():
            logger.info("Téléchargement et préparation des labels d'entreprises...")
            get_labels_entreprises(processed_labels_parquet_path)
        else:
            logger.info(str(processed_labels_parquet_path) + " existe, skipping.")
```

- [ ] **Step 3 : Vérifier de bout en bout sur les vraies sources**

Aucun test automatisé ne télécharge les sources réelles. Vérifier manuellement, dans le répertoire scratch et non dans `data/` :

```bash
uv run python -c "
from datetime import date
from pathlib import Path
from src.tasks.get import get_labels_entreprises
import polars as pl

out = Path('/tmp/labels_check.parquet')
get_labels_entreprises(out)
df = pl.read_parquet(out)
print(df.head())
print('lignes', df.height)
print('bio', df['label_bio'].sum(), '| rge', df['label_rge'].sum())
print('nulls', df['label_bio'].null_count(), df['label_rge'].null_count())
print('longueurs siret', df['siret'].str.len_chars().unique().to_list())
"
```

Attendu : de l'ordre de 90 000 lignes, ~87 000 bio et ~13 500 RGE (le RGE varie selon la date, le filtre de validité écartant les qualifications expirées), zéro null, et `[14]` comme unique longueur de SIRET.

- [ ] **Step 4 : Lancer la suite de tests**

Run : `uv run pytest tests/test_get.py tests/test_transform.py -v`
Expected : PASS.

- [ ] **Step 5 : Commit** (demander validation à Colin avant d'exécuter)

```bash
git add src/tasks/get.py src/flows/sirene_preprocess.py
git commit -m "feat: production de labels_entreprises.parquet dans sirene_preprocess #190"
```

---

### Task 5 : Composition de `titulaire_labels` et `acheteur_labels`

**Files:**

- Modify: `src/tasks/enrich.py` (nouvelle fonction `add_labels`, puis câblage dans `enrich_from_sirene:179-260`)
- Test: `tests/test_enrich.py` (nouvelle classe `TestAddLabels`)

**Interfaces:**

- Consumes: `label_ess` / `label_association` produits par la Task 1 et véhiculés par `add_unite_legale_data` ; `labels_entreprises.parquet` produit par la Task 4.
- Produces: `add_labels(lf_sirets: pl.LazyFrame, lf_labels: pl.LazyFrame, siret_column: str, type_siret: str) -> pl.LazyFrame`, qui ajoute une colonne `{type_siret}_labels` de type `pl.String` et supprime les quatre colonnes booléennes.

**Contexte pour l'implémenteur :**

_Où l'insérer._ Dans `enrich_from_sirene`, chacune des deux branches (acheteurs, titulaires) matérialise son résultat dans un parquet temporaire (`temp_enrich_acheteurs.parquet`, `temp_enrich_titulaires.parquet`) pour rompre le cycle de dépendances du LazyFrame. `add_labels` doit être appelée **après `add_etablissement_data` et avant ce `sink_parquet`** — sinon les quatre booléens seraient écrits sur disque puis joints au dataframe principal.

_Pourquoi des booléens et pas de la concaténation de chaînes._ Concaténer des chaînes provenant de deux jointures successives obligerait à gérer l'ordre et les séparateurs à chaque étape. Partir de booléens rend l'ordre déterministe par construction : il n'est défini qu'à un seul endroit, l'ordre des arguments de `concat_list`.

_Piège Polars 1.43.0._ `list.join(", ")` sur une liste vide retourne `""` et **non** `null`. La conversion doit être explicite, sinon les marchés sans aucun label porteront une chaîne vide au lieu d'un null.

_Pas de préfixe sur les booléens._ Les quatre colonnes sont créées et consommées à l'intérieur d'une même branche, sur deux LazyFrames distincts, et disparaissent avant la jointure finale. Aucune collision de noms n'est possible.

_Acheteurs absents de SIRENE._ `add_unite_legale_data` joint en `inner` : un acheteur absent du stock des unités légales (le Ministère des Armées, traité par `ACHETEURS_NON_SIRENE` et `apply_acheteurs_non_sirene_fallback`) ne figure pas dans `lf_sirets_acheteurs` et n'atteint donc jamais `add_labels`. Son `acheteur_labels` vaudra `null` après la jointure finale — comportement voulu, ses labels sont inconnus et non vides. Le fallback existant n'alimente que `acheteur_nom` et n'a pas à être étendu.

- [ ] **Step 1 : Écrire les tests qui échouent**

Ajouter `add_labels` à l'import depuis `src.tasks.enrich` dans `tests/test_enrich.py`, puis ajouter la classe :

```python
class TestAddLabels:
    @staticmethod
    def _lf_sirets(**flags):
        """Un SIRET titulaire portant les booléens ESS/association demandés."""
        return pl.LazyFrame(
            {
                "titulaire_id": ["11111111111111"],
                "label_ess": [flags.get("ess", False)],
                "label_association": [flags.get("association", False)],
            }
        )

    @staticmethod
    def _lf_labels(**flags):
        return pl.LazyFrame(
            {
                "siret": ["11111111111111"],
                "label_bio": [flags.get("bio", False)],
                "label_rge": [flags.get("rge", False)],
            }
        )

    def test_ordre_des_quatre_labels(self):
        """L'ordre publié est Bio, RGE, ESS, Association, quelle que soit
        l'origine de chaque label."""
        result = add_labels(
            self._lf_sirets(ess=True, association=True),
            self._lf_labels(bio=True, rge=True),
            "titulaire_id",
            "titulaire",
        ).collect()

        assert result["titulaire_labels"].to_list() == [
            "Bio, RGE, ESS, Association"
        ]

    def test_un_seul_label_pas_de_separateur(self):
        result = add_labels(
            self._lf_sirets(),
            self._lf_labels(rge=True),
            "titulaire_id",
            "titulaire",
        ).collect()

        assert result["titulaire_labels"].to_list() == ["RGE"]

    def test_aucun_label_donne_null(self):
        """Régression : list.join sur une liste vide retourne "" en Polars 1.43,
        pas null."""
        result = add_labels(
            self._lf_sirets(),
            self._lf_labels(),
            "titulaire_id",
            "titulaire",
        ).collect()

        assert result["titulaire_labels"].to_list() == [None]

    def test_siret_absent_du_fichier_de_labels(self):
        """Un SIRET connu de SIRENE mais absent de labels_entreprises.parquet
        conserve ses labels ESS/association et n'est pas perdu par la jointure."""
        lf_labels = pl.LazyFrame(
            {
                "siret": ["99999999999999"],
                "label_bio": [True],
                "label_rge": [True],
            }
        )

        result = add_labels(
            self._lf_sirets(ess=True), lf_labels, "titulaire_id", "titulaire"
        ).collect()

        assert result.height == 1
        assert result["titulaire_labels"].to_list() == ["ESS"]

    def test_les_booleens_ne_survivent_pas(self):
        result = add_labels(
            self._lf_sirets(ess=True),
            self._lf_labels(bio=True),
            "titulaire_id",
            "titulaire",
        ).collect()

        assert set(result.columns) == {"titulaire_id", "titulaire_labels"}

    def test_chemin_acheteur(self):
        lf_sirets = pl.LazyFrame(
            {
                "acheteur_id": ["11111111111111"],
                "label_ess": [False],
                "label_association": [True],
            }
        )

        result = add_labels(
            lf_sirets, self._lf_labels(bio=True), "acheteur_id", "acheteur"
        ).collect()

        assert result["acheteur_labels"].to_list() == ["Bio, Association"]
```

- [ ] **Step 2 : Lancer les tests pour vérifier qu'ils échouent**

Run : `uv run pytest tests/test_enrich.py::TestAddLabels -v`
Expected : ÉCHEC à la collecte, `ImportError: cannot import name 'add_labels'`.

- [ ] **Step 3 : Implémenter `add_labels`**

Dans `src/tasks/enrich.py`, après `add_unite_legale_data` :

```python
LABELS = [
    ("label_bio", "Bio"),
    ("label_rge", "RGE"),
    ("label_ess", "ESS"),
    ("label_association", "Association"),
]


def add_labels(
    lf_sirets: pl.LazyFrame,
    lf_labels: pl.LazyFrame,
    siret_column: str,
    type_siret: str,
) -> pl.LazyFrame:
    """Compose {type_siret}_labels à partir des quatre drapeaux booléens.

    L'ordre publié — Bio, RGE, ESS, Association — n'est défini qu'ici, par
    l'ordre de LABELS.
    """
    lf_sirets = lf_sirets.join(
        lf_labels, how="left", left_on=siret_column, right_on="siret"
    ).with_columns(
        pl.col("label_bio").fill_null(False),
        pl.col("label_rge").fill_null(False),
    )

    lf_sirets = lf_sirets.with_columns(
        pl.concat_list(
            [pl.when(pl.col(flag)).then(pl.lit(label)) for flag, label in LABELS]
        )
        .list.drop_nulls()
        .list.join(", ")
        # list.join retourne "" sur une liste vide, pas null
        .replace("", None)
        .alias(f"{type_siret}_labels")
    )

    return lf_sirets.drop([flag for flag, _ in LABELS])
```

- [ ] **Step 4 : Lancer les tests pour vérifier qu'ils passent**

Run : `uv run pytest tests/test_enrich.py::TestAddLabels -v`
Expected : PASS (6 tests).

- [ ] **Step 5 : Câbler dans `enrich_from_sirene`**

Dans `src/tasks/enrich.py`, fonction `enrich_from_sirene`. Ajouter le scan du nouveau fichier à côté des deux existants :

```python
    lf_labels = pl.scan_parquet(SIRENE_DATA_DIR / "labels_entreprises.parquet")
```

Branche acheteurs — insérer **avant** le `sink_parquet` vers `temp_enrich_acheteurs.parquet` :

```python
    logger.info("Ajout des labels (acheteurs)...")
    lf_sirets_acheteurs = add_labels(
        lf_sirets_acheteurs, lf_labels, "acheteur_id", "acheteur"
    )
```

Branche titulaires — insérer **avant** le `sink_parquet` vers `temp_enrich_titulaires.parquet` :

```python
    logger.info("Ajout des labels (titulaires)...")
    lf_sirets_titulaires = add_labels(
        lf_sirets_titulaires, lf_labels, "titulaire_id", "titulaire"
    )
```

- [ ] **Step 6 : Lancer la suite complète**

Run : `uv run pytest tests/test_enrich.py tests/test_transform.py tests/test_get.py -v`
Expected : PASS.

- [ ] **Step 7 : Commit** (demander validation à Colin avant d'exécuter)

```bash
git add src/tasks/enrich.py tests/test_enrich.py
git commit -m "feat: champs titulaire_labels et acheteur_labels #190"
```

---

### Task 6 : Schéma publié et changelog

**Files:**

- Modify: `reference/schema_base.json`
- Modify: `CHANGELOG.md`

**Interfaces:**

- Consumes: les colonnes `acheteur_labels` et `titulaire_labels` produites par la Task 5.
- Produces: rien pour les tâches suivantes (dernière tâche).

**Contexte pour l'implémenteur :** cette étape n'est pas cosmétique. `sort_columns(lf, BASE_DF_COLUMNS)` (`src/tasks/transform.py:429`) ordonne les colonnes de sortie d'après ce fichier et rejette en fin de table, avec un warning « Colonnes inattendues », toute colonne qui n'y figure pas. `generate_final_schema` (`src/tasks/output.py:157`) supprime par ailleurs du schéma publié les entrées dépourvues de `title`. Sans cette tâche, les deux champs sortiraient en fin de table et seraient absents du schéma publié.

- [ ] **Step 1 : Vérifier que les champs sont aujourd'hui « inattendus »**

```bash
uv run python -c "
import json
names = [f['name'] for f in json.load(open('reference/schema_base.json'))['fields']]
print('acheteur_labels' in names, 'titulaire_labels' in names)
print('index acheteur_categorie', names.index('acheteur_categorie'))
print('index titulaire_categorie', names.index('titulaire_categorie'))
"
```

Attendu : `False False`, puis les index des deux champs `_categorie`.

- [ ] **Step 2 : Ajouter les deux champs**

Dans `reference/schema_base.json`, insérer l'objet suivant **immédiatement après** l'entrée `acheteur_categorie` :

```json
    {
      "type": "string",
      "name": "acheteur_labels",
      "title": "Labels de l'acheteur",
      "description": "Labels de l'acheteur, séparés par des virgules : Bio (agriculture biologique), RGE (Reconnu Garant de l'Environnement), ESS (économie sociale et solidaire), Association. La couverture du label RGE est partielle.",
      "short_title": "Labels acheteur"
    },
```

et l'objet suivant **immédiatement après** l'entrée `titulaire_categorie` :

```json
    {
      "type": "string",
      "name": "titulaire_labels",
      "title": "Labels du titulaire",
      "description": "Labels de l'entreprise titulaire, séparés par des virgules : Bio (agriculture biologique), RGE (Reconnu Garant de l'Environnement), ESS (économie sociale et solidaire), Association. La couverture du label RGE est partielle.",
      "short_title": "Labels titulaire"
    },
```

Ces champs n'ont volontairement pas de clé `enum` : la valeur est une combinaison de labels, pas une valeur parmi une liste fermée.

- [ ] **Step 3 : Vérifier le JSON et l'ordre**

```bash
uv run python -c "
import json
names = [f['name'] for f in json.load(open('reference/schema_base.json'))['fields']]
assert names.index('acheteur_labels') == names.index('acheteur_categorie') + 1
assert names.index('titulaire_labels') == names.index('titulaire_categorie') + 1
print('ok')
"
```

Expected : `ok` (un JSON invalide ferait échouer le `json.load`).

- [ ] **Step 4 : Compléter le changelog**

Dans `CHANGELOG.md`, sous la section de version en cours de préparation en tête de fichier :

```markdown
- Ajout des labels des entreprises `titulaire_labels` et `acheteur_labels` — Bio, RGE, ESS, Association ([#190](https://github.com/ColinMaudry/decp-processing/issues/190)). La couverture du label RGE est partielle : l'export de l'ADEME est plafonné à 30 000 lignes.
- Retry sur les téléchargements SIRENE du prétraitement mensuel (unités légales, établissements)
```

- [ ] **Step 5 : Lancer la suite complète**

Run : `uv run pytest -v`

Expected : PASS. Attention, cette commande inclut `tests/test_main.py`, qui lance le flow complet et écrit dans `data/temp` — vérifier auprès de Colin qu'aucun traitement de production n'est en cours avant de la lancer.

- [ ] **Step 6 : Commit** (demander validation à Colin avant d'exécuter)

```bash
git add reference/schema_base.json CHANGELOG.md
git commit -m "docs: schéma et changelog pour les labels d'entreprises #190"
```

---

## Vérification finale

- [ ] Lancer `sirene_preprocess` sur un `SIRENE_DATA_PARENT_DIR` de test et vérifier que `labels_entreprises.parquet` est produit à côté des deux autres fichiers.
- [ ] Sur un échantillon de sortie, vérifier qu'au moins un marché porte `titulaire_labels = "ESS"` et qu'un autre porte une combinaison à deux labels.
- [ ] Vérifier l'absence du warning « Colonnes inattendues » mentionnant `acheteur_labels` ou `titulaire_labels` dans les logs de `decp_processing`.
- [ ] Vérifier que `dist/decp.csv` contient bien les deux colonnes, à la position attendue.
