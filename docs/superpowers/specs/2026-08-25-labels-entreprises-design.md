# Labels des entreprises (Bio, RGE, ESS, Association)

**Statut** : design validé en brainstorming, en attente de plan d'implémentation
**Date** : 2026-08-25
**Issue** : [#190](https://github.com/ColinMaudry/decp-processing/issues/190)

## Contexte et problème

Certaines entreprises portent des labels attestant qu'elles remplissent un cahier des charges : agriculture biologique, RGE (Reconnu Garant de l'Environnement), appartenance à l'économie sociale et solidaire, statut associatif. Ces labels sont des critères discriminants pour qui explore les marchés publics : « quels acheteurs travaillent avec des entreprises de l'ESS ? », « quelle part des marchés de rénovation va à des titulaires RGE ? ».

Aucune de ces informations n'est aujourd'hui présente dans les DECP publiées.

**Objectif** : ajouter deux champs, `titulaire_labels` et `acheteur_labels`, alimentés par quatre sources, dans le flow `sirene_preprocess`.

**Hors périmètre** : l'automatisation de la récupération de la liste RGE auprès de l'ADEME (robot Selenium + mise à jour de la ressource communautaire data.gouv.fr). Ce second volet de l'issue #190 fera l'objet d'un cycle spec → plan → implémentation distinct. Voir « Limite connue » en fin de document.

## Vue d'ensemble

Deux nouveaux champs au schéma de sortie :

| Champ              | Type   | Description                                                                                                |
| ------------------ | ------ | ---------------------------------------------------------------------------------------------------------- |
| `titulaire_labels` | string | Labels du titulaire, séparés par `", "`, dans l'ordre `Bio, RGE, ESS, Association`. `null` si aucun label. |
| `acheteur_labels`  | string | Idem pour l'acheteur.                                                                                      |

Exemples de valeurs : `"Bio, ESS"`, `"RGE"`, `"Bio, RGE, ESS, Association"`, `null`.

Les quatre labels sont appliqués **uniformément** aux acheteurs et aux titulaires ; le code ne comporte aucun cas particulier selon le type de SIRET. Bio et RGE seront rares côté acheteur, mais pas inexistants (un EPLEFPA exploitant une ferme bio, par exemple).

## Sources de données

| Label         | Source                                                    | Clé     | Volume                                 |
| ------------- | --------------------------------------------------------- | ------- | -------------------------------------- |
| `Bio`         | CSV `;` — permalien data.gouv `657789db-…`                | `SIRET` | 88 336 lignes / 87 315 SIRET distincts |
| `RGE`         | Parquet — permalien data.gouv `b614571a-…`                | `siret` | 30 000 qualifications / 13 597 SIRET   |
| `ESS`         | Stock unités légales SIRENE (déjà téléchargé par le flow) | `siren` | 1 405 245 unités légales à `"O"`       |
| `Association` | Stock unités légales SIRENE (déjà téléchargé par le flow) | `siren` | 1 100 897 unités légales               |

URL complètes :

- Bio : `https://www.data.gouv.fr/api/1/datasets/r/657789db-d349-4554-aef6-eabde4bd1c57`
- RGE : `https://www.data.gouv.fr/api/1/datasets/r/b614571a-78f6-4177-a7ad-56b93233c997`

Colonnes SIRENE utilisées, toutes deux confirmées présentes dans le stock des unités légales :

- `economieSocialeSolidaireUniteLegale` (String) — l'entreprise est ESS si la valeur vaut `"O"`
- `identifiantAssociationUniteLegale` (String) — l'entreprise est une association si la valeur est non nulle

### Qualité des données source

Le fichier bio contient deux anomalies qui rendent un `strip_chars()` insuffisant :

1. **742 SIRET valent la chaîne littérale `"None"`** — ce n'est pas une valeur vide, elle traverse donc les filtres de nullité.
2. **Au moins un SIRET porte un caractère de formatage invisible `U+202C`** en fin de valeur (`"41812208100023‬"`, 15 caractères).

S'y ajoutent 63 valeurs à 4 caractères (les `"None"`) et un total de 87 530 valeurs à 14 caractères.

Le nettoyage retenu est donc `str.replace_all(r"\D", "")` puis filtre sur une longueur de 14 : il neutralise les deux anomalies d'un coup, `"None"` devenant une chaîne vide et le `U+202C` étant retiré.

Le fichier RGE est propre : les 30 000 SIRET font tous 14 caractères.

## Architecture

**Principe** : chaque label est porté par le fichier dont il partage la clé naturelle. On ne fabrique aucune jointure inutile, et on ne mélange pas des sources externes dans les fichiers SIRENE.

```
sirene_preprocess
├── unites_legales.parquet      (clé siren)  ← + label_ess, label_association
├── etablissements.parquet      (clé siret)     inchangé
└── labels_entreprises.parquet  (clé siret)  ← NOUVEAU : label_bio, label_rge

decp_processing → enrich_from_sirene
├── add_unite_legale_data   → apporte label_ess, label_association
├── add_etablissement_data     inchangé
└── add_labels              → apporte label_bio, label_rge
                            → compose {type}_labels, supprime les 4 booléens
```

**Décision structurante** : les labels circulent en interne sous forme de **quatre colonnes booléennes**, et la chaîne publiée n'est composée qu'une seule fois, à la toute fin. Concaténer des chaînes provenant de deux jointures successives obligerait à gérer l'ordre et les séparateurs à chaque étape ; partir de booléens rend l'ordre déterministe par construction et réduit l'ajout d'un cinquième label à une ligne.

### Configuration — `src/config.py`

```python
LABELS_BIO_URL = os.getenv("LABELS_BIO_URL", "https://www.data.gouv.fr/api/1/datasets/r/657789db-d349-4554-aef6-eabde4bd1c57")
LABELS_RGE_URL = os.getenv("LABELS_RGE_URL", "https://www.data.gouv.fr/api/1/datasets/r/b614571a-78f6-4177-a7ad-56b93233c997")
```

Surchargeables par `.env` comme `SIRENE_UNITES_LEGALES_URL`, ce qui permet aux tests de pointer sur des fichiers locaux. Les deux variables sont ajoutées à `template.env` et à `ALL_CONFIG`.

Contrairement aux URL SIRENE, une valeur par défaut est fournie en dur : ces permaliens sont stables et ne dépendent pas d'un millésime mensuel.

### Récupération — `src/tasks/get.py`

```python
def get_labels_entreprises(processed_parquet_path):
    ...
```

Sur le modèle exact de `get_unite_legales` : scan des deux sources, `pipe` vers les fonctions de préparation, `sink_parquet`. Le fichier bio se lit avec `pl.scan_csv(separator=";")`, le fichier RGE avec `pl.scan_parquet`.

La fonction est décorée par un retry tenacity, voir la section « Résilience réseau du prétraitement » ci-dessous.

### Préparation — `src/tasks/transform.py`

Trois fonctions nouvelles, plus une modification :

**`prepare_labels_bio(lf) -> pl.LazyFrame`**
Retourne `siret` (String) et `label_bio` (Boolean, toujours `True`).
Nettoyage : `pl.col("SIRET").str.replace_all(r"\D", "")`, filtre `str.len_chars() == 14`, `unique()`.

**`prepare_labels_rge(lf, reference_date: date) -> pl.LazyFrame`**
Retourne `siret` (String) et `label_rge` (Boolean, toujours `True`).
Filtre de validité : `lien_date_debut <= reference_date` **et** `reference_date <= lien_date_fin`. Une ligne dont l'une des deux dates est nulle est écartée.
Puis `unique()` sur le SIRET — le fichier contient une ligne par qualification, donc plusieurs lignes par entreprise (30 000 lignes pour 13 597 SIRET).

`reference_date` est un paramètre explicite, avec `date.today()` pour valeur par défaut à l'appel depuis le flow. Ce choix rend les tests déterministes : un test qui dépendrait de la date du jour deviendrait faux avec le temps, à mesure que les qualifications du jeu de test expirent.

**`prepare_labels_entreprises(lf_bio, lf_rge, reference_date) -> pl.LazyFrame`**
Jointure `full` (avec `coalesce=True`) des deux résultats sur `siret`, puis `fill_null(False)` sur `label_bio` et `label_rge`. Retourne `siret`, `label_bio`, `label_rge` — de l'ordre de 90 000 lignes.

**`prepare_unites_legales(lf)` — modification**
Ajout des deux colonnes sources à la liste `select` existante, dérivation des booléens, puis suppression des colonnes sources dans le `drop` existant :

```python
label_ess=pl.col("economieSocialeSolidaireUniteLegale") == "O",
label_association=pl.col("identifiantAssociationUniteLegale").is_not_null(),
```

`label_ess` doit valoir `False` et non `null` pour les 23,3 M d'unités légales dont la colonne ESS est nulle — l'égalité Polars propageant le null, un `fill_null(False)` explicite est nécessaire.

### Flow — `src/flows/sirene_preprocess.py`

Un troisième bloc, à l'intérieur de la `transaction()` existante et sur le même modèle que les deux autres :

```python
processed_labels_parquet_path = SIRENE_DATA_DIR / "labels_entreprises.parquet"
if not processed_labels_parquet_path.exists():
    logger.info("Téléchargement et préparation des labels d'entreprises...")
    get_labels_entreprises(processed_labels_parquet_path)
else:
    logger.info(str(processed_labels_parquet_path) + " existe, skipping.")
```

Le bloc « établissements » est par ailleurs simplifié : `prepare_etablissements`, la jointure avec `lf_siret_latlong` et le `sink_parquet` migrent dans `get_etablissements` (voir « Résilience réseau du prétraitement »). Le flow appelle alors `get_etablissements(processed_etab_parquet_path, lf_siret_latlong)`, et les trois blocs deviennent symétriques. L'import de `prepare_etablissements` dans le flow disparaît.

### Enrichissement — `src/tasks/enrich.py`

```python
def add_labels(lf_sirets, lf_labels, siret_column: str, type_siret: str) -> pl.LazyFrame:
```

Appelée pour les acheteurs puis pour les titulaires, **après** `add_etablissement_data` et donc après `add_unite_legale_data` — les booléens ESS et association doivent déjà être présents au moment de la composition.

Point d'insertion précis : dans `enrich_from_sirene`, chacune des deux branches matérialise son résultat (`temp_enrich_acheteurs.parquet`, `temp_enrich_titulaires.parquet`) pour rompre le cycle de dépendances du LazyFrame. `add_labels` doit être appelée **avant** ce `sink_parquet`, faute de quoi les quatre booléens seraient écrits sur disque puis rejoints sur le dataframe principal.

Les quatre colonnes booléennes ne portent pas de préfixe `acheteur_` / `titulaire_` : elles sont créées et consommées à l'intérieur d'une même branche, sur deux LazyFrames distincts, et disparaissent avant la jointure finale. Aucune collision de noms n'est possible.

**Acheteurs absents de SIRENE.** `add_unite_legale_data` joint en `inner` : un acheteur absent du stock des unités légales (le Ministère des Armées, traité par `ACHETEURS_NON_SIRENE` et `apply_acheteurs_non_sirene_fallback`) ne figure pas dans `lf_sirets_acheteurs` et n'atteint donc jamais `add_labels`. Son `acheteur_labels` vaudra `null` après la jointure finale, ce qui est le comportement voulu : ses labels sont inconnus, pas vides. Le fallback existant n'alimente que `acheteur_nom` et n'a pas à être étendu.

Elle joint `lf_labels` en `left` sur `siret_column`, comble les nulls des SIRET absents du fichier de labels (`fill_null(False)`), puis compose :

```python
pl.concat_list(
    pl.when(pl.col("label_bio")).then(pl.lit("Bio")),
    pl.when(pl.col("label_rge")).then(pl.lit("RGE")),
    pl.when(pl.col("label_ess")).then(pl.lit("ESS")),
    pl.when(pl.col("label_association")).then(pl.lit("Association")),
)
.list.drop_nulls()
.list.join(", ")
```

La chaîne vide obtenue lorsqu'aucun label ne s'applique est convertie en `null`. Le résultat est aliasé `{type_siret}_labels`, et les quatre colonnes booléennes sont supprimées.

L'ordre `Bio, RGE, ESS, Association` est figé par l'ordre des arguments de `concat_list`. C'est le seul endroit du code où cet ordre est défini.

Le `lf_labels` est chargé une fois dans `enrich_from_sirene` (`pl.scan_parquet(SIRENE_DATA_DIR / "labels_entreprises.parquet")`), à côté des deux scans existants.

### Résilience réseau du prétraitement — `src/tasks/get.py`

Les fonctions de récupération SIRENE passent par `pl.scan_parquet(URL)` : c'est Polars qui effectue la requête HTTP, pas `httpx`. Les décorateurs tenacity déjà en place dans `get.py` (`json_stream_to_parquet:166`, `xml_stream_to_parquet:277`, `_download_file:565`) filtrent sur `httpx.TransportError` ou `BotoConnectionError` et ne couvrent donc pas ce chemin. `get_unite_legales` et `get_etablissements` n'ont aujourd'hui **aucun retry**.

`get_labels_entreprises` suivant le même modèle, elle hériterait du même trou. Les trois fonctions reçoivent donc le décorateur suivant :

```python
@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=2, max=30),
    retry=retry_if_exception_type((pl.exceptions.ComputeError, OSError)),
)
```

Deux points d'attention :

- **Le décorateur doit envelopper `scan` _et_ `sink` ensemble.** Le scan est paresseux : le réseau n'est sollicité qu'à l'exécution du `sink_parquet`. Décorer une fonction qui se contente de retourner un `LazyFrame` produirait un retry qui ne se déclenche jamais.
- **Le type d'exception n'est pas `httpx.TransportError`.** Reprendre ce filtre par mimétisme avec les décorateurs voisins donnerait un décorateur inerte. Polars remonte `ComputeError` ou `OSError` selon la couche qui casse.

**Conséquence sur `get_etablissements`.** Dans sa forme actuelle, elle retourne un `LazyFrame` et c'est `sirene_preprocess` qui enchaîne `prepare_etablissements`, la jointure avec le cache de géolocalisation, puis le `sink_parquet`. La décorer telle quelle serait sans effet. Elle est donc scindée :

- `scan_etablissements() -> pl.LazyFrame` — le scan et le `select` des colonnes, non décoré ;
- `get_etablissements(processed_parquet_path, lf_siret_latlong)` — décoré, il enchaîne `scan_etablissements`, `prepare_etablissements`, la jointure et le `sink_parquet`.

`sirene_preprocess` se contente alors d'appeler `get_etablissements(path, lf_siret_latlong)`, ce qui le rend symétrique des deux autres blocs. `tests/test_get.py:20` (`test_get_etablissements_includes_geocoding_columns`) cible désormais `scan_etablissements` — c'est la seule modification requise côté test, l'assertion sur les colonnes étant inchangée.

`get_unite_legales`, déjà structurée en scan → prepare → sink, ne reçoit que le décorateur.

**Note de périmètre** : l'ajout du retry sur `get_unite_legales` et `get_etablissements`, ainsi que la scission de cette dernière, débordent de l'issue #190. Décision prise sciemment : ces deux fonctions sont sur le chemin critique du prétraitement mensuel, et corriger le trou uniquement sur la nouvelle fonction aurait laissé le pipeline dans un état incohérent.

Le `sink_parquet` peut laisser un fichier partiellement écrit lorsqu'il échoue. Chaque nouvelle tentative écrase le fichier, et un échec définitif entraîne la suppression de `SIRENE_DATA_DIR` par la `transaction()` : aucun parquet tronqué ne peut survivre au flow.

### Schéma publié — `reference/schema_base.json`

Deux champs de type `string`, avec `title` et `description`, positionnés respectivement à côté de `acheteur_categorie` et de `titulaire_categorie`.

Cette étape n'est pas cosmétique : `sort_columns(lf, BASE_DF_COLUMNS)` ordonne les colonnes de sortie d'après ce fichier et rejette en fin de table, avec un warning « Colonnes inattendues », toute colonne qui n'y figure pas. `generate_final_schema` supprime par ailleurs du schéma publié les entrées dépourvues de `title`.

Description proposée :

- `acheteur_labels` — « Labels de l'acheteur, séparés par des virgules : Bio (agriculture biologique), RGE (Reconnu Garant de l'Environnement), ESS (économie sociale et solidaire), Association. »
- `titulaire_labels` — même formulation, pour le titulaire.

## Tests

Développement piloté par les tests. Les tests se lancent avec `uv run pytest`.

**`tests/test_transform.py`**

- `prepare_labels_bio` : un SIRET valide passe ; `"None"` est écarté ; un SIRET suffixé de `U+202C` est nettoyé puis conservé ; les doublons sont dédupliqués.
- `prepare_labels_rge` : avec une `reference_date` fixée, une qualification en cours est conservée, une qualification expirée est écartée, une qualification pas encore commencée est écartée ; une entreprise portant trois qualifications ne produit qu'une ligne.
- `prepare_labels_entreprises` : une entreprise bio non RGE, une RGE non bio, une des deux — les booléens sont corrects et jamais nuls.
- `prepare_unites_legales` : `label_ess` vaut `True` pour `"O"`, `False` pour `"N"`, **`False` et non `null`** pour une valeur nulle ; `label_association` suit la nullité de `identifiantAssociationUniteLegale`.

**`tests/test_enrich.py`**

- Composition de la chaîne : les quatre labels donnent `"Bio, RGE, ESS, Association"` (vérifie l'ordre) ; un seul label donne la chaîne nue sans séparateur ; aucun label donne `null` et non `""`.
- Un SIRET absent de `labels_entreprises.parquet` mais présent dans SIRENE reçoit bien ses labels ESS/association sans être perdu par la jointure.
- Les colonnes booléennes ne subsistent pas en sortie.
- Les deux chemins (`acheteur_labels`, `titulaire_labels`) sont couverts.

**`tests/test_get.py`**

- `test_get_etablissements_includes_geocoding_columns` cible `scan_etablissements` au lieu de `get_etablissements`, cette dernière écrivant désormais un parquet au lieu de retourner un `LazyFrame`. Les assertions sur les colonnes sont inchangées.

**Précaution** : `tests/test_main.py` lance le flow complet et écrit dans un `data/temp` partagé — un worktree ne protège pas de cette écriture, le `.env` parent imposant ses chemins.

## Gestion des erreurs

Les deux sources sont des permaliens externes susceptibles d'être indisponibles. La défense est à deux niveaux :

1. **Coupure passagère** — absorbée par le retry tenacity décrit plus haut : 3 tentatives, backoff exponentiel de 2 à 30 secondes.
2. **Indisponibilité prolongée** — échec franc. `sirene_preprocess` s'exécute déjà dans une `transaction()` qui supprime `SIRENE_DATA_DIR` : un téléchargement définitivement raté fait échouer le prétraitement du mois, exactement comme pour les données SIRENE elles-mêmes.

Aucune dégradation silencieuse n'est prévue. Publier des données sans labels serait indistinguable, pour un consommateur, de données où aucune entreprise n'est labellisée — un mode de défaillance silencieux qu'un échec franc évite.

## Limite connue : le plafond de 30 000 lignes du fichier RGE

Le fichier RGE compte **exactement** 30 000 lignes, pour 13 597 SIRET distincts. C'est la limite d'export de l'interface de `data.ademe.fr`, pas la taille réelle du référentiel : la population RGE française est sensiblement plus large.

Conséquence : le label RGE sera **incomplet** — une entreprise réellement RGE mais absente de ces 30 000 lignes ne portera pas le label. L'absence de label RGE ne signifie donc pas « non RGE ».

Cette limite ne bloque pas le présent enrichissement, mais elle conditionne le second volet de l'issue #190 : le robot Selenium décrit dans l'issue reproduirait ce plafond, puisqu'il actionne le même bouton d'export. La piste à instruire lors de ce second cycle est l'API data-fair de l'ADEME, qui expose vraisemblablement le jeu complet par pagination.

## Fichiers touchés

| Fichier                          | Nature                                                                                                         |
| -------------------------------- | -------------------------------------------------------------------------------------------------------------- |
| `src/config.py`                  | + `LABELS_BIO_URL`, `LABELS_RGE_URL`                                                                           |
| `template.env`                   | + les deux variables, documentées                                                                              |
| `src/tasks/get.py`               | + `get_labels_entreprises` ; scission `scan_etablissements` / `get_etablissements` ; retry sur les 3 fonctions |
| `src/tasks/transform.py`         | + 3 fonctions ; modification de `prepare_unites_legales`                                                       |
| `src/flows/sirene_preprocess.py` | + bloc `labels_entreprises.parquet` ; appel simplifié à `get_etablissements`                                   |
| `src/tasks/enrich.py`            | + `add_labels` ; câblage dans `enrich_from_sirene`                                                             |
| `reference/schema_base.json`     | + `acheteur_labels`, `titulaire_labels`                                                                        |
| `tests/test_get.py`              | `test_get_etablissements_includes_geocoding_columns` cible `scan_etablissements`                               |
| `tests/test_transform.py`        | + tests de préparation                                                                                         |
| `tests/test_enrich.py`           | + tests de composition                                                                                         |
| `CHANGELOG.md`                   | + entrée pour les deux nouveaux champs                                                                         |
