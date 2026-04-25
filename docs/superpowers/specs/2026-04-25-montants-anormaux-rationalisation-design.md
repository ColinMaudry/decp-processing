# Rationalisation des montants anormaux

**Statut** : design validé en brainstorming, en attente de plan d'implémentation
**Date** : 2026-04-25

## Contexte et problème

Le pipeline `decp-processing` consolide ~1,6 million de marchés publics. Pour quelques milliers d'entre eux, le `montant` saisi par l'acheteur est très supérieur au montant réel — typiquement parce que l'acheteur entre un montant **forfaitaire ou maximum estimé** sur un accord-cadre, en surdimensionnant pour "que ça rentre dans l'enveloppe". Cas paradigmatique : une commune de 30 000 habitants qui déclare un marché de 10 milliards d'euros pour la fourniture d'énergie.

Conséquence : ces montants aberrants faussent toute agrégation (sommes, moyennes, médianes) sur le corpus, et donc l'application d'exploration construite dessus.

**Objectif** : exposer aux utilisateurs un montant utilisable pour les agrégations, tout en conservant la valeur déclarée et en expliquant chaque modification, dans une logique pédagogique forte (la méthode doit être suffisamment rationnelle et compréhensible pour être acceptée par les usagers).

## Vue d'ensemble

Trois nouveaux champs ajoutés au schéma de sortie :

| Champ                     | Type          | Description                                                                                                                                                                                      |
| ------------------------- | ------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| `montant_rationalise`     | number        | Égal à `montant` sauf pour les marchés classés `aberrant` ; dans ce cas, prend la valeur de la médiane des pairs (méthode détaillée plus bas). C'est la colonne à utiliser pour les agrégations. |
| `montant_anomalie`        | string (enum) | `null` (RAS), `"suspect"` (anomalie possible, montant conservé), `"aberrant"` (anomalie forte, montant remplacé)                                                                                 |
| `montant_anomalie_raison` | string        | Code lisible expliquant le critère déclenché (ex. `montant_par_habitant_aberrant`, `montant_vs_pairs_suspect`)                                                                                   |

**Principe de transparence** : le `montant` déclaré reste accessible dans la donnée. L'application décide quel champ afficher selon le contexte. Les agrégations utiliseront `montant_rationalise`.

## Architecture

### Intégration dans le pipeline

Nouvelle task Prefect `detect_montant_anomalies` placée dans le flow `decp_processing`, après l'enrichissement SIRENE et avant l'écriture des fichiers :

```
... → transform → enrich_sirene → detect_montant_anomalies → output (sink_to_files)
                                  ↑
                                  utilise les colonnes acheteur_categorie,
                                  acheteur_commune_code, type, codeCPV,
                                  dureeMois, formePrix, techniques,
                                  titulaire_categorie déjà enrichies
```

### Nouveaux fichiers

- `src/tasks/anomaly.py` — task Prefect avec la logique de détection (LazyFrame in → LazyFrame out)
- `reference/population_communes.parquet` — population INSEE par code commune
- `tests/test_anomaly.py` — tests unitaires
- `script/calibrate_anomaly_thresholds.py` — script one-shot de calibration des seuils

### Modifications

- `reference/schema_base.json` — ajout des 3 champs avec leur description frictionless
- `src/config.py` — constantes de seuils, mise à jour de `BASE_DF_COLUMNS`
- `src/flows/decp_processing.py` — branchement de la nouvelle task
- `src/flows/get_cog.py` ou nouvelle task — téléchargement annuel de la population des communes
- `tests/test_main.py` — vérification de la présence des nouveaux champs en sortie

### Implémentation polars

Tout le pipeline reste en `LazyFrame` streaming. La task fonctionne en deux passes :

1. **Première passe** (group-by lazy) : calcul des statistiques (médiane et MAD du log du montant normalisé) par groupe de pairs sur l'ensemble du corpus → LazyFrame intermédiaire
2. **Deuxième passe** : jointure avec les statistiques + scoring + classification → LazyFrame de sortie

## Référentiel population

**Source** : INSEE, Populations légales des communes (mise à jour annuelle).

**Fichier** : `reference/population_communes.parquet`

- Colonnes : `code_commune` (clé, alignée avec `acheteur_commune_code`), `population`, `annee_reference`

**Mise à jour** : intégrée dans le flow `get_cog` (qui récupère déjà des référentiels INSEE/COG) ou dans une task dédiée si plus simple à isoler. Annuelle.

## Détection des anomalies

### Signal A — Comparaison aux pairs (signal principal)

**Définition du groupe de pairs** :

```python
si acheteur_categorie == "Commune":
    cle_groupe = (codeCPV[:2], type, "Commune", tranche_population)
sinon:
    cle_groupe = (codeCPV[:2], type, acheteur_categorie)
```

**Tranches de population** (communes uniquement) :

| Tranche     | Bornes (habitants) |
| ----------- | ------------------ |
| très petite | 0 – 2 000          |
| petite      | 2 000 – 10 000     |
| moyenne     | 10 000 – 50 000    |
| grande      | 50 000 – 200 000   |
| très grande | > 200 000          |

Si la population de la commune est inconnue, le marché bascule sur la clé sans tranche `(codeCPV[:2], type, "Commune")`.

**Fallback en cas de groupe insuffisant** (< 30 marchés) — chaque niveau est essayé jusqu'à trouver un groupe assez grand :

1. _Communes uniquement_ — retirer la tranche : `(codeCPV[:2], type, "Commune")`
2. Retirer le CPV : `(type, acheteur_categorie)` _(pour les communes : `(type, "Commune")`)_
3. Garder le `type` seul : `(type,)`
4. Si toujours insuffisant : `montant_anomalie = null`, `montant_rationalise = null`, `montant_anomalie_raison = "groupe_insuffisant"` (le marché est exclu de l'évaluation, ni `suspect` ni `aberrant`)

Pour un acheteur non-commune, le niveau 1 est sauté et on commence au niveau 2.

**Normalisation du montant** (avant comparaison) :

```python
si type ∈ {"Services", "Fournitures"} et dureeMois > 1 et formePrix != "Forfaitaire":
    montant_normalise = montant / dureeMois  # €/mois
sinon:
    montant_normalise = montant
```

Justification : les accords-cadres et marchés à bons de commande sont déclarés en montant maximum cumulé sur la durée — comparer un marché de 4 ans à un marché ponctuel sans normaliser n'a pas de sens. Pour les Travaux et les marchés Forfaitaires, le montant est ponctuel (le `dureeMois` représente la durée de chantier, pas une période d'amortissement).

**Calcul de l'écart** (sur log, car distribution log-normale) :

```python
log_m = log10(montant_normalise + 1)
ecart_pairs = (log_m - mediane_groupe(log_m)) / MAD_groupe(log_m)
```

avec `MAD = mediane(|x - mediane(x)|)` (Median Absolute Deviation, robuste aux outliers du groupe lui-même).

**Seuils** :

- `ecart_pairs > 4` → suspect
- `ecart_pairs > 6` → aberrant

#### Justification des seuils 4 et 6

Sur une distribution normale, MAD ≈ 0,67 σ (donc 1 σ ≈ 1,48 MAD). Les seuils MAD se traduisent ainsi :

| Seuil MAD                               | Équivalent gaussien | Fréquence théorique en distribution normale |
| --------------------------------------- | ------------------- | ------------------------------------------- |
| 3,5 (standard Iglewicz & Hoaglin, 1993) | ~2,4 σ              | 1 sur 60                                    |
| **4 (notre seuil suspect)**             | **~2,7 σ**          | **1 sur 150**                               |
| **6 (notre seuil aberrant)**            | **~4 σ**            | **1 sur 16 000**                            |
| 8 (très conservateur)                   | ~5,4 σ              | 1 sur 13 millions                           |

**Traduction sur les montants DECP** : on opère sur `log10(montant_normalise)`. Dans un groupe de pairs typique (ex. `Travaux × catégorie acheteur × tranche pop`), la MAD du log est de l'ordre de **0,3 à 0,5** (les marchés du même groupe varient d'un facteur 2 à 3). Avec une MAD ~0,4 :

| Seuil `ecart_pairs` | Multiple de la médiane du groupe |
| ------------------- | -------------------------------- |
| 3,5                 | ~25×                             |
| **4 (suspect)**     | **~40×**                         |
| **6 (aberrant)**    | **~250×**                        |
| 8                   | ~1 600×                          |

**Pourquoi 4 plutôt que 3,5 (standard)** : sur 1,6 M marchés, le seuil standard générerait des dizaines de milliers de flags sur des cas simplement "élevés mais légitimes" (gros marchés réels, queues naturelles d'une distribution large). Volume ingérable, qui décrédibiliserait le système.

**Pourquoi 6 plutôt que 8** : à 250× la médiane, l'erreur de saisie devient l'explication la plus probable. Le cas paradigmatique "10 G€ pour fourniture d'énergie d'une commune de 30k hab" correspond à un `ecart_pairs ≈ 10` (10 G€ vs ~1 M€ médian = 10 000× = `log10(10000)/0,4`) — capté largement. Mais à 8, on raterait des cas un peu moins extrêmes (50-100× la médiane) qui sont aussi très probablement faux.

**Statut** : ces valeurs sont **un point de départ raisonnable**, à calibrer empiriquement après la première run via `script/calibrate_anomaly_thresholds.py`. Le rapport HTML produit par ce script permet de visualiser les marchés flaggés à différents seuils et de figer les valeurs définitives dans `config.py`.

### Signal B — Ratio par habitant (communes uniquement)

Activé seulement si `acheteur_categorie == "Commune"` et `population_commune > 0` :

```python
montant_par_habitant = montant / population_commune
```

**Seuils par `type`** :

| `type`      | Suspect (€/hab) | Aberrant (€/hab) |
| ----------- | --------------- | ---------------- |
| Travaux     | 5 000           | 20 000           |
| Services    | 1 000           | 5 000            |
| Fournitures | 500             | 2 000            |

### Signal C — Cohérence titulaire (modulateur)

**N'est jamais déclencheur seul.** Sert uniquement à escalader la classification quand A ou B s'est déclenché.

```python
si classification == "suspect"
   et titulaire_categorie == "PME"
   et montant > 50 000 000:
       classification = "aberrant"
       raison "titulaire_incoherent_pme_gros_marche" ajoutée
```

Inactif si `titulaire_categorie` est `null` (titulaires non identifiés par SIRET).

## Logique de classification

```python
classification = null
raisons_actives = []  # liste des codes déclenchés

# Signal A
if ecart_pairs > 6:
    raisons_actives.append("montant_vs_pairs_aberrant")
elif ecart_pairs > 4:
    raisons_actives.append("montant_vs_pairs_suspect")

# Signal B
if acheteur_categorie == "Commune" and population_commune > 0:
    seuils = SEUILS_HABITANT_PAR_TYPE[type]
    if montant_par_habitant > seuils["aberrant"]:
        raisons_actives.append("montant_par_habitant_aberrant")
    elif montant_par_habitant > seuils["suspect"]:
        raisons_actives.append("montant_par_habitant_suspect")

# Classification initiale
if any code endswith "_aberrant" in raisons_actives:
    classification = "aberrant"
elif raisons_actives:
    classification = "suspect"

# Modulateur titulaire (Signal C)
if classification == "suspect" \
   and titulaire_categorie == "PME" \
   and montant > 50_000_000:
    classification = "aberrant"
    raisons_actives.append("titulaire_incoherent_pme_gros_marche")

montant_anomalie = classification
```

**Sélection de la raison principale** (pour `montant_anomalie_raison`), par ordre de priorité (la plus pédagogique d'abord) — on prend le premier code de `raisons_actives` qui matche :

1. `montant_par_habitant_aberrant` puis `montant_par_habitant_suspect`
2. `montant_vs_pairs_aberrant` puis `montant_vs_pairs_suspect`
3. `titulaire_incoherent_pme_gros_marche`

Si plusieurs signaux du même niveau ont déclenché, on garde la priorité ci-dessus (€/habitant prime sur pairs).

## Calcul de `montant_rationalise`

```python
si montant_anomalie in (null, "suspect"):
    montant_rationalise = montant      # valeur déclarée préservée

si montant_anomalie == "aberrant":
    si normalisation_par_duree appliquée:
        montant_rationalise = mediane_groupe(montant / dureeMois) × dureeMois
    sinon:
        montant_rationalise = mediane_groupe(montant)
```

Le remplacement n'a lieu que pour les `aberrant` (option Y du brainstorming) : sur les `suspect`, la valeur déclarée est conservée et le flag suffit à informer l'utilisateur.

**Cas particuliers** :

| Situation                                    | Comportement                                                             |
| -------------------------------------------- | ------------------------------------------------------------------------ |
| `montant` null à l'origine                   | `montant_rationalise = null`, pas d'évaluation d'anomalie                |
| `dureeMois` null/0 mais normalisation prévue | Utiliser la médiane des montants bruts du groupe (pas de multiplication) |
| Groupe insuffisant après tous les fallbacks  | `montant_rationalise = null`, raison `groupe_insuffisant`                |

## Configuration

Nouvelles constantes dans `src/config.py` :

```python
# Seuils Signal A (comparaison aux pairs)
ANOMALY_PAIRS_SUSPECT_THRESHOLD = 4.0
ANOMALY_PAIRS_ABERRANT_THRESHOLD = 6.0

# Seuils Signal B (€/habitant, communes)
ANOMALY_HABITANT_THRESHOLDS = {
    "Travaux":     {"suspect": 5_000, "aberrant": 20_000},
    "Services":    {"suspect": 1_000, "aberrant":  5_000},
    "Fournitures": {"suspect":   500, "aberrant":  2_000},
}

# Seuil Signal C (modulateur titulaire)
ANOMALY_TITULAIRE_PME_MONTANT_SEUIL = 50_000_000

# Tranches de population des communes (Signal A)
POPULATION_TRANCHES = [
    (0,       2_000,   "très petite"),
    (2_000,   10_000,  "petite"),
    (10_000,  50_000,  "moyenne"),
    (50_000,  200_000, "grande"),
    (200_000, None,    "très grande"),
]

# Taille minimale d'un groupe de pairs pour calculer une statistique fiable
ANOMALY_GROUPE_MIN_SIZE = 30
```

`BASE_DF_COLUMNS` mis à jour pour inclure les 3 nouveaux champs (alimenté depuis `schema_base.json` comme actuellement).

## Tests

`tests/test_anomaly.py` :

- `test_signal_pairs_basic` — un groupe avec 50 marchés normaux + 1 outlier 1000× la médiane → flag `aberrant`
- `test_signal_pairs_normalisation_duree` — `formePrix=Forfaitaire` n'applique pas la division par durée
- `test_signal_pairs_tranche_pop_commune` — la stratification par tranche s'applique aux communes
- `test_signal_pairs_pas_de_tranche_hors_commune` — pour Département/Région/État, pas de stratification
- `test_signal_population_par_type` — table paramétrée Travaux / Services / Fournitures, valeurs autour des bornes
- `test_modulateur_titulaire_pme` — escalation `suspect`→`aberrant` pour PME + montant > 50 M€
- `test_modulateur_titulaire_etranger_inactif` — `titulaire_categorie = null` n'escalade pas
- `test_montant_rationalise_aberrant_remplacement` — un `aberrant` voit son montant remplacé par `mediane × duree`
- `test_montant_rationalise_suspect_preserve` — un `suspect` garde son `montant` d'origine
- `test_groupe_insuffisant_fallback` — petit groupe → fallback sur groupe plus large
- `test_groupe_insuffisant_apres_fallback` — aucun groupe utilisable → `null` + raison `groupe_insuffisant`

`tests/test_main.py` étendu :

- Vérifie la présence des 3 nouvelles colonnes en sortie
- Vérifie que le décompte d'anomalies sur le jeu de test reste stable (régression visible)

## Observabilité

À la fin de la task `detect_montant_anomalies`, créer un artifact markdown Prefect via `create_markdown_artifact` :

- Nombre de marchés classés `suspect` et `aberrant` (totaux et par catégorie d'acheteur)
- Top 20 raisons les plus fréquentes
- Top 10 marchés `aberrant` par montant déclaré (validation manuelle rapide)
- Volume agrégé : `sum(montant)` vs `sum(montant_rationalise)` — différence chiffrée

L'artifact remonte dans l'UI Prefect et sert de garde-fou : une régression brutale (ex. soudain 100k aberrants) sera immédiatement visible.

## Calibration des seuils

`script/calibrate_anomaly_thresholds.py` (one-shot, hors flow Prefect) :

- Lit le parquet de sortie le plus récent
- Pour une grille de seuils (Signal A : 3-7 par 0.5 ; Signal B : ±50 % autour de la table)
- Produit un CSV listant les marchés flaggés par configuration
- Génère un rapport HTML avec exemples par classe pour validation visuelle
- Lit ses seuils par défaut depuis `src/config.py`

Ce script ne modifie rien : il sert à itérer sur les seuils avant de figer les valeurs dans `config.py`.

## Documentation utilisateur

Section "Rationalisation des montants" à ajouter dans :

- README du repo
- Métadonnées du dataset publié sur data.gouv.fr

Contenu :

- Pourquoi cette colonne existe (cas paradigmatique du forfaitaire surdimensionné)
- Comment elle est calculée (les 3 signaux, sans rentrer dans les formules)
- Quand utiliser `montant` vs `montant_rationalise`
- Comment interpréter `montant_anomalie_raison`

C'est cette section qui porte le contrat pédagogique avec les utilisateurs.

## Hors-scope (YAGNI)

Volontairement non inclus dans cette spec :

- Stratification par population des Départements/Régions (la dispersion intra-catégorie est plus faible que pour les communes)
- Modèle ML (régression sur features) pour la valeur de remplacement (boîte noire, contraire à la pédagogie visée)
- Détection de duplications/quasi-duplications de marchés
- Anomalies sur d'autres champs (durée aberrante, taux d'avance, etc.)

Ces extensions pourront être considérées en v2 selon les retours utilisateurs.
