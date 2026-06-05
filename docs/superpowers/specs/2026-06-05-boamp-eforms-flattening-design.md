# Intégration BOAMP / eForms — aplatissement en parquet

Date : 2026-06-05
Statut : design validé (brainstorming), à transformer en plan d'implémentation.

## Objectif

Intégrer les annonces de marchés publics du BOAMP (avis de marché / appels
d'offres) à l'écosystème [decp.info](https://decp.info), en aplatissant les
XML bruts de l'archive DILA et en publiant **une ressource parquet unique** sur
data.gouv.fr, à raison d'**une ligne par lot**.

Le traitement vit dans le projet `decp-processing` sous la forme d'un **nouveau
flow Prefect dédié** (`boamp_processing`), indépendant du flow `decp_processing`.

## Périmètre

- **Source** : archive DILA `https://echanges.dila.gouv.fr/OPENDATA/BOAMP/AAAA/MM/JJ/NN-XXXXX.xml`
  (un fichier XML par annonce ; pas d'archive bulk, crawl de l'index HTML par jour).
- **Début** : 1ᵉʳ janvier 2026. Pas de reprise d'historique antérieur pour l'instant.
- **Formats** : les deux schémas présents dans le flux 2026 :
  - **eForms** (UBL 2.3, dans `DONNEES/EFORMS`),
  - **BOAMP v240 legacy** (dans `DONNEES/<FAMILLE>`, ex. `MAPA`, `AVIS`,
    `RECTIFICATIF`, `ANNULATION`…).
- **Avis inclus** : avis de marché / appels d'offres et leur cycle de vie
  (rectificatifs, annulations).
- **Avis exclus** : avis d'attribution (ils recoupent les marchés déjà couverts
  par DECP). Concrètement on ignore :
  - eForms : racine `ContractAwardNotice` et/ou `SubTypeCode` ∈ {29, 30} ;
  - legacy : famille / nature `ATTRIBUTION`.
- **Pas d'enrichissement SIRENE** en v1 (données brutes ; SIREN/SIRET conservés
  quand présents dans le XML).

### Distribution observée (échantillon 32 fichiers, 2026-01-31)

| Type (racine eForms / famille legacy)              | Subtype | Inclus ?         |
| -------------------------------------------------- | ------- | ---------------- |
| eForms `ContractNotice`                            | 16, 17  | ✅               |
| eForms `ContractAwardNotice`                       | 29, 30  | ❌ (attribution) |
| legacy `MAPA` (et autres familles non-attribution) | —       | ✅               |
| legacy `ATTRIBUTION`                               | —       | ❌               |

Aucun `PriorInformationNotice` observé dans le flux.

## Architecture

### Flow `boamp_processing`

1. **Énumération des jours** : du 2026-01-01 à aujourd'hui.
2. **Acquisition incrémentale** :
   - sortie parquet **partitionnée par date de publication** (`year/month/day`) ;
   - à chaque run : traiter les jours absents + **re-crawler les N derniers
     jours** (défaut 7) pour rattraper les ajouts tardifs de DILA ;
   - lister l'index HTML du jour → télécharger les `.xml` via `stream_get`
     (réutilise le cache de `src/config.py`).
3. **Détection de format / filtrage** par fichier :
   - présence de `<EFORMS>` → eForms ; sinon → legacy v240 ;
   - fichiers d'attribution → **ignorés** (comptés pour le suivi) ;
   - fichiers ni eForms ni legacy reconnu → comptés et ignorés.
4. **Aplatissement** (voir ci-dessous) → DataFrame polars par jour.
5. **Écriture** de la partition jour.
6. **Concaténation** de toutes les partitions → `sink_parquet` (+ CSV) en
   streaming, comme `decp_processing`.
7. **Publication** sur data.gouv.fr, derrière les mêmes gardes que l'existant
   (`DECP_PROCESSING_PUBLISH`, etc.).

### Aplatissement générique (Python / lxml)

- **Grain = lot.** Les champs de niveau annonce sont répétés sur chaque ligne de
  lot. Une annonce **sans lot** produit **une ligne unique** avec les colonnes
  lot à `null`.
- **Chaque chemin-feuille → une colonne.** Nom de colonne = chemin en **noms
  locaux** (préfixes de namespace retirés), **sans préfixe de format**, segments
  joints par `_` (ex. `ProcurementProjectLot_ProcurementProject_Name`,
  `initial_description_objet`).
- **Éléments répétés au sein d'une ligne → colonnes liste** Arrow (aucune ligne
  supplémentaire créée).
- **Union des colonnes** des deux formats dans la table unique ; `null` là où le
  champ est absent. Les chemins divergent dès la racine
  (`ProcurementProjectLot_…` vs `lots_lot_…`), donc le risque de collision est
  négligeable ; les rares vraies collisions seront suffixées.
- **Exclusions** : `HTML` et `HTMLSYNTHESE` du legacy (gros blobs lisibles).

### Cœur harmonisé

En **plus** des colonnes brutes ci-dessus, un cœur de colonnes typées propres,
mappées depuis les deux formats. Aucune perte de donnée : ces champs figurent
aussi dans les colonnes brutes.

| Colonne                | source eForms                 | source legacy v240             |
| ---------------------- | ----------------------------- | ------------------------------ |
| `source_format`        | `eforms`                      | `boamp`                        |
| `id_annonce`           | `ef_<IDWEB>`                  | `bo_<IDWEB>`                   |
| `type_avis`            | racine (`ContractNotice`)     | `FAMILLE` (`MAPA`…)            |
| `sous_type_avis`       | `SubTypeCode` (16/17)         | `NATURE` / `STATUT`            |
| `date_publication`     | `IssueDate`                   | `GESTION/.../DATE_PUBLICATION` |
| `date_limite_reponse`  | deadline de soumission        | `DATE_LIMITE_REPONSE`          |
| `acheteur_nom`         | organisation déréférencée     | `organisme/acheteurPublic`     |
| `acheteur_siret`       | `CompanyID` (si présent)      | (si présent)                   |
| `objet`                | `ProcurementProject` Name     | `description/objet`            |
| `nature_marche`        | classification                | `natureMarche`                 |
| `type_procedure`       | `ProcedureCode`               | `procedure`                    |
| `cpv_principal`        | `MainCommodityClassification` | descripteurs CPV               |
| `lot_id`               | `ProcurementProjectLot` ID    | numéro de `lots/lot`           |
| `lot_intitule`         | lot Name                      | intitulé du lot                |
| `lot_cpv_principal`    | lot CPV principal             | —                              |
| `lot_cpv_additionnels` | lot CPV additionnels (liste)  | —                              |
| `url_avis`             | construite depuis IDWEB       | construite depuis IDWEB        |
| `fichier_source`       | nom du fichier DILA           | nom du fichier DILA            |

- **Clé de ligne logique** : `id_annonce` + `lot_id`.
- **Déréférencement intra-XML (eForms)** : l'acheteur est référencé par ID vers
  `efac:Organizations` ; le résoudre pour remplir `acheteur_nom` / `acheteur_siret`
  n'est **pas** de l'enrichissement SIRENE, juste une résolution interne au
  document.

> Le cœur est volontairement minimal en v1 et pourra être étendu sans migration
> (les données restent disponibles dans les colonnes brutes).

## Publication

- Nouvelle **ressource parquet** (+ CSV) sur un **nouveau dataset data.gouv.fr**.
- **Prérequis manuel** : créer le dataset et renseigner son ID en
  configuration / variable d'environnement avant la première publication.
- Mêmes gardes anti-publication-de-test que `decp_processing`.

## Tests

- Fixtures XML réduites dans `tests/data/` :
  - un `ContractNotice` eForms,
  - un legacy `MAPA`,
  - un avis d'attribution (eForms ou legacy) pour vérifier qu'il est **bien
    ignoré**.
- Tests unitaires :
  - aplatissement générique (chemins-feuilles, colonnes liste),
  - grain lot + fallback annonce-sans-lot,
  - mapping du cœur harmonisé (dont préfixe `ef_`/`bo_` de `id_annonce`),
  - détection de format et exclusion des avis d'attribution,
  - exclusion `HTML`/`HTMLSYNTHESE`.

## Hors périmètre (v2 potentielle)

- Reprise d'historique avant 2026.
- Versions de schéma legacy antérieures (v010 / v110 / v230).
- Avis d'attribution / `ContractAwardNotice`.
- Enrichissement SIRENE de l'acheteur.
- Liaison rectificatif → annonce d'origine.
- Cœur harmonisé étendu.
