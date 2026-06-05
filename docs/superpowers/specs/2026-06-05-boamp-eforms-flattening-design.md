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

- **Source** : archive DILA `https://echanges.dila.gouv.fr/OPENDATA/BOAMP/…`.
  Un fichier XML par annonce, sous `<année>/<mois>/<jour>/NN-XXXXX.xml`. Deux
  emplacements :
  - flux courant : `/OPENDATA/BOAMP/2026/MM/JJ/` (fichiers quotidiens) ;
  - historique : `/OPENDATA/BOAMP/FluxHistorique/Boamp_v240/<année>/MM/JJ/`
    (2024, 2025), accompagné d'**archives bulk `*.tar.gz`** par année et par
    type (`BOAMP-J-AO`, `BOAMP-N-AO`, `MAPA-AO`, `*-IC-AA`…).
- **Début** : 1ᵉʳ janvier 2024.
  - eForms n'apparaît dans le flux que **vers février 2024** ; **janvier 2024 est
    quasi-intégralement legacy v240**. L'aplatisseur legacy est donc requis dès le
    début de période.
- **Formats** : les deux schémas coexistent sur toute la période 2024 → 2026 :
  - **eForms** (UBL 2.3, dans `DONNEES/EFORMS`),
  - **BOAMP v240 legacy** (dans `DONNEES/<FAMILLE>`, ex. `FNSimple`, `MAPA`,
    `AVIS`, `RECTIFICATIF`, `ANNULATION`…). Le legacy ne disparaît jamais
    totalement (encore fréquent fin 2025).
- **Filtrage par `GESTION/REFERENCE/TYPE_AVIS/NATURE`** : les deux formats sont
  enveloppés dans `jo:ann` et portent le **même** bloc `GESTION` (vérifié :
  eForms `ContractNotice` → `NATURE=APPEL_OFFRE`, `ContractAwardNotice` →
  `NATURE=ATTRIBUTION`). Le filtrage se fait donc par une **seule métadonnée
  commune**, sans distinguer eForms-par-racine vs legacy-par-vocabulaire.
- **Liste blanche `NATURE` (avis inclus)** : `APPEL_OFFRE`, `QUALIFICATION`,
  `RECTIFICATIF`, `MODIFICATION` (+ `STATUT` = `INITIAL` / `RECTIFICATIF` /
  `ANNULATION` / `MODIFICATION` du cycle de vie).
- **Avis exclus** (toute autre `NATURE`, comptés pour le suivi) : `ATTRIBUTION`
  (recoupe DECP), `PRE-INFORMATION`, `PROFIL_ACHETEUR`, `INTENTION_CONCLURE`,
  `PERIODIQUE`, `SOUS-TRAITANCE`, `AUTRE`.
- **Pas d'enrichissement SIRENE** en v1 (données brutes ; SIREN/SIRET conservés
  quand présents dans le XML).

> **Schéma de référence — attention.** Le XSD publié `Boamp_v240.xsd` (racine
> `<ANNONCE>`, `DONNEES/IDENTITE/…` en vocabulaire majuscule) **ne décrit pas**
> les instances réelles (racine `<jo:ann>`, schema **3.2.5**,
> `DONNEES/MAPA|FNSimple|EFORMS` en vocabulaire minuscule). On ne s'appuie donc
> **pas** sur ce XSD pour énumérer les champs (l'aplatisseur est générique). En
> revanche son type `TYPE_AVIS_Type` correspond bien au bloc `GESTION` des
> instances et fournit l'énumération autoritaire des `NATURE` / `STATUT` ci-dessus.

### Distribution observée (échantillonnage 1 jour/mois, 2024-2025)

- **2024-01** : 100 % legacy (`FNSimple`) — pas encore d'eForms.
- **2024-02 → 2025** : eForms dominant — `ContractNotice` (subtypes 16, 17, 19,
  20, 23), `ContractAwardNotice` (29, 30, 32, 33), `PriorInformationNotice`
  (4, 14) — avec du legacy résiduel (`FNSimple`, `MAPA`) en proportion variable
  (forte à nouveau fin 2025).
- **Conséquence** : la sélection ne se fait pas par racine UBL ni par subtype
  mais par la liste blanche `NATURE` (voir Périmètre) — `ContractNotice` et
  legacy d'appel d'offres sont conservés via `NATURE=APPEL_OFFRE/QUALIFICATION`,
  `ContractAwardNotice` et `PriorInformationNotice` écartés via
  `NATURE=ATTRIBUTION/PRE-INFORMATION`.

## Architecture

### Flow `boamp_processing`

1. **Backfill historique (2024-2025)** : récupéré via les **archives bulk
   `*.tar.gz`** de `FluxHistorique/Boamp_v240/<année>/` (un seul téléchargement
   par année/type plutôt que des dizaines de milliers de requêtes), décompressé
   puis aplati. Exécuté une fois.
2. **Acquisition incrémentale (2026 →)** :
   - sortie parquet **partitionnée par date de publication** (`year/month/day`) ;
   - à chaque run : traiter les jours absents + **re-crawler les N derniers
     jours** (défaut 7) pour rattraper les ajouts tardifs de DILA ;
   - lister l'index HTML du jour → télécharger les `.xml` via `stream_get`
     (réutilise le cache de `src/config.py`).
3. **Filtrage (commun aux deux formats)** par `GESTION/REFERENCE/TYPE_AVIS/NATURE` :
   - `NATURE` ∈ liste blanche (`APPEL_OFFRE`, `QUALIFICATION`, `RECTIFICATIF`,
     `MODIFICATION`) → **conservé** ;
   - toute autre `NATURE` → **ignoré** (compté par valeur, pour le suivi) ;
   - **format** déterminé en parallèle pour l'aplatissement : présence de
     `<EFORMS>` → eForms (`source_format=eforms`) ; sinon → legacy v240
     (`source_format=boamp`) ;
   - fichiers sans `GESTION/NATURE` exploitable ou structure non reconnue →
     comptés et ignorés.
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
| `nature_avis`          | `GESTION/…/NATURE` (commun)   | `GESTION/…/NATURE` (commun)    |
| `statut_avis`          | `GESTION/…/STATUT` (commun)   | `GESTION/…/STATUT` (commun)    |
| `famille_avis`         | `GESTION/…/FAMILLE` (commun)  | `GESTION/…/FAMILLE` (commun)   |
| `type_document`        | racine UBL (`ContractNotice`) | famille `DONNEES` (`MAPA`…)    |
| `sous_type_eforms`     | `SubTypeCode` (16/17/19…)     | —                              |
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
  - un legacy de type avis de marché (`FNSimple` ou `MAPA`),
  - un `ContractAwardNotice` (attribution) et un `PriorInformationNotice`
    (préinformation) pour vérifier qu'ils sont **bien ignorés**.
- Tests unitaires :
  - aplatissement générique (chemins-feuilles, colonnes liste),
  - grain lot + fallback annonce-sans-lot,
  - mapping du cœur harmonisé (dont préfixe `ef_`/`bo_` de `id_annonce`),
  - filtrage par liste blanche `NATURE` (commun eForms + legacy) : inclusion de
    `APPEL_OFFRE`/`QUALIFICATION`, exclusion de `ATTRIBUTION`/`PRE-INFORMATION`/etc.,
  - détection du format (`source_format`) via présence de `<EFORMS>`,
  - exclusion `HTML`/`HTMLSYNTHESE`.

## Hors périmètre (v2 potentielle)

- Reprise d'historique avant 2024.
- Versions de schéma legacy antérieures (v010 / v110 / v230).
- Avis d'attribution / `ContractAwardNotice`.
- Avis de préinformation / `PriorInformationNotice`.
- Enrichissement SIRENE de l'acheteur.
- Liaison rectificatif → annonce d'origine.
- Cœur harmonisé étendu.
