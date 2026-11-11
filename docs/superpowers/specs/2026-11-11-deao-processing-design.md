# Flow `deao_processing` — Données essentielles d'appels d'offres (DEAO), BOAMP

Date : 2026-11-11
Issue : #178
Statut : implémenté (plan `docs/superpowers/plans/2026-11-11-deao-processing.md`).
Base : branche `feature/boamp_eforms` (flow `boamp_processing`, spec
`2026-06-05-boamp-eforms-flattening-design.md`), mergée dans `feature/178_boamp` ; le flow
`boamp_processing` est remplacé par `deao_processing`.

## Objectif

Publier sur data.gouv.fr un dataset des **appels d'offres** publiés au BOAMP (y compris
les avis européens transmis au JOUE via le BOAMP), dans un **schéma tabulaire choisi**,
dans l'esprit du dataset DECP, pour une exploitation dans Colibre (recherche d'appels
d'offres).

Perspectives prises en compte dans le design (hors périmètre de cette issue) :

- d'autres sources s'ajouteront (TED en premier) : l'architecture sépare des
  **adaptateurs par source** d'un **tronc commun** ;
- on voudra réconcilier appels d'offres et attributions DECP : on conserve les
  identifiants utiles (`referenceAcheteur`, SIRET acheteur, CPV) et l'archive brute
  contient aussi les avis d'attribution.

## Décisions structurantes

| Sujet               | Décision                                                                                                                                      |
| ------------------- | --------------------------------------------------------------------------------------------------------------------------------------------- |
| Base de code        | On repart de `feature/boamp_eforms` (acquisition, filtrage, parsing eForms/legacy)                                                            |
| Source v1           | Flux DILA seul (`echanges.dila.gouv.fr/OPENDATA/BOAMP`). TED en v2                                                                            |
| Sortie publiée      | **Uniquement le schéma cible**. L'aplatissement générique devient un outil interne (exploration, CSV de définitions)                          |
| Grain               | **Une ligne par appel d'offres × lot** (consolidation des avis initiaux, rectificatifs, annulations)                                          |
| Pré-information     | Incluse, `statut = pre-information` ; en attendant un dataset dédié à la programmation des achats                                             |
| Identifiant         | `uid = <sourceDataset>_<IDWEB de l'avis initial>_<lotId>`, ex. `boamp_26-10255_1`                                                             |
| Nommage             | Noms DECP quand le concept est le même, camelCase sinon                                                                                       |
| Nom du produit      | **DEAO** (Données essentielles d'appels d'offres), dans la continuité de DECP : flow `deao_processing`, variables `DEAO_*`, fichiers `deao.*` |
| Archive brute       | Tous les XML DILA (attributions comprises) archivés par jour sur S3                                                                           |
| Données normalisées | Partitions parquet par jour sur le volume persistant `/data`, reconstructibles depuis S3                                                      |
| Enrichissement      | SIRENE (fonctions DECP), repli géocodage Géoplateforme de l'adresse de l'avis, reconstitution du SIRET par dictionnaire interne               |
| Exécution           | Serveur éphémère Scaleway (#189), instance `STANDARD3-X4C-16G`                                                                                |
| Publication         | `deao.parquet` + `deao.csv.gz` + `schema.json`                                                                                                |

## Constats sur les données (échantillon du 2026-11-11)

Échantillon : 3 jours complets (2025-03-12, 2025-11-05, 2026-10-14), 1 498 fichiers,
1 090 avis retenus. Ce n'est pas une statistique, mais ces constats fondent le design :

- Le dossier d'un jour contient exactement les avis dont `DATE_PUBLICATION` est ce jour.
  Un jour passé est figé ; les dossiers existent aussi le dimanche.
- **Chaînage commun aux deux formats** : tout rectificatif et toute annulation porte
  `GESTION/REFERENCE/ANNONCE_ANTERIEUR/IDWEB` (109/109 rectificatifs).
- ~10 % des avis retenus sont des rectificatifs.
  - eForms : le rectificatif est un **avis complet** (republication intégrale +
    `ChangeDescription`).
  - legacy : le rectificatif est un **différentiel textuel** (`infosRectif`), sans
    `DATE_LIMITE_REPONSE` structurée (0/53).
- `GESTION/INDEXATION/DATE_LIMITE_REPONSE` est **absent des rectificatifs eForms** : la
  date limite eForms se lit dans le corps UBL, **au niveau du lot**
  (`ProcurementProjectLot/TenderingProcess/TenderSubmissionDeadlinePeriod`).
- **Annulations legacy** : `NATURE=ANNULATION` (+ `ANNONCE_ANTERIEUR`), exclues à tort
  par le filtre actuel. Annulations eForms : `NATURE=APPEL_OFFRE`, `STATUT=ANNULATION`.
- **`NATURE=MODIFICATION`** = eForms sous-type 38, modification d'un marché en cours
  d'exécution, chaînée à un avis d'**attribution** : hors appels d'offres, à exclure.
- **SIRET acheteur** : FNSimple 192/192, MAPA 0/47 (aucun champ d'identification),
  eForms 551/866 (64 %, les autres portent un identifiant non SIRET).
- Adresses acheteur structurées dans les deux formats (eForms `PostalAddress` avec NUTS3 ;
  legacy `organisme/adr/{voie,cp,ville}` ou `organisme/{cp,ville}`, `cp` présent 47/47 en MAPA).
- Lots eForms : `LOT-0000` = procédure non allotie, sinon `LOT-0001`, `LOT-0002`…

## Architecture

### Code

```
src/flows/deao_processing.py          # remplace src/flows/boamp_processing.py
src/tasks/deao/
  sources/boamp/
    fetch.py       # repris de src/tasks/boamp/fetch.py (index jour, téléchargement)
    archive.py     # jour <-> .tar.zst sur S3 ; listing des jours archivés
    parse.py       # détection format, filtre NATURE, mapping eForms/legacy -> lignes avis-lot
  consolidate.py   # générique : chaînage, contenu, cycle de vie -> appel d'offres × lot
  enrich.py        # SIRENE, dictionnaire SIRET, géocodage d'adresse, replis géographiques
  schema.py        # contrat avis-lot + schéma publié (noms, types, descriptions)
```

`src/tasks/boamp/flatten.py` et `field_definitions.py` restent disponibles (aplatissement
générique, CSV de définitions des champs) mais sortent du pipeline. Les fonctions de
détection/filtrage de `flatten.py` (`detect_format`, `parse_type_avis`, `is_included`,
`eforms_acheteur`…) sont déplacées ou réutilisées par `parse.py`.

### Déroulé d'un run

0. **Référentiels** — si le répertoire SIRENE du mois est absent, `sirene_preprocess`
   est lancé en sous-flow (comme `decp_processing`) ; un référentiel encore absent arrête
   le run avant toute acquisition.
1. **Acquisition** — pour chaque jour de `BOAMP_START_DATE` (2024-01-01) à aujourd'hui :
   1. si la partition normalisée locale existe et que le jour est hors fenêtre de re-crawl
      (`BOAMP_RECRAWL_DAYS`, défaut **3**) → rien à faire (simple `stat`) ;
   2. sinon, si le jour est dans l'archive S3 (**un seul listing** du préfixe en début de
      run) et hors fenêtre de re-crawl → parsing depuis l'archive ;
   3. sinon → crawl DILA, écriture de l'archive, parsing.
2. **Re-crawl différentiel** (jours dans la fenêtre) : relecture de l'index du jour,
   comparaison avec les noms de fichiers de l'archive, téléchargement des **seuls nouveaux
   fichiers**, réécriture de l'archive et de la partition.
3. **Concaténation** de toutes les partitions (toutes sources) en LazyFrame.
4. **Consolidation** → une ligne par appel d'offres × lot.
5. **Enrichissement** acheteur.
6. **Écriture** `DEAO_DIST_DIR` (défaut `dist/deao/`) : `deao.parquet`, `deao.csv.gz` (sans les colonnes
   liste), `schema.json`.
7. **Publication** data.gouv.fr derrière les garde-fous.
8. **Artefact Prefect** de suivi (voir « Suivi d'exécution »).

### Acquisition DILA

- Index du jour : `…/BOAMP/<AAAA>/<MM>/<JJ>/` ; **repli automatique** sur
  `…/BOAMP/FluxHistorique/Boamp_v240/<AAAA>/<MM>/<JJ>/` en cas de 404. Remplace le
  routage par `BOAMP_ARCHIVE_CUTOFF_YEAR` (supprimé), qui aurait dû être changé chaque année.
- Jour sans dossier (404 sur les deux URL) ou sans fichier → archive vide et **partition
  vide écrite comme marqueur**, pour ne pas re-crawler ce jour à chaque run.
- Parallélisme : on garde le modèle actuel (lots de jours, `run_in_pool`).

### Archive brute S3

- Clé : `boamp/raw/<AAAA>/<MM>/<JJ>.tar.zst`, contenant **tous** les XML du jour (y compris
  attributions et avis exclus), noms de fichiers DILA d'origine.
- Source de vérité immuable, partagée entre prod et dev. Volume estimé : ~3-4 Mo par jour
  compressé (~35 Mo bruts par jour), quelques Go sur la période.
- Fonctions : `list_archived_days()`, `read_day_archive(d) -> dict[nom, bytes]`,
  `write_day_archive(d, files)`.

### Partitions normalisées

- Chemin : `DEAO_DATA_DIR/avis/<source>/<AAAA>/<MM>/<JJ>.parquet` (volume `/data`).
- Contenu : les lignes **avis-lot** au contrat commun (ci-dessous), uniquement pour les
  avis retenus par le filtre.
- `DEAO_REBUILD=true` : suppression de toutes les partitions et reconstruction depuis
  S3, sans requête DILA (à utiliser après tout changement de mapping).

## Filtrage des avis

Sur `GESTION/REFERENCE/TYPE_AVIS` (commun aux deux formats) :

| `NATURE`                                                                | Décision                                      | `typeAvis`     |
| ----------------------------------------------------------------------- | --------------------------------------------- | -------------- |
| `APPEL_OFFRE`, `QUALIFICATION` avec `STATUT=INITIAL`                    | retenu                                        | `initial`      |
| `APPEL_OFFRE`, `QUALIFICATION` avec `STATUT=RECTIFICATIF`               | retenu                                        | `rectificatif` |
| `APPEL_OFFRE` avec `STATUT=ANNULATION`                                  | retenu                                        | `annulation`   |
| `RECTIFICATIF`                                                          | retenu                                        | `rectificatif` |
| `ANNULATION`                                                            | retenu (**nouveau**)                          | `annulation`   |
| `PRE-INFORMATION`, `PERIODIQUE` avec sous-élément `MISE_EN_CONCURRENCE` | retenu (appel à la concurrence)               | `initial`      |
| `PRE-INFORMATION`, `PERIODIQUE` (autres sous-éléments)                  | retenu (**nouveau**), `preInformation = true` | `initial`      |
| `MODIFICATION`                                                          | **exclu** (modification de marché attribué)   | —              |
| toute autre (`ATTRIBUTION`, `EX_ANTE_VOLONTAIRE`, …)                    | exclu, compté                                 | —              |

Les rectificatifs et annulations héritent du `preInformation` de l'avis initial de leur chaîne
(à défaut : `false`).

**Pré-information** : un avis de pré-information simple (eForms sous-types 1 à 9, legacy
`PRE-INFORMATION`/`PERIODIQUE` sans `MISE_EN_CONCURRENCE`) annonce un futur appel d'offres
à une date indéterminée. Il est publié avec les mêmes colonnes ; `dateLimiteReponse` et
`urlConsultation` y sont généralement nuls. Une pré-information valant mise en concurrence
(eForms sous-types 10 à 14) est un appel à la concurrence : traitée comme un appel d'offres.
Aucun lien n'est établi en v1 entre une pré-information et l'avis de marché qui la suit.

Une chaîne composée **uniquement d'annulations** (ex. annulation dont l'avis annulé est
exclu ou antérieur à 2024) est écartée à la consolidation : il n'y a aucun appel d'offres à
décrire.

`sourceFile` (URL DILA du fichier) est recalculé à chaque écriture : un fichier d'une
année antérieure à celle du run pointe vers `FluxHistorique/Boamp_v240/`, où la DILA
déplace les années passées.

## Contrat avis-lot (sortie des adaptateurs de source)

Chaque adaptateur produit des lignes avis-lot comprenant :

- toutes les colonnes descriptives du schéma publié (voir Mapping) ;
- `sourceDataset` (`boamp`), `idAvis` (IDWEB), `avisPrecedent` (IDWEB de
  `ANNONCE_ANTERIEUR`, nul pour un avis initial), `typeAvis`
  (`initial`/`rectificatif`/`annulation`), `preInformation`
  (bool, nul pour un rectificatif ou une annulation, hérité à la consolidation ; non
  publié), `datePublicationAvis`, `sourceFile` ;
- `avisComplet` (bool) : vrai si l'avis décrit entièrement l'appel d'offres (avis initial
  de tout format, rectificatif eForms) ; faux pour un rectificatif ou une annulation legacy ;
- `infosRectificatif` (texte `infosRectif` / `infosAnnulation` legacy, ou
  `ChangeDescription` eForms concaténés) ;
- colonnes d'adresse acheteur (non publiées) : `acheteur_adresse_voie`, `_cp`, `_ville`,
  `_nuts`.

Un avis sans lot produit une ligne avec `lotId = "0"`. Un rectificatif ou une annulation
legacy produit une seule ligne, `lotId = "0"`, qui s'applique à tous les lots.

Ce contrat est **indépendant du format** : la consolidation ne connaît ni eForms ni legacy.

## Mapping des champs

Règles générales :

- **legacy** : priorité au bloc `GESTION/INDEXATION`, commun à toutes les familles
  (`NATURE_MARCHE`, `TYPE_PROCEDURE`, `DATE_LIMITE_REPONSE`, `DEP_PUBLICATION`,
  `RESUME_OBJET`), puis corps de la famille (`FNSimple`, `MAPA`, `DSP`, …) pour le reste ;
- **eForms** : la valeur au niveau du lot l'emporte sur celle de l'avis.

| Colonne                                         | eForms (BT)                                                                                                                     | Legacy                                                                                            | Type / remarques                                                                                                                                                                                                                     |
| ----------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| `objet`                                         | `ProcurementProject/Name` de l'avis (BT-21)                                                                                     | `INDEXATION/RESUME_OBJET`, sinon `intitule` / `objet`                                             | String                                                                                                                                                                                                                               |
| `description`                                   | `Description` lot, sinon avis (BT-24)                                                                                           | `natureMarche/description` (FNSimple), `description/*` (MAPA)                                     | String                                                                                                                                                                                                                               |
| `type`                                          | `ProcurementTypeCode` (BT-23)                                                                                                   | `INDEXATION/NATURE_MARCHE`                                                                        | `Travaux` / `Fournitures` / `Services`                                                                                                                                                                                               |
| `codeCPV`                                       | `MainCommodityClassification` lot, sinon avis (BT-262)                                                                          | `codeCPV/objetPrincipal/classPrincipale`                                                          | String, 8 chiffres                                                                                                                                                                                                                   |
| `procedure`                                     | `ProcedureCode` (BT-105)                                                                                                        | `INDEXATION/TYPE_PROCEDURE`                                                                       | libellés BOAMP (ex. `Procédure adaptée`, `Appel d'offres ouvert`, `Dialogue compétitif`), **pas** l'énumération DECP à 4 valeurs ; codes eForms et legacy ramenés à une liste commune ; valeur non mappée conservée brute et comptée |
| `techniques`                                    | `ContractingSystem` accord-cadre (BT-765), SAD (BT-766)                                                                         | accord-cadre / SAD si présents                                                                    | List(String), valeurs DECP                                                                                                                                                                                                           |
| `lieuExecution_code` / `lieuExecution_typeCode` | `RealizedLocation/…/CountrySubentityCode` (NUTS3) → département                                                                 | `lieuExecution/cp`                                                                                | eForms : `Code département` via table statique NUTS3 → département ; legacy : `Code postal`                                                                                                                                          |
| `dureeMois`                                     | `PlannedPeriod/DurationMeasure` (MONTH/DAY/YEAR)                                                                                | `dureeMois`, `duree/nbMois`, `duree/nbJours`                                                      | Int, **arrondi à l'entier supérieur** (jours / 30)                                                                                                                                                                                   |
| `montantEstime`                                 | `EstimatedOverallContractAmount` lot, sinon avis (BT-27)                                                                        | `valeurEstimee/valeur` (nul si seule une fourchette)                                              | Float                                                                                                                                                                                                                                |
| `dateLimiteReponse`                             | `TenderSubmissionDeadlinePeriod` du lot (`EndDate` + `EndTime`)                                                                 | `INDEXATION/DATE_LIMITE_REPONSE`, sinon `procedure/dateReceptionOffres`, `delais/receptionOffres` | Datetime Europe/Paris                                                                                                                                                                                                                |
| `urlConsultation`                               | `CallForTendersDocumentReference/…/URI` (BT-15)                                                                                 | `urlDocConsul` (FNSimple), URL `adressesComplt/document` (MAPA), sinon `urlProfilAcheteur`        | String                                                                                                                                                                                                                               |
| `referenceAcheteur`                             | `ProcurementProject/cbc:ID` (BT-22)                                                                                             | `numeroReference` si présent                                                                      | String                                                                                                                                                                                                                               |
| `acheteur_id`                                   | organisation acheteuse déréférencée (pas le `ServiceProviderParty`) → `PartyLegalEntity/CompanyID` si `siret_valide`, sinon nul | `codeIdentificationNational` si `typeIdentificationNational=siret` et `siret_valide`              | String 14                                                                                                                                                                                                                            |
| `acheteur_nom`                                  | `PartyName/Name`                                                                                                                | `nomOfficiel` / `acheteurPublic`                                                                  | String                                                                                                                                                                                                                               |
| `lotId`                                         | `LOT-000n` → `n` ; `LOT-0000` → `0`                                                                                             | `numLot` ; absent ou en double dans l'avis → rang du lot ; pas de lot → `0`                       | String                                                                                                                                                                                                                               |
| `lotIntitule`                                   | `Name` du `ProcurementProject` du lot (enfant direct, pas le premier `Name` descendant)                                         | intitulé / description du lot                                                                     | String                                                                                                                                                                                                                               |
| `urlAvis`                                       | `https://www.boamp.fr/pages/avis/?q=idweb:"<IDWEB>"`                                                                            | idem                                                                                              | String                                                                                                                                                                                                                               |

Les chemins XML exacts par famille legacy (MAPA a plusieurs variantes, ex.
`lieuExecution` / `lieuExecutionLivraison`) sont établis pendant l'implémentation à partir
de `reference/boamp_field_definitions.csv` et validés sur un échantillon de plusieurs jours
par le rapport de taux de remplissage.

`siret_valide(s)` (nouveau, `src/tasks/utils.py`) : 14 chiffres et clé de Luhn ; exception
La Poste (SIREN `356000000`) : somme des chiffres divisible par 5.

## Consolidation (`consolidate.py`)

Entrée : lignes avis-lot de toutes les sources. Sortie : une ligne par appel d'offres × lot.

### 1. Chaînage

Remontée de `avisPrecedent` jusqu'à la racine par jointures itératives (point fixe,
plafond de 20 itérations contre les cycles, cycles logués). Si la racine pointe vers un
avis absent des données (antérieur à 2024 ou manquant), **cet IDWEB est tout de même
retenu comme racine** pour que l'`uid` soit stable.

→ `idAvisInitial`, `uid = <sourceDataset>_<idAvisInitial>_<lotId>`.

### 2. Contenu de la ligne

- **Champs descriptifs** : ceux du **dernier avis complet** (`avisComplet = true`)
  contenant ce lot, ordre `datePublicationAvis` puis IDWEB.
- **Avis non complets** (rectificatif / annulation legacy, `lotId = "0"`) : appliqués à
  tous les lots de l'appel d'offres ; ne modifient aucun champ descriptif.
- Un lot présent dans un avis complet antérieur mais absent du dernier conserve sa dernière
  version connue.
- Une chaîne sans aucun avis complet mais avec au moins un rectificatif (ex. rectificatif
  legacy dont l'initial est antérieur à 2024) produit une ligne à partir du dernier
  rectificatif, champs descriptifs éventuellement pauvres.
- Une chaîne composée uniquement d'annulations est écartée (comptée dans le suivi).

### 3. Cycle de vie

| Colonne                    | Règle                                                                                                                                                                                                                                                                                                                                                                                                  |
| -------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| `avisLies`                 | tous les IDWEB de la chaîne, triés                                                                                                                                                                                                                                                                                                                                                                     |
| `idDernierAvis`            | IDWEB du dernier avis de la chaîne                                                                                                                                                                                                                                                                                                                                                                     |
| `datePublicationInitiale`  | date de l'avis initial, à défaut du plus ancien avis disponible                                                                                                                                                                                                                                                                                                                                        |
| `dateDerniereModification` | date du dernier rectificatif ou de l'annulation ; nulle s'il n'y a que l'avis initial                                                                                                                                                                                                                                                                                                                  |
| `rectifie`                 | vrai si la chaîne contient au moins un `rectificatif`                                                                                                                                                                                                                                                                                                                                                  |
| `infosRectificatifs`       | liste des textes `infosRectificatif` de la chaîne                                                                                                                                                                                                                                                                                                                                                      |
| `preInformation` (interne) | celui de l'avis initial de la chaîne (à défaut `false`), non publié                                                                                                                                                                                                                                                                                                                                    |
| `statut`                   | `annule` si la chaîne contient une `annulation` ; sinon **`pre-information`** si `preInformation` (seule date connue : la publication) ; sinon, si `dateLimiteReponse` renseignée : `clos` si elle est antérieure à la date du run, `ouvert` sinon ; si `dateLimiteReponse` nulle : `ouvert` si `datePublicationInitiale` date de moins de `DEAO_STATUT_DELAI_JOURS` (défaut 90) jours, `clos` au-delà |

Limite documentée : un rectificatif legacy qui repousse la date limite ne la met pas à jour
(texte libre) ; l'appel d'offres peut passer `clos` à tort. `rectifie` et
`infosRectificatifs` permettent à l'utilisateur de le voir.

La date du run est un paramètre de la fonction (injectable en test).

### 4. Contrôle

Unicité de `uid` vérifiée ; en cas de doublons (même chaîne, même `lotId`, donc même
dernier avis) : avertissement avec comptage, conservation de la ligne au plus petit
`lotIntitule`, puis `objet`, puis `description` (choix déterministe).

## Enrichissement acheteur (`enrich.py`)

Ordre d'application :

1. **Dictionnaire SIRET interne** — pour les lignes sans `acheteur_id` : table
   `(nom normalisé, code postal) → SIRET` construite à partir des lignes qui ont un SIRET
   (code postal de l'adresse acheteur de l'avis, présent dans les deux formats), en ne
   gardant que les clés non ambiguës (un seul SIRET). Nom normalisé : minuscules, accents
   retirés, ponctuation → espace, espaces multiples réduits. Le code postal (plutôt que le
   code commune géocodé) évite de faire dépendre le dictionnaire du géocodage.
2. **SIRENE** — pour les lignes avec `acheteur_id` : réutilisation de
   `add_unite_legale_data` et `add_etablissement_data` (`src/tasks/enrich.py`,
   `siret_column="acheteur_id"`, `type_siret="acheteur"`) sur les parquets de
   `sirene_preprocess` → `acheteur_categorie`, `acheteur_commune_*`,
   `acheteur_departement_*`, `acheteur_region_*`, `acheteur_latitude/longitude`
   (`siret_latlong`). `acheteur_nom` SIRENE prioritaire, nom de l'avis en repli.
3. **Géocodage d'adresse** — pour les lignes sans SIRET, ou dont le SIRET est absent de
   SIRENE :
   - clé = adresse normalisée `voie | cp | ville`, dédoublonnée ;
   - cache S3 `adresse_geocode.parquet` (`adresse_cle`, `commune_code`, `latitude`,
     `longitude`, `score`, `status`, `geocoded_at`), sur le modèle de `siret_latlong` ;
     seules les adresses absentes du cache sont géocodées ; les échecs sont mis en cache
     (re-tentative après `GEOCODING_RETRY_DAYS`) ;
   - appel batch `/search/csv/` de la Géoplateforme : `build_geocoding_csv` et
     `parse_geocoding_results` de `src/tasks/geocode.py` généralisés (colonne clé
     paramétrable, récupération de `result_citycode`), même `GEOCODING_MIN_SCORE` ;
   - `commune_code` → commune, département, région via le COG (`get_cog`).
4. **Replis géographiques** — si le géocodage échoue : `acheteur_departement_code` déduit
   du code postal (2 premiers chiffres ; `2A`/`2B` pour la Corse via le COG ; 3 chiffres
   pour l'outre-mer), puis du NUTS3 eForms.

`acheteur_id_source` : `avis` (SIRET présent dans l'avis), `dictionnaire`, ou nul.

Hors périmètre v1 : recherche de SIRET par l'API Recherche d'entreprises.

## Schéma publié

Ordre des colonnes calqué sur `reference/schema_base.json` (DECP) : les colonnes homonymes
gardent la position relative qu'elles ont dans les DECP, les colonnes propres aux appels
d'offres sont insérées à côté de leur équivalent DECP le plus proche (indiqué entre
parenthèses).

| #   | Colonne                     | Position DECP de référence                     |
| --- | --------------------------- | ---------------------------------------------- |
| 1   | `uid`                       | `uid`                                          |
| 2   | `referenceAcheteur`         | (`id`)                                         |
| 3   | `acheteur_id`               | `acheteur_id`                                  |
| 4   | `acheteur_id_source`        | (après `acheteur_id`)                          |
| 5   | `acheteur_nom`              | `acheteur_nom`                                 |
| 6   | `objet`                     | `objet`                                        |
| 7   | `lotId`                     | (après `objet`)                                |
| 8   | `lotIntitule`               | (après `objet`)                                |
| 9   | `description`               | (après `objet`)                                |
| 10  | `montantEstime`             | (`montant`)                                    |
| 11  | `type`                      | `type`                                         |
| 12  | `codeCPV`                   | `codeCPV`                                      |
| 13  | `procedure`                 | `procedure`                                    |
| 14  | `techniques`                | `techniques`                                   |
| 15  | `dureeMois`                 | `dureeMois`                                    |
| 16  | `datePublicationInitiale`   | (`dateNotification`, `datePublicationDonnees`) |
| 17  | `dateDerniereModification`  | (idem)                                         |
| 18  | `dateLimiteReponse`         | (idem)                                         |
| 19  | `lieuExecution_code`        | `lieuExecution_code`                           |
| 20  | `lieuExecution_typeCode`    | `lieuExecution_typeCode`                       |
| 21  | `acheteur_categorie`        | `acheteur_categorie`                           |
| 22  | `acheteur_commune_code`     | `acheteur_commune_code`                        |
| 23  | `acheteur_commune_nom`      | `acheteur_commune_nom`                         |
| 24  | `acheteur_departement_code` | `acheteur_departement_code`                    |
| 25  | `acheteur_departement_nom`  | `acheteur_departement_nom`                     |
| 26  | `acheteur_region_code`      | `acheteur_region_code`                         |
| 27  | `acheteur_region_nom`       | `acheteur_region_nom`                          |
| 28  | `acheteur_latitude`         | `acheteur_latitude`                            |
| 29  | `acheteur_longitude`        | `acheteur_longitude`                           |
| 30  | `statut`                    | (`donneesActuelles`)                           |
| 31  | `rectifie`                  | (`donneesActuelles`)                           |
| 32  | `infosRectificatifs`        | (`donneesActuelles`)                           |
| 33  | `urlConsultation`           | (avant `sourceDataset`)                        |
| 34  | `urlAvis`                   | (avant `sourceDataset`)                        |
| 35  | `idAvisInitial`             | (avant `sourceDataset`)                        |
| 36  | `idDernierAvis`             | (avant `sourceDataset`)                        |
| 37  | `avisLies`                  | (avant `sourceDataset`)                        |
| 38  | `sourceDataset`             | `sourceDataset`                                |
| 39  | `sourceFile`                | `sourceFile`                                   |

`type` : mêmes valeurs et même sens que dans les DECP (`Fournitures`, `Services`,
`Travaux`) ; les descriptions des colonnes homonymes sont reprises de `schema_base.json`
et adaptées aux appels d'offres.

Colonnes liste (`avisLies`, `infosRectificatifs`, `techniques`) : présentes dans le
parquet, exclues du CSV (comportement actuel de `sink_to_files`).

`schema.py` porte noms, types polars et descriptions ; `schema.json` (Table Schema) en est
généré et publié avec les données.

## Publication et déploiement

- Dataset data.gouv.fr existant (actuel `BOAMP_DATASET_ID`), réutilisé. Ressources : `deao.parquet`, `deao.csv.gz`,
  `schema.json`. Variables `DEAO_DATASET_ID`, `DEAO_PARQUET_RESOURCE_ID`,
  `DEAO_CSV_RESOURCE_ID`, `DEAO_SCHEMA_RESOURCE_ID`, qui remplacent les
  `BOAMP_*_ID` actuels (mêmes valeurs ; ressource `schema.json` à créer).
- Garde-fous : `DECP_PROCESSING_PUBLISH` **et** au moins `DEAO_MIN_ROWS_TO_PUBLISH`
  (défaut 50 000) lignes.
- Déploiement : `deployments.py` remplace `boamp_processing` par un lanceur
  `deao-processing` (`run_on_scaleway`, pool `local`) + `deao-processing-exec-prod` /
  `-dev` sur `scaleway-prod` / `scaleway-dev`.
  - `instance_type="STANDARD3-X4C-16G"` (disponible en `nl-ams-2`, zone
    du volume `decp-data-<env>`, vérifié).
  - `dist_dir` **séparé** : `/srv/shared/deao/<env>/dist`. Le lanceur rapatrie tout
    `/work/dist/` avec `rsync --delete` puis bascule le répertoire : viser le `dist_dir`
    des DECP effacerait les fichiers DECP du serveur.
  - Quotidien, planifié **après** `decp-processing` (5 h), par ex. 9 h : la limite de
    concurrence `volume-<env>` (= 1) sérialise de toute façon les deux runs.
- Le flow n'a aucune dépendance à son lieu d'exécution (chemins par l'environnement) :
  passer au serveur habituel ne demanderait que `deployments.py` + SIRENE sur ce serveur.
- Estimation mémoire : ~800 000 lignes avis-lot, pic attendu de quelques Go. À mesurer sur
  un rebuild complet en écrivant la sortie complète (`sink_parquet` sous
  `/usr/bin/time -v`), pas avec `select(pl.len())`.

## Configuration

| Variable                                | Défaut                                         | Rôle                                                                        |
| --------------------------------------- | ---------------------------------------------- | --------------------------------------------------------------------------- |
| `BOAMP_BASE_URL`                        | `https://echanges.dila.gouv.fr/OPENDATA/BOAMP` | inchangée                                                                   |
| `BOAMP_START_DATE`                      | `2024-01-01`                                   | inchangée                                                                   |
| `BOAMP_RECRAWL_DAYS`                    | `3`                                            | fenêtre de re-crawl différentiel (était 7, 14 dans `template.env`)          |
| `BOAMP_ARCHIVE_CUTOFF_YEAR`             | —                                              | **supprimée** (repli 404)                                                   |
| `BOAMP_INCLUDED_NATURES`                | voir Filtrage                                  | `+ANNULATION`, `−MODIFICATION`                                              |
| `DEAO_DATA_DIR`                         | `DATA_DIR / "deao"`                            | partitions normalisées                                                      |
| `DEAO_DIST_DIR`                         | `DIST_DIR / "deao"`                            | sorties (ne pas renseigner sur l'instance : seul `/work/dist` est rapatrié) |
| `DEAO_S3_PREFIX`                        | `boamp/raw` (par source)                       | archive brute                                                               |
| `DEAO_REBUILD`                          | `false`                                        | reconstruction des partitions depuis S3                                     |
| `DEAO_STATUT_DELAI_JOURS`               | `90`                                           | statut sans date limite                                                     |
| `DEAO_MIN_ROWS_TO_PUBLISH`              | `50000`                                        | garde-fou de publication                                                    |
| `DEAO_*_RESOURCE_ID`, `DEAO_DATASET_ID` | `""`                                           | publication                                                                 |

## Suivi d'exécution

- XML illisible : compté, logué (nom de fichier), conservé dans l'archive ; le jour est
  traité.
- Avis exclus : comptés par `NATURE`.
- Valeurs non mappées (`procedure`, `type`, unité de durée…) : comptées par valeur,
  loguées.
- Artefact Prefect de fin de run : avis lus / retenus / exclus par `NATURE`, erreurs de
  parsing, appels d'offres × lots par `statut`, **taux de remplissage par colonne et par
  format**, répartition de `acheteur_id_source`.

## Écarts de la spec du 2026-06-05 rattrapés

| Écart (audit du 2026-11-11)                                                                     | Traitement                                                |
| ----------------------------------------------------------------------------------------------- | --------------------------------------------------------- |
| 5 colonnes du cœur manquantes (`nature_marche`, `type_procedure`, `cpv_principal`, `lot_cpv_*`) | absorbées par le mapping (`type`, `procedure`, `codeCPV`) |
| `acheteur_siret` legacy toujours nul                                                            | `codeIdentificationNational` (FNSimple) + dictionnaire    |
| avis ignorés non comptés, XML malformés silencieux                                              | Suivi d'exécution                                         |
| collisions de colonnes non suffixées                                                            | sans objet (mapping explicite)                            |
| fixtures manquantes                                                                             | Tests                                                     |
| `lot_intitule` eForms = premier `Name` descendant                                               | `Name` du `ProcurementProject` enfant direct du lot       |

## Tests

Fixtures : **avis réels** (échantillon DILA), chaînes complétées si besoin, dans
`tests/data/deao/` :

- chaîne eForms initial → rectificatif (26-87868 → 26-99074) ;
- chaîne legacy FNSimple initial → rectificatif (26-92574 → 26-99891) ;
- chaîne legacy initial → annulation (26-99177 → 26-99542) ;
- MAPA sans SIRET ; eForms avec `CompanyID` non SIRET ; eForms `LOT-0000` ;
- exclusions : `MODIFICATION` (sous-type 38), `ATTRIBUTION` ;
- `PRE-INFORMATION` simple (→ `statut = pre-information`) et valant `MISE_EN_CONCURRENCE`
  (eForms sous-type 10, ex. 26-98723 → statut d'appel d'offres).

Tests unitaires :

- filtre et `typeAvis` / `preInformation` / `avisComplet` par cas ;
- mapping par format (colonne par colonne sur les fixtures), `lotId`, `dureeMois` (`ceil`),
  `dateLimiteReponse` eForms au niveau du lot ;
- `siret_valide` (Luhn, exception La Poste, longueur, non numérique) ;
- chaînage : chaîne simple, racine absente, cycle ;
- contenu : dernier avis complet, rectificatif legacy appliqué à tous les lots, lot absent
  du dernier avis ;
- `statut` avec date de run injectée : annulation, date limite passée / future, date limite
  nulle avant / après 90 jours, pré-information → `pre-information`, pré-information annulée → `annule` ;
- dictionnaire SIRET : clé non ambiguë appliquée, clé ambiguë ignorée ;
- replis département : code postal (dont Corse, outre-mer), NUTS3 ;
- acquisition : re-crawl différentiel, partition vide marqueur, repli 404 →
  `FluxHistorique`, lecture depuis l'archive (S3 et HTTP simulés) ;
- bout en bout : flow sur fixtures sans réseau, unicité de `uid`, présence des colonnes du
  schéma publié.

Lancement : `uv run pytest`.

## Hors périmètre (v2 potentielle)

- Source TED (et dédoublonnage BOAMP/TED via le notice-id eForms).
- Recherche de SIRET par l'API Recherche d'entreprises.
- Mapping des avis d'attribution (présents dans l'archive brute).
- Réconciliation appels d'offres ↔ DECP.
- Correspondance `procedure` BOAMP → énumération DECP (pour la réconciliation).
- Lien pré-information → avis de marché correspondant ; dataset dédié à la programmation
  des achats (ex. APProch).
- Historique antérieur à 2024, versions legacy antérieures à 3.2.5.
- Mise à jour de la date limite depuis le texte des rectificatifs legacy.
