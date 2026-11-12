# DEAO — source JOUE (TED)

Date : 2026-11-12
Issue : #165
Statut : design validé, à planifier.
Base : flow `deao_processing` implémenté (spec `2026-11-11-deao-processing-design.md`,
branche `feature/178_boamp`). Cette spec n'en reprend que ce qui change.

Nom de code de la source : **`joue`** (`sourceDataset`, préfixe d'`uid`, module, archive
S3, partitions, `DEAO_SOURCES`) : le JOUE est plus connu en France que TED. « TED » ne
désigne dans ce document que le service de la Commission (API, site, URL) par lequel on
accède aux avis du JOUE.

## Intention

Afficher dans Colibre **le plus grand nombre possible d'appels d'offres publics**. Le BOAMP
seul a un angle mort structurel : l'article R2131-16 du Code de la commande publique
n'impose la publication au BOAMP (en plus du JOUE) qu'à l'État, à ses établissements
publics autres qu'EPIC, aux collectivités territoriales et à leurs établissements. Les
« autres acheteurs » ne publient qu'au JOUE : bailleurs sociaux (OPH, SA d'HLM), opérateurs
de réseaux (SNCF Réseau, Enedis, RTE, ADP, Keolis…), SEM/SPL, EPIC (CEA, CNES…). TED est
la seule source de leurs appels d'offres au-dessus des seuils européens.

## Constats (mesure du 2026-11-12)

Semaine TED du 12 au 18/10/2026, acheteurs français : 1 961 avis dont 1 333 appels à la
concurrence ; BOAMP du 01 au 21/10/2026 (7 878 XML). Scripts de mesure non versionnés.

- **79 %** des appels à la concurrence TED (1 059/1 333) sont au BOAMP avec le **même
  identifiant d'avis eForms** (BT-701, `cbc:ID schemeName="notice-id"`) ; 76 % des
  attributions.
- **100 %** des avis eForms du BOAMP sont sur TED : le BOAMP eForms est un sous-ensemble de
  TED, avec le même corps UBL.
- Délai de publication TED − BOAMP : 0 à 2 jours (une seule exception à −1).
- Les 274 appels à la concurrence TED absents du BOAMP sont presque tous des « autres
  acheteurs » (R2131-16). Environ 5 viennent de collectivités ou de l'État (non-conformité
  ou envoi direct), marginal.
- **Doubles envois** (même procédure envoyée séparément au BOAMP et à TED, identifiants
  eForms différents) : 1 cas détecté sur 274. Négligeable.
- Acheteurs étrangers avec exécution en France : 3 appels à la concurrence sur la semaine.
- Acheteurs français à code pays d'outre-mer, depuis 2024 : `ATF` 6, `PYF` 13, `NCL` 17,
  `SPM` 11, `WLF`/`BLM`/`MAF` 0.
- Volume du périmètre (requête ci-dessous) : 75 929 avis de janvier à octobre 2026, soit
  ~350 par jour ouvré, attributions comprises.
- Champs TED **inutilisables** pour classer : `organisation-name-ted-esen` (l'eSender
  déclaré n'est pas le canal réel ; 51 % non renseigné) et `buyer-legal-type` (beaucoup
  d'OPH et de SEM déclarés `la`, autorité locale).
- **Chaînage TED** : chaque avis, rectificatif compris, a son propre identifiant eForms. Un
  rectificatif ou une annulation porte `efbc:ChangedNoticeIdentifier` =
  `<identifiant eForms de l'avis modifié>-<version sur 2 chiffres>`, et un code motif
  (`cbc:ReasonCode listName="change-corrig-justification"`).
- Le XML TED porte le numéro de publication :
  `efbc:NoticePublicationID schemeName="ojs-notice-id"` (`00700383-2026`, zéros de tête à
  retirer → `700383-2026`), et la date de publication (`efbc:PublicationDate`).
- L'API permet de retrouver le numéro de publication d'un identifiant eForms :
  requête `notice-identifier=<uuid>`.

## Décisions

| Sujet                  | Décision                                                                                                                                                                             |
| ---------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| Périmètre TED          | Acheteur français (`FRA` + `ATF PYF NCL WLF SPM BLM MAF`) **ou** lieu d'exécution en France (mêmes codes)                                                                            |
| Langue                 | Textes en langue étrangère acceptés ; version française prise quand l'avis en contient une                                                                                           |
| Recouvrement BOAMP/TED | **Le BOAMP fait foi** : toute chaîne TED qui recoupe le BOAMP est écartée ; les lignes BOAMP et leurs `uid` sont inchangées                                                          |
| Doubles envois         | Acceptés comme doublons (pas de rapprochement approximatif)                                                                                                                          |
| Identifiant            | `uid = joue_<numéro de publication de l'avis initial>_<lotId>`, ex. `joue_707441-2026_1` (choix de l'utilisateur) ; numéro d'un avis initial absent des données résolu par l'API TED |
| Architecture           | Un adaptateur `sources/joue/` au contrat avis-lot existant ; tronc commun (consolidation, enrichissement, publication) partagé                                                       |
| Historique             | Depuis `BOAMP_START_DATE` (2024-01-01), même variable pour les deux sources _(mon choix)_                                                                                            |
| Acquisition            | API de recherche TED v3 (liste des avis du jour) puis téléchargement du XML de chaque avis _(mon choix)_                                                                             |
| Archive brute          | Tous les XML du périmètre, attributions comprises, par jour sur S3 `joue/raw/AAAA/MM/JJ.tar.zst` _(mon choix, symétrie avec le BOAMP)_                                               |
| Mapping                | Réutilisation du mapping eForms BOAMP, découplé de l'enveloppe DILA _(mon choix)_                                                                                                    |
| Flow                   | Le même flow `deao_processing` acquiert les deux sources, l'une après l'autre ; même dataset publié _(mon choix)_                                                                    |

## Acquisition TED

### Liste des avis d'un jour

`POST https://api.ted.europa.eu/v3/notices/search` (anonyme), `paginationMode=ITERATION`,
`limit=250`, `scope=ALL`, en suivant `iterationNextToken` :

```
(buyer-country IN (FRA ATF PYF NCL WLF SPM BLM MAF)
 OR place-of-performance IN (FRA ATF PYF NCL WLF SPM BLM MAF))
AND publication-date=<AAAAMMJJ>
```

Champs demandés : au minimum `publication-number`, `notice-identifier`. Un jour sans avis
(week-end, jour férié : TED ne publie que du lundi au vendredi) donne une archive vide et
une partition vide, comme les jours BOAMP sans dossier.

### Téléchargement

XML de chaque avis : `https://ted.europa.eu/en/notice/<publication-number>/xml`. Nom de
fichier dans l'archive : `<publication-number>.xml`.

### Reprise du modèle BOAMP

Plan par jour identique (`plan_days` : partition locale → rien ; archive S3 hors fenêtre →
parsing depuis l'archive ; sinon crawl), re-crawl différentiel sur `BOAMP_RECRAWL_DAYS`
(renommage éventuel en `DEAO_RECRAWL_DAYS` laissé au builder), lots de jours en parallèle
via `run_in_pool`, `DEAO_REBUILD` qui reconstruit les partitions des deux sources depuis
S3. Le code d'acquisition aujourd'hui spécifique au BOAMP (`acquire.py`, `archive.py` :
préfixe S3 et chemin de partition codés en dur sur `boamp`) est paramétré par source ;
`DEAO_S3_PREFIX` devient un préfixe par source (`boamp/raw`, `joue/raw`).

Partitions : `DEAO_DATA_DIR/avis/joue/AAAA/MM/JJ.parquet`.

Volume du rattrapage initial : ~250 000 XML depuis 2024. L'API et le site TED peuvent
limiter le débit : gérer les réponses 429/5xx (attente puis nouvel essai), et régler le
parallélisme pour TED indépendamment du BOAMP. Si le débit le rend irréaliste, le builder
peut passer, pour le rattrapage seulement, par les paquets quotidiens de TED (tous les
avis de l'UE, filtrés localement) ; le run quotidien reste sur l'API.

Un jour TED en erreur est compté dans `jour_en_erreur` et fait échouer le run en fin de
flow, comme pour le BOAMP (les jours sains sont écrits et publiés).

## Adaptateur TED (`src/tasks/deao/sources/joue/`)

### Classement

Pas de bloc `GESTION` : le classement se fait sur le corps eForms.

| Avis                                                                                                                              | Décision                        | `typeAvis`     |
| --------------------------------------------------------------------------------------------------------------------------------- | ------------------------------- | -------------- |
| Sous-types 10 à 24 (appels à la concurrence, y compris pré-information valant mise en concurrence) sans `ChangedNoticeIdentifier` | retenu                          | `initial`      |
| Sous-types 1 à 9 (pré-information simple) sans `ChangedNoticeIdentifier`                                                          | retenu, `preInformation = true` | `initial`      |
| Avis portant `ChangedNoticeIdentifier`, motif d'annulation                                                                        | retenu                          | `annulation`   |
| Avis portant `ChangedNoticeIdentifier`, autre motif                                                                               | retenu                          | `rectificatif` |
| Sous-types 25 à 40 (attributions, modifications de marché), sous-type E… éventuels                                                | exclu, compté par sous-type     | —              |

Le code motif d'annulation exact (liste `change-corrig-justification`, SDK eForms) et la
façon dont un rectificatif d'un sous-type exclu se présente sont à établir sur échantillon
par le builder. La règle de fond est celle du BOAMP : ce qui est retenu, c'est ce qui
décrit ou modifie un appel d'offres ou une pré-information.

Précision (plan d'implémentation) : le classement se fait sur le type de formulaire,
`cbc:NoticeTypeCode/@listName` (`planning` → pré-information simple ; `competition` →
appel à la concurrence, y compris pré-information valant mise en concurrence ; toute autre
valeur → exclu), et sur le code motif `cancel` pour les annulations (vérifié sur l'API :
`cancel`, `cor-buy`, `update-add`…). C'est équivalent aux plages de sous-types ci-dessus,
et ça couvre aussi les sous-types « E » hors directives (ex. `E3`, listName `competition`).

Ce découpage est **aligné sur le filtre BOAMP**, vérifié sur les avis eForms du BOAMP du
01 au 21/10/2026 : sous-types 1-10 → `PRE-INFORMATION`, 15 → `QUALIFICATION`, 16-20 et 23
→ `APPEL_OFFRE` (retenus) ; 25-28 → `EX_ANTE_VOLONTAIRE`, 29-36 → `ATTRIBUTION`, 38 →
`MODIFICATION` (exclus). Un même avis est donc retenu ou exclu de la même façon quelle que
soit la source, ce qui permet de faire le contrôle de recouvrement sur les seules
partitions BOAMP (avis retenus). Les sous-types non observés (11-14, 21, 22, 24) suivent la
même règle ; un écart constaté plus tard se corrige dans le classement TED.

Limite acceptée, commune aux deux sources : une procédure déclarée infructueuse ou
abandonnée par un avis d'attribution (sous-types 29-37) n'est pas marquée `annule` ; seuls
les avis de modification portant un motif d'annulation le sont _(mon choix)_.

### Lignes avis-lot

- Corps : le mapping eForms existant (`sources/boamp/eforms.py`) est extrait vers un module
  commun aux deux sources, qui prend en entrée l'élément UBL de l'avis. Les apports propres
  au BOAMP (repli `GESTION/INDEXATION` pour `type` et `procedure`, IDWEB, `urlAvis` BOAMP)
  restent dans l'adaptateur BOAMP. Les lignes BOAMP produites doivent rester identiques
  (tests existants).
- Langue : pour chaque texte multilingue, la valeur `languageID="FRA"` si elle existe,
  sinon la première.
- `sourceDataset = "joue"`, `idAvis` = numéro de publication, `avisComplet = true` (un
  rectificatif eForms est un avis complet), `datePublicationAvis` = `PublicationDate`,
  `formatSource = "eforms"`.
- `avisPrecedent` : numéro de publication de l'avis désigné par `ChangedNoticeIdentifier`
  (voir Identifiants).
- `urlAvis` : `https://ted.europa.eu/fr/notice/-/detail/<publication-number>`.
- `sourceFile` : `https://ted.europa.eu/en/notice/<publication-number>/xml`.
- `lieuExecution_code` : département depuis le NUTS3 français, comme pour le BOAMP ; nul
  pour un lieu hors de France.

### Nouvelles colonnes internes du contrat avis-lot (non publiées)

- `idEforms` : identifiant eForms de l'avis (BT-701). Renseigné par **les deux** adaptateurs
  (BOAMP : avis eForms seulement ; nul pour le legacy).
- `idEformsPrecedent` : identifiant eForms tiré de `ChangedNoticeIdentifier`, sans le
  suffixe de version. Renseigné par les deux adaptateurs quand il existe.
- `acheteur_pays` : code pays ISO 3166-1 alpha-3 de l'adresse de l'acheteur
  (`FRA` pour tout avis BOAMP).

## Identifiants TED

Le chaînage de la consolidation fonctionne sur `idAvis` / `avisPrecedent`. Pour TED, un
rectificatif ne connaît que l'**identifiant eForms** de l'avis modifié. Résolution, après
la lecture des partitions et avant la consolidation :

1. table `idEforms → idAvis` construite sur toutes les lignes TED ;
2. `avisPrecedent = table[idEformsPrecedent]` ;
3. identifiant eForms précédent absent des données (avis initial antérieur au
   2024-01-01, en pratique d'octobre à décembre 2023 puisqu'un rectificatif eForms ne
   modifie qu'un avis eForms) : résolution par l'API TED (`notice-identifier=<uuid>`),
   avec un cache persistant `identifiant eForms → numéro de publication` (S3, sur le modèle
   des autres caches) pour ne pas répéter la requête à chaque run ;
4. échec de la résolution (API en erreur, avis introuvable) : `avisPrecedent` nul ; l'avis
   devient la racine de sa chaîne. Compté dans le rapport. La résolution est retentée au
   run suivant (l'échec n'est pas mis en cache).

`uid = joue_<idAvisInitial>_<lotId>`, `idAvisInitial` = numéro de publication de la racine.

Limite acceptée : si une résolution échoue puis réussit plus tard, l'`uid` des lignes
concernées change entre deux runs.

## Recouvrement BOAMP/TED

À la consolidation, une chaîne TED est **écartée** si l'un de ses avis a un `idEforms` ou
un `idEformsPrecedent` présent parmi les `idEforms` des avis BOAMP. Comptage dans le
rapport : chaînes TED écartées pour recouvrement, chaînes TED conservées.

Conséquence acceptée : un rectificatif envoyé uniquement à TED pour un appel d'offres
publié au BOAMP est perdu (cas jugé rare ; il apparaît dans le comptage).

Garde-fou de publication _(mon choix)_ : si les partitions BOAMP contiennent des avis
eForms mais qu'**aucune** colonne `idEforms` BOAMP n'est renseignée, ou qu'aucune chaîne
TED n'est écartée pour recouvrement alors que TED a des lignes, le run échoue avant
publication. C'est le symptôme de partitions BOAMP antérieures à cette évolution (colonne
absente, remplie de nuls à la lecture), qui publieraient ~79 % des appels d'offres TED en
doublon.

## Mise en service _(mon choix)_

- Les partitions BOAMP existantes n'ont pas `idEforms` : le premier run avec TED se fait
  avec `DEAO_REBUILD=true` (reconstruction depuis S3, sans requête DILA).
- L'archive brute S3 est partagée entre prod et dev : le rattrapage TED (~250 000 XML) est
  fait d'abord par un run **dev** (ou local avec les identifiants S3), qui remplit
  `joue/raw/`. Le run prod suivant ne fait alors que lire les archives, sans télécharger.
- Variable `DEAO_SOURCES` (défaut `boamp,joue`) : liste des sources acquises et publiées,
  pour pouvoir désactiver TED sans redéployer si l'API pose problème. Désactiver une
  source retire ses lignes du dataset publié.

## Enrichissement

Même enrichissement que le BOAMP (SIRET depuis le `CompanyID` de l'acheteur, dictionnaire
SIRET, SIRENE, géocodage, replis), avec une garde nouvelle : **pour un acheteur dont
`acheteur_pays` n'est pas français** (hors codes d'outre-mer listés plus haut), pas de
dictionnaire SIRET, pas de géocodage Géoplateforme, pas de repli code postal → département.
Les colonnes géographiques de l'acheteur restent nulles. `acheteur_nom` vient de l'avis.

## Publication

- Mêmes fichiers, même schéma publié (`deao.parquet`, `deao.csv.gz`, `schema.json`), même
  dataset. `sourceDataset` prend les valeurs `boamp` et `joue` ; descriptions de colonnes
  dans `schema.py` mises à jour pour mentionner TED (`uid`, `urlAvis`, `sourceFile`,
  `idAvisInitial`, `idDernierAvis`, `avisLies`, `sourceDataset`).
- `DEAO_MIN_ROWS_TO_PUBLISH` inchangé.
- Rapport de run : compteurs par source (avis lus, retenus, exclus par sous-type ou
  `NATURE`, erreurs, jours en erreur) ; chaînes TED écartées pour recouvrement ;
  résolutions d'identifiant eForms (cache, API, échec) ; lignes publiées par
  `sourceDataset`.

## Configuration

| Variable                 | Défaut                                                          | Rôle                                     |
| ------------------------ | --------------------------------------------------------------- | ---------------------------------------- |
| `TED_API_URL`            | `https://api.ted.europa.eu/v3`                                  | recherche d'avis                         |
| `TED_NOTICE_XML_URL`     | `https://ted.europa.eu/en/notice/{publication_number}/xml`      | téléchargement                           |
| `JOUE_AVIS_URL_TEMPLATE` | `https://ted.europa.eu/fr/notice/-/detail/{publication_number}` | `urlAvis`                                |
| `JOUE_COUNTRY_CODES`     | `FRA ATF PYF NCL WLF SPM BLM MAF`                               | périmètre (acheteur ou lieu d'exécution) |
| `DEAO_S3_PREFIX`         | remplacé par un préfixe par source                              | `boamp/raw`, `joue/raw`                  |
| `DEAO_SOURCES`           | `boamp,joue`                                                    | sources acquises et publiées             |

Noms exacts et parallélisme TED laissés au builder.

## Tests

Fixtures : avis TED réels dans `tests/data/deao/joue/` :

- avis TED-only (bailleur social) initial → rectificatif (chaîne via
  `ChangedNoticeIdentifier`) ;
- annulation TED ;
- avis TED dont l'identifiant eForms est aussi dans une fixture BOAMP (recouvrement) ;
- acheteur étranger avec exécution en France (texte non français) ;
- avis multilingue avec version française ;
- attribution (exclue) ; pré-information simple.

Tests unitaires : classement par sous-type et motif ; mapping commun (les lignes BOAMP
existantes inchangées) ; choix de langue ; résolution d'identifiant (table interne, cache,
API simulée, échec) ; écartement pour recouvrement ; garde d'enrichissement des acheteurs
étrangers ; acquisition TED (pagination, jour vide, 429 puis succès, re-crawl différentiel,
lecture depuis l'archive) avec HTTP et S3 simulés ; bout en bout sur fixtures des deux
sources, unicité de `uid`.

## Hors périmètre

- Régions frontalières (lieu d'exécution en Belgique, Luxembourg, Allemagne, Italie,
  Espagne…) : faisable par filtre NUTS, volume plus important, textes non français. Suisse,
  Andorre et Monaco ne publient pas (ou presque) sur TED.
- Rapprochement approximatif des doubles envois.
- Attributions TED (présentes dans l'archive brute).
- Avis TED antérieurs à 2024 et formulaires pré-eForms.
- Sources déclarées dans un fichier comme `reference/source_datasets.json` : écarté
  (2026-11-12). BOAMP et JOUE relèvent de la collecte (comme le scraping AWS de DECP),
  pas de la concaténation de jeux de données déjà au bon format.

Fait en marge de cette spec : le schéma publié est décrit dans
`reference/schema_deao.json`, au format de `reference/schema_base.json` (DECP), lu par
`schema.py` (types polars déduits du type Table Schema) et recopié tel quel en
`schema.json`.
