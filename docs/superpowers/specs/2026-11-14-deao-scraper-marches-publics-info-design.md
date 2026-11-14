# Scraper DEAO `www.marches-publics.info` (AWS) — design

Date : 2026-11-14. Suite de la spec `2026-11-13-deao-scraper-megalis-bretagne-design.md`
(scrapers Atexo, `scrap_closed`, dataset b, archivage S3 des DCE), dont les décisions
non contredites ici s'appliquent.

## Intention

`www.marches-publics.info` (éditeur AWS / Avenue Web Systèmes) est l'une des plus
grosses plateformes de profil d'acheteur : environ 4 100 avis en cours le 14/11/2026,
environ 150 nouveaux avis par jour selon AWS. Ses consultations ne sont pas publiées
en open data : comme les plateformes Atexo, elles vont dans le dataset b (privé) via
`scrap_closed`, puis dans l'agrégat `deao.parquet` de `deao_processing` (Colibre).

La page d'avis du site est plus riche que celle d'Atexo : SIRET de l'acheteur, lots
avec leur CPV, durée, procédure, NUTS. La résolution SIRET par dictionnaire n'est donc
qu'un recours.

## Objectifs

- Une ligne par consultation/lot au schéma publié (`published_schema()`), comme Atexo.
- Accumulation d'un passage à l'autre et statuts (`ouvert`, `clos`, `annule`) avec les
  règles Atexo (`accumulation.py`).
- Archivage S3 de la matière brute et des documents accessibles **sans captcha** :
  - page HTML de l'avis : privée, pour ne pas retélécharger (même rôle que les archives
    brutes BOAMP) ;
  - règlement de consultation (RC) : public, pour qu'il reste consultable après la
    dépublication par la plateforme.
- Dédoublonnage avec le BOAMP comme pour les plateformes Atexo.

## Non-objectifs

- **DCE** : le retrait, identifié comme anonyme, impose un captcha. Pas de
  contournement (décision de l'utilisateur et de Claude, 14/11/2026). `urlDce` reste
  nul. Pistes hors de cette spec : outil local où l'utilisateur résout lui-même les
  captchas ; demande d'accès à AWS ou signalement à la DAJ.
- Archivage du RC pour les plateformes Atexo (le DCE archivé le contient déjà).
- Historique des versions : seule la dernière version de chaque fichier est gardée.
- Avis expirés, attributions et données essentielles du site (onglets `IDE` = A, AAA,
  DE).
- Remplissage des nouvelles colonnes depuis le BOAMP et le JOUE (eForms : BT-06,
  BT-71, BT-63, acheteur coordinateur) et colonne `criteresAttribution` (BT-539 à
  541, avec pondérations) : tâche eForms séparée, juste après celle-ci (décision de
  l'utilisateur). Les plateformes Atexo laissent ces colonnes nulles.

## Constat sur le site (14/11/2026, en lecture seule)

### Recherche

- `POST https://www.marches-publics.info/Annonces/lister`, formulaire
  `application/x-www-form-urlencoded` (champs relevés sur `/Annonces/rechercher`) :
  `IDE=EC` (avis en cours), `IDN` (nature : `X` toutes, `T`, `S`, `F`), `IDP`
  (procédure : `X`, `PA`, `AOO`, `AOE`, `MN`, `CO`, `CR`, `DC`, `AU`, `DSP`), `IDR`
  (départements, liste séparée par des virgules ; `X` = tous), `dateParution`
  (vide = toutes), `annee=X`, `Rechercher=Rechercher`, les autres champs vides.
  Corps exact d'une recherche dans le curl fourni par l'utilisateur (conversation du
  14/11/2026).
- Session PHP (`PHPSESSID`) : la pagination dépend de la dernière recherche de la
  session, donc tout est séquentiel sur un même client.
- **Limite** : au-delà d'un seuil (entre 67 et 129 résultats ; probablement 100), le
  site n'affiche aucun avis mais « Votre recherche trouve N résultats. Veuillez
  préciser votre recherche ». Toutes natures confondues, sans filtre : 4 093. Bouches-
  du-Rhône : 129 (refusé) ; découpé en travaux 38, services 67, fournitures 24
  (acceptés).
- Les résultats sont répartis en trois onglets (travaux, services, fournitures) avec
  un badge de compte (`id="badge_travaux"`, `badge_services`, `badge_fournitures`),
  10 avis par page. Pagination en GET : `/Annonces/lister?pager_s=2` pour l'onglet
  services ; `pager_t` et `pager_f` supposés pour les deux autres (**à vérifier**).
- Bloc d'un avis dans la liste : date de publication (« Publié le 10/11/26 »), date
  limite (« le 02/12/26 à 12h00 »), acheteur et code postal en `<h2 class="h2-avis">`
  (« MAIRIE D'AUDINCOURT (25400) »), référence (« [réf. 202641] »), objet, « [Marché
  alloti : 3 lots] », pictogrammes de critères (environnement…), liens :
  - avis : `/Annonces/MPI-pub-{n° avis}.htm` (ex. `MPI-pub-2026314252.htm`) ;
  - DCE : `…/mpiaws/index.cfm?fuseaction=dematEnt.login&type=DCE&IDM={IDM}` (captcha) ;
  - RC : `…/mpiaws/index.cfm?fuseaction=dce.dRC&IDM={IDM}`.

### Page d'avis

`/Annonces/MPI-pub-{n°}.htm` : acheteur (nom, représentant, adresse, téléphone,
**SIRET**), accord-cadre et durée, objet, référence, type de marché (Travaux…), mode
(« Procédure adaptée ouverte »), technique d'achat (« Accord-Cadre »), lieu
d'exécution principal (NUTS, ex. « FRC21 - Doubs »), durée, description, code CPV
principal, lots (n°, libellé, description, lieu d'exécution, estimé € HT, CPV),
critères, date limite (« Remise des offres le 02/12/26 à 12h00 »), date d'envoi
(« Envoi le 10/11/26 »), supports de presse.

### RC

`dce.dRC&IDM=…` redirige vers une URL signée `downloads.awsolutions.fr/dce/attachment/…`
qui sert le PDF (`Content-Disposition: attachment; fileName="AWS-MPI-1902107-RC.pdf"`),
sans identification ni captcha.

## Décisions de l'utilisateur

1. Code de source = nom de domaine : `www.marches-publics.info`, `url_base`
   `https://www.marches-publics.info`.
2. Pas de DCE (captcha), pas de contournement.
3. **Relecture B** : la page d'un avis n'est lue qu'à la première apparition de la
   consultation ou quand sa date limite affichée dans la liste a changé. Les autres
   consultations gardent leurs lignes du passage précédent.
4. Archivage S3 :
   - page d'avis HTML, **privée**, dernière version seulement ;
   - RC, **public** (`public-read`, comme les DCE Atexo), dernière version seulement,
     relu en même temps que la page d'avis ; `urlReglementConsultation` = son URL
     publique S3, ou le lien `dce.dRC` de la plateforme si l'archivage échoue.
5. RC archivé seulement pour ce site, pas pour Atexo.

## Conception

### Registre et branchement

- `reference/deao_sources.yaml` : entrée `www.marches-publics.info`, `open_data: false`,
  `plateforme: aws`, `url_base: https://www.marches-publics.info`, `min_rows: 2000`,
  `resource_id` de la ressource `www.marches-publics.info.parquet` à créer dans le
  dataset b (parquet vide au schéma publié, comme les 32 ressources Atexo du
  14/11/2026).
- `src/config.py` : `PLATEFORMES_DEAO` accepte `aws` ;
  `src/tasks/scrap/deao/contrat.py` associe `plateforme: aws` à un `scraper_aws(code)`
  de même signature que `scraper_atexo` (`Callable[[date], pl.DataFrame]`, peut lever
  `ScrapPartiel`).
- Nouveau paquet `src/tasks/scrap/deao/aws/` (client, parse, lignes, scraper), sur le
  modèle de `atexo/`. Les fonctions génériques d'accumulation (`fusionner`,
  `recherche_complete`, `annulations_du_passage`, `instant_du_passage`) et
  d'archivage S3 (`deposer_dce`, `url_publique`, coupe-circuit `verifier_depot_s3`,
  `ErreurDepotS3`) sont réutilisées. Si elles dépendent de types Atexo, les déplacer
  vers un module commun plutôt que les dupliquer (choix laissé au builder).

### Collecte (client)

- Même `httpx.Client` qu'Atexo : `CLOSED_USER_AGENT`, `Limiteur`
  (`CLOSED_INTERVALLE_REQUETES_S`, 0,2 s), retry tenacity sur erreurs transport,
  429 et 5xx.
- Départements : lus à chaque passage dans les `<option>` du `<select name="IDR">` de
  `/Annonces/rechercher`, en ne gardant que les valeurs d'un seul département
  (exclure `X` et les valeurs de région, qui contiennent des virgules). La liste
  inclut `2a`, `2b` et l'outre-mer.
- Découpage récursif d'une recherche trop large, détectée par le message « trouve N
  résultats » :
  1. un département, toutes natures, toutes procédures ;
  2. puis par nature (`T`, `S`, `F`) ;
  3. puis par procédure (`IDP`).
     Un segment encore trop large en bout de chaîne : erreur du segment, et recherche
     incomplète (pas d'annulation, comme `RechercheIncomplete` chez Atexo).
- Pour chaque recherche acceptée : lire les trois onglets et les paginer jusqu'à
  épuiser le badge de l'onglet ; contrôler que les avis lus par onglet égalent son
  badge.
- **Contrôle de complétude** : une recherche nationale (`IDR=X`, `IDN=X`, `IDP=X`)
  est refusée car trop large, mais affiche le total (« trouve 4093 résultats »,
  observé le 14/11/2026). Le total peut bouger pendant la collecte (nouveaux avis,
  avis échus) : il est lu **avant et après** la collecte, et la recherche est
  complète si aucun segment n'est en erreur et si le nombre d'`IDM` distincts lus
  est compris entre le plus petit et le plus grand des deux totaux (_choix de
  Claude_). Un avis peut-il apparaître dans plusieurs départements ? **À vérifier** ;
  dédoublonner sur `IDM` dans tous les cas.

### Identifiants

- `id` = `IDM` (identifiant de consultation, présent dans les liens DCE, RC et dépôt).
- `idDernierAvis` = numéro de l'avis (`MPI-pub-{n°}`). Un rectificatif crée-t-il un
  nouveau numéro d'avis ? **À vérifier** ; sans incidence sur `id`.
- `uid` = `{code}_{IDM}_{lotId}` (`lotId` = « 0 » pour une consultation non allotie),
  comme Atexo.
- `urlConsultation` = URL absolue de la page d'avis ; `urlAvis` = la même.

### Lignes (champs)

Correspondance indicative, à finaliser par le builder sur le modèle d'Atexo
(`atexo/lignes.py`) et du BOAMP (vocabulaire de `procedure`, `type`, `techniques`,
`lieuExecution_typeCode`) :

| Champ                              | Source                                                                                                                                |
| ---------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------- |
| `acheteur_nom`                     | page d'avis (repli : `<h2>` de la liste, sans le code postal)                                                                         |
| `acheteur_id`                      | SIRET de la page d'avis ; sinon enrichissement par dictionnaire comme Atexo                                                           |
| `acheteur_codePostal`              | liste « (25400) », ou adresse de la page d'avis                                                                                       |
| `referenceAcheteur`                | « Référence » de la page d'avis (repli : « [réf. …] »)                                                                                |
| `objet`                            | « Objet » de la page d'avis                                                                                                           |
| `description`                      | « Description »                                                                                                                       |
| `type`                             | « Type de marché » (Travaux/Services/Fournitures), par lot si différent                                                               |
| `procedure`                        | « Mode », ramené au vocabulaire BOAMP ; inconnu : « Autre procédure » et compteur                                                     |
| `techniques`                       | « Technique d'achat » (Accord-Cadre…)                                                                                                 |
| `dureeMois`                        | « Durée : 24 mois »                                                                                                                   |
| `codeCPV`                          | CPV du lot, sinon CPV principal                                                                                                       |
| `lieuExecution_code` / `_typeCode` | NUTS principal (ex. `FRC21`) ; département dérivable du NUTS ou du code postal du lieu du lot : choix du builder, cohérent avec BOAMP |
| `montantEstime`                    | « Estimé € HT » du lot s'il est renseigné                                                                                             |
| `datePublicationInitiale`          | « Publié le » / « Envoi le »                                                                                                          |
| `dateLimiteReponse`                | « Remise des offres le … à 12h00 » (fuseau Europe/Paris)                                                                              |
| `lotId`, `lotIntitule`             | tableau des lots                                                                                                                      |
| `urlReglementConsultation`         | URL publique S3 du RC, sinon lien `dce.dRC`                                                                                           |
| `urlDce`                           | nul                                                                                                                                   |
| `sourceDataset`                    | `www.marches-publics.info`                                                                                                            |
| `considerationsSociales`           | pictogramme du bloc de la liste, ou mention de la page d'avis (nouvelle colonne)                                                      |
| `considerationsEnvironnementales`  | pictogramme `ico-critere-environnement` de la liste, ou mention de la page d'avis (nouvelle colonne)                                  |
| `marcheReserve`                    | « Marché réservé : Oui/Non » (nouvelle colonne)                                                                                       |
| `variantes`                        | « Les variantes sont exigées / autorisées : … » (nouvelle colonne)                                                                    |
| `groupementCommandes`              | « Groupement de commandes : Oui/Non » (nouvelle colonne)                                                                              |

### Nouvelles colonnes du schéma (décision de l'utilisateur)

Ajoutées à `reference/schema_deao.json`, publiées dans les deux datasets (pas
`closed_only` : le BOAMP et le JOUE les rempliront avec la tâche eForms) :

| Nom                               | Type    | Titre                            | Valeurs                                                                                     |
| --------------------------------- | ------- | -------------------------------- | ------------------------------------------------------------------------------------------- |
| `considerationsSociales`          | booléen | Considérations sociales          | vrai si l'avis affiche une considération ou clause sociale ; nul si la source ne le dit pas |
| `considerationsEnvironnementales` | booléen | Considérations environnementales | idem, environnementale                                                                      |
| `marcheReserve`                   | booléen | Marché réservé                   | « Marché réservé : Oui » → vrai, « Non » → faux                                             |
| `variantes`                       | texte   | Variantes                        | `exigées`, `autorisées`, `interdites`, nul si non dit                                       |
| `groupementCommandes`             | booléen | Groupement de commandes          | « Oui » → vrai, « Non » → faux                                                              |

Les libellés exacts des pages d'avis (formulations des variantes, mention des
considérations sur la page d'avis, et pas seulement en pictogramme dans la liste) sont
**à vérifier** sur plusieurs avis. Pour les considérations, une absence de
pictogramme vaut faux seulement si la page d'avis le confirme ; sinon nul.

Compatibilité : les fichiers déjà publiés (BOAMP, JOUE, Atexo) n'ont pas ces colonnes.
`deao_processing` et la lecture du fichier précédent par les scrapers doivent les
accepter absentes et les remplir à nul, comme pour les colonnes ajoutées le
13/11/2026 (`acheteur_codePostal`, `urlDce`, `urlReglementConsultation`).
Description de `sourceDataset` dans le schéma : inchangée (déjà générique).

Les années à deux chiffres (« 10/11/26 ») sont lues comme 20xx. L'enrichissement
(SIRENE, géographie, catégorie d'acheteur) passe par le même chemin qu'Atexo, avec le
SIRET déjà présent.

### Relecture (B) et archivage

À chaque passage :

1. Collecter la liste complète (`IDM`, n° d'avis, date limite, nombre de lots, champs
   de la liste).
2. À relire : `IDM` absents du fichier précédent, ou dont la date limite (ou le nombre
   de lots) diffère de celle du fichier précédent. Le fichier précédent (schéma
   publié, inchangé : pas de colonne privée) suffit : `dateLimiteReponse` et une
   ligne par lot.
3. Pour chaque consultation à relire :
   - si S3 contient déjà `{DEAO_AWS_S3_PREFIX}/{IDM}.html` avec la même date limite
     (métadonnée `date-limite` de l'objet), la page est lue depuis S3 sans requête au
     site. Ce cache rend le premier passage **reprenable** : après une interruption
     à 80 %, le passage suivant ne retélécharge que le reste (_choix de Claude_) ;
   - sinon GET de la page d'avis, dépôt S3 **privé** `{DEAO_AWS_S3_PREFIX}/{IDM}.html`
     (défaut `www.marches-publics.info/raw`, ACL par défaut du bucket, métadonnée
     `date-limite`), en écrasant la version précédente ;
   - GET du RC, dépôt S3 **public** `{DEAO_RC_S3_PREFIX}/www.marches-publics.info/{IDM}.pdf`
     (nouvelle variable, défaut `rc` ; `public-read`, mêmes métadonnées et même
     coupe-circuit que les DCE Atexo). Préfixe distinct de `dce/` : le rapport des
     DCE de `deao_processing` ne compte pas les RC ;
   - construction des lignes depuis la page d'avis.
4. Les consultations non relues gardent leurs lignes précédentes ; `fusionner()`
   recalcule les statuts sans adaptation propre à AWS : `clos` dès que la date
   limite est passée à l'instant de lecture de la liste (même si l'avis figure encore
   parmi les avis en cours), `annule` si une consultation encore ouverte disparaît
   d'une recherche complète, jamais sans date limite, alerte au-delà de 10 %
   d'annulations.

Dépôts S3 : même bucket et mêmes identifiants que les DCE Atexo. Le coupe-circuit
(`verifier_depot_s3`, `ErreurDepotS3`) couvre les dépôts privés (HTML) comme publics
(RC) : un S3 en panne arrête l'archivage des deux pour le passage, les lignes restant
publiées (`ScrapPartiel`) (_choix de Claude_). Sans S3, les pages d'avis sont lues
sur le site et `urlReglementConsultation` reprend le lien de la plateforme.

Relecture depuis l'archive : si la variable `DEAO_AWS_RELIRE_ARCHIVE=true` est
définie, les lignes des consultations non relues sur le site sont reconstruites
depuis les pages HTML archivées dans S3 (après une évolution du parseur), sans
requête au site pour elles. Une page absente de l'archive est alors relue sur le
site.

Une page d'avis ou un RC en erreur n'arrête pas le passage : la consultation est
gardée telle qu'au passage précédent (ou, nouvelle, ignorée), avec un compteur dans
le rapport, comme chez Atexo. Le rapport des DCE de `deao_processing` (objets sous
`dce/`) ne change pas : aucun DCE pour ce site, les RC (sous `rc/`) ne sont pas
comptés (décision de l'utilisateur).

### Dédoublonnage avec le BOAMP

- Exact : `id_consultation_expr` (`dedoublonnage.py`) ne lit que `id=` et
  `/consultation/{n}` ; l'étendre à `IDM={n}` pour l'hôte `www.marches-publics.info`.
  Vérifier sur `boamp.parquet` publié quelles URL du BOAMP pointent vers ce site
  (domaine, paramètres).
- Strict : inchangé (référence normalisée, SIRET, jour limite). Le SIRET de la page
  d'avis devrait le rendre plus efficace que pour Atexo.

### Volume et durée

Premier passage : environ 100 départements × 1 à 3 recherches, environ 410 pages de
résultats, et environ 4 100 pages d'avis + 4 100 RC. Avec 0,2 s entre requêtes,
compter 1 à 2 heures. Passages suivants : la liste (environ 500 requêtes) plus
environ 150 avis et RC nouveaux ou modifiés.

## Tests

- Fixtures HTML réelles (liste, liste trop large avec « trouve N résultats »,
  page 2 d'un onglet, page d'avis allotie et non allotie, page de formulaire
  `rechercher`). Des exemples ont été relevés le 14/11/2026 (IDM 1902107, avis
  `MPI-pub-2026314252`) ; le builder les capture de nouveau dans `tests/data/aws/`.
- Client simulé par `httpx.MockTransport` : découpage récursif, pagination par
  onglet, contrôle de complétude, session.
- Relecture B : seules les consultations nouvelles ou à date limite changée sont
  relues ; les autres gardent leurs lignes.
- Archivage : clés et ACL (privé pour le HTML, `public-read` pour le RC), repli du
  lien RC si l'archivage échoue, coupe-circuit S3.
- `DEAO_AWS_RELIRE_ARCHIVE` : lignes reconstruites depuis S3 sans requête au site.
- Dédoublonnage : URL BOAMP avec `IDM=` appariée à la consultation.

## Laissé au builder

- Organisation exacte du paquet `aws/` et extraction des fonctions communes.
- Vocabulaire précis de `procedure`, `techniques`, `lieuExecution_*` (aligner sur
  BOAMP).
- Les points marqués **à vérifier** ci-dessus.

## Avant la mise en service

- Vérifier `robots.txt` du site (non consulté lors de la reconnaissance), pour
  `/Annonces/`, `/mpiaws/` et `downloads.awsolutions.fr`. S'il interdit un chemin
  utilisé, ne pas déployer : revenir vers l'utilisateur.
- `scrap_closed` n'impose pas de durée maximale par source ; le premier passage
  (1 à 2 h) tourne en parallèle des plateformes Atexo dans `run_in_pool`. S'il est
  interrompu, le cache S3 des pages d'avis rend le passage suivant plus court.
  `min_rows: 2000` : un premier passage partiel sous ce seuil n'est pas intégré par
  `deao_processing` (source fermée écartée, non bloquante).
- Créer la ressource `www.marches-publics.info.parquet` dans le dataset b et
  renseigner son identifiant dans le registre.
- Premier passage long (1 à 2 h) : à lancer manuellement, de préférence hors des
  heures de pointe du site.
