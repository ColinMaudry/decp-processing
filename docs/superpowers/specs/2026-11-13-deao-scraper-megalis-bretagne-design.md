# DEAO — scrapers Atexo `megalis_bretagne` et `maximilien` (premiers scrapers de `scrap_closed`)

Date : 2026-11-13
Issue : aucune (à préciser par l'utilisateur au moment du commit).
Statut : design à valider.
Base : `dev` après le refactor collecte / traitement
(`2026-11-12-deao-collecte-traitement-design.md`).

## Intention

Colibre doit afficher le plus d'appels d'offres publics possible. Une partie des
consultations n'est publiée ni au BOAMP ni au JOUE (MAPA sous les seuils de publicité,
notamment) : elles ne sont visibles que sur les profils d'acheteur. Mégalis Bretagne
(`https://marches.megalis.bretagne.bzh`) et Maximilien (`https://marches.maximilien.fr`,
Île-de-France) sont les premiers profils d'acheteur scrapés. Tous deux sont édités par
Atexo (logiciel MPE, framework PHP PRADO), comme d'autres plateformes régionales : un
client Atexo commun sert les deux, et servira les suivantes.

Pour un utilisateur de Colibre, une consultation venue d'une plateforme doit offrir **les
mêmes informations qu'un avis BOAMP, plus le lien vers la consultation et le DCE
archivé**. De nouveaux champs au schéma DEAO sont bienvenus si les plateformes en
fournissent.

Le DCE (dossier de consultation des entreprises) est **dépublié dès que le marché est
attribué** : on l'archive dans S3 pendant que la consultation est en ligne, et Colibre
propose un lien de téléchargement public sur la fiche.

## Objectifs

- Deux scrapers, `megalis_bretagne` et `maximilien`, enregistrés dans `SCRAPERS`
  (`src/tasks/scrap/deao/contrat.py`) et dans le registre `reference/deao_sources.yaml`
  (`open_data: false`), lancés par `scrap_closed`, qui publient
  `megalis_bretagne.parquet` et `maximilien.parquet` sur le dataset b au schéma publié.
- **Accumulation** : chaque passage récupère toutes les consultations en cours et les
  fusionne avec celles déjà connues (lues dans le fichier publié de la source). Une
  consultation clôturée reste dans nos données.
- **Archivage du DCE** dans S3, téléchargeable publiquement en HTTPS par les
  utilisateurs de Colibre : téléchargé à la première apparition d'une consultation,
  retéléchargé quand on détecte qu'il a changé ; **seule la dernière version** est
  conservée.
- **SIRET de l'acheteur** retrouvé sur le modèle des acheteurs BOAMP sans SIRET.
- **Dédoublonnage BOAMP ↔ plateformes** dans `deao_processing` : une consultation présente
  dans les deux sources donne **une seule ligne combinée**, dont `sourceDataset` reste
  `boamp`.
- Une source fermée en échec **ne bloque pas** l'Open Data.

## Non-objectifs

- L'historique des consultations déjà clôturées (menu « Consultations clôturées ») :
  plus tard.
- Les autres plateformes Atexo : hors périmètre, mais une plateforme de plus doit se
  réduire à une entrée de registre et quelques lignes de configuration.
- Les annonces d'information et d'attribution des sites.
- Le dédoublonnage plateformes ↔ JOUE (les avis JOUE passent presque tous aussi par le
  BOAMP ; à mesurer plus tard).
- L'historique des versions de DCE.

## Anti-objectifs

- **Fusionner à tort** deux consultations différentes. L'utilisateur préfère afficher
  deux fois la même consultation plutôt que d'en fusionner deux distinctes.
- Faire fuiter des données Closed Data dans le dataset a (Open Data).
- Surcharger les plateformes : ce sont des services publics, pas des cibles de crawl
  agressif.

## Ce que le repérage a établi (2026-11-13)

### Recherche

- `robots.txt` de Mégalis et de Maximilien : `Disallow:` vide (tout est autorisé).
- Page : `/?page=Entreprise.EntrepriseAdvancedSearch&searchAnnCons`. Un **GET** fournit
  cookies (`PHPSESSID`, `SERVERID`) et les champs cachés du formulaire, dont
  `PRADO_PAGESTATE` (~90 Ko sur Mégalis, ~26 Ko sur Maximilien). Un **POST** du formulaire
  (tous les champs lus dans la page, avec
  `PRADO_POSTBACK_TARGET=ctl0$CONTENU_PAGE$AdvancedSearch$lancerRecherche`) renvoie les
  résultats en HTML. Aucune manipulation de navigateur n'est nécessaire (httpx + lxml).
  Le formulaire n'a pas le même nombre de champs sur les deux sites (41 et 45) : il faut
  relire les champs dans la page, pas les coder en dur.
- **Piège** : les champs `dateMiseEnLigneStart/End` filtrent en réalité sur la **date
  limite de remise des plis**, et la recherche `searchAnnCons` ne porte que sur les
  **consultations en cours**. Une plage 01/01/2020 → 31/12/2030 renvoie toutes les
  consultations en cours : 620 sur Mégalis, 369 sur Maximilien le 13/11/2026. Une plage
  passée renvoie 0 résultat.
- Pagination PRADO : 10 ou 20 résultats par page (`listePageSizeTop`), navigation par
  postback (`PagerTop`, `numPageTop`).

### Champs de la liste de résultats

Pour chaque consultation (`div.item_consultation`) : type de procédure (MAPA, AOO, AUT,
SAD-A…), catégorie (Travaux / Services / Fournitures), date de mise en ligne, référence
acheteur, intitulé, objet, organisme avec code postal et ville
(`RENNES METROPOLE (35000 - Rennes )`), nombre de lots, lieu d'exécution (département),
date et heure limite de remise des plis, et en champs cachés `refCons` (identifiant
numérique de la consultation) et `orgCons` (acronyme de l'organisme, ex. `a4h`).

`refCons` est unique au sein d'une plateforme : les identifiants observés forment une
seule séquence partagée par tous les organismes (236965 → 244354 sur 25 consultations
Mégalis de 23 organismes différents). L'`orgCons` reste nécessaire dans les URL du site.

### Page détail

`/entreprise/consultation/{refCons}?orgAcronyme={orgCons}` (c'est aussi l'URL publique à
donner à l'utilisateur). Elle ajoute : code(s) CPV, entité publique, service, avis
complémentaires en pièces jointes, et les tailles des pièces. **Ni SIRET ni numéro d'avis
BOAMP / JOUE.** Le détail des lots est dans une popup
`?page=Entreprise.PopUpDetailLots&orgAccronyme=…&id=…`.

### Pièces

- Règlement de consultation (RC), lien direct :
  `/index.php?page=Entreprise.EntrepriseDownloadReglement&id={base64(refCons)}&orgAcronyme=…`.
- DCE (contient en général le RC) :
  `/index.php?page=Entreprise.EntrepriseDemandeTelechargementDce&id={refCons}&orgAcronyme=…`
  mène à un formulaire PRADO qui propose le **téléchargement anonyme** (radio
  `choixAnonyme`, bouton `validateButton`), sans compte. Non testé jusqu'au bout lors du
  repérage : le builder vérifie la séquence de postbacks et si le téléchargement expose
  un identifiant propre au DCE (voir « DCE dans S3 »).
- Taille des DCE (affichée sur la page détail, échantillon de 25 consultations Mégalis en
  cours) : médiane 12 Mo, moyenne 108 Mo, maximum 946 Mo, total 2,7 Go.

### Recoupement avec le BOAMP

Dans le `deao.parquet` local, 11 178 lignes BOAMP (lignes par lot) ont un
`urlConsultation` Mégalis. **35 %** contiennent l'identifiant de la consultation
(`…&id=192147&orgAcronyme=a9i` ou `/consultation/192147`), qui permet un rapprochement
exact. Les autres pointent vers la racine du site. Non mesuré pour Maximilien.

## Décisions de l'utilisateur

1. Même contenu qu'un avis BOAMP + lien vers la consultation ; nouveaux champs bienvenus.
2. Accumulation au fil des passages ; pas d'historique des consultations clôturées pour
   l'instant.
3. RC ≠ DCE ; le DCE est archivé dans S3 (il est dépublié après attribution).
4. Seule la dernière version du DCE est gardée.
5. Stockage : les 2 To S3 inclus dans l'abonnement de l'utilisateur, « on va voir jusqu'où
   on peut aller » ; au-delà, discussion avec data.gouv.fr. **Même bucket** que le reste
   (`S3_BUCKET`, l'utilisateur ne peut pas en créer d'autres).
6. Clé S3 du DCE : `dce/{code_plateforme}/{id_dce ou à défaut id_consultation}`.
7. Accès aux DCE **public** : il suffit d'avoir l'URL.
8. Dédoublonnage : une seule ligne qui combine les deux sources ; `sourceDataset` = la
   source open (`boamp`).
9. En cas de doute, **ne pas fusionner** (afficher deux fois plutôt que fusionner à tort).
10. SIRET de l'acheteur retrouvé « sur le modèle de ce qui est fait avec les acheteurs
    sans SIRET trouvés dans le BOAMP » (`appliquer_dictionnaire_siret`,
    `src/tasks/deao/enrich.py` : dictionnaire nom normalisé + code postal → SIRET unique,
    appris sur les avis qui ont un SIRET). Le code postal de l'acheteur est ajouté au
    schéma publié, et le dictionnaire est appris sur `boamp.parquet` téléchargé depuis
    data.gouv.fr (`scrap_open` et `scrap_closed` peuvent tourner sur des serveurs
    différents).
11. Une source fermée en échec est écartée du run avec une alerte ; le dataset a et le
    reste du dataset b sont publiés normalement.
12. Les déploiements dev ne tournent qu'à la demande, sans publication
    (`DECP_PROCESSING_PUBLISH=False`).
13. Une consultation qui disparaît du site **avant** sa date limite est annulée : on réutilise le statut `annule` du BOAMP (les plateformes n'envoient pas d'avis d'annulation).
14. Ressources data.gouv.fr (dataset b) : `megalis_bretagne.parquet` =
    `c5a67fe9-fe02-4c9e-b11a-ec6162ec2c76`, `maximilien.parquet` =
    `e9a13bd3-b3d1-43e8-a79b-8e74e75e50d5`.
15. Maximilien est fait en même temps que Mégalis.

## Conception

### Collecte (`src/tasks/scrap/deao/atexo/`)

Un client Atexo générique (base URL en paramètre) et une configuration par plateforme
(code de registre → base URL). À chaque passage, pour une plateforme :

1. GET de la page de recherche, POST de la recherche sur une plage de dates limites large
   (toutes les consultations en cours), pagination à 20 par page.
2. Pour chaque consultation **nouvelle** (`refCons` inconnu) ou dont la date limite a
   changé : GET de la page détail (CPV, entité, service, taille du DCE).
3. DCE : téléchargement anonyme et dépôt dans S3 si la consultation est nouvelle ou si
   la taille affichée du DCE (ou la date limite) a changé ; l'objet S3 est remplacé.
4. Fusion avec les consultations déjà connues (fichier publié de la source sur le dataset
   b, relu au début du passage ; s'il n'existe pas encore, on part de zéro),
   normalisation au schéma publié, enrichissement acheteur, puis retour du DataFrame au
   flow (contrat `Scraper`).

Les deux plateformes tournent en parallèle (`run_in_pool` de `scrap_closed`) ; au sein
d'une plateforme, les requêtes sont séquentielles.

Politesse : un intervalle minimal entre requêtes par plateforme (réutiliser l'idée du
`Limiteur` de `joue/fetch.py`), User-Agent identifiable, nouvelles tentatives espacées
sur 429 / 5xx.

### Granularité

Une ligne **par consultation et par lot**, comme le BOAMP. La popup
`?page=Entreprise.PopUpDetailLots&orgAccronyme={orgCons}&id={refCons}` donne pour chaque
lot : numéro, intitulé, catégorie, description, CPV principal, variante, dispositions
sociales et environnementales (vérifié sur la consultation 242752 : 2 lots). Elle n'est
demandée que pour une consultation allotie, nouvelle ou modifiée : une requête de plus
par consultation au premier passage, quelques-unes ensuite. Une consultation non allotie
donne une seule ligne avec `lotId` = `'0'` (convention du schéma). `lotId` = numéro du
lot, `lotIntitule` = intitulé ; le CPV du lot prime sur celui de la consultation.

On suit les conventions d'identifiants du schéma : `id` = `refCons`, `idDernierAvis`
nul (une plateforme n'a pas de chaîne d'avis, _mon choix_), et
`uid` = `{code_plateforme}_{refCons}_{lotId}`. Les lots d'une même consultation se
regroupent donc, comme pour le BOAMP et le JOUE, sur le couple (`sourceDataset`, `id`).
Une ligne fusionnée avec le BOAMP garde les identifiants BOAMP.

### Renommage `idAvisInitial` → `id`

Décision de l'utilisateur : le champ `idAvisInitial` devient générique dans
`reference/schema_deao.json`, pour toutes les sources :

- `name` : `id` ;
- `title` : « Identifiant appel d'offres » ; `short_title` : « Identifiant AO » ;
- `description` (proposée) : « Identifiant de l'appel d'offres au sein de sa source
  (sourceDataset), commun à tous ses lots : pour boamp, IDWEB de l'avis initial de la
  chaîne d'avis (à défaut, du plus ancien avis connu) ; pour joue, numéro de publication
  de l'avis initial (ex. 677195-2026) ; pour une plateforme de profil d'acheteur,
  identifiant de la consultation sur la plateforme. »

Les descriptions de `uid` (`<sourceDataset>_<id>_<lotId>`), `idDernierAvis` (nul pour les
plateformes) et `sourceDataset` (liste des sources) sont mises à jour en conséquence. Le
code qui produit ou lit `idAvisInitial` (`src/tasks/deao/consolidate.py`, tests) est
renommé ; les valeurs ne changent pas, `uid` non plus.

Pas de rupture pour les consommateurs : Colibre n'intègre pas encore les données DEAO
(information de l'utilisateur, 2026-11-13).

Transition : un fichier source publié avant le déploiement porte encore `idAvisInitial`.
`deao_processing` le renomme en `id` à la lecture (et non le complète par des nuls),
jusqu'à la republication du fichier par `scrap_open`.

### Correspondance des champs (principaux)

| Schéma DEAO                        | Plateforme Atexo                                                 |
| ---------------------------------- | ---------------------------------------------------------------- |
| `uid`                              | `{code_plateforme}_{refCons}_{lotId}` (convention du schéma)     |
| `id`                               | `refCons`                                                        |
| `idDernierAvis`                    | nul                                                              |
| `lotId`, `lotIntitule`             | popup des lots ; `'0'` si non alloti                             |
| `referenceAcheteur`                | Référence                                                        |
| `acheteur_nom`                     | Organisme (sans le « (CP - ville) »)                             |
| `acheteur_codePostal`              | Code postal de l'organisme (nouveau champ, voir « Acheteur »)    |
| `objet` / `description`            | Intitulé / Objet                                                 |
| `type`                             | Catégorie (Travaux, Services, Fournitures), aux valeurs du BOAMP |
| `procedure`                        | Type de procédure (MAPA, AOO…), aux valeurs du BOAMP             |
| `codeCPV`                          | Code CPV principal (page détail)                                 |
| `datePublicationInitiale`          | Date de mise en ligne                                            |
| `dateLimiteReponse`                | Date et heure limite de remise des plis                          |
| `lieuExecution_code` / `_typeCode` | Département                                                      |
| `urlConsultation`                  | `{base}/entreprise/consultation/{refCons}?orgAcronyme={orgCons}` |
| `statut`                           | Règle commune, plus `annule` à la disparition (voir ci-dessous)  |
| `sourceDataset`                    | code de la plateforme                                            |

Nouveaux champs au schéma (`reference/schema_deao.json`), vides pour BOAMP et JOUE :
`urlDce` (URL HTTPS publique du DCE archivé), `urlReglementConsultation`. S'y ajoute
`acheteur_codePostal`, renseigné par toutes les sources (voir « Acheteur »).
Le builder peut en proposer d'autres s'il en trouve d'utiles ; tout ajout passe par le
schéma publié.

_Mon choix_ pour la transition : `urlDce` et `urlReglementConsultation` sont propres au
dataset b : le dataset a (Open Data) et son `schema.json` ne les publient pas.
`acheteur_codePostal` est publié partout. Un fichier source à qui un nouveau champ
manque (les `boamp.parquet` et `joue.parquet` publiés avant le déploiement) est accepté
par `deao_processing` et complété par des nuls, pour qu'aucun run n'échoue entre le
déploiement et la republication de ces fichiers. Le premier `scrap-open` après le
déploiement republie `boamp.parquet` et `joue.parquet` avec `acheteur_codePostal` (le
builder vérifie si un `DEAO_REBUILD=true` est nécessaire pour cela).

### Statut `annule`

Les plateformes ne publient pas d'avis d'annulation. On réutilise donc la valeur `annule`
de l'enum `statut` (décision de l'utilisateur) : une consultation qui était en cours au
passage précédent, qui n'apparaît plus dans les résultats et dont la date limite n'est pas
encore passée est marquée `annule` (et le reste). Une consultation qui disparaît après sa
date limite suit la règle commune (`clos`). Une consultation `annule` qui réapparaîtrait
redevient normale. Le passage ne marque rien comme annulé si la recherche a renvoyé un
nombre de résultats anormalement bas ou a échoué en cours de pagination (garde-fou à
fixer par le builder). La description de `statut` dans le schéma est complétée en
conséquence.

### Acheteur

La clé du dictionnaire SIRET est le nom normalisé + le code postal de l'acheteur. Le code
postal n'est pas publié aujourd'hui (`acheteur_adresse_cp` est une colonne interne de
`scrap_open`). Décision de l'utilisateur : **on ajoute le code postal au schéma publié**,
sous le nom `acheteur_codePostal`, renseigné par BOAMP, JOUE et les plateformes, et
publié aussi dans le dataset a (c'est une donnée ouverte des avis).

`scrap_closed` et `scrap_open` ne tournent pas forcément sur le même serveur : le
scraper ne lit rien de local à `scrap_open`. Au début du passage, il télécharge
`boamp.parquet` depuis le dataset b (`telecharger_fichier_source`), y apprend le
dictionnaire nom normalisé + code postal → SIRET unique (même règle que
`appliquer_dictionnaire_siret`), puis l'applique aux lignes de la plateforme ;
`acheteur_id_source` vaut `dictionnaire` dans ce cas. Puis l'enrichissement SIRENE et
géographique habituel (`enrich_acheteurs`). Si `boamp.parquet` n'a pas encore la colonne
(publié avant le déploiement), pas de dictionnaire pour ce passage : les lignes restent
sans SIRET et l'enrichissement géographique se fait par l'adresse.

### Dédoublonnage dans `deao_processing`

`dedoublonner_sources` (aujourd'hui l'identité) applique la règle BOAMP ↔ plateformes :

- **Rapprochement exact** : l'`urlConsultation` BOAMP pointe vers le même site, avec le
  même identifiant de consultation (`id=` ou `/consultation/`) que la ligne plateforme.
- **Rapprochement strict sans identifiant** : même `referenceAcheteur` (normalisée), même
  acheteur (SIRET identique) et même date limite (jour), **et** un seul candidat de chaque
  côté. Sinon, pas de fusion.
- **Fusion** : les lignes BOAMP (une par lot) sont conservées avec leurs valeurs ; les
  champs nuls sont complétés par la ligne plateforme (`urlConsultation` exacte,
  `urlDce`, `urlReglementConsultation`…). La ligne plateforme est retirée.
  `sourceDataset` reste `boamp`.
- Les statistiques du run comptent les fusions par règle et par plateforme, et les lignes
  plateforme restées seules.
- Le dataset a (Open Data) reste construit à partir des seules sources
  `open_data: true`, **sans** les compléments des plateformes.

### Source fermée en échec

Dans `deao_processing`, un fichier de source `open_data: false` absent, illisible, non
conforme ou sous son `min_rows` est écarté du run avec une alerte (log et rapport) ; le
reste du traitement continue. Une source Open Data en échec garde le comportement
actuel.

### DCE dans S3

- Configuration S3 existante (`S3_ENDPOINT_URL`, `S3_BUCKET`…, même bucket que
  `boamp/raw` et `joue/raw`), préfixe commun `DEAO_DCE_S3_PREFIX` (défaut `dce`), clé
  `{prefixe}/{code_plateforme}/{id}` où `id` est l'identifiant propre du DCE si le site
  en expose un, sinon `refCons` ; extension du fichier telle que servie (`.zip` en
  général).
- **Accès public en HTTPS, sans client S3** : un objet S3 est servi en HTTP(S) par
  l'endpoint (`{endpoint}/{bucket}/{clé}` ou `{bucket}.{endpoint}/{clé}`) dès qu'il est
  lisible publiquement. Le dépôt pose l'ACL `public-read` sur l'objet (ou une politique
  de bucket sur le préfixe `dce/`) ; `urlDce` stocke cette URL publique. Le reste du
  bucket (`boamp/raw`, `joue/raw`…) reste privé. À vérifier par le builder sur le
  fournisseur S3 de l'utilisateur dès la première tâche, avec un objet de test : si les
  ACL d'objet ne sont pas prises en charge, il faut une politique de bucket, à poser par
  l'utilisateur.
- `Content-Type` et `Content-Disposition` posés au dépôt, pour un téléchargement propre
  depuis le navigateur.
- Volumétrie estimée pour Mégalis : ~620 DCE au premier passage × 108 Mo ≈ 65 Go ;
  ensuite, avec une durée de vie moyenne d'une consultation d'environ un mois, de l'ordre
  de 7 000 à 8 000 nouvelles consultations par an, soit ~0,8 To par an. Maximilien
  ajoute environ 60 % de plus (369 consultations en cours). Les 2 To tiennent donc un peu
  plus d'un an pour les deux. Le run journalise le volume archivé.
- Un DCE en échec de téléchargement n'empêche pas la publication de la consultation :
  `urlDce` reste nul, et on réessaie au passage suivant.
- L'archivage S3 n'a lieu que si la publication est activée
  (`DECP_PROCESSING_PUBLISH`), comme la publication sur data.gouv.fr : un run de dev ne
  dépose rien.

### Registre

```yaml
- code: megalis_bretagne
  name: Mégalis Bretagne (profil d'acheteur Atexo)
  open_data: false
  resource_id: c5a67fe9-fe02-4c9e-b11a-ec6162ec2c76
  min_rows: 300
- code: maximilien
  name: Maximilien (profil d'acheteur Atexo, Île-de-France)
  open_data: false
  resource_id: e9a13bd3-b3d1-43e8-a79b-8e74e75e50d5
  min_rows: 150
```

`min_rows` (_mon choix_) : environ la moitié des consultations en cours constatées ;
avec l'accumulation, ce seuil ne fera que s'éloigner.

### Fréquence

`scrap_closed` tourne à 6 h (déploiement existant). Un DCE est donc archivé au plus tard
24 h après sa mise en ligne.

## Laissé au builder

- Découpage exact des modules, noms des fonctions.
- Détection du changement de DCE (taille affichée, date limite, ou les deux).
- Normalisation des valeurs de `procedure` et `type` vers les valeurs BOAMP.
- Intervalle entre requêtes et nombre de tentatives.
- Garde-fou contre un faux `annule` (seuil de résultats, pagination incomplète).
- Fixtures de tests : pages HTML enregistrées lors du repérage
  (`data/temp/megalis/`), à anonymiser si nécessaire.
