### 2.6.0 2025-12-12

- Abandon des données consolidées par le MINEF, récupération des données à la source ([#151](https://github.com/ColinMaudry/decp-processing/issues/151))
  - Xmarchés
  - AWS (officiel et legacy)
  - PES marché (avant et après 2024)
  - Dematis / e-marchespublics
- Ajout du champ `dureeRestanteMois` ([#135](https://github.com/ColinMaudry/decp-processing/issues/135))
- Amélioration des noms des titulaires (personnes physiques et non-diffusibles) ([#145](https://github.com/ColinMaudry/decp-processing/issues/145))
- Ajout de nombreux tests unitaires
- Amélioration de la gestion des modifications ([#148](https://github.com/ColinMaudry/decp-processing/issues/148))
- Traitement des ressources en parallèle ([#113](https://github.com/ColinMaudry/decp-processing/issues/113))
- Optimisation de la consommation de mémoire (matérialisation en parquet) ([#153](https://github.com/ColinMaudry/decp-processing/issues/153))
- Résilience contre les erreurs pendant get_clean (seule la ressource échoue, pas tout le process)
- Mise en place d'un système de cache custom (parquet)
- Protection contre la publication par erreur sur data.gouv.fr ([ffaf0535](https://github.com/ColinMaudry/decp-processing/commit/ffaf0535))
- Utilisation de polars 1.35.2 plutôt que 1.36.1 qui semble ne pas marcher avec polars-ds

### 2.5.0 2025-11-21

- Amélioration de la conso mémoire de la correction des titulaires ([#146](https://github.com/ColinMaudry/decp-processing/issues/146))
- Vérfication de la structude des données scrapées (AWS)
- Gestion propre des erreurs 429 Too Many Redirects ([6fbd71e0](https://github.com/ColinMaudry/decp-processing/commit/6fbd71e0bca0534ee360dc172f3565607dac5bef))
- Skipper et non fail les ressources qui ne sont conformes à aucun schéma (2019 ou 2022)

#### 2.4.3 2025-11-14

- Stabilisation du scrap AWS (mais c'est pas encore ça) ([#143](https://github.com/ColinMaudry/decp-processing/issues/143))
- Ajout du nombre de marchés dans les stats NAF/CPV ([#142](https://github.com/ColinMaudry/decp-processing/issues/142))

#### 2.4.2 2025-11-12

- Correction des montants de marchés supérieurs à 99 milliards, ramenés à 12,311111111 milliards

#### 2.4.1 2025-11-06

- Correction des imports de modules

## 2.4.0 2025-11-05

- Ajout d'une colonne `distance` pour indiquer la distance en kilomètres entre l'acheteur et le titulaire ([#138](https://github.com/ColinMaudry/decp-processing/issues/138)) (financé par [Odialis](https://www.odialis.fr))
- Ajout de colonnes commune, région et département (nom et code) pour les acheteurs et les titulaires basés en France ([#140](https://github.com/ColinMaudry/decp-processing/issues/140)) (financé par [Odialis](https://www.odialis.fr))
- Ajout de la génération d'un fichier de probabilités de code CPV par code NAF ([#142](https://github.com/ColinMaudry/decp-processing/issues/142), voir [probabilites_naf_cpv.csv](https://www.data.gouv.fr/datasets/donnees-essentielles-de-la-commande-publique-consolidees-format-tabulaire/#/resources/b6a502cd-560b-4350-a146-e837692f4b66)) (financé par [Odialis](<(https://www.odialis.fr)>))

#### 2.3.4 2025-10-24

- Correction du nettoyage des backslash AWS

#### 2.3.3 2025-10-21

- Regex générique pour corriger les problèmes d'échappements dans le JSON AWS

#### 2.3.2 2025-10-20

- Remplacements de texte pendant le scraping AWS pour produire du JSON valide

#### 2.3.1 2025-10-20

- stabilisation du scrap de marche-securises.fr (si `parse_result_page()` échoue)
- remplacements dans les données AWS pour redresser le JSON invalide (guillemets, etc.)

## 2.3.0 2025-10-19

- scraping des données DECP de marches-oublics.infos (AWS) ([#118](https://github.com/ColinMaudry/decp-processing/issues/118))
- ajout des [données AWS scrapées](<(https://www.data.gouv.fr/datasets/68caf6b135f19236a4f37a32/)>) à la consolidation
- ajout des[ données officielles AWS](https://www.data.gouv.fr/datasets/declaration-des-donnees-essentielles-avenue-web-systemes/) (a priori incomplètes) à la consolidation
- scripts de scrap plus flexibles

#### 2.2.1 2025-10-18

- Nettoyage des "" id et acheteur_id avant filtrage et uid
- Ne pas parser une page qui retourne None (scrap)

## 2.2.0 2025-10-18

- Extension du timeout pour la publication de nouvelles ressources sur data.gouv.fr
- Correction des titulaires null en cascade pour un marché et ses modifications
- Possibilité d'exclure des ressources ou de solo un dataset depuis .env
- Renommage atomique de decp.parquet pour facilité sa lecture par decp.info
- Support des marchés vides (marches-securises.fr)

#### 2.1.3 2025-10-14

- Exclusion de marches-securises.fr de la consolidation le temps de le réparer

#### 2.1.2 2025-10-13

- Solutionnage des blocs titulaires vides ([#131](https://github.com/ColinMaudry/decp-processing/pull/131)) merci [imanuch](https://github.com/imanuch) !

#### 2.1.1 2025-10-13

- Stabilisation du scraping de marches-securises.fr
- Amélioration du rendu des messages de release

## 2.1.0 2025-10-13

- scraping des données DECP de [marches-securises.fr](https://www.data.gouv.fr/datasets/donnees-essentielles-de-la-commande-publique-de-marches-securises-fr/) ([#111](https://github.com/ColinMaudry/decp-processing/issues/111))
- ajout des données de marches-securises.fr aux données consolidées ([#111](https://github.com/ColinMaudry/decp-processing/issues/111))

#### 2.0.5 - 2025-10-08

- correction des NaN dans les données consolidées par le MINEF ([#127](https://github.com/ColinMaudry/decp-processing/issues/127))
- auto-release à chaque fois que je push un tag

#### 2.0.4 - 2025-10-04

- nettoyage montant invalide de marché ([#125](https://github.com/ColinMaudry/decp-processing/issues/125))
- publication du schéma au format [TableSchema](https://specs.frictionlessdata.io/table-schema) ([#126](https://github.com/ColinMaudry/decp-processing/issues/126))
- amélioration des noms de colonnes dans le schéma pour les GUIs (`title`, `short_name`)

#### 2.0.3 - 2025-09-29

- correction de coquilles dans le schéma

#### 2.0.2 - 2025-09-26

- distinction des différentes sources de données consolidées par le MINEF (`decp_minef_*`)

#### 2.0.1 - 2025-09-25

- correction sommes et médianes des montants achetés par an (artefacts)
- le timeout de l'upload vers data.gouv.fr est configurable
- réduction de la consommation mémoire du chargement en base de données, puis désactivation ([#124](https://github.com/ColinMaudry/decp-processing/issues/124))
- amélioration de la création des chemins de fichiers configurés ([#123](https://github.com/ColinMaudry/decp-processing/issues/123))
- remise à zéro de /dist avant de générer statistiques.csv
- ajout du contributeur vico4445 <3

# 2.0.0 - 2025-09-19

- Refonte totale reposant sur prefect, polars et ijson, au lieu de dataflow et pandas
- Ajout de sources de données en plus de celles consolidées par le MINEF
  - plateformes Atexo
  - données publiées par l'AIFE (PLACE, achatpublic.com)
  - ARNIA (ex Ternum BFC)
  - Mégalis Bretagne
- Support des formats JSON DECP 2019 et DECP 2022 en entrée
- Intégration des modifications de marché
- Traitement effectué en bonne partie en flux pour économiser la mémoire et gérer les gros fichiers en entrée
