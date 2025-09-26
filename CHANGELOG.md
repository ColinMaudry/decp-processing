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
