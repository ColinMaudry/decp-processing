### [2.0.1] 2025-09-25

- amélioration de la création des chemins de fichiers configurés ([#123](https://github.com/ColinMaudry/decp-processing/issues/123))

# [2.0.0] - 2025-09-19

- Refonte totale reposant sur prefect, polars et ijson, au lieu de dataflow et pandas
- Ajout de sources de données en plus de celles consolidées par le MINEF
  - plateformes Atexo
  - données publiées par l'AIFE (PLACE, achatpublic.com)
  - ARNIA (ex Ternum BFC)
  - Mégalis Bretagne
- Support des formats JSON DECP 2019 et DECP 2022 en entrée
- Intégration des modifications de marché
- Traitement effectué en bonne partie en flux pour économiser la mémoire et gérer les gros fichiers en entrée
