# Spec — CSV de définitions des champs BOAMP/eForms (#178)

Date : 2026-06-08
Statut : design validé, en attente de relecture

## Objectif

Produire un fichier CSV permettant de **copier un nom de colonne** depuis le parquet
de sortie de `boamp_processing` (`dist/boamp.parquet`) et d'en **retrouver la définition** :
la définition de chaque élément XML parent (du plus général au parent direct) puis de
l'élément feuille.

Cas d'usage : un humain qui inspecte le parquet/CSV de sortie et veut comprendre ce que
contient une colonne comme
`ann_DONNEES_DSP_initial_renseignementsCompl_introductionRecoursRenseignements_adr_cp`.

## Contexte

Les colonnes génériques du parquet sont produites par `src/tasks/boamp/flatten.py::flatten`,
qui aplatit chaque annonce en `{chemin_feuille: [valeurs...]}`. Le **chemin** est la suite
des noms locaux des éléments XML joints par `_`, préfixée par le nom local de la racine
(`ann`). Chaque segment d'un nom de colonne est donc un **nom d'élément XML**.

Le parquet de sortie contient (état actuel) **1016 colonnes** : 17 scalaires (cœur
harmonisé : `id_annonce`, `acheteur_nom`, `date_publication`…) + 2 dates + **999 colonnes
`List(String)`** génériques. Seules ces 999 colonnes génériques sont concernées : ce sont
elles dont le nom est un chemin XML. Les 17 colonnes du cœur sont des champs harmonisés
construits dans `extract_core` et **exclues** du CSV.

Répartition des 999 colonnes :

| Bloc                                     | ~Colonnes | Format             | Source de définition                                                        | Langue |
| ---------------------------------------- | --------- | ------------------ | --------------------------------------------------------------------------- | ------ |
| MAPA / DSP / DIVERS / FNSimple / GESTION | ~506      | legacy BOAMP 3.2.5 | `jo_boamp.xsd`, `jo_types.xsd`, `jo_boamp_opendata.xsd` (`<documentation>`) | FR     |
| EFORMS / ProcurementProjectLot           | ~493      | UBL eForms         | SDK eForms 1.10.5 (`fields.json` + `*_fr.xml`)                              | FR     |

## Sources de données

### Entrée — liste des colonnes

`dist/boamp.parquet` : lire uniquement le schéma (`pl.scan_parquet(...).collect_schema()`),
filtrer les colonnes de type `List(String)`.

### Définitions legacy (FR)

XSD réels du format 3.2.5, déjà dans le repo :

- `reference/boamp_schemas/jo_boamp_opendata_v3.2.5.1/3.2.5/sdk1.9/main/jo_boamp.xsd`
- `.../jo_types.xsd`
- `.../jo_boamp_opendata.xsd`

Chaque élément y est déclaré avec un `<annotation><documentation>` en français. Les
définitions sont **par nom d'élément local**. Collisions observées (ex. `objet` ×10,
`adr` ×3) mais **bénignes** : les textes sont identiques ou quasi (`objet`→"Objet du
marché", `adr`→"Informations d'adresse"). Les feuilles d'adresse (`cp`, `ville`, `nom`)
sont dans `jo_types.xsd`.

### Définitions eForms (FR) — SDK eForms 1.10.5

Source d'origine (hors repo) :
`/home/colin/kDrive/work/colmo/decp/eforms/eForms-SDK-1.10.5/`

- `fields/fields.json` — 1226 `fields` (chacun : `id`, `btId`, `name`, `xpathAbsolute`,
  `parentNodeId`) + 286 nœuds `xmlStructure` (`id` ND-…, `xpathAbsolute`).
- `translations/field_fr.xml` — entrées `field|description|<field-id>` (texte FR).
- `translations/business-term_fr.xml` — entrées `business-term|description|<BG/BT-id>` (FR).

**Reproductibilité** : ces 3 fichiers seront **copiés (vendorés)** dans
`reference/eforms_sdk/1.10.5/` pour que le script et la régénération du CSV ne dépendent
pas de kDrive. La version SDK est inscrite dans le chemin.

## Sortie

`reference/boamp_field_definitions.csv`, 3 colonnes :

1. `nom_colonne` — le nom exact de la colonne du parquet.
2. `definitions` — définitions par segment, **du plus général à la feuille**, une ligne
   par segment, au format `segment : définition` (séparées par `\n` dans la cellule CSV).
   Les segments purement **structurels sont omis** (voir liste).
3. `definition_feuille` — la définition du dernier segment (la feuille) seule, pour un
   accès rapide à l'essentiel.

Exemple (colonne legacy) :

```
nom_colonne : ann_DONNEES_DSP_initial_descriptionReduite_adr_cp
definitions :
  DSP : Délégation de service public
  initial : Avis initial
  descriptionReduite : Description réduite
  adr : Informations d'adresse
  cp : Code postal
definition_feuille : Code postal
```

### Segments structurels omis

`frozenset({"ann", "DONNEES", "EFORMS", "ContractNotice", "ContractAwardNotice", "PriorInformationNotice", "BusinessRegistrationInformationNotice", "UBLExtensions", "UBLExtension", "ExtensionContent", "EformsExtension"})`

(racines de document + conteneurs d'extension UBL sans sens métier).

### Marqueur des trous

Un segment significatif sans définition trouvée donne `segment : (définition introuvable)`,
de façon à repérer les manques et itérer.

## Architecture du script

`script/build_boamp_field_definitions.py` — fonctions pures + un `main()`. Sans I/O réseau.

- `load_list_columns(parquet_path) -> list[str]`
  Schéma seul ; ne garde que les colonnes `List(String)`.

- `build_legacy_index(xsd_paths) -> dict[str, str]`
  Parse les XSD (lxml). Pour chaque `xs:element name="X"` ayant un
  `xs:annotation/xs:documentation`, mappe `X -> texte` (normalisé : espaces/retours
  compactés). Textes distincts pour un même nom joints par `/`.

- `build_eforms_index(sdk_dir) -> EformsIndex`

  - Parse `field_fr.xml` / `business-term_fr.xml` → `dict[id -> description_fr]`.
  - Parse `fields.json`. Pour chaque `field`, normalise `xpathAbsolute`
    (`normalize_xpath`) en chemin de noms locaux (clé `tuple[str,...]` relative à `/*`) ;
    associe la description FR via `field|description|<field.id>`. Indexe
    `path_tuple -> [descriptions]` (plusieurs si prédicats fusionnés).
  - Idem pour `xmlStructure` (nœuds intermédiaires) si une description exploitable existe.

- `normalize_xpath(xp) -> tuple[str,...]`
  Retire prédicats `[...]`, retire les pas d'attribut (`@x` et suffixes `/@x`), découpe sur
  `/`, ignore vides et `*`, garde le nom local (après `:`).

- `STRUCTURAL: frozenset[str]` (cf. ci-dessus).

- `column_segments(col) -> list[str]` : `col.split("_")`.

- `is_eforms(col) -> bool` : `"EFORMS" in col or col.startswith("ProcurementProjectLot")`.

- `column_to_ubl_path(col) -> tuple[str,...]` :

  - `ann_DONNEES_EFORMS_<DocType>_…` → segments après `<DocType>`.
  - `ProcurementProjectLot_…` → `("ProcurementProjectLot", …)` (les xpaths SDK des lots
    sont sous `/*/cac:ProcurementProjectLot/…`).

- `resolve_segment_def(...)` : selon le régime,

  - legacy : `legacy_index.get(segment)` ;
  - eForms : la **feuille** se résout par match du chemin UBL complet sur `EformsIndex` ;
    les segments intermédiaires se résolvent best-effort (préfixe → nœud), sinon
    `(définition introuvable)`.

- `resolve_column(col, legacy_index, eforms_index) -> (definitions_str, leaf_def)`
  Découpe en segments, retire les structurels, résout chaque segment restant, formate.

- `main()` : assemble, écrit le CSV (`pl.DataFrame.write_csv` ou `csv` stdlib), affiche des
  **stats de couverture** (segments résolus / introuvables, par régime).

## Régimes & couverture attendue

- **Legacy (~506 col.)** : couverture quasi totale, FR. Risque résiduel : noms de segment
  génériques avec docs légèrement contextuelles (acceptable ; raffinement par contexte de
  parent = amélioration future, hors scope).
- **eForms (~493 col.)** : **best-effort**. Test naïf : 304/493 feuilles matchées au premier
  passage (la normalisation lot + prédicats améliorera le taux). Les **conteneurs UBL
  intermédiaires** (aggregates sans BT) resteront souvent `(définition introuvable)` — la
  valeur fiable côté eForms est `definition_feuille`. Amélioration future possible :
  fallback sur les `ccts:Definition` anglaises des XSD UBL 2.3 officiels (non dépouillés)
  pour les éléments intermédiaires — **hors scope** de cette v1 (YAGNI).

## Hors scope (YAGNI)

- Résolution par chemin complet via le graphe de types XSD (legacy) — le dict par nom
  suffit vu les collisions bénignes.
- Définitions anglaises UBL pour les conteneurs eForms intermédiaires.
- Intégration dans le flow Prefect : c'est un script utilitaire ponctuel/régénérable, pas
  une étape du pipeline.
- Capture des attributs XML (le `flatten` v1 ne les capture pas ; aucune colonne attribut).

## Tests

`tests/test_boamp_field_definitions.py` (fonctions pures) :

- `normalize_xpath` : prédicats, attributs, préfixes namespace.
- `build_legacy_index` : un mini-XSD inline → nom→doc, dédoublonnage des textes distincts.
- `resolve_column` legacy : un chemin connu → segments structurels omis + défs attendues.
- `resolve_column` eForms : un chemin dont le BT est connu → `definition_feuille` correcte.
- `is_eforms` / `column_to_ubl_path` : cas EFORMS notice-level et ProcurementProjectLot.

```

```
