# Log des statistiques par colonne (valeurs distinctes / % null)

**Statut** : design validé en brainstorming, en attente de plan d'implémentation
**Date** : 2026-07-24

## Contexte et problème

Le pipeline `decp-processing` publie un jeu de données consolidé d'environ 30 colonnes. Il n'existe aujourd'hui aucune visibilité rapide, dans les logs, sur la qualité de chaque colonne au moment de la publication (colonne quasi entièrement nulle suite à une régression d'enrichissement, colonne avec une cardinalité anormalement basse ou haute, etc.). Cette information n'est visible qu'en interrogeant les fichiers de sortie a posteriori.

**Objectif** : logger, pour chaque colonne du jeu de données final, le nombre de valeurs distinctes et le pourcentage de valeurs nulles, dans le flow Prefect `decp_processing`, à l'endroit où les statistiques de publication sont déjà générées.

## Vue d'ensemble

Nouvelle fonction `log_column_stats(lf: pl.LazyFrame, nb_lignes: int) -> None` dans `src/tasks/utils.py` (section `STATS`, à côté de `generate_stats`), appelée depuis `generate_stats()`.

## Emplacement dans le pipeline

`generate_stats(lf)` est déjà appelée dans `decp_processing()` (`src/flows/decp_processing.py`) juste avant la génération du schéma final et l'écriture des fichiers de sortie. `log_column_stats` est appelée à l'intérieur de `generate_stats`, juste après le calcul de `nb_lignes` (ligne `nb_lignes = lf.select(pl.len()).collect().item()`) — cette valeur est réutilisée telle quelle, sans nouveau `.collect()` pour compter les lignes.

## Calcul

Un seul passage sur les données :

1. `lf.collect_schema()` donne la liste des colonnes. Vérifié empiriquement (Polars 1.36.1) : `n_unique()` fonctionne sur tous les types présents dans le pipeline, y compris `List(Struct)` (ex. `titulaires`) — pas d'exclusion nécessaire par type.
2. Construction d'une seule requête lazy avec, pour chaque colonne, deux expressions : `pl.col(c).null_count().alias(f"{c}__null_count")` et `pl.col(c).n_unique().alias(f"{c}__n_unique")`.
3. Un seul `.collect()` sur cette requête (une seule ligne de résultat, une passe sur les données).
4. **Piège Polars** : `n_unique()` compte `null` comme une valeur distincte à part entière (ex. `[100, 200, None]` → `n_unique() == 3`, pas 2). Le nombre de valeurs distinctes _non-null_ est donc `n_unique - (1 si null_count > 0 sinon 0)` — même convention que le code existant (`nb_acheteurs_uniques = df_uid["acheteur_id"].n_unique() - 1` dans `generate_stats`).
5. `% null` = `null_count / nb_lignes * 100` (colonne vide si `nb_lignes == 0`, cas déjà exclu ailleurs dans le pipeline).

## Format du log

Un seul `logger.info(...)`, contenant un tableau texte aligné (une ligne par colonne, colonnes triées par ordre alphabétique), dans le style de `print_all_config` (accumulation d'un message multi-lignes puis un unique appel `logger.info`) :

```
Statistiques par colonne (valeurs distinctes / % null) :
acheteur_id                    123456   0.00%
titulaires                     n/a      2.31%
...
```

## Modifications

- `src/tasks/utils.py` — nouvelle fonction `log_column_stats`, appelée dans `generate_stats`
- `tests/test_stats.py` (fichier de test existant pour les fonctions de stats) — test unitaire sur un petit LazyFrame de démonstration couvrant : colonne avec nulls, colonne sans null, colonne `List(Struct)`

## Hors scope

- Pas de nouvel artefact Prefect (`create_table_artifact`) pour cette donnée — uniquement un log texte.
- Pas de seuil d'alerte ni de détection d'anomalie sur ces stats — c'est de la visibilité, pas de la validation.
