# Détection et rationalisation des montants anormaux

Ce document explique les règles appliquées dans `src/tasks/anomaly.py` pour classer un marché comme **suspect** ou **aberrant**, et pour calculer le `montant_rationalise`.

---

## Comprendre les concepts clés

Avant de détailler les règles, voici une explication en langage courant des outils statistiques utilisés.

### Le groupe de pairs

L'idée de base est simple : **un montant n'est anormal que par rapport à des marchés comparables**. Un marché de travaux de voirie à 10 M€ n'est pas du tout la même chose qu'un marché de fournitures de bureau à 10 M€.

On constitue donc un **groupe de pairs** : l'ensemble des marchés de la base qui partagent les mêmes caractéristiques que le marché analysé — même famille d'achat (CPV), même type (Travaux / Services / Fournitures), même catégorie d'acheteur, taille de commune similaire. C'est à l'intérieur de ce groupe qu'on juge si le montant est normal ou non.

Pour que la comparaison soit fiable, un groupe doit contenir **au moins 30 marchés**. Si le groupe le plus précis est trop petit, on élargit progressivement les critères (on retire la taille de commune, puis la famille d'achat, etc.) jusqu'à trouver un groupe assez grand. Si même le groupe le plus large ne suffit pas, le Signal A n'est tout simplement pas calculé pour ce marché.

### Le montant normalisé (`montant_normalise`)

Comparer un marché de nettoyage sur 1 mois et un marché de nettoyage sur 36 mois n'a pas de sens si on compare les montants totaux : le second sera mécaniquement plus élevé. On commence donc par **ramener tous les montants à une base mensuelle** pour les Services et Fournitures non forfaitaires :

```
montant_normalise = montant ÷ dureeMois
```

Exemple : un marché de maintenance informatique à 120 000 € sur 24 mois devient 5 000 €/mois. Un autre à 60 000 € sur 6 mois devient aussi 10 000 €/mois. On peut maintenant les comparer.

Pour les Travaux ou les marchés forfaitaires, le montant est une somme ponctuelle — on ne divise pas.

### Pourquoi utiliser le logarithme (`log_montant_normalise`)

Les montants de marchés publics s'étalent sur des ordres de grandeur énormes : de quelques centaines d'euros à plusieurs milliards. Sur une échelle ordinaire, un marché à 1 milliard d'euros écraserait visuellement tous les marchés à 10 000 € — même s'ils sont parfaitement normaux dans leur catégorie.

Le logarithme permet de **comprimer cette échelle** pour que les écarts soient comparables à tous les niveaux de montant. Concrètement :

| Montant normalisé | log₁₀(montant + 1) |
| ----------------- | ------------------ |
| 1 000 €           | ≈ 3,0              |
| 10 000 €          | ≈ 4,0              |
| 100 000 €         | ≈ 5,0              |
| 1 000 000 €       | ≈ 6,0              |
| 1 000 000 000 €   | ≈ 9,0              |

Passer de 1 000 € à 10 000 € (×10) ou de 1 M€ à 10 M€ (×10) représente le même écart de 1,0 sur l'échelle logarithmique. Cela rend les comparaisons équitables quel que soit le niveau de prix habituel du groupe.

### La médiane

La médiane est **la valeur du milieu** quand on trie tous les montants d'un groupe du plus petit au plus grand. Si le groupe a 100 marchés, la médiane est le 50ème montant.

Contrairement à la moyenne, la médiane n'est **pas perturbée par les valeurs extrêmes**. Si un groupe contient 99 marchés à 50 000 € et un marché à 500 000 000 €, la médiane reste proche de 50 000 €, tandis que la moyenne serait faussée par le cas aberrant.

C'est précisément pour ça qu'on l'utilise ici : on cherche à détecter des anomalies, pas à les laisser contaminer la référence.

### Le MAD — dispersion typique du groupe (`mad_log`)

La médiane dit où se situe le centre du groupe. Mais deux groupes peuvent avoir la même médiane avec des comportements très différents : dans l'un, tous les marchés coûtent quasiment pareil ; dans l'autre, les prix varient énormément.

Le **MAD** (Median Absolute Deviation, ou écart absolu médian) mesure **à quel point les montants du groupe sont dispersés autour de leur médiane**. On calcule l'écart de chaque marché à la médiane, puis on prend la médiane de ces écarts.

Illustration :

- Groupe A (homogène) : montants logarithmiques de 4,8 / 4,9 / 5,0 / 5,1 / 5,2 → médiane = 5,0, MAD = 0,1
- Groupe B (hétérogène) : montants logarithmiques de 3,0 / 4,0 / 5,0 / 6,0 / 7,0 → médiane = 5,0, MAD = 1,0

Un marché avec un log-montant de 6,5 serait **à 15 MAD de la médiane dans le groupe A** (très anormal) mais **à seulement 1,5 MAD dans le groupe B** (tout à fait ordinaire). Le MAD s'adapte ainsi automatiquement à la variabilité naturelle de chaque catégorie.

Comme pour la médiane, le MAD est calculé sur les **valeurs logarithmiques** des montants (d'où le nom `mad_log`).

### L'écart en nombre de MAD (`ecart_pairs`)

Une fois qu'on dispose de la médiane et du MAD du groupe, on exprime l'écart du marché analysé en **"nombre de dispersions typiques"** :

```
ecart_pairs = (log_montant_normalise − mediane_log) ÷ mad_log
```

Un `ecart_pairs` de 1 signifie que le marché est à une "dispersion typique" au-dessus de la médiane — courant. À 4, il est déjà très au-dessus de ce qu'on observe normalement dans le groupe. À 6, il sort vraiment de l'ordinaire.

C'est ce score qui sert de seuil pour classer un marché comme suspect (> 4) ou aberrant (> 6).

---

## Vue d'ensemble des signaux

Trois signaux indépendants contribuent à la classification :

| Signal | Colonne résultat       | Description courte                              |
| ------ | ---------------------- | ----------------------------------------------- |
| A      | `ecart_pairs`          | Écart du montant par rapport au groupe de pairs |
| B      | `montant_par_habitant` | Montant rapporté à la population de l'acheteur  |
| C      | `modulateur_titulaire` | Reclassement si PME avec montant > 50 M€        |

La classification finale est dans `montant_anomalie` (valeurs : `null`, `"suspect"`, `"aberrant"`).
La raison principale est dans `montant_anomalie_raisons`.

---

## Étape 1 — Normalisation du montant (`montant_normalise`)

**Règle :**

- Pour les marchés de type **Services** ou **Fournitures**, avec `dureeMois > 1` et `formePrix != "Forfaitaire"` :
  `montant_normalise = montant / dureeMois`
  _(on ramène à un coût mensuel — voir explication ci-dessus)_

- Dans tous les autres cas (Travaux, forfaitaires, durée ≤ 1 mois) :
  `montant_normalise = montant`

Puis : `log_montant_normalise = log10(montant_normalise + 1)`
_(passage à l'échelle logarithmique — voir explication ci-dessus)_

---

## Étape 2 — Groupe de pairs et statistiques (`n_groupe`, `niveau_groupe`, `mediane_log`, `mad_log`, `median_montant_norm`)

Chaque marché est comparé à un groupe de marchés similaires. Le groupe est construit selon quatre niveaux de granularité décroissante :

| Niveau              | Critères de regroupement                                            |
| ------------------- | ------------------------------------------------------------------- |
| L4 (le plus précis) | `codeCPV_court`, `type`, `acheteur_categorie`, `tranche_population` |
| L3                  | `codeCPV_court`, `type`, `acheteur_categorie`                       |
| L2                  | `type`, `acheteur_categorie`                                        |
| L1 (le plus large)  | `type` uniquement                                                   |

Le niveau retenu est le plus précis dont le groupe contient **au moins 30 marchés** (configurable via `ANOMALY_GROUPE_MIN_SIZE` dans `src/config.py`).

> **`tranche_population`** est calculée à partir de la `population` de la commune de l'acheteur (jointure via les 9 premiers caractères du SIRET = SIREN) :
>
> - < 2 000 hab → `"très petite"`
> - 2 000 – 9 999 → `"petite"`
> - 10 000 – 49 999 → `"moyenne"`
> - 50 000 – 199 999 → `"grande"`
> - ≥ 200 000 → `"très grande"`

Pour chaque groupe, on calcule :

- **`mediane_log`** : la médiane des log-montants normalisés (le montant "typique" du groupe, en échelle log)
- **`mad_log`** : la dispersion typique du groupe (voir explication du MAD ci-dessus)
- **`median_montant_norm`** : la médiane des montants normalisés en euros (utilisée pour la rationalisation)

Si aucun niveau n'atteint 30 marchés, ces colonnes restent `null` et le Signal A est inactif pour ce marché.

> **Marchés modifiés et historique.** Un marché public peut être modifié plusieurs fois au cours de sa vie (avenants) ; chaque version (donnée initiale + chaque modification) est une ligne distincte, identifiée par `modification_id`, et `donneesActuelles` indique la version actuellement en vigueur.
>
> Les groupes de pairs (médiane, MAD) sont calculés **uniquement sur les marchés actuels** (`donneesActuelles = true`) : sinon un marché modifié 5 fois pèserait 5 fois plus qu'un marché normal dans le calcul des seuils de son groupe, biaisant la référence.
>
> En revanche, **toutes les lignes sont classifiées** contre ces mêmes seuils de référence — la version actuelle comme chacune des versions historiques. Un montant historique a réellement été engagé à un moment donné et mérite d'être évalué pour lui-même, plutôt que d'hériter silencieusement de la classification de la version actuelle ou d'être ignoré.

---

## Signal A — Écart par rapport aux pairs (`ecart_pairs`)

```
ecart_pairs = (log_montant_normalise - mediane_log) / mad_log
```

`ecart_pairs` est `null` si `mad_log` est nulle ou null (groupe absent ou sans dispersion).

**Seuils (configurables) :**

| Classification | Condition           | Nom de la variable de configuration |
| -------------- | ------------------- | ----------------------------------- |
| suspect        | `ecart_pairs > 4,0` | `ANOMALY_PAIRS_SUSPECT_THRESHOLD`   |
| aberrant       | `ecart_pairs > 6,0` | `ANOMALY_PAIRS_ABERRANT_THRESHOLD`  |

**Exemple concret :**
Un marché de Services informatiques (CPV 72) passé par une grande ville. Dans son groupe de pairs, la médiane des log-montants est 4,7 (soit environ 50 000 €/mois) et le MAD vaut 0,8.

- Marché à 5 000 000 € → `log ≈ 6,7` → `ecart_pairs = (6,7 − 4,7) / 0,8 = 2,5` → normal
- Marché à 500 000 000 € → `log ≈ 8,7` → `ecart_pairs = (8,7 − 4,7) / 0,8 = 5,0` → **suspect**
- Marché à 5 000 000 000 € → `log ≈ 9,7` → `ecart_pairs ≈ 12,5` → **aberrant**

---

## Signal B — Montant par habitant (`montant_par_habitant`)

Ce signal pose une question différente : **le montant est-il raisonnable au regard de la taille de la commune qui achète ?** Une commune de 1 000 habitants qui signe un marché de travaux à 50 M€ est plus suspecte qu'une métropole de 500 000 habitants.

```
montant_par_habitant = montant / population
```

`population` est obtenue en croisant le SIREN de l'acheteur (`acheteur_id[:9]`) avec le fichier `identifiants-communes.csv`. Si ce fichier est absent ou que l'acheteur n'y figure pas, `montant_par_habitant` est `null` et le Signal B est inactif.

**Seuils (fixes) :**

| Type de marché | Suspect (€/hab) | Aberrant (€/hab) |
| -------------- | --------------- | ---------------- |
| Travaux        | > 5 000         | > 20 000         |
| Services       | > 1 000         | > 5 000          |
| Fournitures    | > 500           | > 2 000          |

**Exemple :**
Une commune de 10 000 habitants signe un marché de Travaux à 300 000 000 €.
`montant_par_habitant = 300 000 000 / 10 000 = 30 000 €/hab` → **aberrant** (> 20 000).

---

## Signal C — Modulateur titulaire PME (`modulateur_titulaire`)

Ce signal ne génère pas de classification initiale mais **aggrave une classification existante** : il passe un marché déjà classé `"suspect"` en `"aberrant"` si les trois conditions suivantes sont réunies simultanément :

- classification initiale = `"suspect"` (Signal A ou B)
- `titulaire_categorie == "PME"`
- `montant > 50 000 000 €`

**Logique :** une PME remportant un marché de plus de 50 M€ est économiquement incohérente — une PME ne dispose généralement pas de la capacité financière et humaine pour absorber un tel volume. Ce constat, combiné à un montant déjà jugé élevé par le Signal A ou B, justifie une classification plus sévère.

---

## Règle de priorité et classification finale (`montant_anomalie`)

1. **aberrant** si Signal A en zone aberrante OU Signal B en zone aberrante
2. **suspect** si Signal A en zone suspecte OU Signal B en zone suspecte (et pas aberrant)
3. Le Signal C remonte un **suspect → aberrant** si les conditions PME sont réunies
4. `null` si aucun signal n'est déclenché

`montant_anomalie_raisons` est une **liste de chaînes** qui contient **toutes** les raisons déclenchées pour ce marché (pas seulement la principale). Elle est `null` quand il n'y a pas d'anomalie.

| Valeur possible dans la liste          | Signification                                 |
| -------------------------------------- | --------------------------------------------- |
| `montant_par_habitant_aberrant`        | Signal B : montant/habitant en zone aberrante |
| `montant_par_habitant_suspect`         | Signal B : montant/habitant en zone suspecte  |
| `montant_vs_pairs_aberrant`            | Signal A : écart pairs en zone aberrante      |
| `montant_vs_pairs_suspect`             | Signal A : écart pairs en zone suspecte       |
| `titulaire_incoherent_pme_gros_marche` | Signal C : PME + montant > 50 M€              |

**Exemples :**

- Un marché où à la fois le Signal A et le Signal B sont en zone aberrante aura `["montant_par_habitant_aberrant", "montant_vs_pairs_aberrant"]`.
- Un marché PME suspect escaladé en aberrant par le Signal C aura `["montant_vs_pairs_suspect", "titulaire_incoherent_pme_gros_marche"]` — ce qui permet de retrouver le signal d'origine qui avait déclenché la suspicion.

---

## Calcul du montant rationalisé (`montant_rationalise`)

Pour les marchés **aberrants**, le montant déclaré est remplacé par une estimation de référence afin que les agrégations statistiques ne soient pas faussées par des valeurs manifestement erronées.

| Condition                                             | `montant_rationalise`                   |
| ----------------------------------------------------- | --------------------------------------- |
| Marché non aberrant (`null` ou `"suspect"`)           | `montant` (inchangé)                    |
| Marché aberrant, normalisation durée appliquée\*      | `median_montant_norm × dureeMois`       |
| Marché aberrant, pas de normalisation durée           | `median_montant_norm`                   |
| Marché aberrant mais `median_montant_norm` est `null` | `null` (groupe trop petit pour estimer) |

\*La normalisation durée est appliquée si `type ∈ {Services, Fournitures}` ET `dureeMois > 1` ET `formePrix != "Forfaitaire"`.

**Exemple :**
Un marché de Services aberrant, 24 mois, non forfaitaire. La médiane mensuelle de son groupe est `median_montant_norm = 8 000 €/mois`.
`montant_rationalise = 8 000 × 24 = 192 000 €`

---

## Colonnes ajoutées au LazyFrame de sortie

| Colonne                    | Type         | Description                                                |
| -------------------------- | ------------ | ---------------------------------------------------------- |
| `montant_rationalise`      | Float        | Montant à utiliser pour les agrégations                    |
| `montant_anomalie`         | String       | `null`, `"suspect"` ou `"aberrant"`                        |
| `montant_anomalie_raisons` | List[String] | Toutes les raisons déclenchées (`null` si aucune anomalie) |

Les colonnes intermédiaires de calcul (`population`, `tranche_population`, `montant_normalise`, `log_montant_normalise`, `codeCPV_court`, `n_groupe`, `niveau_groupe`, `mediane_log`, `mad_log`, `median_montant_norm`, `ecart_pairs`, `montant_par_habitant`) sont supprimées du LazyFrame final.
