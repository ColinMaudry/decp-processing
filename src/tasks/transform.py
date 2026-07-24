from pathlib import Path

import polars as pl
import polars.selectors as cs

from src.config import (
    DATA_DIR,
    DECP_COLMO_DATASET,
    DECP_COLMO_RESOURCE_URL,
    DIST_DIR,
    LOG_LEVEL,
    POPULATION_COMMUNES_CSV,
)
from src.tasks.output import save_to_files
from src.tasks.utils import (
    calculate_duplicates_across_source,
    check_parquet_file,
    get_logger,
)


def apply_modifications(lff: pl.LazyFrame):
    """
    Gère les modifications dans le DataFrame des DECP.
    À ce stade les modifications ont été exploded dans write_marche_rows().
    Cette fonction récupère les informations des modifications (ex : modification_montant) et les insère dans les champs de base (ex : montant).
    (chaque ligne contient les informations complètes à jour à la date de notification)
    donneesActuelles et modification_id sont ajoutées après concaténation de toutes les ressources.
    """
    # Étape 1: Extraire les données des modifications en renommant les colonnes
    columns = lff.collect_schema().names()
    columns_no_modif = [col for col in columns if not (col.startswith("modification_"))]

    lf_mods = (
        lff.select(cs.by_name("uid") | cs.starts_with("modification_"))
        .rename(
            {
                column: column.removeprefix("modification_").removesuffix(
                    "Modification"
                )
                for column in columns
                if column.startswith("modification_") and column != "modification_id"
            }
        )
        .filter(~pl.all_horizontal(pl.all().exclude("uid").is_null()))
    )  # sans les lignes de données initiales

    # Étape 2: Dédupliquer et créer une copie du DataFrame initial sans les colonnes "modifications"
    # On peut dédupliquer aveuglément car la seule chose qui varient dans les lignes d'un même
    # uid, c'est les données de modifs
    lff = lff.unique("uid")

    # Garder toutes les colonnes sauf les colonnes modification_*
    lf_base = lff.drop(cs.starts_with("modification_"))

    # Étape 3 : concaténation des données modifiées et des colonnes normales (pas modification_)

    # Colonnes qui peuvent changer avec les modifications
    modified_columns = [
        "uid",
        "dateNotification",
        "datePublicationDonnees",
        "montant",
        "dureeMois",
        "titulaires",
    ]

    lff = pl.concat(
        [
            lf_base.select(modified_columns),
            lf_mods,
        ],
        how="diagonal",
    )

    # Étape 4: Réintroduire le tout avec les colonnes fixes (qui ne changent pas avec les modifications)
    # Sélectionner uniquement les colonnes qui ne sont pas dans modification_columns
    columns_to_keep = [col for col in columns_no_modif if col not in modified_columns]

    # Créer un DataFrame avec uniquement les colonnes fixes, dédupliqué par uid
    lf_fixed_columns = lf_base.select(["uid"] + columns_to_keep).unique("uid")

    # Joindre pour réintroduire les colonnes fixes
    lf_final = lff.join(
        lf_fixed_columns,
        on="uid",
        how="left",
    )

    # Étape 5: Remplir les valeurs nulles en utilisant les dernières valeurs non-nulles pour chaque id
    lf_final = lf_final.sort(
        ["uid", "dateNotification"],
        descending=[False, False],
    )
    lf_final = lf_final.with_columns(
        pl.col("montant", "dureeMois", "titulaires")
        .fill_null(strategy="forward")
        .over("uid")
    )

    return lf_final


def sort_modifications(lff: pl.LazyFrame) -> pl.LazyFrame:
    lff = lff.with_columns(
        pl.col("dateNotification")
        .rank(method="dense")
        .over("uid")
        .cast(pl.Int16)
        .sub(1)
        .alias("modification_id")
    )

    lff = lff.with_columns(
        (
            pl.col("modification_id") == pl.col("modification_id").max().over("uid")
        ).alias("donneesActuelles")
    )

    lff = lff.sort(["uid", "dateNotification"], descending=[False, True])

    return lff


# Clé identifiant une VERSION de marché (pas un marché) : dateNotification
# discrimine les versions (= modification_id), codeCPV discrimine les contrats
# distincts qui partagent le même uid (collisions d'id, cf. issue #186).
VERSION_KEY = ["uid", "dateNotification", "codeCPV"]

# Fichier intermédiaire de la barrière de matérialisation de la consolidation
# (voir consolidate_across_datasets) ; supprimé par concat_parquet_files une fois
# la consolidation sinkée.
FLAGGED_TMP_PATH = DATA_DIR / "temp" / "decp_flagged.parquet"


def consolidate_across_datasets(lf: pl.LazyFrame) -> pl.LazyFrame:
    """Consolide une même version de marché rapportée par plusieurs datasets.

    Un même marché-version peut apparaître dans plusieurs datasets avec des données
    divergentes (objet bruité, complétude différente — titulaire null vs SIRET). On
    fusionne ces occurrences sur la clé de version (uid, dateNotification, codeCPV) :

    - champs scalaires : coalesce champ par champ sur la valeur non-nulle, en
      privilégiant la ligne la plus complète (moins de nulls) puis, à égalité, la
      datePublicationDonnees la plus récente ;
    - titulaires : union des titulaires distincts non-nuls (les cotraitants sont
      conservés ; une ligne sans titulaire est écartée si un titulaire existe) ;
    - source : si la version provient de >1 dataset, elle est estampillée decp_colmo
      (mélange de sources), sinon le dataset d'origine est conservé.

    Remplace le dédoublonnage exact historique (qu'elle subsume) et doit tourner
    AVANT sort_modifications (attribution de modification_id).
    """
    all_cols = lf.collect_schema().names()
    tit_cols = [c for c in all_cols if c.startswith("titulaire")]
    scalar_cols = [c for c in all_cols if c not in tit_cols and c not in VERSION_KEY]

    # Garde-fou anti perte de données : on ne consolide qu'une version dont AUCUN
    # dataset ne contient à lui seul plusieurs objets distincts. Sinon c'est une vraie
    # collision d'id intra-dataset (contrats distincts partageant uid+date+cpv, ex.
    # lots d'une même opération) qu'il ne faut surtout pas fusionner. La multiplicité
    # d'objets ne provenant que du croisement de datasets est, elle, du bruit à fusionner.
    #
    # Calculé par agrégation (group_by) sur une projection étroite plutôt que par
    # window functions sur le frame large : les fenêtres matérialisent tout en mémoire,
    # alors que group_by + join se streament et ne portent que les colonnes utiles.
    multi_obj_flag = (
        lf.select(VERSION_KEY + ["sourceDataset", "objet"])
        .group_by(VERSION_KEY + ["sourceDataset"])
        .agg(pl.col("objet").n_unique().alias("_nobj"))
        .group_by(VERSION_KEY)
        .agg((pl.col("_nobj").max() > 1).alias("_multi_obj"))
    )
    lf = lf.join(multi_obj_flag, on=VERSION_KEY, how="left", nulls_equal=True)

    # Barrière de matérialisation : le frame flaggé alimente trois branches
    # (passthrough, scalar, tit) réunies dans un même graphe par le concat final.
    # Sans barrière, Polars soit met en cache le sous-plan partagé en RAM (dataset
    # complet en pleine largeur, pendant que le tri de scalar tourne par-dessus),
    # soit ré-exécute le sous-plan par branche → pics mémoire additionnés (OOM).
    # Matérialisé sur disque, chaque branche redevient un scan bon marché avec sa
    # projection étroite. Même mécanisme que la barrière aval de concat_parquet_files.
    FLAGGED_TMP_PATH.parent.mkdir(parents=True, exist_ok=True)
    lf.sink_parquet(FLAGGED_TMP_PATH, engine="streaming")
    lf = pl.scan_parquet(FLAGGED_TMP_PATH)

    passthrough = lf.filter(pl.col("_multi_obj")).drop("_multi_obj")
    lf = lf.filter(~pl.col("_multi_obj")).drop("_multi_obj")

    # Les versions non consolidées gardent leur source, on retire seulement les
    # doublons exacts (l'ancien dédoublonnage, en préservant les objets distincts).
    passthrough = passthrough.select(all_cols).unique(
        subset=VERSION_KEY + ["objet", "titulaire_id", "titulaire_typeIdentifiant"],
        maintain_order=False,
    )

    # Score de complétude par ligne : nombre de champs renseignés, hors clé, hors
    # colonnes de source (toujours présentes) et hors datePublicationDonnees (départage
    # séparément la récence).
    comp_cols = [
        c
        for c in all_cols
        if c not in VERSION_KEY
        and c not in ("sourceDataset", "sourceFile", "datePublicationDonnees")
    ]
    lf = lf.with_columns(
        _completeness=pl.sum_horizontal(
            [pl.col(c).is_not_null().cast(pl.Int32) for c in comp_cols]
        )
    )

    # 1) Consolidation scalaire : une ligne par version. Pour chaque champ, on prend la
    # première valeur non-nulle dans l'ordre de priorité (complétude, puis récence, puis
    # sourceDataset pour un départage déterministe). On trie globalement une fois puis
    # group_by(maintain_order).first : plus rapide qu'un sort_by répété par champ, à pic
    # mémoire équivalent. La barrière de matérialisation en aval (concat_parquet_files)
    # isole ce tri de l'enrichissement SIRENE pour éviter que les pics s'additionnent.
    scalar_payload = [c for c in scalar_cols if c != "sourceDataset"]
    scalar = (
        lf.select(VERSION_KEY + scalar_cols + ["_completeness"])
        .sort(
            VERSION_KEY + ["_completeness", "datePublicationDonnees", "sourceDataset"],
            descending=[False, False, False, True, True, False],
            nulls_last=True,
        )
        .group_by(VERSION_KEY, maintain_order=True)
        .agg(
            *[pl.col(c).drop_nulls().first().alias(c) for c in scalar_payload],
            pl.col("sourceDataset").n_unique().alias("_n_datasets"),
            pl.col("sourceDataset").drop_nulls().first().alias("_first_dataset"),
        )
        .with_columns(
            sourceDataset=pl.when(pl.col("_n_datasets") > 1)
            .then(pl.lit(DECP_COLMO_DATASET))
            .otherwise(pl.col("_first_dataset"))
        )
        .with_columns(
            sourceFile=pl.when(pl.col("sourceDataset") == DECP_COLMO_DATASET)
            .then(pl.lit(DECP_COLMO_RESOURCE_URL))
            .otherwise(pl.col("sourceFile"))
        )
        .drop("_n_datasets", "_first_dataset")
    )

    # 2) Titulaires : on écarte les lignes sans titulaire quand un titulaire existe
    # pour la version, puis on garde les titulaires distincts (union des cotraitants).
    tit = (
        lf.select(VERSION_KEY + tit_cols)
        .with_columns(
            _has_real=pl.col("titulaire_id").is_not_null().any().over(VERSION_KEY)
        )
        .filter(pl.col("titulaire_id").is_not_null() | ~pl.col("_has_real"))
        .drop("_has_real")
        .unique(subset=VERSION_KEY + tit_cols)
    )

    # 3) Recombinaison : une ligne par (version × titulaire). nulls_equal pour apparier
    # les clés dont dateNotification/codeCPV sont nuls.
    consolidated = scalar.join(
        tit, on=VERSION_KEY, how="left", nulls_equal=True
    ).select(all_cols)

    # On réunit les versions consolidées et les versions laissées intactes (garde-fou).
    return pl.concat([consolidated, passthrough], how="vertical")


def concat_parquet_files(parquet_files: list, output_dir=DIST_DIR) -> pl.LazyFrame:
    """Concatenation par morceaux (chunks) pour éviter de charger trop de fichiers en mémoire
    # et pour éviter "OSError: Too many open files"

    # Mise de côté des parquet
    # - qui n'existent pas (s'il y a eu une erreur par exemple)
    # - qui ont une hauteur de 0"""
    logger = get_logger(level=LOG_LEVEL)

    if len(parquet_files) == 0:
        raise ValueError("No parquet file to concat.")

    checked_parquet_files = [file for file in parquet_files if check_parquet_file(file)]

    chunk_size = 500
    chunks = [
        checked_parquet_files[i : i + chunk_size]
        for i in range(0, len(checked_parquet_files), chunk_size)
    ]

    intermediate_files = []
    for i, chunk in enumerate(chunks):
        logger.info(f"Concatenation du chunk {i + 1}/{len(chunks)}")
        lfs = [pl.scan_parquet(file) for file in chunk]
        lf_chunk = pl.concat(lfs, how="vertical")

        # On sauvegarde chaque chunk concaténé
        chunk_path = DATA_DIR / "temp" / f"chunk_{i}.parquet"
        chunk_path.parent.mkdir(parents=True, exist_ok=True)

        # Utilisation de sink_parquet pour écrire sans tout charger en RAM
        lf_chunk.sink_parquet(chunk_path, engine="streaming")
        intermediate_files.append(chunk_path)

    # Concatenation finale des fichiers intermédiaires
    logger.info("Concatenation finale...")
    lfs = [pl.scan_parquet(file) for file in intermediate_files if Path(file).exists()]
    lf_concat: pl.LazyFrame = pl.concat(lfs, how="vertical")

    logger.info("Calcul des % de doublons entre sources...")
    calculate_duplicates_across_source(lf_concat, output_dir=output_dir)

    logger.info("Consolidation des versions de marché entre datasets...")

    # Fusionne une même version de marché rapportée par plusieurs datasets (coalesce
    # des champs, union des titulaires, estampille decp_colmo). Subsume l'ancien
    # dédoublonnage exact sur (uid, titulaire_id, titulaire_typeIdentifiant,
    # dateNotification). Cf. issue #186. Exemple de doublon : 20005584600014157140791205100
    lf_concat = consolidate_across_datasets(lf_concat)

    # Barrière de matérialisation : on écrit le résultat de la consolidation sur disque
    # et on repart d'un scan. Sans cette barrière, la consolidation (tri lourd) reste
    # fusionnée dans un même graphe lazy avec l'enrichissement SIRENE en aval → les pics
    # mémoire s'additionnent et provoquent un OOM. Avec la barrière, la consolidation
    # s'exécute seule, libère sa mémoire, puis l'aval repart d'un scan à froid.
    consolidated_path = DATA_DIR / "temp" / "decp_consolidated.parquet"
    consolidated_path.parent.mkdir(parents=True, exist_ok=True)
    lf_concat.sink_parquet(consolidated_path, engine="streaming")

    # Le fichier flaggé intermédiaire (barrière interne de la consolidation)
    # n'est plus référencé une fois la consolidation matérialisée.
    FLAGGED_TMP_PATH.unlink(missing_ok=True)

    return pl.scan_parquet(consolidated_path)


def extract_unique_acheteurs_siret(lf: pl.LazyFrame):
    # Extraction des SIRET des DECP dans une copie du df de base
    lf = lf.select("acheteur_id")
    lf = lf.unique()
    return lf


def extract_unique_titulaires_siret(lf: pl.LazyFrame):
    # Extraction des SIRET des DECP dans une copie du df de base
    lf = lf.select("titulaire_id", "titulaire_typeIdentifiant")
    lf = lf.unique().filter(
        pl.col("titulaire_id") != "", pl.col("titulaire_typeIdentifiant") == "SIRET"
    )
    return lf


def prepare_unites_legales(lf: pl.LazyFrame) -> pl.LazyFrame:
    return (
        lf.select(
            [
                "siren",
                "denominationUniteLegale",
                "prenomUsuelUniteLegale",
                "nomUniteLegale",  # toujours rempli pour personnes physique
                "nomUsageUniteLegale",  # parfois rempli, a la priorité sur nomUniteLegale
                "statutDiffusionUniteLegale",  # P = non-diffusible
                "categorieEntreprise",  # PME, ETI, GE
                "categorieJuridiqueUniteLegale",  # 1000, etc.
            ]
        )
        .filter(
            pl.col("siren").is_not_null()
        )  # utilisation du fichier Stock, normalement pas de siren null
        .unique()  # utilisation du fichier Stock, normalement pas de doublons
        .with_columns(
            pl.when(pl.col("nomUsageUniteLegale").is_not_null())
            .then(pl.col("nomUsageUniteLegale"))
            .otherwise(pl.col("nomUniteLegale"))
            .alias("nomUniteLegale")
        )
        .with_columns(
            pl.when(pl.col("nomUniteLegale").is_not_null())
            .then(
                pl.concat_str(
                    pl.col("prenomUsuelUniteLegale"),
                    pl.col("nomUniteLegale"),
                    separator=" ",
                )
            )
            .otherwise(pl.col("denominationUniteLegale"))
            .alias("denominationUniteLegale")
        )
        .with_columns(
            pl.when(pl.col("statutDiffusionUniteLegale") == "P")
            .then(pl.lit("[Données personnelles non-diffusibles]"))
            .otherwise(pl.col("denominationUniteLegale"))
            .alias("denominationUniteLegale")
        )
        .drop(
            [
                "prenomUsuelUniteLegale",
                "statutDiffusionUniteLegale",
                "nomUniteLegale",
                "nomUsageUniteLegale",
            ]
        )
    )


def prepare_etablissements(lff: pl.LazyFrame) -> pl.LazyFrame:
    lff = lff.with_columns(
        [
            pl.col("codeCommuneEtablissement").str.pad_start(5, "0"),
            pl.col("siret").str.pad_start(14, "0"),
            # Si enseigne1Etablissement est null, on utilise denominationUsuelleEtablissement
            pl.coalesce(
                "enseigne1Etablissement", "denominationUsuelleEtablissement"
            ).alias("etablissement_nom"),
        ]
    )
    lff = lff.drop("denominationUsuelleEtablissement", "enseigne1Etablissement")
    lff = lff.rename(
        {
            "codeCommuneEtablissement": "commune_code",
            "activitePrincipaleEtablissement": "activite_code",
            "nomenclatureActivitePrincipaleEtablissement": "activite_nomenclature",
        }
    )

    # Ajout des noms de commune, départements, régions
    lf_cog = pl.scan_parquet(DATA_DIR / "code_officiel_geographique.parquet")
    lff = lff.join(lf_cog, on="commune_code", how="left")

    return lff


def sort_columns(lf: pl.LazyFrame, config_columns):
    logger = get_logger(level=LOG_LEVEL)

    # Les colonnes présentes mais absentes des colonnes attendues sont mises à la fin de la liste
    schema = lf.collect_schema()
    other_columns = []
    for col in schema.keys():
        if col not in config_columns:
            other_columns.append(col)

    if other_columns:
        logger.warning("Colonnes inattendues: " + str(other_columns))

    lf = lf.select(config_columns + other_columns)
    lf = lf.sort(
        by=["dateNotification", "uid"], descending=[True, False], nulls_last=True
    )

    return lf


def calculate_naf_cpv_matching(lf_naf_cpv: pl.LazyFrame, output_dir=DIST_DIR):
    # Unité de base pour le comptage : dernière version d'un marché attribué (donc pas forcément attributaire initial)
    lf_naf_cpv = (
        lf_naf_cpv.select(
            "uid",
            "codeCPV",
            # Le NAF du titulaire est préfixé titulaire_ en amont ; on le ramène aux
            # noms activite_* utilisés (et publiés) par cette fonction.
            pl.col("titulaire_activite_code").alias("activite_code"),
            pl.col("titulaire_activite_nomenclature").alias("activite_nomenclature"),
            "donneesActuelles",
        )
        .filter(pl.col("donneesActuelles"))
        # On écarte d'abord les lignes inexploitables (NAF ou CPV absent) AVANT de
        # dédupliquer par marché : sinon .unique("uid") peut conserver arbitrairement
        # un titulaire sans NAF et faire perdre la paire NAF/CPV du marché.
        .drop_nulls(["codeCPV", "activite_code", "activite_nomenclature"])
        # Une seule ligne par marché, de façon déterministe (tri préalable).
        .sort("uid", "activite_nomenclature", "activite_code", "codeCPV")
        .unique("uid", keep="first", maintain_order=True)
    )

    # Nettoyage et normalisation
    lf_naf_cpv = lf_naf_cpv.select(
        [
            pl.col("activite_code")
            .str.strip_chars()
            .str.to_uppercase()
            .alias("activite_code"),
            pl.col("activite_nomenclature")
            .str.strip_chars()
            .str.to_uppercase()
            .alias("activite_nomenclature"),
            pl.col("codeCPV").str.strip_chars().alias("cpv"),
        ]
    )

    # Nombre de marchés par paire (NAF, CPV). C'est aussi la base du calcul de
    # probabilité : inutile de matérialiser une matrice dense NAF×CPV (majoritairement
    # nulle) ni de dérouler le produit cartésien en Python. On reste sur les seules
    # paires réellement observées.
    counts = lf_naf_cpv.group_by("activite_nomenclature", "activite_code", "cpv").agg(
        pl.len().alias("nb_marches")
    )

    # Probabilité conditionnelle P(cpv | naf) = nb_marches(naf, cpv) / nb_marches(naf)
    total_par_naf = (
        pl.col("nb_marches").sum().over("activite_nomenclature", "activite_code")
    )

    df_results = (
        counts.with_columns((pl.col("nb_marches") / total_par_naf).alias("score"))
        .with_columns(
            pl.col("score")
            .rank(method="dense", descending=True)
            .over("activite_nomenclature", "activite_code")
            .alias("rank")
        )
        # On ne garde que les 10 meilleurs CPV par NAF (score > 0 par construction).
        .filter((pl.col("rank") <= 10) & (pl.col("score") > 0))
        .select(
            "activite_nomenclature",
            "activite_code",
            "cpv",
            "score",
            "rank",
            "nb_marches",
        )
        .sort(
            ["activite_nomenclature", "activite_code", "score"],
            descending=[False, False, True],
        )
        .collect(engine="streaming")
    )

    save_to_files(df_results, output_dir / "probabilites_naf_cpv", "csv")


def join_population(
    lf: pl.LazyFrame, population_csv_path: Path = POPULATION_COMMUNES_CSV
) -> pl.LazyFrame:
    """Joint le LazyFrame avec le CSV des communes via le SIREN extrait de acheteur_id.

    SIREN = 9 premiers caractères du SIRET (acheteur_id est une chaîne).
    Renvoie le LazyFrame avec une nouvelle colonne 'acheteur_population' (null si non trouvé).
    Si le fichier CSV est absent, la colonne est ajoutée avec des nulls.
    """
    logger = get_logger(level=LOG_LEVEL)
    if not population_csv_path.exists():
        logger.warning(
            f"Fichier population communes introuvable : {population_csv_path}. "
            "La colonne 'acheteur_population' sera nulle."
        )
        return lf.with_columns(pl.lit(None).cast(pl.Int64).alias("acheteur_population"))
    population_lf = pl.scan_csv(population_csv_path).select(
        pl.col("SIREN").cast(pl.Utf8),
        pl.col("population").cast(pl.Int64),
    )
    return (
        lf.with_columns(pl.col("acheteur_id").str.slice(0, 9).alias("_siren_acheteur"))
        .join(population_lf, left_on="_siren_acheteur", right_on="SIREN", how="left")
        .drop("_siren_acheteur")
        .rename({"population": "acheteur_population"})
    )
