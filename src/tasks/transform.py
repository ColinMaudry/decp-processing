from datetime import datetime
from pathlib import Path

import polars as pl
import polars.selectors as cs

from src.config import DATA_DIR, DIST_DIR, LOG_LEVEL
from src.tasks.output import save_to_files
from src.tasks.utils import check_parquet_file, get_logger


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
    logger = get_logger(level=LOG_LEVEL)

    logger.info(
        lff.collect()
        .filter(pl.col("uid") == "219740222000192022VI2022_242")
        .select("uid", "titulaire_id", "dateNotification")
        .sort(["uid", "dateNotification"])
    )

    lff = lff.with_columns(
        pl.col("dateNotification")
        .rank(method="ordinal")
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

    lff = lff.sort(
        ["uid", "dateNotification", "modification_id"],
        descending=[False, True, True],
    )

    # Étape 4: Remplir les valeurs nulles en utilisant les dernières valeurs non-nulles pour chaque id
    lff = lff.with_columns(
        pl.col("montant", "dureeMois", "titulaire_id", "titulaire_typeIdentifiant")
        .fill_null(strategy="backward")
        .over("uid")
    )
    return lff


def concat_parquet_files(parquet_files: list) -> pl.LazyFrame:
    """Concatenation par morceaux (chunks) pour éviter de charger trop de fichiers en mémoire
    # et pour éviter "OSError: Too many open files"

    # Mise de côté des parquet
    # - qui n'existent pas (s'il y a eu une erreur par exemple)
    # - qui ont une hauteur de 0"""
    logger = get_logger(level=LOG_LEVEL)

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

    logger.debug(
        "Suppression des lignes en doublon par UID + titulaire ID + titulaire type ID + dateNotification"
    )

    # Exemple de doublon : 20005584600014157140791205100

    lf_concat = lf_concat.unique(
        subset=["uid", "titulaire_id", "titulaire_typeIdentifiant", "dateNotification"],
        maintain_order=False,
    )

    return lf_concat


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

    logger.warning("Colonnes inattendues:", other_columns)

    lf = lf.select(config_columns + other_columns)
    lf = lf.sort(
        by=["dateNotification", "uid"], descending=[True, False], nulls_last=True
    )

    return lf


def calculate_naf_cpv_matching(lf_naf_cpv: pl.LazyFrame):
    # Unité de base pour le comptage : dernière version d'un marché attribué (donc pas forcément attributaire initial)
    lf_naf_cpv = (
        lf_naf_cpv.select(
            "uid",
            "codeCPV",
            "activite_code",
            "activite_nomenclature",
            "donneesActuelles",
        )
        .filter(pl.col("donneesActuelles"))
        .unique("uid")
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
            pl.col("codeCPV").str.strip_chars().alias("cpv_code"),
        ]
    ).drop_nulls()

    cpv_naf_counts = lf_naf_cpv.group_by(
        "cpv_code", "activite_code", "activite_nomenclature"
    ).agg(pl.count().alias("nb_marches"))

    # Fusion temporaire du code NAF et de sa nomenclature par simplicité
    lf_naf_cpv = lf_naf_cpv.with_columns(
        pl.concat_str(
            pl.col("activite_nomenclature"), pl.lit("__"), pl.col("activite_code")
        ).alias("activite")
    ).drop("activite_code", "activite_nomenclature")

    # Groupage par NAF, CPV, avec compte des CPV
    lf_naf_cpv = lf_naf_cpv.group_by(["activite", "cpv_code"]).agg(
        pl.count().alias("compte")
    )

    # Pas de pivot en Lazy, donc on repasse en DataFrame
    df_occurences_cpv = lf_naf_cpv.collect(engine="streaming")
    df_occurences_cpv = df_occurences_cpv.pivot(
        index="activite", on="cpv_code", values="compte", aggregate_function=None
    )
    df_occurences_cpv = df_occurences_cpv.fill_null(0)

    # Extraire les NAF et CPVliste_activite
    liste_activite = df_occurences_cpv["activite"]
    cpv_occurrence_only = df_occurences_cpv.drop("activite")
    cpv_occurrence_only_np = cpv_occurrence_only.to_numpy()

    # Normalisation par ligne (somme = 1) et DataFrame de probabilités
    row_sums = cpv_occurrence_only_np.sum(axis=1, keepdims=True)
    prob_matrix = cpv_occurrence_only_np / row_sums  # Probabilité conditionnelle
    df_proba = pl.DataFrame(
        prob_matrix,
        schema=[str(col) for col in cpv_occurrence_only.columns],  # CPV comme colonnes
    ).with_columns(liste_activite)
    df_proba = df_proba.select(cs.by_name("activite"), cs.exclude("activite"))

    # (en option) Matrice de similarité
    # similarity_matrix = cosine_similarity(prob_matrix)
    # df_similarity = pl.DataFrame(
    #     similarity_matrix,
    #     schema=liste_activite.to_list()
    # ).with_columns(pl.Series("activite", liste_activite))

    # Formatage des résultats en tableau utilisable
    results = []
    cpv_cols = df_proba.select(
        ~cs.by_name("activite")
    ).columns  # Tous les CPV (hors 'activite')

    for row in df_proba.iter_rows(named=True):
        activite = row["activite"]
        scores = {cpv: row[cpv] for cpv in cpv_cols}
        # Trier par score décroissant
        for cpv, score in scores.items():
            results.append({"activite": activite, "cpv": cpv, "score": score})

    df_results = pl.DataFrame(results)
    df_results = df_results.with_columns(
        pl.col("score")
        .rank(method="dense", descending=True)
        .over("activite")
        .alias("rank")
    )
    df_results = df_results.filter(pl.col("rank") <= 10)
    df_results = df_results.filter(pl.col("score") > 0)
    df_results = df_results.sort(by=["activite", "score"], descending=[False, True])
    df_results = (
        df_results.with_columns(
            pl.col("activite").str.split("__").list[0].alias("activite_nomenclature")
        )
        .with_columns(pl.col("activite").str.split("__").list[1].alias("activite_code"))
        .drop("activite")
    )
    df_results = df_results.select(
        cs.starts_with("activite"), ~cs.starts_with("activite")
    )

    # Nombre de marchés par CPV
    df_results = df_results.join(
        cpv_naf_counts.collect(),
        left_on=["cpv", "activite_code", "activite_nomenclature"],
        right_on=["cpv_code", "activite_code", "activite_nomenclature"],
        how="left",
    )

    save_to_files(df_results, DIST_DIR / "probabilites_naf_cpv", "csv")


def add_duree_restante(lff: pl.LazyFrame):
    today = datetime.now().date()
    duree_mois_days_int = pl.col("dureeMois") * 30.5
    end_date = pl.col("dateNotification") + pl.duration(days=duree_mois_days_int)
    duree_restante_mois = ((end_date - today).dt.total_days() / 30).round(1)

    # Pas de valeurs négatives.
    lff = lff.with_columns(
        pl.when(duree_restante_mois < 0)
        .then(pl.lit(0))
        .otherwise(duree_restante_mois)
        .alias("dureeRestanteMois")
    )

    # Si dureeRestanteMois > dureeMois, dureeRestanteMois = dureeMois
    lff = lff.with_columns(
        pl.when(pl.col("dureeRestanteMois") > pl.col("dureeMois"))
        .then(pl.col("dureeMois").cast(pl.Float32))
        .otherwise(pl.col("dureeRestanteMois"))
        .alias("dureeRestanteMois")
    )
    return lff


#
# ⬇️⬇️⬇️ Fonctions à refactorer avec Polars et le format DECP 2022 ⬇️⬇️⬇️
#


# def make_acheteur_nom(decp_acheteurs_df: pl.LazyFrame):
#     # Construction du champ acheteur_id
#
#     from numpy import nan as NaN
#
#     def construct_nom(row):
#         if row["enseigne1Etablissement"] is NaN:
#             return row["denominationUniteLegale"]
#         else:
#             return f"{row['denominationUniteLegale']} - {row['enseigne1Etablissement']}"
#
#     decp_acheteurs_df["acheteur_id"] = decp_acheteurs_df.apply(construct_nom, axis=1)
#
#
#     return decp_acheteurs_df
#
#
# def improve_titulaire_unite_legale_data(df_sirets_titulaires: pl.DataFrame):
#     # Raccourcissement du code commune
#     df_sirets_titulaires["departement"] = df_sirets_titulaires[
#         "codeCommuneEtablissement"
#     ].str[:2]
#     df_sirets_titulaires = df_sirets_titulaires.drop(
#         columns=["codeCommuneEtablissement"]
#     )
#
#     # # Raccourcissement de l'activité principale
#     # pas sûr de pourquoi je voulais raccourcir le code NAF/APE. Pour récupérérer des libellés ?
#     # decp_titulaires_sirets_df['activitePrincipaleEtablissement'] = decp_titulaires_sirets_df['activitePrincipaleEtablissement'].str[:-3]
#
#     # Correction des données ESS et état
#     df_sirets_titulaires["etatAdministratifUniteLegale"] = df_sirets_titulaires[
#         "etatAdministratifUniteLegale"
#     ].cat.rename_categories({"A": "Active", "C": "Cessée"})
#     df_sirets_titulaires["economieSocialeSolidaireUniteLegale"] = df_sirets_titulaires[
#         "economieSocialeSolidaireUniteLegale"
#     ].replace({"O": "Oui", "N": "Non"})
#
#     df_sirets_titulaires = improve_categories_juridiques(df_sirets_titulaires)
#
#     return df_sirets_titulaires
#
#
# def improve_categories_juridiques(df_sirets_titulaires: pl.DataFrame):
#     # Récupération et raccourcissement des categories juridiques du fichier SIREN
#     df_sirets_titulaires["categorieJuridiqueUniteLegale"] = (
#         df_sirets_titulaires["categorieJuridiqueUniteLegale"].astype(str).str[:2]
#     )
#
#     # Récupération des libellés des catégories juridiques
#     cj_df = pl.read_csv(DATA_DIR / "cj.csv", index_col=None, dtype="object")
#     df_sirets_titulaires = pl.merge(
#         df_sirets_titulaires,
#         cj_df,
#         how="left",
#         left_on="categorieJuridiqueUniteLegale",
#         right_on="Code",
#     )
#     df_sirets_titulaires["categorieJuridique"] = df_sirets_titulaires["Libellé"]
#     df_sirets_titulaires = df_sirets_titulaires.drop(
#         columns=["Code", "categorieJuridiqueUniteLegale", "Libellé"]
#     )
#     return df_sirets_titulaires
#
#
# def rename_titulaire_sirene_columns(df_sirets_titulaires: pl.DataFrame):
#     # Renommage des colonnes
#
#     renaming = {
#         "activitePrincipaleEtablissement": "codeAPE",
#         "etatAdministratifUniteLegale": "etatEntreprise",
#         "etatAdministratifEtablissement": "etatEtablissement",
#     }
#
#     df_sirets_titulaires = df_sirets_titulaires.rename(columns=renaming)
#
#     return df_sirets_titulaires
