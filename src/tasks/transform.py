from datetime import datetime

import polars as pl
import polars.selectors as cs

from src.config import DATA_DIR, DIST_DIR, DecpFormat
from src.tasks.output import save_to_files


def explode_titulaires(lf: pl.LazyFrame, decp_format: DecpFormat):
    # Explosion des champs titulaires sur plusieurs lignes (un titulaire de marché par ligne)
    # et une colonne par champ

    # Structure originale 2022
    # [{"titulaire": {"id": "abc", "typeIdentifiant": "SIRET"}}]
    # mais simpliée dans filter_titulaires() (équivalent format 2019)
    # [{"id": "abc", "typeIdentifiant": "SIRET"}]

    # Explosion de la liste de titulaires en autant de nouvelles lignes
    lf = lf.explode("titulaires").unnest("titulaires")

    # Cast l'identifiant en string
    # lf = lf.with_columns(pl.col("titulaire_id").cast(pl.String))

    return lf


def replace_with_modification_data(lff: pl.LazyFrame):
    """
    Gère les modifications dans le DataFrame des DECP.
    À ce stade les modifications ont été exploded dans write_marche_rows().
    Cette fonction récupère les informations des modifications (ex : modification_montant) et les insère dans les champs de base (ex : montant).
    (chaque ligne contient les informations complètes à jour à la date de notification)
    Elle ajoute également la colonne "donneesActuelles" pour indiquer si la modification est la plus récente.
    """
    # Étape 1: Extraire les données des modifications en renommant les colonnes
    schema = lff.collect_schema().names()
    lf_mods = (
        lff.select(cs.by_name("uid") | cs.starts_with("modification_"))
        .rename(
            {
                column: column.removeprefix("modification_").removesuffix(
                    "Modification"
                )
                for column in schema
                if column.startswith("modification_") and column != "modification_id"
            }
        )
        .filter(~pl.all_horizontal(pl.all().exclude("uid").is_null()))
    )  # sans les lignes de données initiales

    # Étape 2: Dédupliquer et créer une copie du DataFrame initial sans les colonnes "modifications"
    # On peut dédupliquer aveuglément car la seule chose qui varient dans les lignes d'un même
    # uid, c'est les données de modifs
    lff = lff.unique("uid")

    # Garder TOUTES les colonnes sauf les colonnes modification_*
    lf_base = lff.drop(cs.starts_with("modification_"))

    # Étape 3: Ajouter le modification_id et la colonne données actuelles pour chaque modif
    # Colonnes qui peuvent changer avec les modifications
    modification_columns = [
        "uid",
        "dateNotification",
        "datePublicationDonnees",
        "montant",
        "dureeMois",
        "titulaires",
    ]

    lff = pl.concat(
        [
            lf_base.select(modification_columns),
            lf_mods,
        ],
        how="diagonal",
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
        pl.col("montant", "dureeMois", "titulaires")
        .fill_null(strategy="backward")
        .over("uid")
    )

    # Étape 5: Réintroduire les colonnes fixes (qui ne changent pas avec les modifications)
    # Sélectionner uniquement les colonnes qui ne sont pas dans modification_columns
    columns_to_keep = [
        col
        for col in lf_base.collect_schema().names()
        if col not in modification_columns
    ]

    # Créer un DataFrame avec uniquement les colonnes fixes, dédupliqué par uid
    lf_fixed_columns = lf_base.select(["uid"] + columns_to_keep).unique("uid")

    # Joindre pour réintroduire les colonnes fixes
    lf_final = lff.join(
        lf_fixed_columns,
        on="uid",
        how="left",
    )

    return lf_final


def process_modifications(lf: pl.LazyFrame) -> pl.LazyFrame:
    # Pas encore au point, risque de trop gros effets de bord
    # lf = remove_modifications_duplicates(lf)

    lf = replace_with_modification_data(lf)

    # Si il n'y avait aucun modifications dans le fichier, il faut ajouter les colonnes
    if "donneesActuelles" not in lf.collect_schema():
        lf = lf.with_columns(
            pl.lit(0).alias("modification_id"), pl.lit(True).alias("donneesActuelles")
        )

    return lf


def concat_decp_json(dfs: list) -> pl.DataFrame:
    df = pl.concat(dfs, how="diagonal_relaxed")

    del dfs

    print(
        "Suppression des lignes en doublon par UID + titulaire ID + titulaire type ID + modification_id"
    )

    # Exemple de doublon : 20005584600014157140791205100
    index_size_before = df.height
    df_clean = df.unique(
        subset=["uid", "titulaire_id", "titulaire_typeIdentifiant", "modification_id"],
        maintain_order=False,
    )

    del df

    print("-- ", index_size_before - df_clean.height, " doublons supprimés")

    return df_clean


def extract_unique_acheteurs_siret(lf: pl.LazyFrame):
    # Extraction des SIRET des DECP dans une copie du df de base
    lf = lf.select("acheteur_id")
    lf = lf.unique()
    lf = lf.sort(by="acheteur_id")
    return lf


def extract_unique_titulaires_siret(lf: pl.LazyFrame):
    # Extraction des SIRET des DECP dans une copie du df de base
    lf = lf.select("titulaire_id", "titulaire_typeIdentifiant")
    lf = lf.unique().filter(
        pl.col("titulaire_id") != "", pl.col("titulaire_typeIdentifiant") == "SIRET"
    )
    lf = lf.sort(by="titulaire_id")
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
            ).alias("enseigne1Etablissement"),
        ]
    )
    lff = lff.drop("denominationUsuelleEtablissement")
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


def sort_columns(df: pl.DataFrame, config_columns):
    # Les colonnes présentes mais absentes des colonnes attendues sont mises à la fin de la liste
    other_columns = []
    for col in df.columns:
        if col not in config_columns:
            other_columns.append(col)

    print("Colonnes inattendues:", other_columns)

    return df.select(config_columns + other_columns)


def calculate_naf_cpv_matching(df: pl.DataFrame):
    lf_naf_cpv = df.lazy()

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
    df_occurences_cpv = lf_naf_cpv.collect()
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
#     # TODO: ne garder que les colonnes acheteur_id et acheteur_nom
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
