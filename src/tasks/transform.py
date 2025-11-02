from pathlib import Path

import polars as pl
import polars.selectors as cs
from prefect import task

from config import DATA_DIR, SIRENE_UNITES_LEGALES_URL, DecpFormat


def process_string_lists(lf: pl.LazyFrame):
    string_lists_col_to_rename = [
        "considerationsSociales_considerationSociale",
        "considerationsEnvironnementales_considerationEnvironnementale",
        "techniques_technique",
        "typesPrix_typePrix",
        "modalitesExecution_modaliteExecution",
    ]
    columns = lf.collect_schema().names()

    # Pour s'assurer qu'on renomme pas une colonne si le bon nom de colonne existe déjà
    for bad_col in string_lists_col_to_rename:
        new_col = bad_col.split("_")[0]
        if new_col in columns and bad_col in columns:
            lf = lf.with_columns(
                pl.when(pl.col(new_col).len() == 0)
                .then(pl.col(bad_col))
                .otherwise(pl.col(new_col))
            )
            lf = lf.drop(bad_col)
        elif new_col not in columns and bad_col in columns:
            lf = lf.rename({bad_col: new_col})

    # Et on remplace la liste Python par une liste séparée par des virgules
    lf = lf.with_columns(cs.by_dtype(pl.List(pl.String)).list.join(", ").name.keep())

    return lf


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


def remove_modifications_duplicates(df):
    """On supprime les marches avec un suffixe correspondant à un autre marché"""
    if "modifications" not in df.collect_schema().names():
        return df
    # Index sans les suffixes
    df_cleaned = remove_suffixes_from_uid_column(df)
    df_cleaned = df_cleaned.with_columns(
        modifications_len=pl.col("modifications").list.len(),
    )

    df_cleaned = df_cleaned.sort("modifications_len").unique("uid", keep="last")
    return df_cleaned


def remove_suffixes_from_uid_column(df):
    """Supprimer les suffixes des uid quand ce suffixe correspond au nombre de mofifications apportées au marché.
    Exemple : uid = [acheteur_id]12302 et le marché a deux modifications. uid => [acheteur_id]123.
    """
    df = df.with_columns(
        expected_suffix=pl.col("modifications").list.len().cast(pl.Utf8).str.zfill(2)
    )
    df = df.with_columns(
        uid=pl.when(pl.col("uid").str.ends_with(pl.col("expected_suffix")))
        .then(pl.col("uid").str.head(-2))
        .otherwise(pl.col("uid"))
    )
    return df


def replace_with_modification_data(lf: pl.LazyFrame):
    """
    Gère les modifications dans le DataFrame des DECP.
    À ce stade les modifications ont été exploded dans write_marche_rows().
    Cette fonction récupère les informations des modifications (ex : modification_montant) et les insère dans les champs de base (ex : montant).
    (chaque ligne contient les informations complètes à jour à la date de notification)
    Elle ajoute également la colonne "donneesActuelles" pour indiquer si la modification est la plus récente.
    """
    # Étape 1: Extraire les données des modifications en renommant les colonnes
    schema = lf.collect_schema().names()
    lf_mods = (
        lf.select(cs.by_name("uid") | cs.starts_with("modification_"))
        .rename(
            {
                column: column.removeprefix("modification_").removesuffix(
                    "Modification"
                )
                for column in schema
                if column.startswith("modification_")
            }
        )
        .filter(~pl.all_horizontal(pl.all().exclude("uid").is_null()))
    )  # sans les lignes de données initiales

    # Étape 2: Dédupliquer et créer une copie du DataFrame initial sans les colonnes "modifications"
    # On peut dédupliquer aveuglément car la seule chose qui varient dans les lignes d'un même
    # uid, c'est les données de modifs
    lf = lf.unique("uid")

    # Garder TOUTES les colonnes sauf les colonnes modification_*
    lf_base = lf.drop(cs.starts_with("modification_"))

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

    lf = (
        pl.concat(
            [
                lf_base.select(modification_columns),
                lf_mods,
            ],
            how="vertical_relaxed",
        )
        .with_columns(
            pl.col("dateNotification")
            .rank(method="ordinal")
            .over("uid")
            .cast(pl.Int64)
            .sub(1)
            .alias("modification_id")
        )
        .with_columns(
            (
                pl.col("modification_id") == pl.col("modification_id").max().over("uid")
            ).alias("donneesActuelles")
        )
        .sort(
            ["uid", "dateNotification", "modification_id"],
            descending=[False, True, True],
        )
    )

    # Étape 4: Remplir les valeurs nulles en utilisant les dernières valeurs non-nulles pour chaque id
    lf = lf.with_columns(
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
    lf_final = lf.join(
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


@task
def get_prepare_unites_legales(processed_parquet_path):
    print("Téléchargement des données unité légales et sélection des colonnes...")
    (
        pl.scan_parquet(SIRENE_UNITES_LEGALES_URL)
        .select(["siren", "denominationUniteLegale"])
        .filter(pl.col("siren").is_not_null())
        .filter(pl.col("denominationUniteLegale").is_not_null())
        .unique()
        .sink_parquet(processed_parquet_path)
    )


def prepare_etablissements(lf: pl.LazyFrame, processed_parquet_path: Path) -> None:
    lf = lf.rename({"codeCommuneEtablissement": "commune_code"})

    # Ajout des noms de départements, noms régions,
    lf_cog = pl.scan_parquet(DATA_DIR / "code_officiel_geographique.parquet")
    lf = lf.join(lf_cog, on="commune_code", how="left")

    lf.sink_parquet(processed_parquet_path)


def sort_columns(df: pl.DataFrame, config_columns):
    # Les colonnes présentes mais absentes des colonnes attendues sont mises à la fin de la liste
    other_columns = []
    for col in df.columns:
        if col not in config_columns:
            other_columns.append(col)

    print("Colonnes inattendues:", other_columns)

    return df.select(config_columns + other_columns)


#
# ⬇️⬇️⬇️ Fonctions à refactorer avec Polars et le format DECP 2022 ⬇️⬇️⬇️
#


def make_acheteur_nom(decp_acheteurs_df: pl.LazyFrame):
    # Construction du champ acheteur_id

    from numpy import nan as NaN

    def construct_nom(row):
        if row["enseigne1Etablissement"] is NaN:
            return row["denominationUniteLegale"]
        else:
            return f"{row['denominationUniteLegale']} - {row['enseigne1Etablissement']}"

    decp_acheteurs_df["acheteur_id"] = decp_acheteurs_df.apply(construct_nom, axis=1)

    # TODO: ne garder que les colonnes acheteur_id et acheteur_id

    return decp_acheteurs_df


def improve_titulaire_unite_legale_data(df_sirets_titulaires: pl.DataFrame):
    # Raccourcissement du code commune
    df_sirets_titulaires["departement"] = df_sirets_titulaires[
        "codeCommuneEtablissement"
    ].str[:2]
    df_sirets_titulaires = df_sirets_titulaires.drop(
        columns=["codeCommuneEtablissement"]
    )

    # # Raccourcissement de l'activité principale
    # pas sûr de pourquoi je voulais raccourcir le code NAF/APE. Pour récupérérer des libellés ?
    # decp_titulaires_sirets_df['activitePrincipaleEtablissement'] = decp_titulaires_sirets_df['activitePrincipaleEtablissement'].str[:-3]

    # Correction des données ESS et état
    df_sirets_titulaires["etatAdministratifUniteLegale"] = df_sirets_titulaires[
        "etatAdministratifUniteLegale"
    ].cat.rename_categories({"A": "Active", "C": "Cessée"})
    df_sirets_titulaires["economieSocialeSolidaireUniteLegale"] = df_sirets_titulaires[
        "economieSocialeSolidaireUniteLegale"
    ].replace({"O": "Oui", "N": "Non"})

    df_sirets_titulaires = improve_categories_juridiques(df_sirets_titulaires)

    return df_sirets_titulaires


def improve_categories_juridiques(df_sirets_titulaires: pl.DataFrame):
    # Récupération et raccourcissement des categories juridiques du fichier SIREN
    df_sirets_titulaires["categorieJuridiqueUniteLegale"] = (
        df_sirets_titulaires["categorieJuridiqueUniteLegale"].astype(str).str[:2]
    )

    # Récupération des libellés des catégories juridiques
    cj_df = pl.read_csv(DATA_DIR / "cj.csv", index_col=None, dtype="object")
    df_sirets_titulaires = pl.merge(
        df_sirets_titulaires,
        cj_df,
        how="left",
        left_on="categorieJuridiqueUniteLegale",
        right_on="Code",
    )
    df_sirets_titulaires["categorieJuridique"] = df_sirets_titulaires["Libellé"]
    df_sirets_titulaires = df_sirets_titulaires.drop(
        columns=["Code", "categorieJuridiqueUniteLegale", "Libellé"]
    )
    return df_sirets_titulaires


def rename_titulaire_sirene_columns(df_sirets_titulaires: pl.DataFrame):
    # Renommage des colonnes

    renaming = {
        "activitePrincipaleEtablissement": "codeAPE",
        "etatAdministratifUniteLegale": "etatEntreprise",
        "etatAdministratifEtablissement": "etatEtablissement",
    }

    df_sirets_titulaires = df_sirets_titulaires.rename(columns=renaming)

    return df_sirets_titulaires
