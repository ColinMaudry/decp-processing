from pathlib import Path

import polars as pl
from prefect import task

from config import DIST_DIR
from tasks.output import save_to_files
from tasks.transform import explode_titulaires


@task
def clean_decp_json(files: list[Path]):
    return_files = []
    for file in files:
        #
        # CLEAN DATA
        #

        lf: pl.LazyFrame = pl.scan_parquet(f"{file}.parquet")

        # Explosion des titulaires
        print("Explode titulaires...")
        lf = explode_titulaires(lf)

        # Colonnes exclues pour l'instant
        # lf = df.rename({
        #     "typesPrix_typePrix": "typesPrix",
        #     "considerationsEnvironnementales_considerationEnvironnementale": "considerationsEnvironnementales",
        #     "considerationsSociales_considerationSociale": "considerationsSociales",
        #     "techniques_technique": "techniques",
        #     "modalitesExecution_modaliteExecution": "modalitesExecution"
        # })

        # Remplacement des valeurs nulles
        lf = lf.with_columns(pl.col(pl.String).replace("NC", None))
        # Nettoyage des identifiants de marchés
        lf = lf.with_columns(pl.col("id").str.replace_all(r"[ ,\\./]", "_"))

        # Ajout du champ uid
        # TODO: à déplacer autre part, dans transform
        lf = lf.with_columns((pl.col("acheteur_id") + pl.col("id")).alias("uid"))

        # Suppression des lignes en doublon par UID (acheteur id + id)
        # Exemple : 20005584600014157140791205100
        # index_size_before = df.height
        # df = df.unique(subset=["uid"], maintain_order=False)
        # print("-- ", index_size_before - df.height, " doublons supprimés (uid)")

        # Dates
        date_replacements = {
            # ID marché invalide et SIRET de l'acheteur
            "0002-11-30": "",
            "September, 16 2021 00:00:00": "2021-09-16",  # 2000769
            # 5800012 19830766200017 (plein !)
            "16 2021 00:00:00": "",
            "0222-04-29": "2022-04-29",  # 202201L0100
            "0021-12-05": "2022-12-05",  # 20222022/1400
            "0001-06-21": "",  # 0000000000000000 21850109600018
            "0019-10-18": "",  # 0000000000000000 34857909500012
            "5021-02-18": "2021-02-18",  # 20213051200 21590015000016
            "2921-11-19": "",  # 20220057201 20005226400013
            "0022-04-29": "2022-04-29",  # 2022AOO-GASL0100 25640454200035
        }

        # Using replace_many for efficient replacement of multiple date values
        lf = lf.with_columns(
            pl.col(["datePublicationDonnees", "dateNotification"])
            .str.replace_many(date_replacements)
            .cast(pl.Utf8)
        )

        # Nature
        lf = lf.with_columns(
            pl.col("nature").str.replace_many(
                {"Marche": "Marché", "subsequent": "subséquent"}
            )
        )

        # Fix datatypes
        lf = fix_data_types(lf)

        output_file = DIST_DIR / "clean" / file.name
        return_files.append(output_file)
        output_file.parent.mkdir(exist_ok=True)

        df: pl.DataFrame = lf.collect()
        save_to_files(df, output_file, ["parquet"])

    return return_files


def fix_data_types(df: pl.LazyFrame):
    numeric_dtypes = {
        "dureeMois": pl.Int16,
        # "dureeMoisModification": pl.Int16,
        # "dureeMoisActeSousTraitance": pl.Int16,
        # "dureeMoisModificationActeSousTraitance": pl.Int16,
        "offresRecues": pl.Int16,
        "montant": pl.Float64,
        # "montantModification": pl.Float64,
        # "montantActeSousTraitance": pl.Float64,
        # "montantModificationActeSousTraitance": pl.Float64,
        "tauxAvance": pl.Float64,
        # "variationPrixActeSousTraitance": pl.Float64,
    }

    for column, dtype in numeric_dtypes.items():
        print("Fixing column", column, "...")
        # Les valeurs qui ne sont pas des chiffres sont converties en null
        df = df.with_columns(pl.col(column).cast(dtype, strict=False))

    # Convert date columns to datetime using str.strptime
    print("Fixing dates...")
    df = df.with_columns(
        # Les valeurs qui ne sont pas des dates sont converties en null
        pl.col(
            [
                "dateNotification",
                # "dateNotificationActeSousTraitance",
                # "dateNotificationModificationModification",
                # "dateNotificationModificationSousTraitanceModificationActeSousTraitance",
                "datePublicationDonnees",
                # "datePublicationDonneesActeSousTraitance",
                # "datePublicationDonneesModificationActeSousTraitance",
                # "datePublicationDonneesModificationModification",
            ]
        ).str.strptime(pl.Date, format="%Y-%m-%d", strict=False)
    )

    # Champs booléens

    # TODO: À améliorer
    # print("Fixing booleans...")
    # df = df.with_columns(
    #     pl.col(["sousTraitanceDeclaree", "attributionAvance", "marcheInnovant"]).eq(
    #         True
    #     )
    # )

    return df
