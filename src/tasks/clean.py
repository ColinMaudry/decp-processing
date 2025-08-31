import datetime

import polars as pl
import polars.selectors as cs

from tasks.transform import explode_titulaires, process_modifications


def clean_decp(lf: pl.LazyFrame) -> pl.LazyFrame:
    #
    # CLEAN DATA
    #

    # Suppression des marchés qui n'ont pas d'id ou d'acheteur_id
    lf = lf.filter(pl.col("id").is_not_null() & pl.col("acheteur_id").is_not_null())

    # Nettoyage des identifiants de marchés
    lf = lf.with_columns(pl.col("id").str.replace_all(r"[ ,\\./]", "_"))

    # Ajout du champ uid
    # TODO: à déplacer autre part, dans transform
    lf = lf.with_columns((pl.col("acheteur_id") + pl.col("id")).alias("uid"))

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

    # Explosion et traitement des modifications
    lf = process_modifications(lf)

    # Explosion des titulaires
    lf = explode_titulaires(lf)

    # NC
    lf = lf.with_columns(pl.col(pl.Utf8).replace("NC", None))

    # Correction des datatypes
    lf = fix_data_types(lf)

    return lf


def fix_data_types(lf: pl.LazyFrame):
    numeric_dtypes = {
        "dureeMois": pl.Int16,
        # "dureeMoisActeSousTraitance": pl.Int16,
        # "dureeMoisModificationActeSousTraitance": pl.Int16,
        "offresRecues": pl.Int16,
        "montant": pl.Float64,
        # "montantActeSousTraitance": pl.Float64,
        # "montantModificationActeSousTraitance": pl.Float64,
        "tauxAvance": pl.Float64,
        # "variationPrixActeSousTraitance": pl.Float64,
        "origineFrance": pl.Float64,
        "origineUE": pl.Float64,
        "modification_id": pl.Int16,
    }

    # Champs numériques
    for column, dtype in numeric_dtypes.items():
        # Les valeurs qui ne sont pas des chiffres sont converties en null
        lf = lf.with_columns(pl.col(column).cast(dtype, strict=False))

    # Convert date columns to datetime using str.strptime
    dates_col = [
        "dateNotification",
        # "dateNotificationActeSousTraitance",
        # "dateNotificationModificationModification",
        # "dateNotificationModificationSousTraitanceModificationActeSousTraitance",
        "datePublicationDonnees",
        # "datePublicationDonneesActeSousTraitance",
        # "datePublicationDonneesModificationActeSousTraitance",
        # "datePublicationDonneesModificationModification",
    ]

    # Fix dates
    lf = lf.with_columns(
        # Les valeurs qui ne sont pas des dates sont converties en null
        pl.col(dates_col).str.strptime(pl.Date, format="%Y-%m-%d", strict=False)
    )

    # Suppression des dates dans le futur
    for col in dates_col:
        lf = lf.with_columns(
            pl.when(pl.col(col) > datetime.datetime.now())
            .then(None)
            .otherwise(pl.col(col))
            .alias(col)
        )

    # Champs booléens
    cols = ("sousTraitanceDeclaree", "attributionAvance", "marcheInnovant")
    str_cols = cs.by_name(cols) & cs.string()
    float_cols = cs.by_name(cols) & cs.float()
    lf = lf.with_columns(
        pl.when(str_cols.str.to_lowercase() == "true")
        .then(True)
        .when(str_cols.str.to_lowercase() == "false")
        .then(False)
        .otherwise(None)
        .name.keep()
    ).with_columns(float_cols.fill_nan(None).cast(pl.Boolean).name.keep())

    return lf
