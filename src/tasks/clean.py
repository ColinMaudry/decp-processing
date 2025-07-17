import datetime
import math

import polars as pl
import polars.selectors as cs
from prefect import task

from tasks.transform import explode_titulaires, process_modifications


@task
def clean_decp(lf: pl.LazyFrame) -> pl.LazyFrame:
    #
    # CLEAN DATA
    #

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

    # Correction des datatypes
    lf = fix_data_types(lf)

    return lf


def fix_data_types(lf: pl.LazyFrame):
    print("Correction des datatypes...")
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
        "origineFrance": pl.Float64,
        "origineUE": pl.Float64,
        "modification.id": pl.Int32,
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

    # Suppression dans dates dans le futur
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


def clean_decp_json_modifications(input_json_: dict):
    """
    Nettoyage des données JSON des DECP pour les modifications des titulaires.
    Suppression des données qui ne correspondent pas au format attendu (ex: {"typeIdentifiant": "SIRET", "id": "12345678901234"}).
    """
    clean_json = []
    titulaires_cleaned_cpt = 0
    for entry in input_json_:
        # entry = {} représentant un marché
        modifications_entries = entry.get("modifications", [])
        # modifications_entries = [] représentant les modifications du marché
        clean_modifications_entries = []
        for modification_entry in modifications_entries:
            # modification_entry = {} représentant une modification du marché
            modification_entry_clean = modification_entry["modification"]
            if "titulaires" in modification_entry_clean.keys():
                modification_titulaires_clean = []
                for modification_titulaire in modification_entry_clean.get(
                    "titulaires", []
                ):
                    # mofification_titulaire = {} représentant un titulaire de la modification
                    if modification_titulaire is not None and isinstance(
                        modification_titulaire["titulaire"], dict
                    ):
                        # Si le titulaire est un dictionnaire, on récupère l'id et le typeIdentifiant
                        modification_titulaires_clean.append(
                            {
                                "titulaire": {
                                    "typeIdentifiant": modification_titulaire[
                                        "titulaire"
                                    ].get("typeIdentifiant"),
                                    "id": modification_titulaire["titulaire"].get("id"),
                                }
                            }
                        )
                if modification_titulaires_clean:
                    modification_entry_clean["titulaires"] = (
                        modification_titulaires_clean
                    )
                else:
                    modification_entry_clean.pop("titulaires", None)
                    titulaires_cleaned_cpt += 1
            clean_modifications_entries.append(
                {"modification": modification_entry_clean}
            )
        entry["modifications"] = clean_modifications_entries
        clean_json.append(entry)
    # print(f"Nombre de titulaires nettoyés : {titulaires_cleaned_cpt}")
    return clean_json


def fix_nan_nc(obj):
    """Paroure tout le JSON pour remplacer NaN et NC par null."""
    if (isinstance(obj, float) and math.isnan(obj)) or obj == "NC":
        return None
    elif isinstance(obj, dict):
        return {k: fix_nan_nc(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [fix_nan_nc(item) for item in obj]
    return obj


def load_and_fix_json(decp_json: dict):
    json_data = decp_json["marches"]["marche"]

    # if type(json_data["marches"]):
    #     json_data = fix_nan_nc(json_data["marches"])
    # elif type(json_data["marches"]["marche"]) == list:
    #     json_data = fix_nan_nc(json_data["marches"]["marche"])
    # else:
    #     print("Structure de fichier JSON non reconnue")
    #     print(json_data)
    #     raise ValueError

    json_data = fix_nan_nc(json_data)

    json_data = clean_decp_json_modifications(json_data)

    # fixed_buffer = io.StringIO()
    # json.dump(json_data, fixed_buffer)
    # fixed_buffer.seek(0)  # rewind to beginning so it can be read
    return json_data
