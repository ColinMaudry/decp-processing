import datetime
import re

import polars as pl

from config import MARCHES_BAD_MONTANT, DecpFormat
from tasks.transform import (
    explode_titulaires,
    process_modifications,
    process_string_lists,
)


def clean_decp(lf: pl.LazyFrame, decp_format: DecpFormat) -> pl.LazyFrame:
    #
    # CLEAN DATA
    #

    # Si format 2019 : parfois c'est "acheteur.id": ..., parfois c'est "acheteur": {id : ...}
    if decp_format.label == "DECP 2019":
        lf = lf.with_columns(pl.coalesce("acheteur_id", "acheteur.id"))
        lf = lf.drop("acheteur.id")

    # Suppression des marchés qui n'ont pas d'id ou d'acheteur_id
    lf = lf.filter(pl.col("id").is_not_null() & pl.col("acheteur_id").is_not_null())

    # Nettoyage des identifiants de marchés
    lf = lf.with_columns(pl.col("id").str.replace_all(r"[ ,\\./]", "_"))

    # Ajout du champ uid
    # TODO: à déplacer autre part, dans transform
    lf = lf.with_columns((pl.col("acheteur_id") + pl.col("id")).alias("uid"))

    # Montants
    # Certains marchés ont des montants qui posent problème, donc on les met à 1 euro
    # ex : 221300015002472020F00075, 1.0E17
    lf = lf.with_columns(
        pl.when(pl.col("uid").is_in(MARCHES_BAD_MONTANT))
        .then(pl.lit(1))
        .otherwise(pl.col("montant"))
        .alias("montant")
    )

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

    # Champs liste
    lf = process_string_lists(lf)

    # Valeurs équivalentes à null transformées en null
    lf = clean_null_equivalent(lf)

    # Normalisation des titulaires
    lf = clean_titulaires(lf)

    # Application des modifications
    lf = process_modifications(lf)

    # Explosion des titulaires
    lf = explode_titulaires(lf, decp_format)

    # Remplacement des "" par null
    lf = lf.with_columns(
        pl.when(pl.col(pl.String).str.len_chars() == 0)
        .then(None)
        .otherwise(pl.col(pl.String))
        .name.keep()
    )

    # Type identifiant = SIRET si vide (marches-securises.fr)
    lf = lf.with_columns(
        pl.when(
            (pl.col("titulaire_typeIdentifiant").is_null())
            & (pl.col("titulaire_id").str.len_chars() == 14)
        )
        .then(pl.lit("SIRET"))
        .otherwise(pl.col("titulaire_typeIdentifiant"))
        .alias("titulaire_typeIdentifiant")
    )

    # NC
    lf = lf.with_columns(pl.col(pl.Utf8).replace("NC", None))

    # Correction des datatypes
    lf = fix_data_types(lf)

    return lf


def extract_innermost_struct(x):
    """Récupération des structs présents dans un nombre inconnu de listes imbriquées.
    Uniquement utilisé pour decp-2019.json du MINEF."""
    while isinstance(x, list):
        if len(x) == 0 or isinstance(x[0], dict):
            return x
        else:
            x = x[0]  # On ouvre la liste suivante
    return None  # fallback


def clean_invalid_characters(chunk: bytes):
    """Supprime les "ASCII control characters", caractères invalides en XML."""
    chunk = chunk.decode("utf-8")
    chunk = re.sub(r"[\x00-\x08\x0B\x0C\x0E-\x1F\x7F]", "", chunk)
    return bytes(chunk, "utf-8")


def clean_null_equivalent(lf: pl.LazyFrame) -> pl.LazyFrame:
    """Supprime les strings équivalente à null"""
    mapping_null = {
        "considerationsSociales": "Pas de considération sociale",
        "considerationsEnvironnementales": "Pas de considération environnementale",
        # "modalitesExecution": "Sans objet",
        "ccag": "Pas de CCAG",
        "typeGroupement": "Pas de groupement",
    }

    columns = lf.collect_schema().names()

    lf = lf.with_columns(
        [
            pl.when(pl.col(col_name) == pl.lit(mapping_null[col_name]))
            .then(pl.lit("Sans objet"))
            .otherwise(pl.col(col_name))
            .name.keep()
            for col_name in mapping_null
            if col_name in columns
        ]
    )

    return lf


def clean_titulaires(lf: pl.LazyFrame) -> pl.LazyFrame:
    # Étape 1: Remplacer les listes de titulaires "vides" (contenant uniquement des structs avec des nulls) par null
    # Car Polars ne considère pas [{{null,null}}] comme une valeur null lors du fill_null()
    # Structure: List(Struct({'titulaire': Struct({'typeIdentifiant': String, 'id': String})}))
    def filter_titulaires(titulaires_list):
        """
        Filter a list of titulaire objects:
        - If object has 'titulaire' key → check id/typeIdentifiant inside it.
        - Else → check id/typeIdentifiant directly.
        - Keep only if at least one of id or typeIdentifiant is NOT null.
        - Return None if the resulting list is empty.
        """
        # if not isinstance(titulaires_list, list):
        #     print("not a list")
        #     return []

        valid_items = []
        for item in titulaires_list:
            new_item = {}
            # Extract id and typeIdentifiant
            if isinstance(item, dict):
                if "titulaire" in item and isinstance(item["titulaire"], dict):
                    item = item["titulaire"]

                if isinstance(item, dict) and "id" in item and item["id"] is not None:
                    new_item["titulaire_id"] = item.get("id")
                    new_item["titulaire_typeIdentifiant"] = item.get("typeIdentifiant")
                else:
                    continue
                # Keep if at least one is NOT null
                if (
                    new_item["titulaire_id"] is not None
                    or new_item["titulaire_typeIdentifiant"] is not None
                ):
                    valid_items.append(new_item)

        # Return an empty list if no valid items left
        return valid_items

    # Apply to your Polars DataFrame
    lf = lf.with_columns(
        pl.col("titulaires")
        .map_elements(
            filter_titulaires,
            return_dtype=pl.List(
                pl.Struct(
                    {"titulaire_id": pl.String, "titulaire_typeIdentifiant": pl.String}
                )
            ),
        )
        .alias("titulaires"),
        pl.col("modification_titulaires")
        .map_elements(
            filter_titulaires,
            return_dtype=pl.List(
                pl.Struct(
                    {"titulaire_id": pl.String, "titulaire_typeIdentifiant": pl.String}
                )
            ),
        )
        .alias("modification_titulaires"),
    )

    # Remplacer les listes de titulaires vides par null (filter_titulaires() ne peut retourner None)
    lf = lf.with_columns(
        [
            pl.when(pl.col(col).list.len() == 0)
            .then(None)
            .otherwise(pl.col(col))
            .alias(col)
            for col in ["titulaires", "modification_titulaires"]
        ]
    )

    return lf


def fix_data_types(lf: pl.LazyFrame) -> pl.LazyFrame:
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
    lf = lf.with_columns(
        [
            pl.when(pl.col(col).str.to_lowercase().is_in(["true", "1", "oui"]))
            .then(True)
            .when(pl.col(col).str.to_lowercase().is_in(["false", "0", "non"]))
            .then(False)
            .otherwise(None)
            .name.keep()
            for col in cols
        ]
    )
    return lf
