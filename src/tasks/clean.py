import datetime
import re

import polars as pl
from polars import selectors as cs

from src.config import DecpFormat
from src.tasks.transform import (
    apply_modifications,
)


def clean_decp(lf: pl.LazyFrame, decp_format: DecpFormat) -> pl.LazyFrame:
    """
    The bulk of Polars data cleaning is grouped here, with the exception of process_modifications and explode_titulaires that are not
    cleaning tasks.
    :param lf:
    :param decp_format:
    :return:
    """
    #
    # CLEAN DATA
    #

    # Si format 2019 : parfois c'est "acheteur.id": ..., parfois c'est "acheteur": {id : ...}
    if decp_format.label == "DECP 2019":
        lf = lf.with_columns(pl.coalesce("acheteur_id", "acheteur.id"))
        lf = lf.drop("acheteur.id")

    # Suppression des marchés qui n'ont pas d'id ou d'acheteur_id
    # Remplacement des "" par null pour les id et acheteur_id
    lf = lf.with_columns(
        [
            pl.when(pl.col(col).str.len_chars() == 0)
            .then(None)
            .otherwise(pl.col(col))
            .alias(col)
            for col in ["id", "acheteur_id"]
        ]
    )
    lf = lf.filter(pl.col("id").is_not_null() & pl.col("acheteur_id").is_not_null())

    # Nettoyage des identifiants de marchés
    lf = lf.with_columns(pl.col("id").str.replace_all(r"[ ,\\./]", "_"))

    # Ajout du champ uid
    # TODO: à déplacer autre part, dans transform
    lf = lf.with_columns((pl.col("acheteur_id") + pl.col("id")).alias("uid"))

    # Normalisation des titulaires
    # Cela permet de s'assurer que les titulaires mal formés ne vont pas être appliqués à d'autres marchés
    for column in ["titulaires", "modification_titulaires"]:
        lf = clean_titulaires(lf, decp_format, column)

    # Application des modifications
    # le plus tôt possible pour que les fonctions suivantes clean les
    # champs modifiés (dateNotification, datePublicationDonnnes, montant, titulaires, dureeMois)
    lf = apply_modifications(lf)

    # Explosion des titulaires
    lf = lf.explode("titulaires").unnest("titulaires")

    # Montants
    # Certains marchés ont des montants qui posent problème, donc on les met à 12,311111111 milliards (pour les retrouver facilement)
    # ex : 221300015002472020F00075, 1.0E17
    lf = lf.with_columns(
        pl.when(pl.col("montant").str.split(".").list.get(0).str.len_chars() > 11)
        .then(pl.lit("12311111111"))
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
        .name.keep()
    )

    # suppression des suffixes de fuseau horaire
    lf = lf.with_columns(
        pl.col(["datePublicationDonnees", "dateNotification"])
        .str.split("+")
        .list[0]
        .name.keep()
    )

    # Nature
    lf = lf.with_columns(
        pl.col("nature")
        .str.replace_many({"Marche": "Marché", "subsequent": "subséquent"})
        .alias("nature")
    )

    # Codes CPV, suppression du caractères de contrôle ("-[0-9]$")
    lf = lf.with_columns(pl.col("codeCPV").str.split("-").list[0].alias("codeCPV"))

    # Champs liste
    lf = process_string_lists(lf)

    # Valeurs équivalentes à null transformées en null
    lf = clean_null_equivalent(lf)

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


def clean_titulaires(lf: pl.LazyFrame, decp_format: DecpFormat, column) -> pl.LazyFrame:
    """
    Normalise les listes de titulaires en utilisant des expressions Polars natives.
    `column` peut être titulaires ou modification_titulaires
    Codée avec l'assistance du LLM Gemini 3 Pro et révisée par le développeur.
    """
    # Définition des expressions de nettoyage selon le format
    if decp_format.label == "DECP 2022":
        # Format 2022 : [{"titulaire": {"id": ..., "typeIdentifiant": ...}}]
        # On veut extraire les champs de la struct imbriquée
        expr_titulaire = pl.struct(
            titulaire_id=pl.element().struct.field("titulaire").struct.field("id"),
            titulaire_typeIdentifiant=pl.element()
            .struct.field("titulaire")
            .struct.field("typeIdentifiant"),
        )

    else:
        # Format 2019 : [{"id": ..., "typeIdentifiant": ...}]
        # On renomme juste les champs
        expr_titulaire = pl.struct(
            titulaire_id=pl.element().struct.field("id"),
            titulaire_typeIdentifiant=pl.element().struct.field("typeIdentifiant"),
        )

    # Skip processing if column dtype is Null (all values are null)
    # This happens when modification_titulaires has no actual data
    if lf.collect_schema()[column] != pl.Null:
        lf = lf.with_columns(
            pl.col(column)
            .list.eval(expr_titulaire)
            .list.eval(
                # Filtrer les éléments où id ET typeIdentifiant sont null
                pl.element().filter(
                    pl.element().struct.field("titulaire_id").is_not_null()
                    | pl.element()
                    .struct.field("titulaire_typeIdentifiant")
                    .is_not_null()
                )
            )
            .alias(column)
        )

    # Remplacer les listes de titulaires vides par null
    # Only process columns that have List dtype

    if lf.collect_schema()[column] != pl.Null:
        lf = lf.with_columns(
            [
                pl.when(pl.col(column).list.len() == 0)
                .then(None)
                .otherwise(pl.col(column))
                .alias(column)
            ]
        )

    return lf


def fix_data_types(lf: pl.LazyFrame) -> pl.LazyFrame:
    """
    To enable easier data ingestion, everything is initially cast as strings... until here. This
    function casts the right datatype for each column.
    :param lf:
    :return:
    """
    numeric_dtypes = {
        "dureeMois": pl.Int16,
        # "dureeMoisActeSousTraitance": pl.Int16,
        # "dureeMoisModificationActeSousTraitance": pl.Int16,
        "offresRecues": pl.Int16,
        "montant": pl.Float64,
        # "montantActeSousTraitance": pl.Float64,
        # "montantModificationActeSousTraitance": pl.Float64,
        "tauxAvance": pl.Float32,
        # "variationPrixActeSousTraitance": pl.Float64,
        "origineFrance": pl.Float32,
        "origineUE": pl.Float32,
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
                pl.when(pl.col(new_col).list.len() == 0)
                .then(pl.col(bad_col))
                .otherwise(pl.col(new_col))
                .alias(new_col)
            )
            lf = lf.drop(bad_col)
        elif new_col not in columns and bad_col in columns:
            lf = lf.rename({bad_col: new_col})

    # Et on remplace la liste Python par une liste séparée par des virgules
    lf = lf.with_columns(cs.by_dtype(pl.List(pl.String)).list.join(", ").name.keep())

    return lf
