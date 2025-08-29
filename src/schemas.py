import polars as pl

SCHEMA_TITULAIRE_2022 = pl.Struct(
    {
        "titulaire": pl.Struct(
            {
                "typeIdentifiant": pl.String,
                "id": pl.String,
            }
        )
    }
)

SCHEMA_MODIFICATION_2022 = pl.Struct(
    {
        "modification": pl.Struct(
            {
                "id": pl.String,
                # can switch down to UInt8 when https://github.com/pola-rs/polars/pull/16105 is merged
                "dateNotificationModification": pl.String,
                "datePublicationDonneesModification": pl.String,
                "montant": pl.String,
                "dureeMois": pl.String,
                "titulaires": pl.List(SCHEMA_TITULAIRE_2022),
            }
        )
    }
)

MODIFICATION_SCHEMA_PLAT_2022 = {
    "modification.id": pl.String,  # can switch down to UInt8 when https://github.com/pola-rs/polars/pull/16105 is merged
    "modification.dateNotificationModification": pl.String,
    "modification.datePublicationDonneesModification": pl.String,
    "modification.montant": pl.String,
    "modification.dureeMois": pl.String,
    "modification.titulaires.typeIdentifiant": pl.String,
    "modification.titulaires.id": pl.String,
}


SCHEMA_MARCHE_BASE = {
    "procedure": pl.String,
    "nature": pl.String,
    "codeCPV": pl.String,
    "dureeMois": pl.String,
    "datePublicationDonnees": pl.String,
    "titulaires": pl.List(SCHEMA_TITULAIRE_2022),
    "modifications": pl.List(SCHEMA_MODIFICATION_2022),
    "id": pl.String,
    "formePrix": pl.String,
    "dateNotification": pl.String,
    "objet": pl.String,
    "montant": pl.String,
    "acheteur_id": pl.String,
    "source": pl.String,
    "lieuExecution_code": pl.String,
    "lieuExecution_typeCode": pl.String,
    "uid": pl.String,
    "marcheInnovant": pl.String,
    "attributionAvance": pl.String,
    "sousTraitanceDeclaree": pl.String,
    "ccag": pl.String,
    "typePrix": pl.String,
    "offresRecues": pl.String,
    "typeGroupementOperateurs": pl.String,
    "idAccordCadre": pl.String,
    "TypePrix": pl.String,
    "tauxAvance": pl.String,
    "origineUE": pl.String,
    "origineFrance": pl.String,
    "typesPrix": pl.List(pl.String),
    "typesPrix_typePrix": pl.List(pl.String),
    "modaliteExecution": pl.List(pl.String),
    "techniques_technique": pl.List(pl.String),
    # Présents dans peu de marchés et peu pertinents :
    # "created_at": pl.String,
    # "term.acheteur.id": pl.String,
    # "updated_at": pl.String,
    # "uuid": pl.String,
    # On ne traite que les marchés publics pour l'instant, pas les concessions, donc non pertinent :
    # "_type": pl.String,
}

# SCHEMA_MARCHE_2019 = {
#     **SCHEMA_MARCHE_BASE,
#     "titulaires": pl.List(SCHEMA_TITULAIRE_2019),
#     "considerationsSociales": pl.List(pl.String),
#     "considerationsEnvironnementales": pl.List(pl.String),
#     **SCHEMA_MODIFICATION_2019,
# }

SCHEMA_MARCHE_2022 = {
    **SCHEMA_MARCHE_BASE,
    "titulaires": pl.List(SCHEMA_TITULAIRE_2022),
    "considerationsSociales_considerationSociale": pl.List(pl.String),
    "considerationsEnvironnementales_considerationEnvironnementale": pl.List(pl.String),
    **SCHEMA_MODIFICATION_2022,
}
