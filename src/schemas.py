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


SCHEMA_TITULAIRE_2019 = pl.Struct(
    {
        "typeIdentifiant": pl.String,
        "id": pl.String,
    }
)

SCHEMA_MODIFICATION_BASE = {
    "modification.id": pl.Int32,  # can switch down to UInt8 when https://github.com/pola-rs/polars/pull/16105 is merged
    "modification.objetModification": pl.String,
    "modification.dateNotificationModification": pl.String,
    "modification.datePublicationDonneesModification": pl.String,
    "modification.typeIdentifiant": pl.String,
    "modification.montant": pl.String,
    "modification.dateSignatureModification": pl.String,
    "modification.dureeMois": pl.String,
}

SCHEMA_MODIFICATION_2022 = {
    **SCHEMA_MODIFICATION_BASE,
    "modification.titulaires": pl.List(SCHEMA_TITULAIRE_2022),
}

SCHEMA_MODIFICATION_2019 = {
    **SCHEMA_MODIFICATION_BASE,
    "modification.titulaires": pl.List(SCHEMA_TITULAIRE_2019),
}


SCHEMA_MARCHE_BASE = {
    "procedure": pl.String,
    "nature": pl.String,
    "codeCPV": pl.String,
    "dureeMois": pl.String,
    "datePublicationDonnees": pl.String,
    "id": pl.String,
    "formePrix": pl.String,
    "dateNotification": pl.String,
    "objet": pl.String,
    "montant": pl.String,
    "acheteur.id": pl.String,
    "source": pl.String,
    "lieuExecution.code": pl.String,
    "lieuExecution.typeCode": pl.String,
    "_type": pl.String,
    "uid": pl.String,
    "uuid": pl.String,
    # "modaliteExecution": pl.List(pl.String),
    "marcheInnovant": pl.Boolean,
    "attributionAvance": pl.Boolean,
    "sousTraitanceDeclaree": pl.Boolean,
    "ccag": pl.String,
    "offresRecues": pl.String,
    "typeGroupementOperateurs": pl.String,
    "idAccordCadre": pl.String,
    # "technique": pl.List(pl.String),
    "TypePrix": pl.String,
    "tauxAvance": pl.String,
    "origineUE": pl.String,
    "origineFrance": pl.String,
    "created_at": pl.String,
    "typesPrix": pl.List(pl.String),
    "typePrix": pl.String,
    "term.acheteur.id": pl.String,
    "updated_at": pl.String,
}

SCHEMA_MARCHE_2019 = {
    **SCHEMA_MARCHE_BASE,
    "titulaires": pl.List(SCHEMA_TITULAIRE_2019),
    "considerationsSociales": pl.List(pl.String),
    "considerationsEnvironnementales": pl.List(pl.String),
    **SCHEMA_MODIFICATION_2019,
}

SCHEMA_MARCHE_2022 = {
    **SCHEMA_MARCHE_BASE,
    "titulaires": pl.List(SCHEMA_TITULAIRE_2022),
    "considerationsSociales.considerationsSociale": pl.List(pl.String),
    "considerationsEnvironnementales.considerationsEnvironnementale": pl.List(
        pl.String
    ),
    **SCHEMA_MODIFICATION_2022,
}
