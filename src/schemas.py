import polars as pl

# Rappel : avec pl.scan_ndjson() (pl.DataFrame([some data]) fonctionne différemment) :
# - si le champ en entrée est dans le schéma, il est ingéré (erreur au moment du collect() si mismatch de dtype)
# - si le champ en entrée n'est pas dans le schéma, il est ignoré
# - si un champ est présent dans le schéma mais absent en entrée, il est ajouté mais null

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
    "modification_id": pl.Int32,  # can switch down to UInt8 when https://github.com/pola-rs/polars/pull/16105 is merged
    "modification_dateNotificationModification": pl.String,
    "modification_datePublicationDonneesModification": pl.String,
    "modification_typeIdentifiant": pl.String,
    "modification_montant": pl.String,
    "modification_dureeMois": pl.String,
}

SCHEMA_MODIFICATION_2022 = {
    **SCHEMA_MODIFICATION_BASE,
    "modification_titulaires": pl.List(SCHEMA_TITULAIRE_2022),
}

SCHEMA_MODIFICATION_2019 = {
    **SCHEMA_MODIFICATION_BASE,
    "modification_titulaires": pl.List(SCHEMA_TITULAIRE_2019),
    # TODO ajouter la gestion de ces champs
    # "modification_objetModification": pl.String,
    # "modification_dateSignatureModification": pl.String,
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
    "acheteur_id": pl.String,
    "source": pl.String,
    "lieuExecution_code": pl.String,
    "lieuExecution_typeCode": pl.String,
    # "_type": pl.String,
    "uid": pl.String,
    # "uuid": pl.String,
    # "modaliteExecution": pl.List(pl.String),
    "marcheInnovant": pl.Boolean,
    "attributionAvance": pl.Boolean,
    "sousTraitanceDeclaree": pl.Boolean,
    "ccag": pl.String,
    "offresRecues": pl.String,
    "typeGroupementOperateurs": pl.String,
    "idAccordCadre": pl.String,
    # "technique": pl.List(pl.String),
    # "TypePrix": pl.String,
    "tauxAvance": pl.String,
    "origineUE": pl.String,
    "origineFrance": pl.String,
    # Les champs listes de strings ne sont pas encore gérés
    # Présents parfois dans les données mais non pertinents :
    # "created_at": pl.String,
    # "typesPrix": pl.List(pl.String),
    # "typePrix": pl.String,
    # "term.acheteur.id": pl.String,
    # "updated_at": pl.String,
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
    # "considerationsSociales_considerationSociale": pl.List(pl.String),
    # "considerationsEnvironnementales_considerationEnvironnementale": pl.List(pl.String),
    # Les champs listes de strings ne sont pas encore gérés
    **SCHEMA_MODIFICATION_2022,
}
