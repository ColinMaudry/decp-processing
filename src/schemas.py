import polars as pl

# Rappel : pl.scan_ndjson() fonctionne différemment de pl.DataFrame([some data]) :
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
    "marcheInnovant": pl.String,
    "attributionAvance": pl.String,
    "sousTraitanceDeclaree": pl.String,
    "ccag": pl.String,
    "offresRecues": pl.String,
    "typeGroupementOperateurs": pl.String,
    "idAccordCadre": pl.String,
    "tauxAvance": pl.String,
    "origineUE": pl.String,
    "origineFrance": pl.String,
    # Présents dans peu de marchés et peu pertinents :
    # "created_at": pl.String,
    # "term.acheteur.id": pl.String,
    # "updated_at": pl.String,
    # "uuid": pl.String,
    # On ne traite que les marchés publics pour l'instant, pas les concessions, donc non pertinent :
    # "_type": pl.String,
}

SCHEMA_MARCHE_2019 = {
    **SCHEMA_MARCHE_BASE,
    "titulaires": pl.List(SCHEMA_TITULAIRE_2019),
    "considerationsSociales": pl.List(pl.String),
    "considerationsEnvironnementales": pl.List(pl.String),
    "modalitesExecution": pl.List(pl.String),
    "techniques": pl.List(pl.String),
    "typesPrix": pl.List(pl.String),
    **SCHEMA_MODIFICATION_2019,
}

SCHEMA_MARCHE_2022 = {
    **SCHEMA_MARCHE_BASE,
    "titulaires": pl.List(SCHEMA_TITULAIRE_2022),
    "considerationsSociales_considerationSociale": pl.List(pl.String),
    "considerationsEnvironnementales_considerationEnvironnementale": pl.List(pl.String),
    "modalitesExecution_modaliteExecution": pl.List(pl.String),
    "techniques_technique": pl.List(pl.String),
    "typesPrix_typePrix": pl.List(pl.String),
    **SCHEMA_MODIFICATION_2022,
}
