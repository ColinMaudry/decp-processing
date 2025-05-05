import polars as pl
from httpx import get

from tasks.output import save_to_sqlite


def explode_titulaires(df: pl.DataFrame):
    # Explosion des champs titulaires sur plusieurs lignes (un titulaire de marché par ligne)
    # et une colonne par champ

    # Structure originale
    # [{{"id": "abc", "typeIdentifiant": "SIRET"}}]

    # Explosion de la liste de titulaires en autant de nouvelles lignes
    df = df.explode("titulaires")

    # Renommage du premier objet englobant
    df = df.select(
        pl.col("*"),
        pl.col("titulaires")
        .struct.rename_fields(["titulaires.object"])
        .alias("titulaires_renamed"),
    )

    # Extraction du premier objet dans une nouvelle colonne
    df = df.unnest("titulaires_renamed")

    # Renommage des champs de l'objet titulaire
    df = df.select(
        pl.col("*"),
        pl.col("titulaires.object")
        .struct.rename_fields(["titulaire_typeIdentifiant", "titulaire_id"])
        .alias("titulaire"),
    )

    # Extraction de l'objet titulaire
    df = df.unnest("titulaire")

    # Suppression des anciennes colonnes
    df = df.drop(["titulaires", "titulaires.object"])

    # Cast l'identifiant en string
    df = df.with_columns(pl.col("titulaire_id").cast(pl.String))

    return df

def remove_modfications_duplicates(df):
    """On supprime les marches avec un suffixe correspondant à un autre marché"""
    if "modifications" not in df.collect_schema().names():
        return df
    # Index sans les suffixes
    df_cleaned = df.with_columns(short_id=pl.col("uid").str.head(-2),
                        modifications_len=pl.col("modifications").list.len())
    df_cleaned = df_cleaned.with_columns(modif_id=pl.col("short_id") + pl.col("objet"))
    df_cleaned = df_cleaned.with_columns(uid=pl.when(pl.col("modif_id").is_duplicated())
                            .then(pl.col("short_id"))
                            .otherwise(pl.col("uid")))
    df_cleaned = df_cleaned.sort("modifications_len").unique("modif_id", keep="last")
    return df_cleaned


def replace_by_modification_data(df: pl.DataFrame):
    """
    Gère les modifications dans le DataFrame des DECP.
    Cette fonction extrait les informations des modifications et les fusionne avec le DataFrame de base en ajoutant une ligne par modification 
    (chaque ligne contient les informations complètes à jour à la date de notification)
    Elle ajoute également la colonne "estDerniereNotification" pour indiquer si la notification est la plus récente.
    """

    # Étape 1: Créer une copie du DataFrame initial sans la colonne "modifications"
    df_base = df.select([col for col in df.columns if col != "modifications"])

    # Étape 2: Explode le DataFrame pour avoir une ligne par modification
    df_exploded = df.select("id", "modifications").explode("modifications").drop_nulls()

    # Étape 3: Extraire les données des modifications
    df_mods = df_exploded.select(
        "id",
        pl.col("modifications").struct.field("modification").struct.field("id").alias("modification_id"),
        pl.col("modifications").struct.field("modification").struct.field("dateNotificationModification").alias("dateNotification"),
        pl.col("modifications").struct.field("modification").struct.field("datePublicationDonneesModification").alias("datePublicationDonnees"),
        pl.col("modifications").struct.field("modification").struct.field("montant").alias("montant"),
        pl.col("modifications").struct.field("modification").struct.field("dureeMois").alias("dureeMois"),
    )

    # Étape 4: Joindre les données de base pour chaque ligne de modification
    df_concat = (pl.concat([df_base
                            .with_columns(pl.lit(0).alias("modification_id"))
                            .select("id", "modification_id", "dateNotification", "datePublicationDonnees", "montant", "dureeMois"),
                            df_mods], how="vertical_relaxed")
                        .sort(["id", "dateNotification"], descending=[False, True])
                        .with_columns(pl.when(pl.col("dateNotification") == pl.col("dateNotification").max().over("id")).then(True).otherwise(False).alias("estDerniereNotification"))
                )

    # Étape 5: Remplir les valeurs nulles en utilisant les dernières valeurs non-nulles pour chaque id
    df_concat = df_concat.with_columns(pl.col("montant").fill_null(strategy="backward").over("id"),
                                    pl.col("dureeMois").fill_null(strategy="backward").over("id")
                                    )

    # Étape 5: Ajouter les données du DataFrame de base
    df_final = df_concat.join(df_base.drop(["dateNotification", "datePublicationDonnees", "montant", "dureeMois"]),
                            on="id", how="left")

    return df_final


def process_modifications(df):
    df = remove_modfications_duplicates(df)
    df = replace_by_modification_data(df)
    return df

def normalize_tables(df):
    # MARCHES

    df_marches: pl.DataFrame = pl.DataFrame(df.to_arrow()).drop(
        "titulaire_id", "titulaire_typeIdentifiant"
    )
    df_marches = df_marches.unique("uid").sort(
        by="datePublicationDonnees", descending=True
    )
    save_to_sqlite(df_marches, "datalab", "marches", "uid")
    del df_marches

    # ACHETEURS

    df_acheteurs: pl.DataFrame = df.select("acheteur_id")
    df_acheteurs = df_acheteurs.rename({"acheteur_id": "id"})
    df_acheteurs = df_acheteurs.unique().sort(by="id")
    save_to_sqlite(df_acheteurs, "datalab", "acheteurs", "id")
    del df_acheteurs

    # TITULAIRES

    ## Table entreprises
    df_titulaires: pl.DataFrame = df.select("titulaire_id", "titulaire_typeIdentifiant")

    ### On garde les champs id et typeIdentifiant en clé primaire composite
    df_titulaires = df_titulaires.rename(
        {"titulaire_id": "id", "titulaire_typeIdentifiant": "typeIdentifiant"}
    )
    df_titulaires = df_titulaires.unique().sort(by=["id"])
    save_to_sqlite(df_titulaires, "datalab", "entreprises", "id, typeIdentifiant")
    del df_titulaires

    ## Table marches_titulaires
    df_marches_titulaires: pl.DataFrame = df.select(
        "uid", "titulaire_id", "titulaire_typeIdentifiant"
    )
    df_marches_titulaires = df_marches_titulaires.rename({"uid": "marche_uid"})
    save_to_sqlite(
        df_marches_titulaires,
        "datalab",
        "marches_titulaires",
        '"marche_uid", "titulaire_id", "titulaire_typeIdentifiant"',
    )
    del df_marches_titulaires

    # TODO ajouter les sous-traitants quand ils seront ajoutés aux données


def merge_decp_json(files: list) -> pl.DataFrame:
    dfs = []
    for file in files:
        df: pl.DataFrame = pl.read_parquet(f"{file}.parquet")
        dfs.append(df)

    df = pl.concat(dfs, how="diagonal")

    print(
        "Suppression des lignes en doublon par UID + titulaire ID + titulaire type ID"
    )
    # Exemple : 20005584600014157140791205100
    index_size_before = df.height
    df = df.unique(
        subset=["uid", "titulaire_id", "titulaire_typeIdentifiant"],
        maintain_order=False,
    )
    print("-- ", index_size_before - df.height, " doublons supprimés")

    # Ordre des colonnes
    df = df.select(
        "uid",
        "id",
        "nature",
        "acheteur_id",
        "titulaire_id",
        "titulaire_typeIdentifiant",
        "objet",
        "montant",
        "codeCPV",
        "procedure",
        "dureeMois",
        "dateNotification",
        "datePublicationDonnees",
        "formePrix",
        "attributionAvance",
        "offresRecues",
        "marcheInnovant",
        "ccag",
        "sousTraitanceDeclaree",
        "typeGroupementOperateurs",
        "tauxAvance",
        "origineUE",
        "origineFrance",
        "lieuExecution_code",
        "lieuExecution_typeCode",
        "idAccordCadre",
    )
    return df


def setup_tableschema_columns(df: pl.DataFrame):
    # Ajout colonnes manquantes
    df = df.with_columns(pl.lit("").alias("acheteur_nom"))  # TODO
    df = df.with_columns(pl.lit("").alias("titulaire_denominationSociale"))  # TODO
    df = df.with_columns(pl.lit("").alias("lieuExecution_nom"))  # TODO
    df = df.with_columns(pl.lit("").alias("objetModification"))  # TODO
    df = df.with_columns(pl.lit("").alias("donneesActuelles"))  # TODO
    df = df.with_columns(pl.lit("").alias("anomalies"))  # TODO

    tableschema = get(
        "https://raw.githubusercontent.com/ColinMaudry/decp-table-schema/refs/heads/main/schema.json",
        follow_redirects=True,
    ).json()
    fields = [field["name"] for field in tableschema["fields"]]
    df = df.select(fields)

    return df


def make_decp_sans_titulaires(df: pl.DataFrame):
    df_decp_sans_titulaires = df.drop(
        [
            "titulaire_id",
            "titulaire_typeIdentifiant",
        ]
    )
    df_decp_sans_titulaires = df_decp_sans_titulaires.unique()
    return df_decp_sans_titulaires


#
# ⬇️⬇️⬇️ Fonctions à refactorer avec Polars et le format DECP 2022 ⬇️⬇️⬇️
#


def extract_unique_acheteurs_siret(df: pl.DataFrame):
    # Extraction des SIRET des DECP
    decp_acheteurs_df = df[["acheteur_id"]]
    decp_acheteurs_df = decp_acheteurs_df.drop_duplicates().loc[
        decp_acheteurs_df["acheteur_id"] != ""
    ]
    print(f"{decp_acheteurs_df.index.size} acheteurs uniques")

    return decp_acheteurs_df


def extract_unique_titulaires_siret(df: pl.DataFrame):
    # Extraction des SIRET des DECP
    df_sirets_titulaires = df[["titulaire_id", "titulaire_typeIdentifiant"]]

    df_sirets_titulaires = df_sirets_titulaires.drop_duplicates()
    df_sirets_titulaires = df_sirets_titulaires[
        df_sirets_titulaires["titulaire_typeIdentifiant"] == "SIRET"
    ]
    print(f"{len(df_sirets_titulaires)} titulaires uniques")

    return df_sirets_titulaires


def make_acheteur_nom(decp_acheteurs_df: pl.DataFrame):
    # Construction du champ acheteur_id

    from numpy import nan as NaN

    def construct_nom(row):
        if row["enseigne1Etablissement"] is NaN:
            return row["denominationUniteLegale"]
        else:
            return f'{row["denominationUniteLegale"]} - {row["enseigne1Etablissement"]}'

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
    cj_df = pl.read_csv("data/cj.csv", index_col=None, dtype="object")
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
