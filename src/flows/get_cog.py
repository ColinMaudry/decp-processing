import polars as pl
from prefect import flow
from prefect.logging import get_run_logger

from src.config import DATA_DIR
from src.tasks.get import get_insee_cog_data


@flow(log_prints=True)
def get_cog():
    """Téléchargement et préparation des données du Code Officiel Géographique"""

    logger = get_run_logger()
    logger.info(
        "Téléchargement et préparation des données du Code Officiel Géographique..."
    )

    # # # # # # # # #
    # Communes      #
    # # # # # # # # #

    # Métropole et DOM
    df_com = get_insee_cog_data(
        "https://www.insee.fr/fr/statistiques/fichier/8377162/v_commune_2025.csv",
        schema_overrides={"COM": pl.String, "DEP": pl.String, "REG": pl.String},
        columns=["COM", "LIBELLE", "REG", "DEP"],
    )

    # Territoires d'outre mer
    df_com_tom = get_insee_cog_data(
        "https://www.insee.fr/fr/statistiques/fichier/8377162/v_commune_comer_2025.csv",
        schema_overrides={"COM_COMER": pl.String, "COMER": pl.String},
        columns=["COM_COMER", "LIBELLE", "COMER"],
    )
    df_com_tom = df_com_tom.rename({"COM_COMER": "COM", "COMER": "DEP"})
    df_com_tom = df_com_tom.with_columns(pl.col("DEP").alias("REG"))
    df_com_tom = df_com_tom.select(df_com.columns)

    # Fusion des deux
    df_com = df_com.extend(df_com_tom)

    # Corrections
    df_com = df_com.rename({"LIBELLE": "commune_nom"})
    df_com = df_com.filter(pl.col("DEP") != "")

    # # # # # # # # #
    # Départements  #
    # # # # # # # # #

    df_dep = get_insee_cog_data(
        "https://www.insee.fr/fr/statistiques/fichier/8377162/v_departement_2025.csv",
        schema_overrides={"DEP": pl.String},
        columns=["DEP", "LIBELLE"],
    )

    df_dep_tom = get_insee_cog_data(
        "https://www.insee.fr/fr/statistiques/fichier/8377162/v_comer_2025.csv",
        schema_overrides={"COMER": pl.String},
        columns=["COMER", "LIBELLE"],
    ).rename({"COMER": "DEP"})

    df_dep = df_dep.extend(df_dep_tom)
    df_dep = df_dep.rename({"LIBELLE": "departement_nom"})

    # # # # # # # # #
    # Régions       #
    # # # # # # # # #

    df_reg = get_insee_cog_data(
        "https://www.insee.fr/fr/statistiques/fichier/8377162/v_region_2025.csv",
        schema_overrides={"REG": pl.String},
        columns=["REG", "LIBELLE"],
    )
    # On utilise les noms de départements des TOM comme noms de région
    df_dep_tom = df_dep_tom.rename({"DEP": "REG"}).select(df_reg.columns)
    df_reg = df_reg.extend(df_dep_tom).rename({"LIBELLE": "region_nom"})

    # # # # # # # # #
    # Jointures     #
    # # # # # # # # #

    df = df_com.join(df_dep, on="DEP", how="left")
    df = df.join(df_reg, on="REG", how="left")
    df = df.rename(
        {"COM": "commune_code", "DEP": "departement_code", "REG": "region_code"}
    ).sort(by="commune_code")

    df.write_parquet(DATA_DIR / "code_officiel_geographique.parquet")
