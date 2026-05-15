from datetime import date, datetime, timedelta

import polars as pl
import polars.selectors as cs

from src.config import (
    ACHETEURS_NON_SIRENE,
    DATA_DIR,
    GEOCODING_RETRY_DAYS,
    LOG_LEVEL,
    SIRENE_DATA_DIR,
    SIRET_LATLONG_SCHEMA,
)
from src.tasks.get import bootstrap_siret_latlong, get_from_s3
from src.tasks.transform import (
    extract_unique_acheteurs_siret,
    extract_unique_titulaires_siret,
)
from src.tasks.utils import get_logger


def add_etablissement_data(
    lf_sirets: pl.LazyFrame,
    lf_etablissements: pl.LazyFrame,
    siret_column: str,
    type_siret: str,
) -> pl.LazyFrame:
    # Pas besoin de garder les SIRET qui ne matchent pas dans ce df intermédiaire, puisqu'on
    # merge in fine avec le reste des données (how = inner)
    lf_sirets = lf_sirets.join(
        lf_etablissements, how="inner", left_on=siret_column, right_on="siret"
    )

    # On ne prend pas l'activité des acheteurs
    if type_siret == "acheteur":
        lf_sirets = lf_sirets.drop(cs.starts_with("activite_"))

    # Si il y a un etablissement_nom (Enseigne1Etablissement ou denominationUsuelleEtablissement),
    # on l'ajoute au nom de l'organisme, entre parenthèses
    lf_sirets = lf_sirets.with_columns(
        pl.when(
            pl.col("etablissement_nom").is_not_null()
            & (pl.col("etablissement_nom") != pl.col(f"{type_siret}_nom"))
        )
        .then(
            pl.concat_str(
                pl.col(f"{type_siret}_nom"),
                pl.lit(" ("),
                pl.col("etablissement_nom"),
                pl.lit(")"),
            )
        )
        .otherwise(pl.col(f"{type_siret}_nom"))
        .alias(f"{type_siret}_nom")
    ).drop("etablissement_nom")

    lf_sirets = lf_sirets.rename(
        {
            "latitude": f"{type_siret}_latitude",
            "longitude": f"{type_siret}_longitude",
            "commune_code": f"{type_siret}_commune_code",
            "commune_nom": f"{type_siret}_commune_nom",
            "departement_code": f"{type_siret}_departement_code",
            "departement_nom": f"{type_siret}_departement_nom",
            "region_code": f"{type_siret}_region_code",
            "region_nom": f"{type_siret}_region_nom",
        }
    )
    return lf_sirets


def add_unite_legale_data(
    lf_sirets: pl.LazyFrame,
    unites_legales_lf: pl.LazyFrame,
    siret_column: str,
    type_siret: str,
) -> pl.LazyFrame:
    # Extraction du SIREN à partir du SIRET (9 premiers caractères)
    lf_sirets = lf_sirets.with_columns(pl.col(siret_column).str.head(9).alias("siren"))

    # Pas besoin de garder les SIRET qui ne matchent pas dans ce df intermédiaire, puisqu'on
    # merge in fine avec le reste des données
    lf_sirets = lf_sirets.join(unites_legales_lf, how="inner", on="siren")
    lf_sirets = lf_sirets.rename(
        {
            "denominationUniteLegale": f"{type_siret}_nom",
            "siren": f"{type_siret}_siren",
            "categorieEntreprise": f"{type_siret}_categorie",
        }
    )

    if type_siret == "acheteur":
        lf_sirets = lf_sirets.with_columns(
            pl.col("categorieJuridiqueUniteLegale")
            .cast(pl.String)
            .alias("categorieJuridiqueUniteLegale")
        )

        categories_acheteurs = {
            "État": pl.col("categorieJuridiqueUniteLegale").str.starts_with("71"),
            "EPIC": pl.col("categorieJuridiqueUniteLegale").is_in(
                ["4140", "4110", "4120"]
            ),
            "Commune": pl.col("categorieJuridiqueUniteLegale").is_in(["7210", "7312"]),
            "Groupement de communes": pl.col(
                "categorieJuridiqueUniteLegale"
            ).str.starts_with("734"),
            "Syndicat mixte": pl.col("categorieJuridiqueUniteLegale").is_in(
                ["7354", "7355"]
            ),
            "Département": pl.col("categorieJuridiqueUniteLegale") == "7220",
            "Département outre-mer": pl.col("categorieJuridiqueUniteLegale") == "7225",
            "Région": pl.col("categorieJuridiqueUniteLegale") == "7230",
            "Établissement hospitalier": pl.col("categorieJuridiqueUniteLegale")
            == "7364",
        }
        lf_sirets = lf_sirets.with_columns(
            acheteur_categorie=pl.coalesce(
                pl.when(condition).then(pl.lit(value)).otherwise(None)
                for value, condition in categories_acheteurs.items()
            )
        )

        # Les acheteurs suivants ne rentrent officiellement dans aucune des *
        # catégorie ci-dessus ((Autre) Collectivité territoriale selon l'INSEE). Nous
        # devons donc les catégoriser manuellement :
        # - Ville de Paris (217500016) : Commune
        # - Métropole de Lyon (200046977) : Groupement de communes
        # - Collectivité de Corse (200076958) : Région
        # - Collectivité territoriale de Guyane (200052678) : Département outre-mer
        # - Collectivité territoriale de Martinique (200055507) : Département outre-mer
        # - Département-région de Mayotte (229850003) : Département outre-mer
        # - Territoires de terres australes et antarctiques françaises (229840004) : Département outre-mer
        special_acheteurs_siren = {
            "217500016": "Commune",
            "200046977": "Groupement de communes",
            "200076958": "Région",
            "200052678": "Département outre-mer",
            "200055507": "Département outre-mer",
            "229850003": "Département outre-mer",
            "229840004": "Département outre-mer",
        }

        lf_sirets = lf_sirets.with_columns(
            acheteur_categorie=pl.coalesce(
                pl.when(pl.col("acheteur_id").str.starts_with(siren))
                .then(pl.lit(category))
                .otherwise(pl.col("acheteur_categorie"))
                for siren, category in special_acheteurs_siren.items()
            )
        )

    lf_sirets = lf_sirets.drop("categorieJuridiqueUniteLegale")

    return lf_sirets


def enrich_from_sirene(lf: pl.LazyFrame):
    logger = get_logger(level=LOG_LEVEL)

    # Récupération des données SIRET/SIREN préparées dans sirene-preprocess()
    lf_etablissements = pl.scan_parquet(SIRENE_DATA_DIR / "etablissements.parquet")
    lf_unites_legales = pl.scan_parquet(SIRENE_DATA_DIR / "unites_legales.parquet")

    lf_base = lf.clone()

    # DONNÉES SIRENE ACHETEURS

    logger.info("Extraction des SIRET des acheteurs...")
    lf_sirets_acheteurs = extract_unique_acheteurs_siret(lf_base)

    logger.info("Ajout des données unités légales (acheteurs)...")
    lf_sirets_acheteurs = add_unite_legale_data(
        lf_sirets_acheteurs,
        lf_unites_legales,
        siret_column="acheteur_id",
        type_siret="acheteur",
    )

    logger.info("Ajout des données établissements (acheteurs)...")
    lf_sirets_acheteurs = add_etablissement_data(
        lf_sirets_acheteurs, lf_etablissements, "acheteur_id", "acheteur"
    )

    # Matérialisation de sirets_acheteurs pour rompre
    # le cercle de dépendances du lf
    acheteurs_path = SIRENE_DATA_DIR / "temp_enrich_acheteurs.parquet"
    lf_sirets_acheteurs.sink_parquet(acheteurs_path)
    lf_sirets_acheteurs = pl.scan_parquet(acheteurs_path)

    # DONNÉES SIRENE TITULAIRES

    logger.info("Extraction des SIRET des titulaires...")
    lf_sirets_titulaires = extract_unique_titulaires_siret(lf_base)

    logger.info("Ajout des données unités légales (titulaires)...")
    lf_sirets_titulaires = add_unite_legale_data(
        lf_sirets_titulaires,
        lf_unites_legales,
        siret_column="titulaire_id",
        type_siret="titulaire",
    )

    logger.info("Ajout des données établissements (titulaires)...")
    lf_sirets_titulaires = add_etablissement_data(
        lf_sirets_titulaires, lf_etablissements, "titulaire_id", "titulaire"
    )

    #  # Matérialisation de sirets_titulaires pour rompre
    # le cercle de dépendances du lf
    titulaires_path = SIRENE_DATA_DIR / "temp_enrich_titulaires.parquet"
    lf_sirets_titulaires.sink_parquet(titulaires_path)
    lf_sirets_titulaires = pl.scan_parquet(titulaires_path)

    # JOINTURES

    lf = lf.join(lf_sirets_acheteurs, how="left", on="acheteur_id")

    # Fallback pour les acheteurs absents de SIRENE (ex: Ministère des Armées)
    lf = apply_acheteurs_non_sirene_fallback(lf)

    # En joignant en utilisant à la fois le SIRET et le typeIdentifiant, on s'assure qu'on ne joint pas sur
    # des id de titulaires non-SIRET
    lf = lf.join(
        lf_sirets_titulaires,
        how="left",
        on=["titulaire_id", "titulaire_typeIdentifiant"],
    )

    del lf_sirets_titulaires
    # logger.info("Amélioration des données unités légales des titulaires...")
    # lf_sirets_titulaires = improve_titulaire_unite_legale_data(lf_sirets_titulaires)

    lf = calculate_distance(lf)

    lf = lf.drop(cs.ends_with("_siren"))

    return lf


def apply_acheteurs_non_sirene_fallback(lf: pl.LazyFrame) -> pl.LazyFrame:
    """Applique les données d'acheteurs non présents dans SIRENE en fallback.

    Les acheteurs absents de SIRENE ET du fallback conservent acheteur_nom = NULL.
    """
    if not ACHETEURS_NON_SIRENE:
        return lf

    # Créer un DataFrame de fallback à partir du dictionnaire
    fallback_data = [
        {"acheteur_id": siret, "acheteur_nom_fallback": data["nom"]}
        for siret, data in ACHETEURS_NON_SIRENE.items()
    ]
    lf_fallback = pl.LazyFrame(fallback_data)

    # Joindre avec les données de fallback
    lf = lf.join(lf_fallback, on="acheteur_id", how="left")

    # Remplacer acheteur_nom NULL par la valeur de fallback (si disponible)
    lf = lf.with_columns(
        pl.coalesce("acheteur_nom", "acheteur_nom_fallback").alias("acheteur_nom")
    ).drop("acheteur_nom_fallback")

    return lf


def calculate_distance(lf: pl.LazyFrame) -> pl.LazyFrame:
    # Utilisation de polars_ds.haversine
    # https://polars-ds-extension.readthedocs.io/en/latest/num.html#polars_ds.exprs.num.haversine
    lf = lf.with_columns(
        haversine(
            pl.col("acheteur_latitude"),
            pl.col("acheteur_longitude"),
            pl.col("titulaire_latitude"),
            pl.col("titulaire_longitude"),
        )
        .round(mode="half_away_from_zero")
        .cast(pl.Int16)
        .alias("titulaire_distance")
    )
    return lf


def haversine(
    lat1: pl.Expr, lon1: pl.Expr, lat2: pl.Expr, lon2: pl.Expr, R: float = 6371.0
) -> pl.Expr:
    """
    Calcule la distance haversine entre deux points (lat1, lon1) et (lat2, lon2) en km.
    Utilise des opérations vectorisées Polars.
    Généré par la LLM Euria, développée et hébergée en Suisse par Infomaniak.
    """
    # Convertir en radians
    lat1 = pl.col("acheteur_latitude").radians()
    lon1 = pl.col("acheteur_longitude").radians()
    lat2 = pl.col("titulaire_latitude").radians()
    lon2 = pl.col("titulaire_longitude").radians()

    # Différences
    dlat = lat2 - lat1
    dlon = lon2 - lon1

    # Formule haversine
    a = (dlat / 2.0).sin().pow(2) + lat1.cos() * lat2.cos() * (dlon / 2.0).sin().pow(2)
    c = 2.0 * a.sqrt().arcsin()

    # Distance
    return R * c


def add_type_marche(lf: pl.LazyFrame) -> pl.LazyFrame:
    cpv_division = pl.col("codeCPV").str.slice(0, 2).cast(pl.Int8, strict=False)

    lf = lf.with_columns(
        pl.when(cpv_division == 45)
        .then(pl.lit("Travaux"))
        .when(cpv_division.is_in(range(1, 45)) | (cpv_division == 48))
        .then(pl.lit("Fournitures"))
        .when(cpv_division.is_in(range(50, 99)))
        .then(pl.lit("Services"))
        .otherwise(pl.lit("Non catégorisé"))
        .fill_null(pl.lit("Code CPV invalide"))
        .alias("type")
    )
    return lf


def select_sirets_to_geocode(
    lf_decp: pl.LazyFrame,
    lf_siret_latlong: pl.LazyFrame,
    today: date,
    retry_days: int,
) -> pl.LazyFrame:
    """Retourne les SIRETs DECP à géocoder, après exclusion des déjà traités."""
    sirets_decp = (
        pl.concat(
            [
                lf_decp.select(pl.col("acheteur_id").cast(pl.String).alias("siret")),
                lf_decp.select(pl.col("titulaire_id").cast(pl.String).alias("siret")),
            ]
        )
        .filter(pl.col("siret").is_not_null() & (pl.col("siret").str.len_chars() == 14))
        .unique()
    )

    cutoff = today - timedelta(days=retry_days)
    excluded = lf_siret_latlong.filter(
        (pl.col("status") == "success")
        | (pl.col("status") == "not_in_sirene")
        | ((pl.col("status") == "failed") & (pl.col("geocoded_at") >= cutoff))
    ).select("siret")

    return sirets_decp.join(excluded, on="siret", how="anti")


def geocode_sirene(lf: pl.LazyFrame):
    lf_siret_latlong = get_from_s3(key="siret_latlong.parquet", prefix="")
    if not isinstance(lf_siret_latlong, pl.LazyFrame):
        lf_siret_latlong = bootstrap_siret_latlong()
    lf_etab = pl.scan_parquet(SIRENE_DATA_DIR / "etablissements.parquet")
    lf_siret_latlong_updated = geocode_missing_sirets(lf, lf_siret_latlong, lf_etab)

    siret_latlong_path = DATA_DIR / "siret_latlong.parquet"
    lf_siret_latlong_updated.sink_parquet(siret_latlong_path)
    return lf, siret_latlong_path


def geocode_missing_sirets(
    lf_decp: pl.LazyFrame,
    lf_siret_latlong: pl.LazyFrame,
    lf_etablissements: pl.LazyFrame,
) -> pl.LazyFrame:
    """Géocode les SIRETs DECP manquants et retourne le siret_latlong mis à jour.

    Tolère un échec de l'API : le flow continue sans ajouter de nouvelles entrées.
    """
    import httpx
    from tenacity import RetryError

    from src.tasks.geocode import geocode_csv

    logger = get_logger(level=LOG_LEVEL)
    today = date.today()

    to_geocode = select_sirets_to_geocode(
        lf_decp, lf_siret_latlong, today, GEOCODING_RETRY_DAYS
    )

    with_address = to_geocode.join(lf_etablissements, on="siret", how="inner")
    not_in_sirene = to_geocode.join(
        lf_etablissements.select("siret"), on="siret", how="anti"
    )

    not_in_sirene_entries = not_in_sirene.with_columns(
        pl.lit(None, dtype=pl.Float64).alias("latitude"),
        pl.lit(None, dtype=pl.Float64).alias("longitude"),
        pl.lit("geoplateforme").alias("source"),
        pl.lit(None, dtype=pl.Float64).alias("score"),
        pl.lit(today).alias("geocoded_at"),
        pl.lit("not_in_sirene").alias("status"),
    ).select(list(SIRET_LATLONG_SCHEMA.keys()))

    try:
        df_addresses = with_address.collect()
        if df_addresses.height > 0:
            geocoded = geocode_csv(df_addresses)
        else:
            geocoded = pl.DataFrame(schema=SIRET_LATLONG_SCHEMA)
    except (httpx.HTTPError, RetryError) as exc:
        logger.warning(
            f"⚠️  API Géoplateforme indisponible ({exc}) — skip géocodage du jour"
        )
        geocoded = pl.DataFrame(schema=SIRET_LATLONG_SCHEMA)

    updated = pl.concat(
        [
            lf_siret_latlong.select(list(SIRET_LATLONG_SCHEMA.keys())),
            not_in_sirene_entries,
            geocoded.lazy(),
        ],
        how="vertical",
    ).unique(subset=["siret"], keep="last")

    return updated


def add_duree_restante(lff: pl.LazyFrame):
    today = datetime.now().date()
    duree_mois_days_int = pl.col("dureeMois") * 30.5
    end_date = pl.col("dateNotification") + pl.duration(days=duree_mois_days_int)
    duree_restante_mois = ((end_date - today).dt.total_days() / 30).round(1)

    # Pas de valeurs négatives.
    lff = lff.with_columns(
        pl.when(duree_restante_mois < 0)
        .then(pl.lit(0))
        .otherwise(duree_restante_mois)
        .alias("dureeRestanteMois")
    )

    # Si dureeRestanteMois > dureeMois, dureeRestanteMois = dureeMois
    lff = lff.with_columns(
        pl.when(pl.col("dureeRestanteMois") > pl.col("dureeMois"))
        .then(pl.col("dureeMois").cast(pl.Float32))
        .otherwise(pl.col("dureeRestanteMois"))
        .alias("dureeRestanteMois")
    )
    return lff
