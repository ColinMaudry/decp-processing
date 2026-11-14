import json
import os
import shutil
from collections.abc import Coroutine
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from urllib.parse import urlsplit

import httpx
import polars as pl
import yaml
from dotenv import find_dotenv, load_dotenv
from ijson import sendable_list
from prefect.logging import get_logger

logger = get_logger(__name__)

dotenv_path = find_dotenv()
if dotenv_path == "":
    logger.info("Création du fichier .env à partir de template.env")
    template_dotenv_path = Path(find_dotenv("template.env"))
    dotenv_path = template_dotenv_path.with_name(".env")
    shutil.copyfile(template_dotenv_path, dotenv_path)

load_dotenv(dotenv_path, override=False)


def make_path_from_env(env: str, alternative_path: Path) -> Path:
    # J'ai eu des comportements erratiques avec os.getenv("env", alternative_path), donc j'utilise "or"
    return Path(os.getenv(env) or alternative_path)


ALL_CONFIG = {}

# Niveau des logs
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")
ALL_CONFIG["LOG_LEVEL"] = LOG_LEVEL

# Nombre maximal de workers utilisables par Prefect. Défaut : 16
MAX_PREFECT_WORKERS = int(os.getenv("MAX_PREFECT_WORKERS", 4))
ALL_CONFIG["MAX_PREFECT_WORKERS"] = MAX_PREFECT_WORKERS

# Taille minimale d'un fichier source pour être traité, par format.
# Établies empiriquement (2026-10) : le plus petit JSON "vide" des datasets trackés
# fait 55 o, le plus petit XML contenant un marché fait 1077 o (squelette sans
# marché : ~91 o). Sous ces seuils, le fichier ne peut pas contenir de marché.
# Un fichier sous le seuil ne doit pas être ingéré : il produirait un parquet
# de 0 ligne en cache, qui n'en ressortirait jamais (voir get_clean).
MIN_FILESIZE = {"json": 180, "xml": 1000}
ALL_CONFIG["MIN_FILESIZE"] = MIN_FILESIZE

# Durée avant l'expiration du cache des ressources (en heure). Défaut : 168 (7 jours)
CACHE_EXPIRATION_TIME_HOURS = int(os.getenv("CACHE_EXPIRATION_TIME_HOURS", 168))
ALL_CONFIG["CACHE_EXPIRATION_TIME_HOURS"] = CACHE_EXPIRATION_TIME_HOURS


DATE_NOW = datetime.now().isoformat()[0:10]  # YYYY-MM-DD
MONTH_NOW = DATE_NOW[:7]  # YYYY-MM

# Publication ou non des fichiers produits sur data.gouv.fr
DECP_PROCESSING_PUBLISH = os.getenv("DECP_PROCESSING_PUBLISH", "").lower() == "true"
ALL_CONFIG["DECP_PROCESSING_PUBLISH"] = DECP_PROCESSING_PUBLISH

# Configuration S3 (service S3-compatible générique, pas spécifique à AWS)
S3_ENDPOINT_URL = os.getenv("S3_ENDPOINT_URL", "")
S3_BUCKET = os.getenv("S3_BUCKET", "")
S3_ACCESS_KEY_ID = os.getenv("S3_ACCESS_KEY_ID", "")
S3_SECRET_ACCESS_KEY = os.getenv("S3_SECRET_ACCESS_KEY", "")
S3_REGION = os.getenv("S3_REGION", "")


# Client HTTP
HTTP_CLIENT = httpx.Client()
HTTP_HEADERS = {
    "Connection": "keep-alive",
    "User-agent": "colibre.fr",
}

# Timeout pour la publication de chaque ressource sur data.gouv.fr
DECP_PROCESSING_PUBLISH_TIMEOUT = os.getenv("DECP_PROCESSING_PUBLISH_TIMEOUT", 300)
if DECP_PROCESSING_PUBLISH_TIMEOUT == "":
    DECP_PROCESSING_PUBLISH_TIMEOUT = 300
else:
    DECP_PROCESSING_PUBLISH_TIMEOUT = int(DECP_PROCESSING_PUBLISH_TIMEOUT)

# URL de l'API data.gouv.fr
DATAGOUVFR_API = "https://www.data.gouv.fr/api/1"

# Clé d'API data.gouv.fr
DATAGOUVFR_API_KEY = os.getenv("DATAGOUVFR_API_KEY") or None

# URL API Prefect
PREFECT_API_URL = os.getenv("PREFECT_API_URL")

# Dossier racine
BASE_DIR = make_path_from_env("DECP_BASE_DIR", Path(__file__).absolute().parent.parent)
ALL_CONFIG["BASE_DIR"] = BASE_DIR

# Les variables configurées sur le serveur doivent avoir la priorité
DATA_DIR = make_path_from_env("DECP_DATA_DIR", BASE_DIR / "data")
DATA_DIR.mkdir(exist_ok=True, parents=True)
ALL_CONFIG["DATA_DIR"] = DATA_DIR

RESOURCE_CACHE_DIR = make_path_from_env(
    "RESOURCE_CACHE_DIR", DATA_DIR / "resource_cache"
)
RESOURCE_CACHE_DIR.mkdir(exist_ok=True, parents=True)
ALL_CONFIG["RESOURCE_CACHE_DIR"] = RESOURCE_CACHE_DIR

DIST_DIR = make_path_from_env("DECP_DIST_DIR", BASE_DIR / "dist" / "decp")
DIST_DIR.mkdir(exist_ok=True, parents=True, mode=0o777)
ALL_CONFIG["DIST_DIR"] = DIST_DIR


def make_sirene_data_dir(sirene_data_parent_dir) -> Path:
    default_dir = sirene_data_parent_dir / f"sirene_{MONTH_NOW}"
    # Si on est au début du mois, utiliser les données SIRENE du mois précédent
    # car les nouvelles données n'ont peut-être pas été encore générées
    if int(DATE_NOW[-2:]) <= 5:
        last_month = datetime.today() - timedelta(days=27)
        last_month = f"{str(last_month.year)}-{str(last_month.month).zfill(2)}"
        return sirene_data_parent_dir / f"sirene_{last_month}"
    return default_dir


SIRENE_DATA_PARENT_DIR = make_path_from_env("SIRENE_DATA_PARENT_DIR", DATA_DIR)
SIRENE_DATA_DIR = make_sirene_data_dir(SIRENE_DATA_PARENT_DIR)

# SIRENE_DATA_DIR on ne le crée que si nécessaire, dans sirene_preprocess.py
ALL_CONFIG["SIRENE_DATA_PARENT_DIR"] = SIRENE_DATA_PARENT_DIR
ALL_CONFIG["SIRENE_DATA_DIR"] = SIRENE_DATA_DIR


SIRENE_UNITES_LEGALES_URL = os.getenv("SIRENE_UNITES_LEGALES_URL", "")
SIRENE_ETABLISSEMENTS_URL = os.getenv("SIRENE_ETABLISSEMENTS_URL", "")

# Labels des entreprises (issue #190)
# Les deux ressources du jeu de données « Données des entreprises utilisées dans
# l'Annuaire des Entreprises », qui agrège les labels de leurs sources primaires
# respectives (Agence Bio, ADEME, ESS France, Ministère du Travail, Marché de
# l'Inclusion, Conseil National des Barreaux).
# Contrairement aux URL SIRENE, une valeur par défaut est fournie : ces permaliens
# sont stables et redirigent vers le millésime du jour.
#
# Les deux ressources sont volumineuses (1,1 et 1,7 Go) mais scannées en
# projection de colonnes : seules les colonnes de labels transitent réellement
# sur le réseau, d'où des durées de l'ordre de la minute et non de l'heure.

# Une ligne par unité légale (30 M) : ESS, association, Qualiopi, SIAE, avocat,
# achats responsables.
ANNUAIRE_UNITES_LEGALES_URL = os.getenv(
    "ANNUAIRE_UNITES_LEGALES_URL",
    "https://www.data.gouv.fr/api/1/datasets/r/77f09ee6-8ccb-4eb8-ad10-382be6416065",
)
ALL_CONFIG["ANNUAIRE_UNITES_LEGALES_URL"] = ANNUAIRE_UNITES_LEGALES_URL

# Une ligne par établissement (44 M) : Bio et RGE, qui sont des propriétés
# d'établissement et non d'unité légale.
ANNUAIRE_ETABLISSEMENTS_URL = os.getenv(
    "ANNUAIRE_ETABLISSEMENTS_URL",
    "https://www.data.gouv.fr/api/1/datasets/r/58427078-8afb-4651-9469-c9043991d892",
)
ALL_CONFIG["ANNUAIRE_ETABLISSEMENTS_URL"] = ANNUAIRE_ETABLISSEMENTS_URL

# API de géocodage Géoplateforme
GEOCODING_API_URL = os.getenv("GEOCODING_API_URL", "https://data.geopf.fr/geocodage")
GEOCODING_MIN_SCORE = float(os.getenv("GEOCODING_MIN_SCORE", "0.5"))
GEOCODING_RETRY_DAYS = int(os.getenv("GEOCODING_RETRY_DAYS", "30"))
GEOCODING_CHUNK_SIZE = int(os.getenv("GEOCODING_CHUNK_SIZE", "5000"))

# Mode de scraping
SCRAPING_MODE = os.getenv("SCRAPING_MODE", "month")
ALL_CONFIG["SCRAPING_MODE"] = SCRAPING_MODE

# Target (plateforme cible pour le scraping)
SCRAPING_TARGET = os.getenv("SCRAPING_TARGET")
ALL_CONFIG["SCRAPING_TARGET"] = SCRAPING_TARGET

# Year (année cible pour le scraping)
SCRAPING_YEAR = os.getenv("SCRAPING_YEAR")
ALL_CONFIG["SCRAPING_YEAR"] = SCRAPING_YEAR

# Month (mois cible pour le scraping)
SCRAPING_MONTH = os.getenv("SCRAPING_MONTH")
ALL_CONFIG["SCRAPING_MONTH"] = SCRAPING_MONTH

# Lecture ou non des ressource en cache
DECP_USE_CACHE = os.getenv("DECP_USE_CACHE", "false").lower() == "true"

# POSTGRESQL
POSTGRESQL_DB_URI = os.getenv("POSTGRESQL_DB_URI", "")

# Données de référence
REFERENCE_DIR = BASE_DIR / "reference"
ALL_CONFIG["REFERENCE_DIR"] = REFERENCE_DIR

# Liste et ordre des colonnes pour le mono dataframe de base (avant normalisation et spécialisation)
# Sert aussi à vérifier qu'au moins ces colonnes sont présentes (d'autres peuvent être présentes en plus, les colonnes "innatendues")
schema_fields = json.load(open(REFERENCE_DIR / "schema_base.json", "r"))["fields"]
BASE_DF_COLUMNS = [field["name"] for field in schema_fields]

# === Détection des anomalies de montant ===
# Voir docs/superpowers/specs/2026-04-25-montants-anormaux-rationalisation-design.md

# Population des communes (SIREN → population) : fichier « identifiants-communes »
# du jeu de données Datactivist « Identifiants des collectivités territoriales et
# leurs établissements ». Téléchargé à chaque exécution par get_population_communes ;
# le permalien data.gouv.fr redirige toujours vers la dernière version publiée.
POPULATION_COMMUNES_URL = os.getenv(
    "POPULATION_COMMUNES_URL",
    "https://www.data.gouv.fr/api/1/datasets/r/863a5d6f-cc5c-40e0-a349-8a15719cb25e",
)
ALL_CONFIG["POPULATION_COMMUNES_URL"] = POPULATION_COMMUNES_URL
POPULATION_COMMUNES_CSV = make_path_from_env(
    "POPULATION_COMMUNES_CSV", DATA_DIR / "identifiants-communes.csv"
)
ALL_CONFIG["POPULATION_COMMUNES_CSV"] = POPULATION_COMMUNES_CSV

# Seuils Signal A (écart MAD sur log du montant normalisé)
ANOMALY_PAIRS_SUSPECT_THRESHOLD = float(
    os.getenv("ANOMALY_PAIRS_SUSPECT_THRESHOLD", "4.0")
)
ANOMALY_PAIRS_ABERRANT_THRESHOLD = float(
    os.getenv("ANOMALY_PAIRS_ABERRANT_THRESHOLD", "6.0")
)

# Seuils Signal B (€/habitant, par type de marché)
ANOMALY_HABITANT_THRESHOLDS = {
    "Travaux": {"suspect": 5_000, "aberrant": 20_000},
    "Services": {"suspect": 1_000, "aberrant": 5_000},
    "Fournitures": {"suspect": 500, "aberrant": 2_000},
}

# Seuil Signal C (modulateur titulaire PME)
ANOMALY_TITULAIRE_PME_MONTANT_SEUIL = 50_000_000

# Tranches de population (bornes inférieures) pour la stratification du Signal A
POPULATION_TRANCHES = [
    (0, "très petite"),
    (2_000, "petite"),
    (10_000, "moyenne"),
    (50_000, "grande"),
    (200_000, "très grande"),
]

# Taille minimale d'un groupe de pairs pour calculer une statistique fiable
ANOMALY_GROUPE_MIN_SIZE = int(os.getenv("ANOMALY_GROUPE_MIN_SIZE", "30"))
ALL_CONFIG["ANOMALY_PAIRS_SUSPECT_THRESHOLD"] = ANOMALY_PAIRS_SUSPECT_THRESHOLD
ALL_CONFIG["ANOMALY_PAIRS_ABERRANT_THRESHOLD"] = ANOMALY_PAIRS_ABERRANT_THRESHOLD
ALL_CONFIG["ANOMALY_GROUPE_MIN_SIZE"] = ANOMALY_GROUPE_MIN_SIZE

COLUMNS_TO_DROP = [
    # Pas encore incluses
    "actesSousTraitance",
    "modificationsActesSousTraitance",
    # Inutilisée (on se base sur la date de la modification pour trier les modifications)
    "modification_id"
    # Champs de concessions
    "_type",  # Marché ou Contrat de concession
    "autoriteConcedante",
    "concessionnaires",
    "donneesExecution",
    "valeurGlobale",
    "montantSubventionPublique",
    "dateSignature",
    "dateDebutExecution",
    # Champs ajoutés par e-marchespublics (decp-2022)
    "offresRecues_source",
    "marcheInnovant_source",
    "attributionAvance_source",
    "sousTraitanceDeclaree_source",
    "dureeMois_source",
]

# Liste des ID de ressources présentes dans un dataset à traiter, au format JSON ou XML, mais exclues du traitement
EXCLUDED_RESOURCES = os.getenv("EXCLUDED_RESOURCES", "").replace(" ", "")
ALL_CONFIG["EXCLUDED_RESOURCES"] = EXCLUDED_RESOURCES

EXCLUDED_RESOURCES = EXCLUDED_RESOURCES.split(",")
EXCLUDED_RESOURCES = (
    [
        "17046b18-8921-486a-bc31-c9196d5c3e9c",  # decp.xml : fichier XML consolidé par le MINEF mais abandonné
        "68bd2001-3420-4d94-bc49-c90878df322c",  # decp.ocds.json : fichier au format JSON mais OCDS, pas DECP
        "59ba0edb-cf94-4bf1-a546-61f561553917",  # decp-2022.json : format bizarre, entre 2019 et 2022 ~8000 marchés
        "9c4f84d6-6fc9-4c82-a7f8-fb60d54fa188",  # données Région Bretagne inexistante (404)
        "8133b5b6-d097-4a5f-91cc-e94b11db3e2a",  # données Région Bretagne inexistante (404)
        "e10ea7c4-4992-45d8-a191-07dac6991f89",  # données Région Bretagne inexistante (404)
        "2d6dd1a6-8471-48c0-a207-6715cff06a99",  # données Région Bretagne JSON non-réglementaire
        "7629a6a1-3b8a-4570-8562-3a7cf82be88e",  # données Région Bretagne XML non-réglementaire
        "3bdd5a64-c28e-4c6a-84fd-5a28bcaa53e9",  # remplacements de chaînes de caractères dans les fichiers JSON AWS
    ]
    + EXCLUDED_RESOURCES
)

# Liste des datasets à traiter

# Ne traiter qu'un seul dataset identifier par son ID
SOLO_DATASETS = os.getenv("SOLO_DATASETS", "")
if len(SOLO_DATASETS) > 0:
    SOLO_DATASETS = SOLO_DATASETS.strip().split(",")
else:
    SOLO_DATASETS = []
ALL_CONFIG["SOLO_DATASETS"] = SOLO_DATASETS

# Acheteurs absents de la base SIRENE (pour raisons de sécurité ou autre)
# Format: SIRET -> {"nom": "...", ...}
# Ces données sont utilisées en fallback si l'acheteur n'est pas trouvé dans SIRENE
ACHETEURS_NON_SIRENE = {
    "13001536500013": {"nom": "Ministère des Armées"},
}

# ---------------------------------------------------------------------------
# BOAMP / eForms (source de deao_processing) — issue #174
# ---------------------------------------------------------------------------

BOAMP_BASE_URL = os.getenv(
    "BOAMP_BASE_URL", "https://echanges.dila.gouv.fr/OPENDATA/BOAMP"
)

# Première date traitée (incluse). Les fichiers < cette date sont ignorés.
BOAMP_START_DATE = os.getenv("BOAMP_START_DATE", "2024-01-01")

# Nombre de derniers jours re-crawlés à chaque run (rattrapage des ajouts tardifs).
BOAMP_RECRAWL_DAYS = int(os.getenv("BOAMP_RECRAWL_DAYS", "3"))

# Filtrage par NATURE (cf. spec). Inclusions directes :
BOAMP_INCLUDED_NATURES = {
    "APPEL_OFFRE",
    "QUALIFICATION",
    "RECTIFICATIF",
    "ANNULATION",
}
# Pré-informations : conservées comme telles (statut "pre-information") par deao_processing.
BOAMP_PRE_INFORMATION_NATURES = {"PRE-INFORMATION", "PERIODIQUE"}

# URL publique d'un avis (construite depuis l'IDWEB).
BOAMP_AVIS_URL_TEMPLATE = os.getenv(
    "BOAMP_AVIS_URL_TEMPLATE", 'https://www.boamp.fr/pages/avis/?q=idweb:"{idweb}"'
)

# ---------------------------------------------------------------------------
# Source JOUE (via l'API et le site TED)
# ---------------------------------------------------------------------------
TED_API_URL = os.getenv("TED_API_URL", "https://api.ted.europa.eu/v3")
TED_NOTICE_XML_URL = os.getenv(
    "TED_NOTICE_XML_URL", "https://ted.europa.eu/en/notice/{publication_number}/xml"
)
JOUE_AVIS_URL_TEMPLATE = os.getenv(
    "JOUE_AVIS_URL_TEMPLATE",
    "https://ted.europa.eu/fr/notice/-/detail/{publication_number}",
)
# Pays français (non configurable) : hors de cette liste, l'acheteur est neutralisé.
PAYS_FRANCAIS = ("FRA", "ATF", "PYF", "NCL", "WLF", "SPM", "BLM", "MAF")
# Périmètre de la requête TED : pays de l'acheteur ou du lieu d'exécution
JOUE_COUNTRY_CODES = tuple(
    os.getenv("JOUE_COUNTRY_CODES", "FRA ATF PYF NCL WLF SPM BLM MAF").split()
)
JOUE_S3_PREFIX = os.getenv("JOUE_S3_PREFIX", "joue/raw")
JOUE_MAX_WORKERS = int(os.getenv("JOUE_MAX_WORKERS", "4"))
# Débit vers TED (issue #203) : le CDN de ted.europa.eu renvoie des 429 au-delà de
# ~2 requêtes/s par IP, sans en-tête Retry-After. Intervalle minimal entre deux requêtes
# (partagé par tous les fils) et attente de base avant un nouvel essai après une 429.
JOUE_INTERVALLE_REQUETES_S = float(os.getenv("JOUE_INTERVALLE_REQUETES_S", "0.6"))
JOUE_ATTENTE_429_S = float(os.getenv("JOUE_ATTENTE_429_S", "5"))

# ---------------------------------------------------------------------------
# DEAO (flow deao_processing) — Données essentielles d'appels d'offres, issue #178
# ---------------------------------------------------------------------------

DEAO_DATA_DIR = make_path_from_env("DEAO_DATA_DIR", DATA_DIR / "deao")
DEAO_DATA_DIR.mkdir(exist_ok=True, parents=True)
DEAO_PARTITIONS_DIR = DEAO_DATA_DIR / "avis"
DEAO_PARTITIONS_DIR.mkdir(exist_ok=True, parents=True)
DEAO_DIST_DIR = make_path_from_env("DEAO_DIST_DIR", BASE_DIR / "dist" / "deao")
DEAO_DIST_DIR.mkdir(exist_ok=True, parents=True, mode=0o777)
ALL_CONFIG["DEAO_DATA_DIR"] = DEAO_DATA_DIR
ALL_CONFIG["DEAO_DIST_DIR"] = DEAO_DIST_DIR

# Préfixes S3 : archives quotidiennes des avis bruts (<préfixe>/<AAAA>/<MM>/<JJ>.tar.zst)
# et résultats de géocodage.
BOAMP_S3_PREFIX = os.getenv("BOAMP_S3_PREFIX", "boamp/raw")
DEAO_GEOCODE_S3_PREFIX = os.getenv("DEAO_GEOCODE_S3_PREFIX", "deao")


# Sources lues par le flow (séparées par des virgules) : boamp, joue.
def parse_deao_sources(valeur: str) -> tuple[str, ...]:
    """Noms de sources séparés par des virgules, dédupliqués, ordre conservé."""
    return tuple(dict.fromkeys(n.strip() for n in valeur.split(",") if n.strip()))


DEAO_SOURCES = parse_deao_sources(os.getenv("DEAO_SOURCES", "boamp,joue"))

# Reconstruire depuis les archives plutôt que de reprendre les partitions existantes.
DEAO_REBUILD = os.getenv("DEAO_REBUILD", "false").lower() == "true"

# Délai (jours) après lequel un avis sans date limite est considéré clos.
DEAO_STATUT_DELAI_JOURS = int(os.getenv("DEAO_STATUT_DELAI_JOURS", "90"))

# Garde-fou : pas de publication en dessous de ce nombre de lignes.
DEAO_MIN_ROWS_TO_PUBLISH = int(os.getenv("DEAO_MIN_ROWS_TO_PUBLISH", "50000"))

# Publication data.gouv.fr.
# a = dataset public (Open Data), b = dataset privé (toutes les sources)
DEAO_OPEN_DATASET_ID = os.getenv("DEAO_OPEN_DATASET_ID", "")
DEAO_CLOSED_DATASET_ID = os.getenv("DEAO_CLOSED_DATASET_ID", "")
DEAO_CLOSED_PARQUET_RESOURCE_ID = os.getenv("DEAO_CLOSED_PARQUET_RESOURCE_ID", "")

# Ne lancer que ces scrapers DEAO (codes séparés par des virgules) ; aucune publication
SOLO_DEAO_SCRAPS = os.getenv("SOLO_DEAO_SCRAPS", "")
if len(SOLO_DEAO_SCRAPS) > 0:
    SOLO_DEAO_SCRAPS = SOLO_DEAO_SCRAPS.strip().split(",")
else:
    SOLO_DEAO_SCRAPS = []
ALL_CONFIG["SOLO_DEAO_SCRAPS"] = SOLO_DEAO_SCRAPS


@dataclass(frozen=True)
class SourceDeao:
    code: str
    name: str
    open_data: bool
    resource_id: str
    min_rows: int
    skip: bool = False
    # Racine du site d'une plateforme (dédoublonnage BOAMP ↔ plateformes)
    url_base: str | None = None
    # Logiciel de la plateforme scrapée par scrap_closed (seul connu : "atexo")
    plateforme: str | None = None


PLATEFORMES_DEAO = {"atexo", "aws"}


def charger_registre_deao(path: Path) -> tuple[SourceDeao, ...]:
    """Charge et valide le registre YAML des sources DEAO."""
    with open(path, "r") as f:
        entrees = yaml.safe_load(f) or []
    sources = []
    codes = set()
    for entree in entrees:
        manquants = [
            c
            for c in ("code", "name", "open_data", "resource_id", "min_rows")
            if c not in entree
        ]
        if manquants:
            raise ValueError(
                f"Registre DEAO {path} : champs manquants {manquants} dans {entree}"
            )
        if entree["code"] in codes:
            raise ValueError(f"Registre DEAO {path} : code en double {entree['code']}")
        codes.add(entree["code"])
        url_base = entree.get("url_base")
        if url_base is not None and not str(url_base).startswith("https://"):
            raise ValueError(
                f"Registre DEAO {path} : url_base doit commencer par https:// "
                f"({entree['code']} : {url_base})"
            )
        plateforme = entree.get("plateforme")
        if plateforme is not None:
            if plateforme not in PLATEFORMES_DEAO:
                raise ValueError(
                    f"Registre DEAO {path} : plateforme inconnue {plateforme} "
                    f"({entree['code']})"
                )
            if url_base is None:
                raise ValueError(
                    f"Registre DEAO {path} : url_base requise pour une plateforme "
                    f"({entree['code']})"
                )
        # Code d'une plateforme = son nom d'hôte, pour relier sans ambiguïté
        # sourceDataset, uid, ressource data.gouv.fr et préfixe S3 des DCE au site.
        if url_base is not None and urlsplit(str(url_base)).hostname != entree["code"]:
            raise ValueError(
                f"Registre DEAO {path} : le code doit être le nom d'hôte de url_base "
                f"({entree['code']} : {url_base})"
            )
        sources.append(
            SourceDeao(
                code=entree["code"],
                name=entree["name"],
                open_data=bool(entree["open_data"]),
                resource_id=str(entree["resource_id"]),
                min_rows=int(entree["min_rows"]),
                skip=bool(entree.get("skip", False)),
                url_base=str(url_base) if url_base is not None else None,
                plateforme=plateforme,
            )
        )
    return tuple(sources)


DEAO_REGISTRE = charger_registre_deao(
    make_path_from_env(
        "DEAO_SOURCES_REFERENCE_FILEPATH", REFERENCE_DIR / "deao_sources.yaml"
    )
)
DEAO_PARQUET_RESOURCE_ID = os.getenv("DEAO_PARQUET_RESOURCE_ID", "")
DEAO_CSV_RESOURCE_ID = os.getenv("DEAO_CSV_RESOURCE_ID", "")
DEAO_SCHEMA_RESOURCE_ID = os.getenv("DEAO_SCHEMA_RESOURCE_ID", "")

# Scrapers des plateformes Atexo (dataset b) : intervalle minimal entre deux requêtes
# vers une même plateforme (chaque plateforme a son propre limiteur) et User-Agent.
CLOSED_INTERVALLE_REQUETES_S = float(os.getenv("CLOSED_INTERVALLE_REQUETES_S", "0.2"))
CLOSED_USER_AGENT = os.getenv(
    "CLOSED_USER_AGENT",
    "Mozilla/5.0 (X11; Linux x86_64; rv:140.0) Gecko/20100101 Firefox/140.0",
)
# Préfixe S3 des DCE archivés : <préfixe>/<code plateforme>/<id DCE ou refCons><ext>
DEAO_DCE_S3_PREFIX = os.getenv("DEAO_DCE_S3_PREFIX", "dce")
# Préfixe S3 des pages d'avis AWS archivées (privées) et des règlements de consultation
# (publics) : <préfixe>/<idm>.html et <préfixe RC>/<code plateforme>/<idm>.pdf
DEAO_AWS_S3_PREFIX = os.getenv("DEAO_AWS_S3_PREFIX", "www.marches-publics.info/raw")
DEAO_RC_S3_PREFIX = os.getenv("DEAO_RC_S3_PREFIX", "rc")
# Vrai : reconstruire les lignes AWS depuis les pages d'avis archivées dans S3 (toutes
# les consultations listées, sans requête au site pour les pages archivées)
DEAO_AWS_RELIRE_ARCHIVE = (
    os.getenv("DEAO_AWS_RELIRE_ARCHIVE", "false").lower() == "true"
)
# Base des URL publiques des DCE ; vide = <S3_ENDPOINT_URL>/<S3_BUCKET> (voir template.env)
DEAO_DCE_URL_PUBLIQUE = os.getenv("DEAO_DCE_URL_PUBLIQUE", "")
# Repartir de zéro quand le fichier publié d'une plateforme est non conforme (fichier
# provisoire) ; une indisponibilité de data.gouv.fr reste toujours une erreur.
DEAO_SCRAP_PARTIR_DE_ZERO = (
    os.getenv("DEAO_SCRAP_PARTIR_DE_ZERO", "false").lower() == "true"
)

with open(
    make_path_from_env(
        "DATASETS_REFERENCE_FILEPATH", REFERENCE_DIR / "source_datasets.json"
    ),
    "r",
) as f:
    tracked_datasets_complete = json.load(f)
TRACKED_DATASETS = []
for dataset in tracked_datasets_complete:
    if (len(SOLO_DATASETS) == 0 and not (dataset.get("skip"))) or dataset[
        "id"
    ] in SOLO_DATASETS:
        logger.debug(f"{dataset['name']} added.")
        TRACKED_DATASETS.append(dataset)


# Source attribuée aux lignes consolidées à partir de plusieurs datasets
# (coalesce inter-dataset d'une même version de marché). Comme ces lignes sont
# un mélange de sources, on ne peut pas honnêtement conserver un dataset d'origine.
DECP_COLMO_DATASET = "decp_colmo"
DECP_COLMO_RESOURCE_URL = (
    "https://www.data.gouv.fr/api/1/datasets/r/11cea8e8-df3e-4ed1-932b-781e2635e432"
)


def check_s3_config() -> list:
    return [
        name
        for name, value in [
            ("S3_ENDPOINT_URL", S3_ENDPOINT_URL),
            ("S3_BUCKET", S3_BUCKET),
            ("S3_ACCESS_KEY_ID", S3_ACCESS_KEY_ID),
            ("S3_SECRET_ACCESS_KEY", S3_SECRET_ACCESS_KEY),
            ("S3_REGION", S3_REGION),
        ]
        if not value
    ]


SIRET_LATLONG_SCHEMA = {
    "siret": pl.String,
    "latitude": pl.Float64,
    "longitude": pl.Float64,
    "source": pl.String,
    "score": pl.Float64,
    "geocoded_at": pl.Date,
    "status": pl.String,
}


@dataclass
class DecpFormat:
    label: str
    schema: dict
    prefixe_json_marches: str
    liste_marches_ijson: sendable_list | None = None
    coroutine_ijson: Coroutine | None = None


# === Serveur éphémère Scaleway (issue #189) ===

# Clé privée SSH du serveur actuel, utilisée pour se connecter à l'instance
# éphémère (rsync du répertoire dist et lecture du boot log). dotenv n'expanse
# pas `~` : le résultat est donc expansé explicitement.
SCALEWAY_SSH_KEY_PATH = make_path_from_env(
    "SCALEWAY_SSH_KEY_PATH", Path("~/.ssh/id_ed25519")
).expanduser()

# Clé publique correspondante, injectée dans le user-data de l'instance
# éphémère. Par défaut : le fichier .pub voisin de la clé privée. Elle est lue
# directement, sans passer par `ssh-keygen -y` (qui exige une clé privée sans
# passphrase dans un environnement non interactif).
SCALEWAY_SSH_PUBLIC_KEY_PATH = make_path_from_env(
    "SCALEWAY_SSH_PUBLIC_KEY_PATH",
    SCALEWAY_SSH_KEY_PATH.parent / f"{SCALEWAY_SSH_KEY_PATH.name}.pub",
).expanduser()

# Volume de données Scaleway contenant le cache, par environnement.
SCALEWAY_DATA_VOLUME_NAMES = {"prod": "decp-data-prod", "dev": "decp-data-dev"}

# Variables d'environnement du serveur actuel transmises à l'instance éphémère
# via le user-data. Leurs valeurs sont des secrets : elles sont masquées dans
# les logs (secret_strings) et ne doivent jamais être journalisées.
INSTANCE_ENV_VARS = [
    "PREFECT_API_URL",
    "PREFECT_API_AUTH_STRING",
    "DATAGOUVFR_API_KEY",
    "S3_ENDPOINT_URL",
    "S3_BUCKET",
    "S3_ACCESS_KEY_ID",
    "S3_SECRET_ACCESS_KEY",
    "S3_REGION",
    "SOLO_DATASETS",
]
