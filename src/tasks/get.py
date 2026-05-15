import tempfile
from collections.abc import Iterator
from functools import partial
from pathlib import Path
from time import sleep

import boto3
import httpx
import ijson
import orjson
import polars as pl
from botocore.config import Config
from botocore.exceptions import ClientError
from lxml import etree
from prefect.transactions import transaction
from tenacity import (
    retry,
    stop_after_attempt,
    wait_exponential,
)

from src.config import (
    DATA_DIR,
    DECP_PROCESSING_PUBLISH,
    DECP_USE_CACHE,
    HTTP_CLIENT,
    HTTP_HEADERS,
    LOG_LEVEL,
    RESOURCE_CACHE_DIR,
    S3_ACCESS_KEY_ID,
    S3_BUCKET,
    S3_ENDPOINT_URL,
    S3_REGION,
    S3_SECRET_ACCESS_KEY,
    SIRENE_ETABLISSEMENTS_URL,
    SIRENE_UNITES_LEGALES_URL,
    DecpFormat,
    check_s3_config,
)
from src.schemas import SCHEMA_MARCHE_2019, SCHEMA_MARCHE_2022
from src.tasks.clean import (
    clean_decp,
    clean_invalid_characters,
    extract_innermost_struct,
)
from src.tasks.output import sink_to_files
from src.tasks.publish import publish_to_s3
from src.tasks.transform import prepare_unites_legales
from src.tasks.utils import (
    full_resource_name,
    gen_artifact_row,
    get_logger,
    stream_replace_bytestring,
)


@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=1, max=20))
def stream_get(url: str, chunk_size=1024**2):  # chunk_size en octets (1 Mo par défaut)
    logger = get_logger(level=LOG_LEVEL)

    if url.startswith("http"):
        try:
            with HTTP_CLIENT.stream(
                "GET", url, headers=HTTP_HEADERS, follow_redirects=True, timeout=20
            ) as response:
                yield from response.iter_bytes(chunk_size)
        except httpx.TooManyRedirects:
            logger.error(f"⛔️ Erreur 429 Too Many Requests pour {url}")
            return

    else:
        # Données de test.
        with open(url, "rb") as f:
            for chunk in iter(partial(f.read, chunk_size), b""):
                yield chunk


def get_resource(
    r: dict, resources_artifact: list[dict] | list
) -> tuple[pl.LazyFrame | None, DecpFormat | None]:
    logger = get_logger(level=LOG_LEVEL)

    if DECP_PROCESSING_PUBLISH is False:
        logger.info(f"➡️  {full_resource_name(r)}")

    output_path = DATA_DIR / "get" / r["filename"]
    output_path.parent.mkdir(exist_ok=True, parents=True)
    url = r["url"]
    file_format = r["format"]
    if file_format == "json":
        fields, decp_format = json_stream_to_parquet(url, output_path, r)
    elif file_format == "xml":
        try:
            fields, decp_format = xml_stream_to_parquet(
                url, output_path, fix_chars=False
            )
        except etree.XMLSyntaxError:
            fields, decp_format = xml_stream_to_parquet(
                url, output_path, fix_chars=True
            )
            logger.info(f"♻️  {full_resource_name(r)} nettoyé et traité")
    else:
        logger.warning(f"▶️  Format de fichier non supporté : {full_resource_name(r)}")
        return None, None

    if decp_format is None:
        return None, None

    lf: pl.LazyFrame = pl.scan_parquet(output_path.with_suffix(".parquet"))

    # Ajout des stats de la ressource à l'artifact
    # https://github.com/ColinMaudry/decp-processing/issues/89
    if DECP_PROCESSING_PUBLISH:
        artifact_row = gen_artifact_row(r, lf, url, fields, decp_format)  # noqa
        resources_artifact.append(artifact_row)

    # Exemple https://www.data.gouv.fr/datasets/5cd57bf68b4c4179299eb0e9/#/resources/bb90091c-f0cb-4a59-ad41-b0ab929aad93
    resource_web_url = (
        f"https://www.data.gouv.fr/datasets/{r['dataset_id']}/#/resources/{r['id']}"
    )

    lf = lf.with_columns(pl.lit(resource_web_url).alias("sourceFile"))

    if r["dataset_code"] == "decp_minef":
        lf = lf.with_columns(
            (pl.lit("decp_minef_") + pl.col("source")).alias("sourceDataset")
        )
        lf = lf.drop("source")
    else:
        lf = lf.rename({"source": "sourceDataset"})
        lf = lf.with_columns(pl.lit(r["dataset_code"]).alias("sourceDataset"))

    return lf, decp_format


def find_json_decp_format(chunk, decp_formats, resource: dict):
    logger = get_logger(level=LOG_LEVEL)

    for decp_format in decp_formats:
        decp_format.coroutine_ijson.send(chunk)
        if len(decp_format.liste_marches_ijson) > 0:
            # Le parser a trouvé au moins un marché correspondant à ce format, donc on a
            # trouvé le bon format.
            return decp_format
    logger.warning(
        f"⚠️  Pas de match trouvé parmis les schémas passés : {full_resource_name(resource)}"
    )
    return None


def json_stream_to_parquet(
    url: str, output_path: Path, resource: dict
) -> tuple[set, DecpFormat or None]:
    logger = get_logger(level=LOG_LEVEL)

    decp_format_2019 = DecpFormat("DECP 2019", SCHEMA_MARCHE_2019, "marches")
    decp_format_2022 = DecpFormat("DECP 2022", SCHEMA_MARCHE_2022, "marches.marche")
    decp_formats = [decp_format_2019, decp_format_2022]

    fields = set()
    for decp_format in decp_formats:
        decp_format.liste_marches_ijson = ijson.sendable_list()
        decp_format.coroutine_ijson = ijson.items_coro(
            decp_format.liste_marches_ijson,
            f"{decp_format.prefixe_json_marches}.item",
            use_float=True,
        )

    tmp_file = tempfile.NamedTemporaryFile(mode="wb", suffix=".ndjson", delete=False)

    http_stream_iter = stream_get(url)

    stream_replace_iter = stream_replace_bytestring(
        http_stream_iter, rb"NaN([,\n])", rb"null\1"
    )  # Nan => null

    # Le dataset AWS scraping a pas mal de bugs de backslash
    if "/68caf6b135f19236a4f37a32/" in url or "/aws/" in url:
        stream_replace_iter = stream_replace_bytestring(
            stream_replace_bytestring(
                stream_replace_bytestring(stream_replace_iter, rb"(\\\\\\)", rb"\\"),
                rb"\\\\",
                rb"\\",
            ),
            rb"\\ ",
            rb" ",
        )

    # In first iteration, will find the right format
    try:
        chunk = next(stream_replace_iter)
    except StopIteration:
        logger.error(f"⚠️  Flux vide pour {url}")
        return set(), None

    decp_format = find_json_decp_format(chunk, decp_formats, resource)
    if decp_format is None:
        return set(), None

    for marche in decp_format.liste_marches_ijson:
        new_fields = write_marche_rows(marche, tmp_file, decp_format)
        fields = fields.union(new_fields)

    del decp_format.liste_marches_ijson[:]

    for chunk in stream_replace_iter:
        decp_format.coroutine_ijson.send(chunk)
        for marche in decp_format.liste_marches_ijson:
            new_fields = write_marche_rows(marche, tmp_file, decp_format)
            fields = fields.union(new_fields)

        del decp_format.liste_marches_ijson[:]

    decp_format.coroutine_ijson.close()
    tmp_file.seek(0)

    lf = pl.scan_ndjson(tmp_file.name, schema=decp_format.schema)
    sink_to_files(lf, output_path, file_format="parquet")

    tmp_file.close()

    return fields, decp_format


def xml_stream_to_parquet(
    url: str, output_path: Path, fix_chars=False
) -> tuple[set, DecpFormat]:
    """Uniquement utilisé pour les données publiées par l'AIFE."""

    decp_format_2022 = DecpFormat("DECP 2022", SCHEMA_MARCHE_2022, "marches.marche")

    fields = set()
    parser = etree.XMLPullParser(tag="marche", recover=True)
    with tempfile.NamedTemporaryFile(
        mode="wb", suffix=".ndjson", delete=True
    ) as tmp_file:
        for chunk in stream_get(url):
            if fix_chars:
                chunk = clean_invalid_characters(chunk)
            parser.feed(chunk)
            for _, elem in parser.read_events():
                marche = parse_element(elem)
                new_fields = write_marche_rows(marche, tmp_file, decp_format_2022)
                fields = fields.union(new_fields)
        lf = pl.scan_ndjson(tmp_file.name, schema=decp_format_2022.schema)
        sink_to_files(lf, output_path, file_format="parquet")
    return fields, decp_format_2022


# Générée par la LLM Euria, développée par Infomaniak
def parse_element(elem):
    """
    Parse un élément XML en dictionnaire Python.
    Pour les tags comme <modalitesExecution>, <considerationsSociales>, etc.,
    on conserve la structure : {"modaliteExecution": [...]} au lieu de [...] (format 2022)
    """

    # Si l'élément n'a ni enfants ni texte → retourne None
    if len(elem) == 0 and not elem.text:
        return None

    # Si l'élément n'a pas d'enfants → retourne son texte (nettoyé)
    if len(elem) == 0:
        return elem.text.strip() if elem.text else ""

    # Collecte les enfants sous forme de listes (même si un seul enfant)
    children = {}
    for child in elem:
        tag = child.tag
        if tag not in children:
            children[tag] = []
        children[tag].append(parse_element(child))

    # Cas spéciaux : ces éléments doivent toujours être des objets avec une clé liste
    if elem.tag == "titulaires":
        # Chaque <titulaire> devient un objet dans une liste
        return [{"titulaire": item} for item in children.get("titulaire", [])]

    # Pour les tags comme <considerationsSociales>, <modalitesExecution>, etc.
    # on utilise le premier tag enfant existant
    elif elem.tag in [
        "considerationsSociales",
        "considerationsEnvironnementales",
        "techniques",
        "modalitesExecution",
        "typesPrix",
    ]:
        # On récupère le premier tag enfant (s’il existe)
        if children:
            # On prend le premier tag enfant comme clé
            first_child_tag = next(iter(children.keys()))
            # On retourne un objet avec cette clé → valeur = liste des enfants
            return {first_child_tag: children[first_child_tag]}
        else:
            # Si pas d'enfant → retourne un objet vide
            return {next(iter(children.keys())) if children else "": []}

    # Pour tous les autres éléments : si un seul enfant → valeur simple, sinon liste
    result = {}
    for tag, values in children.items():
        if len(values) == 1:
            result[tag] = values[0]  # Pas de liste si un seul élément
        else:
            result[tag] = values  # Sinon, on garde la liste

    return result


def write_marche_rows(marche: dict, file, decp_format: DecpFormat) -> set[str]:
    """Ajout d'une ligne ndjson pour chaque modification/version du marché."""
    fields = set()
    if marche:  # marche peut être null (marches-securises.fr)
        for mod in yield_modifications(marche):
            if mod is None:
                continue
            # Pour decp-2019.json : désimbrication des données des titulaires
            # voir https://github.com/ColinMaudry/decp-processing/issues/114
            # complète probablement norm_titulaires(), qui ne faisait pas complètement le taff, donc à fusionner
            if decp_format.label == "DECP 2019":
                for f in ["titulaires", "modification_titulaires"]:
                    liste_titulaires = mod.get(f)
                    if liste_titulaires and isinstance(liste_titulaires[0], list):
                        mod[f] = extract_innermost_struct(liste_titulaires)
            file.write(orjson.dumps(mod))
            file.write(b"\n")
            fields = fields.union(mod.keys())
    return fields


def yield_modifications(row: dict, separator="_") -> Iterator[dict] or None:
    """Pour chaque modification, génère un objet/dict marché aplati."""
    raw_mods = row.pop("modifications", [])
    # Couvre le format 2022:
    if isinstance(raw_mods, dict) and "modification" in raw_mods:
        raw_mods = raw_mods["modification"]
    # Couvre le (non-)format dans lequel "modifications" ou "modification" mène
    # directement à un dict contenant les métadonnées liées à une modification.
    if isinstance(raw_mods, dict):
        raw_mods = [raw_mods]
    elif isinstance(raw_mods, str) or raw_mods is None:
        raw_mods = []

    mods = [{}] + raw_mods
    for i, mod in enumerate(mods):
        mod["id"] = i
        if "modification" in mod:
            mod = mod["modification"]
        titulaires = norm_titulaires(mod)
        if titulaires is not None:
            mod["titulaires"] = titulaires
        row["modification"] = mod
        yield pl.convert.normalize._simple_json_normalize(
            row, separator, 10, lambda x: x
        )


def norm_titulaires(titulaires):
    """
    Corrige les blocs titulaires imbriqués dans n niveaux de listes.

    :param titulaires:
    :return: titulaires:
    """
    if isinstance(titulaires, list):
        titulaires_clean = []
        for t in titulaires:
            if isinstance(t, dict):
                titulaires_clean.append(norm_titulaire(t))
            elif isinstance(t, list):
                # Traite les listes de titulaires écrites en listes de listes.
                for inner_t in t:
                    if isinstance(inner_t, dict):
                        titulaires_clean.append(norm_titulaire(inner_t))
        return titulaires_clean
    return None


# Récupération des données des établissements
def norm_titulaire(titulaire: dict):
    if "titulaire" in titulaire:
        titulaire = titulaire["titulaire"]
    return titulaire


def get_etablissements() -> pl.LazyFrame:
    columns = [
        "siret",
        "codeCommuneEtablissement",
        "activitePrincipaleEtablissement",
        "nomenclatureActivitePrincipaleEtablissement",
        "enseigne1Etablissement",
        "denominationUsuelleEtablissement",
        "libelleVoieEtablissement",
        "typeVoieEtablissement",
        "numeroVoieEtablissement",
        "indiceRepetitionEtablissement",
        "codePostalEtablissement",
        "libelleCommuneEtablissement",
    ]

    lf_etablissements = pl.scan_parquet(SIRENE_ETABLISSEMENTS_URL)
    lf_etablissements = lf_etablissements.select(columns)
    lf_etablissements = lf_etablissements.with_columns()

    return lf_etablissements


def get_insee_cog_data(url, schema_overrides, columns) -> pl.DataFrame:
    logger = get_logger(level=LOG_LEVEL)

    try:
        df_insee = pl.read_csv(url, schema_overrides=schema_overrides, columns=columns)
    except ConnectionResetError:
        logger.error("Connection error, retrying in 2 seconds...")
        sleep(2)
        df_insee = get_insee_cog_data(
            url, schema_overrides=schema_overrides, columns=columns
        )
    return df_insee


def get_clean(
    resource, resources_artifact: list, available_parquet_files: set
) -> pl.DataFrame or None:
    logger = get_logger(level=LOG_LEVEL)

    with transaction():
        checksum = resource["checksum"]
        parquet_path = RESOURCE_CACHE_DIR / f"{checksum}"

        # Si la ressource n'est pas en cache ou que l'utilisation du cache est désactivée
        if (
            DECP_USE_CACHE is False
            or f"{checksum}.parquet" not in available_parquet_files
        ):
            # Récupération des données source...
            lf, decp_format = get_resource(resource, resources_artifact)

            # Nettoyage des données source et typage des colonnes...
            # si la ressource est dans un format supporté
            if lf is not None and not Path(parquet_path).exists():
                lf: pl.LazyFrame = clean_decp(lf, decp_format)
                sink_to_files(
                    lf, parquet_path, file_format="parquet", compression="zstd"
                )
                return parquet_path.with_suffix(".parquet")
            else:
                return None
        else:
            # Le fichier parquet est déjà disponible pour ce checksum
            logger.debug(f"👍 Ressource déjà en cache : {resource['dataset_code']}")
            return parquet_path.with_suffix(".parquet")


def bootstrap_siret_latlong() -> pl.LazyFrame:
    """Crée siret_latlong.parquet à partir des coordonnées présentes dans
    decp.parquet publié sur data.gouv.fr.

    Utilisé quand siret_latlong.parquet n'existe pas encore sur S3.
    """
    logger = get_logger(level=LOG_LEVEL)
    output_path = DATA_DIR / "siret_latlong.parquet"

    decp_url = (
        "https://www.data.gouv.fr/datasets/r/11cea8e8-df3e-4ed1-932b-781e2635e432"
    )
    logger.info(f"Bootstrap de siret_latlong depuis {decp_url}...")

    lf = pl.scan_parquet(decp_url)

    acheteurs = lf.select(
        pl.col("acheteur_id").alias("siret"),
        pl.col("acheteur_latitude").alias("latitude"),
        pl.col("acheteur_longitude").alias("longitude"),
    )
    titulaires = lf.select(
        pl.col("titulaire_id").alias("siret"),
        pl.col("titulaire_latitude").alias("latitude"),
        pl.col("titulaire_longitude").alias("longitude"),
    )

    output_path.parent.mkdir(exist_ok=True, parents=True)
    (
        pl.concat([acheteurs, titulaires])
        .filter(
            pl.col("siret").is_not_null()
            & (pl.col("siret").str.len_chars() == 14)
            & pl.col("latitude").is_not_null()
            & pl.col("longitude").is_not_null()
        )
        .unique(subset=["siret"], keep="first")
        .sink_parquet(output_path)
    )

    publish_to_s3(output_path)

    return pl.scan_parquet(output_path)


def get_from_s3(key: str, prefix: str = "") -> pl.LazyFrame | None:
    logger = get_logger(level=LOG_LEVEL)

    missing = check_s3_config()

    if missing:
        raise ValueError(
            f"Variables d'environnement S3 non définies : {', '.join(missing)}"
        )

    full_key = f"{prefix.strip('/')}/{key}" if prefix else key
    full_s3_path = f"s3://{S3_ENDPOINT_URL}/{S3_BUCKET}/{full_key}"
    local_path = DATA_DIR / "s3" / full_key
    local_path.parent.mkdir(exist_ok=True, parents=True)

    client = boto3.client(
        "s3",
        endpoint_url=S3_ENDPOINT_URL,
        region_name=S3_REGION,
        aws_access_key_id=S3_ACCESS_KEY_ID,
        aws_secret_access_key=S3_SECRET_ACCESS_KEY,
        config=Config(
            signature_version="s3v4",
            s3={"addressing_style": "path"},
        ),
    )

    logger.info(f"Téléchargement de {full_s3_path}...")
    try:
        client.download_file(S3_BUCKET, full_key, str(local_path))
    except ClientError as e:
        if e.response.get("Error", {}).get("Code") in ("404", "NoSuchKey"):
            logger.info(f"Fichier absent sur S3 : {full_s3_path}")
            return None

    return pl.scan_parquet(local_path)


def get_unite_legales(processed_parquet_path):
    logger = get_logger(level=LOG_LEVEL)

    logger.info("Téléchargement des données unité légales et sélection des colonnes...")
    (
        pl.scan_parquet(SIRENE_UNITES_LEGALES_URL)
        .pipe(prepare_unites_legales)
        .sink_parquet(processed_parquet_path)
    )
