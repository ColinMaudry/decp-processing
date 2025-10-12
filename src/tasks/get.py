import json
import tempfile
from collections.abc import Iterator
from functools import partial
from pathlib import Path
from time import sleep

import httpx
import ijson
import orjson
import polars as pl
from bs4 import BeautifulSoup
from httpx import get, stream
from lxml import etree
from prefect import task

from config import (
    DECP_FORMAT_2019,
    DECP_FORMATS,
    DECP_PROCESSING_PUBLISH,
    DIST_DIR,
    DecpFormat,
)
from tasks.clean import clean_invalid_characters, extract_innermost_struct
from tasks.output import sink_to_files
from tasks.publish import publish_scrap_to_datagouv
from tasks.utils import gen_artifact_row, stream_replace_bytestring


@task(retries=3, retry_delay_seconds=3)
def stream_get(url: str, chunk_size=1024**2):  # chunk_size en octets (1 Mo par défaut)
    if url.startswith("http"):
        with stream("GET", url, follow_redirects=True) as response:
            yield from response.iter_bytes(chunk_size)
    else:
        # Données de test.
        with open(url, "rb") as f:
            for chunk in iter(partial(f.read, chunk_size), b""):
                yield chunk


@task(persist_result=False)
def get_resource(
    r: dict, resources_artifact: list[dict] | list
) -> tuple[pl.LazyFrame | None, DecpFormat | None]:
    decp_formats: list[DecpFormat] = DECP_FORMATS

    print(f"➡️  {r['ori_filename']} ({r['dataset_name']})")

    output_path = DIST_DIR / "get" / r["filename"]
    output_path.parent.mkdir(exist_ok=True)
    url = r["url"]
    file_format = r["format"]
    if file_format == "json":
        fields, decp_format = json_stream_to_parquet(url, output_path, decp_formats)
    elif file_format == "xml":
        try:
            fields, decp_format = xml_stream_to_parquet(
                url, output_path, fix_chars=False
            )
        except etree.XMLSyntaxError:
            fields, decp_format = xml_stream_to_parquet(
                url, output_path, fix_chars=True
            )
            print(f"♻️  {r['ori_filename']} nettoyé et traité")
    else:
        print(
            f"▶️  Format de fichier non supporté : {file_format} ({r['dataset_name']})"
        )
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


def find_json_decp_format(chunk, decp_formats):
    for decp_format in decp_formats:
        decp_format.coroutine_ijson.send(chunk)
        if len(decp_format.liste_marches_ijson) > 0:
            # Le parser a trouvé au moins un marché correspondant à ce format, donc on a
            # trouvé le bon format.
            return decp_format
    raise ValueError("Pas de match trouvé parmis les schémas passés")


@task(persist_result=False)
def json_stream_to_parquet(
    url: str, output_path: Path, decp_formats: list[DecpFormat] | None = None
) -> tuple[set, DecpFormat]:
    if decp_formats is None:
        decp_formats: list[DecpFormat] = DECP_FORMATS

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
    )

    # In first iteration, will find the right format
    chunk = next(stream_replace_iter)

    decp_format = find_json_decp_format(chunk, decp_formats)

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


@task(persist_result=False)
def xml_stream_to_parquet(
    url: str, output_path: Path, fix_chars=False
) -> tuple[set, DecpFormat]:
    # Pour l'instant tous les fichiers XML (AIFE), sont au format 2019, donc pas de détection.
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
                _, marche = xml_to_dict(elem)
                new_fields = write_marche_rows(marche, tmp_file, DECP_FORMAT_2019)
                fields = fields.union(new_fields)
        lf = pl.scan_ndjson(tmp_file.name, schema=DECP_FORMAT_2019.schema)
        sink_to_files(lf, output_path, file_format="parquet")
    return fields, DECP_FORMAT_2019


def xml_to_dict(element: etree.Element):
    return element.tag, dict(map(xml_to_dict, element)) or element.text


def write_marche_rows(marche: dict, file, decp_format: DecpFormat) -> set[str]:
    """Ajout d'une ligne ndjson pour chaque modification/version du marché."""
    fields = set()
    for mod in yield_modifications(marche):
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


def yield_modifications(row: dict, separator="_") -> Iterator[dict]:
    """Pour chaque modification, génère un objet/dict marché aplati."""
    raw_mods = row.pop("modifications", [])
    # Couvre le format 2022:
    if isinstance(raw_mods, dict) and "modification" in raw_mods:
        raw_mods = raw_mods["modification"]
    # Couvre le (non-)format dans lequel "modifications" ou "modification" mène
    # directement à un dict contenant les métadonnées liées à une modification.
    if isinstance(raw_mods, dict):
        raw_mods = [raw_mods]

    raw_mods = [] if raw_mods is None else raw_mods

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


def norm_titulaire(titulaire: dict):
    if "titulaire" in titulaire:
        titulaire = titulaire["titulaire"]
    return titulaire


def get_html(url: str, root: str = "") -> str or None:
    def get_response() -> httpx.Response:
        return get(url, timeout=timeout).raise_for_status()

    if url.startswith("/"):
        if root == "":
            print("Root not specified and URL starts with /")
            raise ValueError
        url = root + url
    timeout = httpx.Timeout(20.0, connect=60.0, pool=20.0, read=20.0)
    try:
        response = get_response()
    except (httpx.ConnectError, httpx.ReadTimeout, httpx.HTTPStatusError):
        print("3s break and retrying...")
        sleep(3)
        try:
            response = get_response()
        except (httpx.ConnectError, httpx.ReadTimeout, httpx.HTTPStatusError):
            print("Skipped")
            return None

    html = response.text
    sleep(0.1)
    return html


@task(log_prints=True)
def scrap_marches_securises_month(year: str, month: str) -> list:
    marches = []
    page = 1
    while True:
        print("Year: ", year, "Month: ", month, "Page: ", str(page))

        search_url = (
            f"https://www.marches-securises.fr/entreprise/?module=liste_donnees_essentielles&page={str(page)}&siret_pa=&siret_pa1=&date_deb={year}-{month}-01&date_fin={year}-{month}-31&date_deb_ms={year}-{month}-01&date_fin_ms={year}-{month}-31&ref_ume=&cpv_et=&type_procedure=&type_marche=&objet=&rs_oe=&dep_liste=&ctrl_key=aWwwS1pLUlFzejBOYitCWEZzZTEzZz09&text=&donnees_essentielles=1&search="
            f"table_ms&"
        )
        html_result_page = get_html(search_url)
        soup = BeautifulSoup(html_result_page, "html.parser")
        result_div = soup.find("div", attrs={"id": "liste_consultations"})
        json_links = result_div.find_all(
            "a", attrs={"title": "Télécharger au format Json"}
        )
        if json_links is None:
            break
        else:
            page += 1
        for json_link in json_links:
            json_href = "https://www.marches-securises.fr" + json_link["href"]
            print(json_href)
            json_html_page = get_html(json_href)
            if json_html_page:
                json_html_page = (
                    json_html_page.replace("</head>", "</head><body>")
                    + "</body></html>"
                )
            else:
                "json_html_page is None, skipping..."
                continue
            json_html_page_soup = BeautifulSoup(json_html_page, "html.parser")
            try:
                decp_json = json.loads(json_html_page_soup.find("body").string)
            except Exception as e:
                print(json_html_page)
                print(e)
                continue
            marches.append(decp_json)
    dicts = {"marches": marches}
    json_path = DIST_DIR / f"marches-securises_{year}-{month}.json"
    with open(json_path, "w") as f:
        f.write(json.dumps(dicts))
    publish_scrap_to_datagouv(year, month, json_path)
    return marches
