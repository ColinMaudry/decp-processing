import tempfile
from collections.abc import Iterator
from functools import partial
from pathlib import Path

import ijson
import orjson
import polars as pl
from httpx import stream
from lxml import etree
from prefect import task

from config import DATE_NOW, DIST_DIR, SCHEMA_DECP_2019, SCHEMAS_DECP, SchemaDECP
from tasks.output import sink_to_files


@task(retries=3, retry_delay_seconds=3)
def stream_get(url: str, chunk_size=1024**2):
    if url.startswith("http"):
        with stream("GET", url, follow_redirects=True) as response:
            yield from response.iter_bytes(chunk_size)
    else:
        # Données de test.
        with open(url, "rb") as f:
            for chunk in iter(partial(f.read, chunk_size), b""):
                yield chunk


@task
def get_resource(
    r: dict, decp_schemas: list[SchemaDECP] | None = None
) -> pl.LazyFrame | None:
    if decp_schemas is None:
        decp_schemas = SCHEMAS_DECP

    print(f"➡️  {r['ori_filename']} ({r['dataset_name']})")

    if r["filesize"] < 100:
        print(f"🗑️  {r['ori_filename']} - Ressource ignorée : inférieure à 100 octets.")
        return None

    output_path = DIST_DIR / "get" / r["filename"]
    output_path.parent.mkdir(exist_ok=True)
    url = r["url"]
    file_format = r["format"]
    if file_format == "json":
        fields, decp_schema = json_stream_to_parquet(url, output_path, decp_schemas)
    elif file_format == "xml":
        fields, decp_schema = xml_stream_to_parquet(url, output_path, decp_schemas)
    else:
        print(f"▶️ Format de fichier non supporté : {file_format} ({r['dataset_name']})")
        return None

    lf = pl.scan_parquet(output_path.with_suffix(".parquet"))

    # TODO: do something with it
    artifact_row = gen_artifact_row(r, lf, url, fields)  # noqa

    # Exemple https://www.data.gouv.fr/datasets/5cd57bf68b4c4179299eb0e9/#/resources/bb90091c-f0cb-4a59-ad41-b0ab929aad93
    resource_web_url = (
        f"https://www.data.gouv.fr/datasets/{r['dataset_id']}/#/resources/{r['id']}"
    )
    lf = lf.with_columns(
        pl.lit(resource_web_url).alias("sourceOpenData"),
        pl.lit(r["dataset_code"]).alias("source"),
    )
    return lf


def find_json_format(chunk, decp_formats):
    for fmt in decp_formats:
        fmt.coroutine_ijson.send(chunk)
        if len(fmt.liste_marches_ijson) > 0:
            # Le parser a trouvé au moins un marché correspondant à ce format, donc on a
            # trouvé le bon format.
            return fmt
    raise ValueError("Pas de match trouvé parmis les formats passés")


@task(persist_result=False)
def json_stream_to_parquet(
    url: str, output_path: Path, decp_schemas: list[SchemaDECP] | None = None
) -> tuple[SchemaDECP, set[str]]:
    if decp_schemas is None:
        decp_schemas = SCHEMAS_DECP

    fields = set()
    for fmt in decp_schemas:
        fmt.liste_marches_ijson = ijson.sendable_list()
        fmt.coroutine_ijson = ijson.items_coro(
            fmt.liste_marches_ijson, f"{fmt.prefixe_json_marches}.item", use_float=True
        )

    with tempfile.NamedTemporaryFile(mode="wb", suffix=".ndjson") as tmp_file:
        chunk_iter = stream_get(url)

        # In first iteration, will find the right format
        chunk = next(chunk_iter)
        chunk = chunk.replace(b"NaN,", b"null,")

        decp_schema = find_json_format(chunk, decp_schemas)

        for marche in decp_schema.liste_marches_ijson:
            new_fields = write_marche_rows(marche, tmp_file)
            fields = fields.union(new_fields)

        del decp_schema.liste_marches_ijson[:]

        for chunk in chunk_iter:
            chunk = chunk.replace(b"NaN,", b"null,")

            decp_schema.coroutine_ijson.send(chunk)
            for marche in decp_schema.liste_marches_ijson:
                new_fields = write_marche_rows(marche, tmp_file)
                fields = fields.union(new_fields)

            del decp_schema.liste_marches_ijson[:]

        decp_schema.coroutine_ijson.close()

        lf = pl.scan_ndjson(tmp_file.name, schema=decp_schema.schema)

        sink_to_files(lf, output_path, file_format="parquet")

    return fields, decp_schema


@task(persist_result=False)
def xml_stream_to_parquet(url: str, output_path: Path) -> tuple[SchemaDECP, set[str]]:
    fields = set()
    parser = etree.XMLPullParser(tag="marche")
    with tempfile.NamedTemporaryFile(mode="wb", suffix=".ndjson") as tmp_file:
        for chunk in stream_get(url):
            parser.feed(chunk)
            for _, elem in parser.read_events():
                _, marche = xml_to_dict(elem)
                new_fields = write_marche_rows(marche, tmp_file)
                fields = fields.union(new_fields)
        lf = pl.scan_ndjson(tmp_file.name, schema=SCHEMA_DECP_2019.schema)
        sink_to_files(lf, output_path, file_format="parquet")
    return fields, SCHEMA_DECP_2019


def xml_to_dict(element: etree.Element):
    return element.tag, dict(map(xml_to_dict, element)) or element.text


def write_marche_rows(marche: dict, file) -> set[str]:
    """Ajout d'une ligne ndjson pour chaque modification/version du marché."""
    fields = set()
    for mod in yield_modifications(marche):
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


def gen_artifact_row(file_info: dict, lf: pl.LazyFrame, url: str, fields: set[str]):
    artifact_row = {
        "open_data_dataset_id": file_info["dataset_id"],
        "open_data_dataset_name": file_info["dataset_name"],
        "download_date": DATE_NOW,
        "data_fields": list(fields),
        "data_fields_number": len(fields),
        "row_number": lf.select(pl.len()).collect().item(),
    }

    # TODO trouver un meilleur nom : c'est les métadonnées de ressource data.gouv.fr
    online_artifact_row = {
        "open_data_filename": file_info["ori_filename"],
        "open_data_id": file_info["id"],
        "sha1": file_info["checksum"],
        "created_at": file_info["created_at"],
        "last_modified": file_info["last_modified"],
        "filesize": file_info["filesize"],
        "views": file_info["views"],
    }
    if url.startswith("http"):
        artifact_row |= online_artifact_row
    return artifact_row
