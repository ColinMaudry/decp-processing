import tempfile
from functools import partial
from pathlib import Path

import ijson
import orjson
import polars as pl
from httpx import stream
from lxml import etree
from prefect import task

from config import DATE_NOW, DIST_DIR, FORMAT_DECP_2019, FORMATS_DECP, FormatDECP
from tasks.output import sink_to_files


@task(retries=3, retry_delay_seconds=3)
def stream_get(url: str, chunk_size=1024**2):
    if url.startswith("http"):
        with stream("GET", url, follow_redirects=True) as response:
            yield from response.iter_bytes(chunk_size)
    else:
        with open(url, "rb") as f:
            for chunk in iter(partial(f.read, chunk_size), b""):
                yield chunk


@task
def get_resource(
    r: dict, decp_formats: list[FormatDECP] | None = None
) -> pl.LazyFrame | None:
    if decp_formats is None:
        decp_formats = FORMATS_DECP

    print(f"➡️  {r['ori_filename']} ({r['dataset_name']})")
    output_path = DIST_DIR / "get" / r["{filename}"]
    output_path.parent.mkdir(exist_ok=True)
    url = r["url"]
    file_format = r["format"]
    if file_format == "json":
        format_decp = json_stream_to_parquet(url, output_path, decp_formats)
    elif file_format == "xml":
        format_decp = xml_stream_to_parquet(url, output_path, decp_formats)
    else:
        print(f"▶️ Format de fichier non supporté : {file_format} ({r['dataset_name']})")
        return

    lf = pl.scan_parquet(output_path.with_extension(".parquet"))

    # TODO: do something with it
    artifact_row = gen_artifact_row(r, format_decp, lf, url)  # noqa

    # Exemple https://www.data.gouv.fr/datasets/5cd57bf68b4c4179299eb0e9/#/resources/bb90091c-f0cb-4a59-ad41-b0ab929aad93
    resource_web_url = (
        f"https://www.data.gouv.fr/datasets/{r['dataset_id']}/#/resources/{r['id']}"
    )
    lf = lf.with_columns(pl.lit(resource_web_url).alias("sourceOpenData"))
    return lf


@task
def json_stream_to_parquet(
    url: str, output_path: Path, decp_formats: list[FormatDECP] | None = None
) -> FormatDECP:
    if decp_formats is None:
        decp_formats = FORMATS_DECP

    for fmt in decp_formats:
        fmt.liste_marches_ijson = ijson.sendable_list()
        fmt.coroutine_ijson = ijson.items_coro(
            fmt.liste_marches_ijson, f"{fmt.prefixe_json_marches}.item", use_float=True
        )

    with tempfile.NamedTemporaryFile(mode="wb", suffix=".ndjson", delete=False) as f:
        print(f.name)
        chunk_iter = stream_get(url)

        # In first iteration, will find the right format
        chunk = next(chunk_iter)
        found_marche = False
        for fmt in decp_formats:
            chunk = chunk.replace(b"NaN,", b"null,")
            fmt.coroutine_ijson.send(chunk)
            for marche in fmt.liste_marches_ijson:
                # We found at least one corresponding event in the chunk, so we're
                # using the right format!
                found_marche = True
                write_marche_row(marche, f)
            if found_marche:
                right_fmt = fmt
                break

        if not found_marche:
            raise ValueError("pas de match trouvé parmis les formats passés")

        del right_fmt.liste_marches_ijson[:]

        for chunk in chunk_iter:
            chunk = chunk.replace(b"NaN,", b"null,")
            right_fmt.coroutine_ijson.send(chunk)
            for marche in right_fmt.liste_marches_ijson:
                write_marche_row(marche, f)

            del right_fmt.liste_marches_ijson[:]

        right_fmt.coroutine_ijson.close()

        lf = pl.scan_ndjson(f.name, schema=right_fmt.schema)
        sink_to_files(lf, output_path, file_format="parquet")

    return right_fmt


@task
def xml_stream_to_parquet(url: str, output_path: Path) -> FormatDECP:
    parser = etree.XMLPullParser(tag="marche")
    with tempfile.NamedTemporaryFile(mode="wb", suffix=".ndjson", delete=False) as f:
        for chunk in stream_get(url):
            parser.feed(chunk)
            for _, elem in parser.read_events():
                _, marche = xml_to_dict(elem)
                write_marche_row(marche, f)
        lf = pl.scan_ndjson(f.name, schema=FORMAT_DECP_2019.schema)
        sink_to_files(lf, output_path, file_format="parquet")
    return FORMAT_DECP_2019


def xml_to_dict(element: etree.Element):
    return element.tag, dict(map(xml_to_dict, element)) or element.text


def write_marche_row(marche: dict, file):
    for mod in yield_modifications(marche):
        file.write(orjson.dumps(mod))
        file.write(b"\n")


def yield_modifications(row: dict, separator="."):
    raw_mods = row.pop("modifications", [])
    if isinstance(raw_mods, dict) and "modification" in raw_mods:
        raw_mods = raw_mods["modification"]
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


def gen_artifact_row(file_info: dict, format: FormatDECP, lf: pl.LazyFrame, url: str):
    artifact_row = {
        "open_data_dataset_id": file_info["dataset_id"],
        "open_data_dataset_name": file_info["dataset_name"],
        "download_date": DATE_NOW,
        "columns": sorted(format.schema.keys()),
        "column_number": len(format.schema),
        "row_number": lf.select(pl.len()).collect().item(),
    }

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
