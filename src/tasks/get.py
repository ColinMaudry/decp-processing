import re
import tempfile
from functools import partial
from pathlib import Path

import ijson
import orjson
import polars as pl
import xmltodict
from httpx import get, stream
from prefect import task

from config import DATE_NOW, DIST_DIR, FORMATS_DECP, FormatDECP
from tasks.output import sink_to_files


def clean_xml(xml_str):
    # Suppresion des caractères invalides XML 1.0
    return re.sub(r"[^\x09\x0A\x0D\x20-\uD7FF\uE000-\uFFFD]", "", xml_str)


def parse_xml(url: str) -> dict:
    """
    Récupère et parse un fichier XML à partir d'une URL, puis le convertit en JSON.

    Cette fonction est conçue pour traiter les fichiers XML issus des DECP. Elle applique
    un post-traitement sur les nœuds "modifications" et "titulaires" pour uniformiser leur structure.

    Paramètre :
        url (str) : L'URL du fichier XML à récupérer et à parser.

    Retour :
        dict : Une représentation sous forme de dictionnaire du contenu XML, avec une structure adaptée pour un usage en JSON.
    """

    request = get(url, follow_redirects=True)
    data = xmltodict.parse(
        clean_xml(request.text),
        force_list={
            "considerationSociale",
            "considerationEnvironnementale",
            "modaliteExecution",
            "technique",
            "typePrix",
            "modification",
            "titulaire",
            "marche",
        },
    )

    if "marches" in data:
        if "marche" in data["marches"]:
            # modifications des titulaires et des modifications
            for row in data["marches"]["marche"]:
                mods = row.get("modifications", [])
                if mods is None:
                    row["modifications"] = []
                elif "modification" in mods:
                    mods_list = mods["modification"]
                    row["modifications"] = [{"modification": m} for m in mods_list]

                titulaires = row.get("titulaires", [])
                if titulaires is None:
                    row["titulaires"] = []
                elif "titulaire" in titulaires:
                    titulaires_list = titulaires["titulaire"]
                    row["titulaires"] = [{"titulaire": t} for t in titulaires_list]

            for row in data["marches"]["marche"]:
                mods = row.get("modifications", [])
                _mods = []
                for mod in mods:
                    if "titulaires" in mod["modification"]:
                        mod["modification"].update(
                            {"titulaires": [mod["modification"]["titulaires"]]}
                        )
                    _mods.append(mod)

                if _mods:
                    row["modifications"] = _mods

    return data


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
def get_resource(r: dict, decp_formats: list[FormatDECP] | None = None) -> pl.LazyFrame:
    if decp_formats is None:
        decp_formats = FORMATS_DECP

    print(f"➡️  {r['ori_filename']} ({r['dataset_name']})")
    output_path = DIST_DIR / "get" / r["{filename}"]
    output_path.parent.mkdir(exist_ok=True)
    if r["format"] == "xml":
        pass  # TODO

    url = r["url"]
    format_decp = json_stream_to_parquet(url, output_path, decp_formats)
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

        for chunk in chunk_iter:
            chunk = chunk.replace(b"NaN,", b"null,")
            fmt.coroutine_ijson.send(chunk)
            for marche in right_fmt.liste_marches_ijson:
                write_marche_row(marche, f)

            del right_fmt.liste_marches_ijson[:]

        right_fmt.coroutine_ijson.close()

        lf = pl.scan_ndjson(f.name, schema=right_fmt.schema)
        sink_to_files(lf, output_path, file_format="parquet")

    return right_fmt


def write_marche_row(marche, file):
    for mod in yield_modifications(marche):
        file.write(orjson.dumps(mod))
        file.write(b"\n")


def yield_modifications(row: dict, separator="."):
    raw_mods = row.pop("modifications", [])
    if isinstance(raw_mods, dict) and "modification" in raw_mods:
        raw_mods = raw_mods["modification"]
    raw_mods = [] if raw_mods is None else raw_mods

    mods = [{}] + raw_mods
    for i, m in enumerate(mods):
        m["id"] = i
        if "modification" in m:
            m = m["modification"]
        titulaires = norm_titulaires(m)
        if titulaires is not None:
            m["titulaires"] = titulaires
        row["modification"] = m
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
