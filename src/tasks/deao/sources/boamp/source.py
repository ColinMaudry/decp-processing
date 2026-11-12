"""Source BOAMP : jours DILA, XML legacy et eForms."""

from src.config import BOAMP_S3_PREFIX, MAX_PREFECT_WORKERS
from src.tasks.deao.acquire import Source
from src.tasks.deao.sources.boamp import fetch, parse

BOAMP = Source(
    nom="boamp",
    s3_prefix=BOAMP_S3_PREFIX,
    list_day_files=fetch.list_day_files,
    download=fetch.download_xml,
    parse_day=parse.parse_day,
    max_workers=MAX_PREFECT_WORKERS,
)
