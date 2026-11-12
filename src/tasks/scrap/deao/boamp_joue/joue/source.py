"""Source JOUE : avis eForms listés et téléchargés via TED."""

from src.config import JOUE_MAX_WORKERS, JOUE_S3_PREFIX
from src.tasks.deao.acquire import Source
from src.tasks.scrap.deao.boamp_joue.joue import fetch, parse

JOUE = Source(
    nom="joue",
    s3_prefix=JOUE_S3_PREFIX,
    list_day_files=fetch.list_day_files,
    download=fetch.download_xml,
    parse_day=parse.parse_day,
    max_workers=JOUE_MAX_WORKERS,
)
