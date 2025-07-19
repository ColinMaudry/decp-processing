import time
from pathlib import Path

from prefect import task

from config import CACHE_EXPIRATION_TIME_HOURS, PREFECT_LOCAL_STORAGE_PATH


@task
def remove_unused_cache(
    cache_dir: Path = PREFECT_LOCAL_STORAGE_PATH,
    cache_expiration_time_hours: int = CACHE_EXPIRATION_TIME_HOURS,
):
    now = time.time()
    age_limit = cache_expiration_time_hours * 3600  # seconds
    if cache_dir.exists():
        for file in cache_dir.rglob("*"):
            if file.is_file():
                if now - file.stat().st_atime > age_limit:
                    print(f"Deleting cache file: {file}")
                    file.unlink()
