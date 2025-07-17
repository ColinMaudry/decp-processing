import time
from pathlib import Path

from prefect import task

from src.config import CACHE_DIR, REMOVE_UNUSED_CACHE_AFTER_DAYS


@task
def remove_unused_cache(
    cache_dir: Path = CACHE_DIR,
    remove_unused_cache_after_days: int = REMOVE_UNUSED_CACHE_AFTER_DAYS,
):
    now = time.time()
    age_limit = remove_unused_cache_after_days * 86400  # seconds
    if cache_dir.exists():
        for file in cache_dir.rglob("*"):
            if file.is_file():
                if now - file.stat().st_atime > age_limit:
                    print(f"Deleting cache file: {file}")
                    file.unlink()
