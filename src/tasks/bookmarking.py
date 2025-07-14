import json

from config import BOOKMARK_FILEPATH


def create_dir_if_not_exists():
    """Create the directory for bookmarks if it does not exist."""
    BOOKMARK_FILEPATH.parent.mkdir(parents=True, exist_ok=True)


def load_bookmarks() -> set:
    """Load bookmarks from the bookmark file."""
    if BOOKMARK_FILEPATH.exists():
        with open(BOOKMARK_FILEPATH, "r") as f:
            return set(json.load(f))
    else:
        create_dir_if_not_exists()
        return set()


def save_bookmarks(bookmarks: set | list):
    create_dir_if_not_exists()
    with open(BOOKMARK_FILEPATH, "w") as f:
        json.dump(list(bookmarks), f)


def reset_bookmarks():
    """Reset the bookmarks by clearing the bookmark file."""
    save_bookmarks([])


def bookmark(resource_id: str):
    bookmarks = load_bookmarks()
    if resource_id not in bookmarks:
        bookmarks.add(resource_id)
        save_bookmarks(bookmarks)
    else:
        print(f"{resource_id} is already bookmarked.")


def bookmark_multiple(resources: list):
    bookmarks = load_bookmarks()
    bookmarks.update(resources)
    # save deduped bookmarks
    save_bookmarks(bookmarks)


def is_processed(resource_id: set) -> bool:
    bookmarks = load_bookmarks()
    return resource_id in bookmarks
