import json

from config import BOOKMARK_FILEPATH


def load_bookmarks():
    with open(BOOKMARK_FILEPATH, "r") as f:
        return set(json.load(f))
    return set()


def save_bookmarks(bookmarks):
    with open(BOOKMARK_FILEPATH, "w") as f:
        json.dump(list(bookmarks), f)


def reset_bookmarks():
    with open(BOOKMARK_FILEPATH, "w") as f:
        json.dump([], f)


def bookmark(resource_id):
    bookmarks = load_bookmarks()
    if resource_id not in bookmarks:
        bookmarks.add(resource_id)
        save_bookmarks(bookmarks)
    else:
        print(f"{resource_id} is already bookmarked.")


def is_processed(resource_id):
    bookmarks = load_bookmarks()
    return resource_id in bookmarks
