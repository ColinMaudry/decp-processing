from bs4 import BeautifulSoup
from httpx import get


def get_parse_html(url):
    html = get(url).text
    soup = BeautifulSoup(html, "lxml")
    return soup


def get_search_results_pages(search_results_soup) -> list:
    results = []
    while len(results) > 0:
        pass


# Pour chaque page de recherche, extraction de la liste des pages

# On vise en résultat un JSON par recherche

# Pour chaque page, une liste de HTML+JSON

# Pour chaque JSON, extraction + redressement

# fusion des JSON par plage temporelle

# publication sur data.gouv.fr
