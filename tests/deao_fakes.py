"""Faux client S3 en mémoire, partagé par les tests DEAO."""

import io
import re
from datetime import date, datetime
from pathlib import Path

import httpx
import pytest
from botocore.exceptions import ClientError


class FakeS3:
    def __init__(self):
        self.objects: dict[str, bytes] = {}

    def put_object(self, Bucket, Key, Body, **kwargs):
        self.objects[Key] = Body if isinstance(Body, bytes) else Body.read()

    def get_object(self, Bucket, Key, **kwargs):
        if Key not in self.objects:
            raise ClientError(
                {"Error": {"Code": "NoSuchKey", "Message": Key}}, "GetObject"
            )
        return {"Body": io.BytesIO(self.objects[Key])}

    def get_paginator(self, operation):
        assert operation == "list_objects_v2"
        return _Paginator(self)


class _Paginator:
    def __init__(self, fake):
        self.fake = fake

    def paginate(self, Bucket, Prefix=""):
        keys = sorted(k for k in self.fake.objects if k.startswith(Prefix))
        yield {"Contents": [{"Key": k} for k in keys]} if keys else {}


@pytest.fixture
def fake_s3(monkeypatch):
    from src.tasks.deao import archive

    fake = FakeS3()
    monkeypatch.setattr(archive, "s3_client", lambda: fake)
    return fake


XML_TED_TEST = b'<?xml version="1.0" encoding="UTF-8"?><ContractNotice/>'


class FakeTed:
    """Faux TED : recherche paginée (POST) et XML (GET), avec pannes simulées."""

    def __init__(self):
        # numéro de publication -> XML, par date de publication
        self.avis_par_jour: dict[date, dict[str, bytes]] = {}
        # uuid eForms -> numéro de publication
        self.identifiants: dict[str, str] = {}
        # codes forcés avant succès pour le XML d'un numéro, consommés dans l'ordre
        self.reponses_xml: dict[str, list[int]] = {}
        self.recherche_en_erreur: set[date] = set()
        self.identifiants_en_erreur: set[str] = set()
        self.appels: list[str] = []

    def post(self, url, payload):
        query = payload["query"]
        self.appels.append(f"POST {url} {query}")
        if m := re.search(r"publication-date=(\d{8})", query):
            jour = datetime.strptime(m.group(1), "%Y%m%d").date()
            if jour in self.recherche_en_erreur:
                return httpx.Response(500)
            numeros = sorted(self.avis_par_jour.get(jour, {}))
        elif m := re.search(r"notice-identifier=(\S+)", query):
            if m.group(1) in self.identifiants_en_erreur:
                return httpx.Response(500)
            pn = self.identifiants.get(m.group(1))
            numeros = [pn] if pn else []
        else:
            return httpx.Response(400)
        debut = int(payload.get("iterationNextToken") or 0)
        fin = debut + payload["limit"]
        corps = {
            "notices": [{"publication-number": n} for n in numeros[debut:fin]],
            "totalNoticeCount": len(numeros),
        }
        if fin < len(numeros):
            corps["iterationNextToken"] = str(fin)
        return httpx.Response(200, json=corps)

    def get(self, url):
        self.appels.append(f"GET {url}")
        pn = url.split("/notice/")[1].split("/")[0]
        forces = self.reponses_xml.get(pn)
        if forces:
            code = forces.pop(0)
            if code != 200:
                return httpx.Response(code)
        for avis in self.avis_par_jour.values():
            if pn in avis:
                return httpx.Response(200, content=avis[pn])
        return httpx.Response(404)


@pytest.fixture
def fake_ted(monkeypatch):
    from src.tasks.scrap.deao.boamp_joue.joue import fetch

    fake = FakeTed()
    fake.avis_par_jour[date(2026, 10, 14)] = {"706679-2026": XML_TED_TEST}
    fake.identifiants["c2dbf0bf-bd32-4b44-9a85-c27a0f0c3e9b"] = "677195-2026"
    monkeypatch.setattr(fetch, "_post_raw", fake.post)
    monkeypatch.setattr(fetch, "_get_raw", fake.get)
    return fake


@pytest.fixture
def no_sleep(monkeypatch):
    from src.tasks.scrap.deao.boamp_joue.joue import fetch

    for fn in (fetch.http_get, fetch.http_post):
        monkeypatch.setattr(fn.retry, "sleep", lambda _: None)


class FakeDataGouv:
    """Faux data.gouv.fr (dataset b) : aller-retour publication -> lecture."""

    def __init__(self):
        # contenu des ressources, par resource_id
        self.fichiers: dict[str, bytes] = {}
        # (dataset_id, resource_id, nom du fichier) à chaque publication
        self.publies: list[tuple[str, str, str]] = []
        self.en_erreur: set[str] = set()
        self.echecs_publication: set[str] = set()
        self.appels: list[str] = []
        self.api_key_vue: list[str | None] = []

    def get(self, url, headers):
        self.appels.append(url)
        self.api_key_vue.append(headers.get("X-API-KEY"))
        assert "X-API-KEY" in headers, f"X-API-KEY absente sur GET {url}"
        if url.startswith("https://fake.data.gouv.fr/fichiers/"):
            rid = url.rsplit("/", 1)[1]
            if rid in self.en_erreur:
                return httpx.Response(500)
            return httpx.Response(200, content=self.fichiers[rid])
        m = re.search(r"/datasets/[^/]+/resources/([^/]+)/$", url)
        assert m, url
        rid = m.group(1)
        if rid in self.en_erreur:
            return httpx.Response(500)
        if rid not in self.fichiers:
            return httpx.Response(404)
        return httpx.Response(
            200, json={"url": f"https://fake.data.gouv.fr/fichiers/{rid}"}
        )

    def update_resource(self, dataset_id, resource_id, file_path, api_key):
        assert api_key is not None, "clé API absente sur le PUT"
        self.publies.append((dataset_id, resource_id, Path(file_path).name))
        if resource_id in self.echecs_publication:
            return {"success": False}
        self.fichiers[resource_id] = Path(file_path).read_bytes()
        return {"success": True}


@pytest.fixture
def fake_datagouv(monkeypatch):
    from src.tasks import publish
    from src.tasks.deao import sources_publiees

    monkeypatch.setattr(sources_publiees.config, "DEAO_CLOSED_DATASET_ID", "ds-b")
    monkeypatch.setattr(sources_publiees.config, "DATAGOUVFR_API_KEY", "cle-test")
    fake = FakeDataGouv()
    monkeypatch.setattr(sources_publiees, "http_get", fake.get)
    monkeypatch.setattr(sources_publiees, "update_resource", fake.update_resource)
    # Publication du dataset a (publish_deao_to_datagouv)
    monkeypatch.setattr(publish, "update_resource", fake.update_resource)
    return fake
