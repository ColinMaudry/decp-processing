# ruff: noqa: F811
import io
from dataclasses import replace
from datetime import date
from pathlib import Path

import polars as pl
import pytest

from src import config
from src.config import SourceDeao
from src.tasks.deao.geocode_cache import ADRESSE_GEOCODE_SCHEMA
from src.tasks.deao.schema import published_schema
from src.tasks.scrap.deao import plateformes
from src.tasks.scrap.deao.atexo import dce
from src.tasks.scrap.deao.aws import archive, scraper
from src.tasks.scrap.deao.aws.client import ResultatRecherche
from src.tasks.scrap.deao.aws.parse import AvisListe, parse_avis
from src.tasks.scrap.deao.erreurs import ScrapPartiel
from tests.deao_fakes import FakeS3, fake_datagouv  # noqa: F401
from tests.test_deao_enrich import COG, _lf_etab, _lf_ul

DOSSIER = Path(__file__).parent / "data" / "aws"
CODE = "www.marches-publics.info"
BASE = "https://www.marches-publics.info"
TODAY = date(2026, 11, 14)
REGISTRE = (
    SourceDeao(CODE, "AWS", False, "res-aws", 1, url_base=BASE),
    SourceDeao("boamp", "BOAMP", True, "res-boamp", 1),
)
PROVISOIRE = pl.DataFrame({"uid": ["x"], "sourceDataset": ["provisoire"]})
URL_RC_S3 = "https://s3.example.com/b/rc/www.marches-publics.info/{}.pdf"


def _html(numero: str) -> str:
    return (DOSSIER / f"avis_{numero}.html").read_text(encoding="utf-8")


def _liste(numero: str, **kw) -> AvisListe:
    """Avis de la liste cohérent avec la page d'avis de fixture `numero`."""
    avis = parse_avis(_html(numero))
    base = AvisListe(
        idm=numero,
        numero_avis=numero,
        chemin_avis=f"/Annonces/MPI-pub-{numero}.htm",
        acheteur_nom="Acheteur liste",
        code_postal="25400",
        reference=None,
        objet="Objet liste",
        nombre_lots=len(avis.lots),
        date_publication=date(2026, 11, 10),
        date_limite=avis.date_limite,
        nature="Services",
        considerations_sociales=False,
        considerations_environnementales=False,
    )
    return replace(base, **kw)


A1, A2 = "2026314252", "2026275758"


class FauxClient:
    """Client AWS en mémoire : liste, pages d'avis et RC scénarisés."""

    def __init__(self, avis, complete=True):
        self.avis = avis
        self.complete = complete
        self.pages: dict[str, str] = {
            f"/Annonces/MPI-pub-{n}.htm": _html(n) for n in (A1, A2, "2026260690")
        }
        self.rc_en_erreur: set[str] = set()
        self.appels: list[tuple[str, str]] = []
        self.ferme = False

    def rechercher(self):
        self.appels.append(("rechercher", ""))
        n = len(self.avis)
        return ResultatRecherche(
            avis=list(self.avis),
            total_avant=n if self.complete else None,
            total_apres=n,
        )

    def page_avis(self, chemin):
        self.appels.append(("page_avis", chemin))
        return self.pages[chemin]

    def telecharger_rc(self, idm, dest_dir):
        self.appels.append(("rc", idm))
        if idm in self.rc_en_erreur:
            raise ValueError("Page HTML au lieu d'un PDF")
        chemin = Path(dest_dir) / f"{idm}.pdf"
        chemin.write_bytes(b"%PDF " + idm.encode())
        return chemin

    def close(self):
        self.ferme = True

    def nb(self, nom):
        return sum(1 for a, _ in self.appels if a == nom)


class FakeS3Panne(FakeS3):
    def put_object(self, **kwargs):
        raise RuntimeError("AccessDenied")


@pytest.fixture
def env(monkeypatch, tmp_path, fake_datagouv):
    s3 = FakeS3()
    monkeypatch.setattr(config, "DEAO_REGISTRE", REGISTRE)
    monkeypatch.setattr(config, "DEAO_DATA_DIR", tmp_path)
    monkeypatch.setattr(config, "DEAO_SCRAP_PARTIR_DE_ZERO", True)
    monkeypatch.setattr(config, "DECP_PROCESSING_PUBLISH", True)
    monkeypatch.setattr(config, "SOLO_DEAO_SCRAPS", [])
    monkeypatch.setattr(config, "S3_BUCKET", "b")
    monkeypatch.setattr(config, "S3_ENDPOINT_URL", "https://s3.example.com")
    monkeypatch.setattr(config, "DEAO_DCE_URL_PUBLIQUE", "")
    monkeypatch.setattr(config, "DEAO_DCE_S3_PREFIX", "dce")
    monkeypatch.setattr(config, "DEAO_AWS_S3_PREFIX", "www.marches-publics.info/raw")
    monkeypatch.setattr(config, "DEAO_RC_S3_PREFIX", "rc")
    monkeypatch.setattr(config, "DEAO_AWS_RELIRE_ARCHIVE", False)
    monkeypatch.setattr(dce, "s3_client", lambda: s3)
    monkeypatch.setattr(archive, "s3_client", lambda: s3)
    monkeypatch.setattr(
        plateformes, "charger_referentiels", lambda: (_lf_ul(), _lf_etab(), COG)
    )
    monkeypatch.setattr(
        plateformes,
        "geocoder_adresses",
        lambda adresses, today: pl.DataFrame(schema=ADRESSE_GEOCODE_SCHEMA),
    )
    _publier(fake_datagouv, "res-aws", PROVISOIRE)
    return fake_datagouv, s3, tmp_path


def _publier(datagouv, resource_id, df):
    buf = io.BytesIO()
    df.write_parquet(buf)
    datagouv.fichiers[resource_id] = buf.getvalue()


def _lancer(client):
    return scraper.scraper_aws(CODE, client_factory=lambda url_base: client)(TODAY)


def _premier(env):
    datagouv, _, _ = env
    df = _lancer(FauxClient([_liste(A1), _liste(A2)]))
    _publier(datagouv, "res-aws", df)
    return df


def test_premier_passage(env):
    _, s3, tmp_path = env
    client = FauxClient([_liste(A1), _liste(A2)])
    df = _lancer(client)
    assert df.schema == pl.Schema(published_schema())
    assert sorted(df["uid"]) == sorted(
        [f"{CODE}_{A1}_{lot.numero}" for lot in parse_avis(_html(A1)).lots]
        + [f"{CODE}_{A2}_0"]
    )
    assert set(df["sourceDataset"]) == {CODE}
    assert set(df["statut"]) == {"ouvert"}
    assert df["acheteur_id"].is_not_null().all()
    assert client.nb("page_avis") == 2 and client.nb("rc") == 2
    for idm in (A1, A2):
        cle = archive.cle_avis(idm)
        assert cle in s3.objects
        assert "ACL" not in s3.extra_args[cle]
        assert s3.extra_args[cle]["Metadata"] == {
            "date-limite": archive.marque_date_limite(_liste(idm).date_limite)
        }
        rc = archive.cle_rc(CODE, idm)
        assert s3.extra_args[rc]["ACL"] == "public-read"
        assert set(df.filter(pl.col("id") == idm)["urlReglementConsultation"]) == {
            URL_RC_S3.format(idm)
        }
    assert client.ferme
    assert not list((tmp_path / "rc_tmp" / CODE).glob("*"))


def test_relecture_b(env):
    premier = _premier(env)
    client = FauxClient([_liste(A1), _liste(A2)])
    df = _lancer(client)
    assert client.nb("page_avis") == 0 and client.nb("rc") == 0
    assert sorted(df["uid"]) == sorted(premier["uid"])
    assert df["urlReglementConsultation"].to_list() == (
        premier["urlReglementConsultation"].to_list()
    )

    # Date limite changée (à la minute) : seule cette consultation est relue.
    autre = _liste(A2).date_limite.replace(hour=14)
    client = FauxClient([_liste(A1), _liste(A2, date_limite=autre)])
    _lancer(client)
    assert client.appels.count(("page_avis", f"/Annonces/MPI-pub-{A2}.htm")) == 1
    assert client.nb("page_avis") == 1


def test_relecture_b_nombre_de_lots(env):
    _premier(env)
    client = FauxClient([_liste(A1, nombre_lots=4), _liste(A2)])
    _lancer(client)
    assert client.appels.count(("page_avis", f"/Annonces/MPI-pub-{A1}.htm")) == 1
    assert client.nb("page_avis") == 1


def test_cache_s3_reprise(env):
    _, s3, _ = env
    # Passage précédent interrompu : page déjà archivée, fichier publié vide.
    archive.deposer_avis(
        A1, _html(A1), archive.marque_date_limite(_liste(A1).date_limite)
    )
    client = FauxClient([_liste(A1)])
    df = _lancer(client)
    assert client.nb("page_avis") == 0
    assert df["id"].unique().to_list() == [A1]
    assert df.height == 3


def test_cache_s3_date_limite_differente_relue(env):
    archive.deposer_avis(A1, _html(A1), "2020-01-01T12:00")
    client = FauxClient([_liste(A1)])
    _lancer(client)
    assert client.nb("page_avis") == 1


def test_relire_archive(env, monkeypatch):
    _, s3, _ = env
    premier = _premier(env)
    monkeypatch.setattr(config, "DEAO_AWS_RELIRE_ARCHIVE", True)
    # L'objet a changé dans l'archive (nouveau parseur) : lignes reconstruites.
    cle = archive.cle_avis(A2)
    s3.objects[cle] = s3.objects[cle].replace(
        "GRAND BESANÇON MÉTROPOLE".encode(), "GRAND BESANCON".encode()
    )
    client = FauxClient([_liste(A1), _liste(A2)])
    df = _lancer(client)
    assert client.appels == [("rechercher", "")]
    assert sorted(df["uid"]) == sorted(premier["uid"])
    assert set(df.filter(pl.col("id") == A2)["acheteur_nom"]) != set(
        premier.filter(pl.col("id") == A2)["acheteur_nom"]
    )
    assert df["urlReglementConsultation"].to_list() == (
        premier["urlReglementConsultation"].to_list()
    )

    # Page archivée illisible : relue sur le site, elle seule.
    s3.objects[archive.cle_avis(A1)] = b"<html><body></body></html>"
    client = FauxClient([_liste(A1), _liste(A2)])
    _lancer(client)
    assert client.appels[1:] == [("page_avis", f"/Annonces/MPI-pub-{A1}.htm")]


def test_rc_en_erreur_lien_plateforme(env):
    _, s3, _ = env
    client = FauxClient([_liste(A1), _liste(A2)])
    client.rc_en_erreur.add(A1)
    df = _lancer(client)
    assert set(df.filter(pl.col("id") == A1)["urlReglementConsultation"]) == {
        f"{BASE}/mpiaws/index.cfm?fuseaction=dce.dRC&IDM={A1}"
    }
    assert set(df.filter(pl.col("id") == A2)["urlReglementConsultation"]) == {
        URL_RC_S3.format(A2)
    }
    assert archive.cle_rc(CODE, A1) not in s3.objects


def test_sans_publication_pas_d_archivage(env, monkeypatch):
    _, s3, _ = env
    monkeypatch.setattr(config, "DECP_PROCESSING_PUBLISH", False)
    client = FauxClient([_liste(A1)])
    df = _lancer(client)
    assert client.nb("page_avis") == 1 and client.nb("rc") == 0
    assert s3.objects == {}
    assert set(df["urlReglementConsultation"]) == {
        f"{BASE}/mpiaws/index.cfm?fuseaction=dce.dRC&IDM={A1}"
    }


def test_s3_en_panne_scrap_partiel(env, monkeypatch):
    panne = FakeS3Panne()
    monkeypatch.setattr(dce, "s3_client", lambda: panne)
    monkeypatch.setattr(archive, "s3_client", lambda: panne)
    client = FauxClient([_liste(A1), _liste(A2)])
    with pytest.raises(ScrapPartiel, match="S3") as exc:
        _lancer(client)
    assert client.nb("rc") == 0
    assert client.nb("page_avis") == 2
    assert sorted(exc.value.lignes["id"].unique()) == sorted([A1, A2])


def test_depot_s3_en_panne_coupe_circuit(env, monkeypatch):
    _, s3, _ = env

    def panne(**kwargs):
        raise RuntimeError("AccessDenied")

    monkeypatch.setattr(s3, "upload_file", panne)
    client = FauxClient([_liste(A1), _liste(A2)])
    with pytest.raises(ScrapPartiel) as exc:
        _lancer(client)
    # Archivage arrêté au premier dépôt de RC en échec.
    assert client.nb("rc") == 1
    assert exc.value.lignes["id"].n_unique() == 2


def test_recherche_incomplete_pas_d_annulation(env):
    _premier(env)
    df = _lancer(FauxClient([_liste(A2)], complete=False))
    assert set(df.filter(pl.col("id") == A1)["statut"]) == {"ouvert"}
    df = _lancer(FauxClient([_liste(A2)]))
    assert set(df.filter(pl.col("id") == A1)["statut"]) == {"annule"}


def test_page_avis_en_erreur_nouvelle_ignoree(env):
    client = FauxClient([_liste(A1), _liste(A2)])
    client.pages[f"/Annonces/MPI-pub-{A1}.htm"] = "<html><body></body></html>"
    df = _lancer(client)
    assert df["id"].unique().to_list() == [A2]


class _Journal:
    def __init__(self):
        self.infos: list[str] = []

    def info(self, message):
        self.infos.append(message)

    def __getattr__(self, nom):
        return lambda *a, **k: None


def test_log_de_synthese(env, monkeypatch):
    journal = _Journal()
    monkeypatch.setattr(scraper, "get_logger", lambda level: journal)
    client = FauxClient([_liste(A1), _liste(A2)])
    client.rc_en_erreur.add(A1)
    _lancer(client)
    (synthese,) = [m for m in journal.infos if m.startswith(CODE)]
    for attendu in (
        "2 consultations lues (IDM)",
        "0 hors plateforme",
        "total 2 avant / 2 après",
        "0 segments en erreur",
        "recherche complète",
        "2 relues",
        "0 cache S3",
        "2 site",
        "RC : 1 déposés, 1 en erreur",
    ):
        assert attendu in synthese
