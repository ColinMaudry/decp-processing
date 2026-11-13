# ruff: noqa: F811
import io
from datetime import date, datetime
from zoneinfo import ZoneInfo

import polars as pl
import pytest

from src import config
from src.config import SourceDeao
from src.tasks.deao.geocode_cache import ADRESSE_GEOCODE_SCHEMA
from src.tasks.deao.schema import published_schema
from src.tasks.deao.sources_publiees import FichierSourceIndisponible
from src.tasks.scrap.deao.atexo import dce, scraper
from src.tasks.scrap.deao.atexo.client import DceTelecharge, RechercheIncomplete
from src.tasks.scrap.deao.atexo.parse import ConsultationListe, DetailConsultation, Lot
from tests.deao_fakes import FakeS3, fake_datagouv  # noqa: F401
from tests.test_deao_enrich import COG, _lf_etab, _lf_ul

CODE = "megalis_bretagne"
BASE = "https://marches.megalis.bretagne.bzh"
TODAY = date(2026, 11, 13)
FUTUR = datetime(2026, 12, 1, 12, 0, tzinfo=ZoneInfo("Europe/Paris"))
REGISTRE = (
    SourceDeao(CODE, "Mégalis", False, "res-megalis", 1, url_base=BASE),
    SourceDeao("boamp", "BOAMP", True, "res-boamp", 1),
)


def _cons(ref="242752", nombre_lots=0, **kw) -> ConsultationListe:
    defaut = dict(
        ref_cons=ref, org_cons="e3x", procedure_code="MAPA", categorie="Services",
        date_mise_en_ligne=date(2026, 11, 2), reference=f"REF-{ref}",
        intitule=f"Intitulé {ref}", objet="Objet", organisme="Commune de Test",
        code_postal="35000", ville="Rennes", nombre_lots=nombre_lots,
        lieux_execution=["35"], date_limite=FUTUR,
    )  # fmt: skip
    return ConsultationListe(**{**defaut, **kw})


class FauxClient:
    """Client Atexo en mémoire : consultations, détails, lots et DCE scénarisés."""

    def __init__(self, consultations, taille="72,09 Mo"):
        self.consultations = consultations
        self.taille = taille
        self.lots_par_ref: dict[str, list[Lot]] = {}
        self.details_en_erreur: set[str] = set()
        self.dce_en_erreur = False
        self.recherche_incomplete = False
        self.appels: list[tuple[str, str]] = []
        self.ferme = False

    def rechercher(self):
        self.appels.append(("rechercher", ""))
        if self.recherche_incomplete:
            raise RechercheIncomplete(
                "panne", consultations=self.consultations, annoncees=99
            )
        return self.consultations

    def detail(self, ref_cons, org_cons):
        self.appels.append(("detail", ref_cons))
        if ref_cons in self.details_en_erreur:
            raise RuntimeError("détail en panne")
        return DetailConsultation("45000000", None, None, self.taille)

    def lots(self, ref_cons, org_cons):
        self.appels.append(("lots", ref_cons))
        return self.lots_par_ref.get(ref_cons, [])

    def telecharger_dce(self, ref_cons, org_cons, dest_dir):
        self.appels.append(("dce", ref_cons))
        chemin = dest_dir / f"{ref_cons}.zip"
        if self.dce_en_erreur:
            chemin.write_bytes(b"partiel")
            raise RuntimeError("téléchargement interrompu")
        chemin.write_bytes(f"zip {self.taille}".encode())
        return DceTelecharge(chemin, "1-DCE.zip", "application/zip", None)

    def close(self):
        self.ferme = True

    def nb(self, nom):
        return sum(1 for a, _ in self.appels if a == nom)


@pytest.fixture
def env(monkeypatch, tmp_path, fake_datagouv):
    s3 = FakeS3()
    monkeypatch.setattr(config, "DEAO_REGISTRE", REGISTRE)
    monkeypatch.setattr(config, "DEAO_DATA_DIR", tmp_path)
    monkeypatch.setattr(config, "DEAO_SCRAP_PARTIR_DE_ZERO", True)
    monkeypatch.setattr(config, "DECP_PROCESSING_PUBLISH", False)
    monkeypatch.setattr(config, "SOLO_DEAO_SCRAPS", [])
    monkeypatch.setattr(config, "S3_BUCKET", "b")
    monkeypatch.setattr(config, "S3_ENDPOINT_URL", "https://s3.example.com")
    monkeypatch.setattr(config, "DEAO_DCE_S3_PREFIX", "dce")
    monkeypatch.setattr(dce, "s3_client", lambda: s3)
    monkeypatch.setattr(
        scraper, "charger_referentiels", lambda: (_lf_ul(), _lf_etab(), COG)
    )
    monkeypatch.setattr(
        scraper,
        "geocoder_adresses",
        lambda adresses, today: pl.DataFrame(schema=ADRESSE_GEOCODE_SCHEMA),
    )
    return fake_datagouv, s3, tmp_path


def _lancer(client):
    return scraper.scraper_atexo(CODE, client_factory=lambda url_base: client)(TODAY)


def _publier(datagouv, resource_id, df):
    buf = io.BytesIO()
    df.write_parquet(buf)
    datagouv.fichiers[resource_id] = buf.getvalue()


def _dce_tmp(tmp_path):
    dossier = tmp_path / "dce_tmp" / CODE
    return list(dossier.iterdir()) if dossier.exists() else []


def test_premier_passage_partir_de_zero(env):
    client = FauxClient([_cons("1"), _cons("2")])
    df = _lancer(client)
    assert df.schema == pl.Schema(published_schema())
    assert df.columns == list(published_schema())
    assert sorted(df["uid"]) == [f"{CODE}_1_0", f"{CODE}_2_0"]
    assert set(df["sourceDataset"]) == {CODE}
    assert set(df["statut"]) == {"ouvert"}
    assert df["urlConsultation"][0].startswith(BASE)
    assert client.ferme


def test_precedent_indisponible_echoue(env, monkeypatch):
    monkeypatch.setattr(config, "DEAO_SCRAP_PARTIR_DE_ZERO", False)
    client = FauxClient([_cons("1")])
    with pytest.raises(FichierSourceIndisponible):
        _lancer(client)
    assert client.appels == []


def test_url_base_obligatoire(env, monkeypatch):
    monkeypatch.setattr(
        config, "DEAO_REGISTRE", (SourceDeao(CODE, "M", False, "res-megalis", 1),)
    )
    with pytest.raises(ValueError, match="url_base"):
        _lancer(FauxClient([]))


def test_siret_par_dictionnaire_boamp(env):
    datagouv, _, _ = env
    _publier(
        datagouv,
        "res-boamp",
        pl.DataFrame(
            {
                "acheteur_nom": ["COMMUNE DE TEST"],
                "acheteur_codePostal": ["35000"],
                "acheteur_id": ["12345678900022"],
            }
        ),
    )
    df = _lancer(FauxClient([_cons("1")]))
    assert df["acheteur_id"].to_list() == ["12345678900022"]
    assert df["acheteur_id_source"].to_list() == ["dictionnaire"]
    assert df["acheteur_codePostal"].to_list() == ["35000"]


def test_sans_boamp_pas_de_dictionnaire(env):
    df = _lancer(FauxClient([_cons("1")]))
    assert df["acheteur_id"].to_list() == [None]


def test_detail_en_echec_nouvelle_ignoree(env):
    client = FauxClient([_cons("1"), _cons("2")])
    client.details_en_erreur = {"2"}
    df = _lancer(client)
    assert df["id"].to_list() == ["1"]


def test_detail_en_echec_connue_conservee(env):
    datagouv, _, _ = env
    premier = _lancer(FauxClient([_cons("1"), _cons("2")]))
    _publier(datagouv, "res-megalis", premier)
    client = FauxClient([_cons("1"), _cons("2", intitule="Modifié")])
    client.details_en_erreur = {"2"}
    df = _lancer(client)
    assert sorted(df["id"]) == ["1", "2"]
    ligne = df.filter(pl.col("id") == "2")
    assert ligne["objet"].to_list() == ["Intitulé 2"]
    assert ligne["statut"].to_list() == ["ouvert"]


def test_dce_depose_si_publication(env, monkeypatch):
    _, s3, tmp_path = env
    monkeypatch.setattr(config, "DECP_PROCESSING_PUBLISH", True)
    df = _lancer(FauxClient([_cons("242752")]))
    cle = "dce/megalis_bretagne/242752.zip"
    assert set(s3.objects) == {cle}
    assert df["urlDce"].to_list() == [dce.url_publique(cle)]
    assert _dce_tmp(tmp_path) == []


def test_dce_non_depose_en_dev(env):
    _, s3, _ = env
    client = FauxClient([_cons("242752")])
    df = _lancer(client)
    assert s3.objects == {}
    assert client.nb("dce") == 0
    assert df["urlDce"].to_list() == [None]


def test_dce_non_depose_en_solo(env, monkeypatch):
    _, s3, _ = env
    monkeypatch.setattr(config, "DECP_PROCESSING_PUBLISH", True)
    monkeypatch.setattr(config, "SOLO_DEAO_SCRAPS", [CODE])
    df = _lancer(FauxClient([_cons("242752")]))
    assert s3.objects == {}
    assert df["urlDce"].to_list() == [None]


def test_dce_en_echec_retente(env, monkeypatch):
    datagouv, s3, tmp_path = env
    monkeypatch.setattr(config, "DECP_PROCESSING_PUBLISH", True)
    client = FauxClient([_cons("242752")])
    client.dce_en_erreur = True
    premier = _lancer(client)
    assert premier["urlDce"].to_list() == [None]
    assert s3.objects == {}
    # Le fichier partiel n'est ni déposé ni laissé sur disque.
    assert _dce_tmp(tmp_path) == []

    _publier(datagouv, "res-megalis", premier)
    second = _lancer(FauxClient([_cons("242752")]))
    cle = "dce/megalis_bretagne/242752.zip"
    assert set(s3.objects) == {cle}
    assert second["urlDce"].to_list() == [dce.url_publique(cle)]


def test_dce_inchange_non_retelecharge(env, monkeypatch):
    datagouv, s3, _ = env
    monkeypatch.setattr(config, "DECP_PROCESSING_PUBLISH", True)
    premier = _lancer(FauxClient([_cons("242752")]))
    _publier(datagouv, "res-megalis", premier)
    client = FauxClient([_cons("242752")])
    second = _lancer(client)
    assert client.nb("dce") == 0
    assert second["urlDce"].to_list() == premier["urlDce"].to_list()


def test_dce_modifie_meme_date_limite_retelecharge(env, monkeypatch):
    datagouv, s3, _ = env
    monkeypatch.setattr(config, "DECP_PROCESSING_PUBLISH", True)
    premier = _lancer(FauxClient([_cons("242752")]))
    _publier(datagouv, "res-megalis", premier)
    client = FauxClient([_cons("242752")], taille="80,00 Mo")
    second = _lancer(client)
    cle = "dce/megalis_bretagne/242752.zip"
    assert client.nb("dce") == 1
    assert set(s3.objects) == {cle}
    assert s3.objects[cle] == b"zip 80,00 Mo"
    assert second["urlDce"].to_list() == [dce.url_publique(cle)]


def test_detail_lu_a_chaque_passage_lots_non_relus(env):
    datagouv, _, _ = env
    lots = [
        Lot("1", "Lot un", None, None, None),
        Lot("2", "Lot deux", None, None, None),
    ]
    client = FauxClient([_cons("1", nombre_lots=2), _cons("2")])
    client.lots_par_ref["1"] = lots
    premier = _lancer(client)
    assert client.nb("lots") == 1
    assert sorted(premier["uid"]) == [f"{CODE}_1_1", f"{CODE}_1_2", f"{CODE}_2_0"]

    _publier(datagouv, "res-megalis", premier)
    client = FauxClient([_cons("1", nombre_lots=2), _cons("2")])
    second = _lancer(client)
    assert client.nb("detail") == 2
    assert client.nb("lots") == 0
    assert sorted(second["uid"]) == sorted(premier["uid"])
    assert second.filter(pl.col("lotId") == "1")["lotIntitule"].to_list() == ["Lot un"]


def test_recherche_incomplete_n_annule_rien(env):
    datagouv, _, _ = env
    premier = _lancer(FauxClient([_cons("1"), _cons("2")]))
    _publier(datagouv, "res-megalis", premier)
    client = FauxClient([_cons("1", intitule="Modifié")])
    client.recherche_incomplete = True
    df = _lancer(client)
    assert dict(zip(df["id"], df["statut"])) == {"1": "ouvert", "2": "ouvert"}
    # Les consultations lues sont bien rafraîchies.
    assert client.nb("detail") == 1
    assert df.filter(pl.col("id") == "1")["objet"].to_list() == ["Modifié"]


def test_disparue_avant_date_limite_annulee(env):
    datagouv, _, _ = env
    premier = _lancer(FauxClient([_cons("1"), _cons("2")]))
    _publier(datagouv, "res-megalis", premier)
    df = _lancer(FauxClient([_cons("1")]))
    assert dict(zip(df["id"], df["statut"])) == {"1": "ouvert", "2": "annule"}


def test_sirene_absent_lance_preprocess(env, monkeypatch, tmp_path):
    appels = []
    monkeypatch.setattr(config, "SIRENE_DATA_DIR", tmp_path / "absent")
    monkeypatch.setattr(scraper, "sirene_preprocess", lambda: appels.append(1))
    scraper._referentiels()
    assert appels == [1]


def test_precedent_non_conforme_partir_de_zero(env):
    datagouv, _, _ = env
    # Fichier provisoire déposé à la création de la ressource (autre schéma).
    _publier(
        datagouv,
        "res-megalis",
        pl.DataFrame({"uid": ["x"], "techniques": ["a"], "sourceDataset": ["edf"]}),
    )
    df = _lancer(FauxClient([_cons("1")]))
    assert df["uid"].to_list() == [f"{CODE}_1_0"]


def test_precedent_non_conforme_echoue_sans_drapeau(env, monkeypatch):
    datagouv, _, _ = env
    monkeypatch.setattr(config, "DEAO_SCRAP_PARTIR_DE_ZERO", False)
    _publier(
        datagouv,
        "res-megalis",
        pl.DataFrame({"uid": ["x"], "sourceDataset": ["edf"]}),
    )
    client = FauxClient([_cons("1")])
    with pytest.raises(FichierSourceIndisponible, match="non conforme"):
        _lancer(client)
    assert client.appels == []


def _lignes_en_panne_pour(monkeypatch, ref):
    reelle = scraper.lignes_consultation

    def lignes(code, url_base, c, *args, **kwargs):
        if c.ref_cons == ref:
            raise ValueError("valeur inattendue")
        return reelle(code, url_base, c, *args, **kwargs)

    monkeypatch.setattr(scraper, "lignes_consultation", lignes)


def test_lignes_en_echec_nouvelle_ignoree(env, monkeypatch):
    _lignes_en_panne_pour(monkeypatch, "2")
    df = _lancer(FauxClient([_cons("1"), _cons("2"), _cons("3")]))
    assert sorted(df["id"]) == ["1", "3"]


def test_lignes_en_echec_connue_conservee(env, monkeypatch):
    datagouv, _, _ = env
    premier = _lancer(FauxClient([_cons("1"), _cons("2")]))
    _publier(datagouv, "res-megalis", premier)
    _lignes_en_panne_pour(monkeypatch, "2")
    df = _lancer(
        FauxClient([_cons("1", intitule="Modifié"), _cons("2", intitule="Modifié")])
    )
    assert dict(zip(df["id"], df["objet"])) == {"1": "Modifié", "2": "Intitulé 2"}
    assert set(df["statut"]) == {"ouvert"}


class _Journal:
    def __init__(self):
        self.avertissements: list[str] = []

    def warning(self, message):
        self.avertissements.append(message)

    def __getattr__(self, nom):
        return lambda *a, **k: None


def test_annulations_massives_journalisees(env, monkeypatch):
    datagouv, _, _ = env
    premier = _lancer(FauxClient([_cons(str(i)) for i in range(10)]))
    _publier(datagouv, "res-megalis", premier)
    journal = _Journal()
    monkeypatch.setattr(scraper, "get_logger", lambda level: journal)
    df = _lancer(FauxClient([_cons(str(i)) for i in range(8)]))
    assert (df["statut"] == "annule").sum() == 2
    assert any("2 consultations ouvertes sur 10" in m for m in journal.avertissements)


def test_annulations_sous_le_seuil_non_journalisees(env, monkeypatch):
    datagouv, _, _ = env
    premier = _lancer(FauxClient([_cons(str(i)) for i in range(10)]))
    _publier(datagouv, "res-megalis", premier)
    journal = _Journal()
    monkeypatch.setattr(scraper, "get_logger", lambda level: journal)
    _lancer(FauxClient([_cons(str(i)) for i in range(9)]))
    assert not any("annulées" in m for m in journal.avertissements)
