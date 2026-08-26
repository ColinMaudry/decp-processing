import polars as pl
import pytest

from src.config import SIRET_LATLONG_SCHEMA
from src.tasks.get import (
    bootstrap_siret_latlong,
    get_etablissements,
    get_labels_etablissements,
    get_labels_unites_legales,
    get_unite_legales,
    json_stream_to_parquet,
    scan_etablissements,
    xml_stream_to_parquet,
)

REQUIRED_GEO_COLUMNS = {
    "numeroVoieEtablissement",
    "indiceRepetitionEtablissement",
    "codePostalEtablissement",
    "libelleCommuneEtablissement",
}


def test_scan_etablissements_includes_geocoding_columns():
    lf = scan_etablissements()
    assert isinstance(lf, pl.LazyFrame)
    columns = set(lf.collect_schema().names())
    missing = REQUIRED_GEO_COLUMNS - columns
    assert not missing, f"Colonnes manquantes : {missing}"


def test_get_unite_legales_a_un_retry():
    assert hasattr(get_unite_legales, "retry"), (
        "get_unite_legales n'est pas décorée par tenacity"
    )


def test_get_etablissements_materialise_dans_la_fonction_decoree(tmp_path, monkeypatch):
    """Le retry n'a d'effet que si le sink_parquet est DANS la fonction décorée :
    scan_parquet est paresseux, le réseau n'est sollicité qu'à la matérialisation.
    Une fonction qui se contenterait de `return scan_etablissements().pipe(...)`
    sans sink interne passerait le test hasattr(..., "retry") sans jamais matérialiser
    quoi que ce soit — ce test vérifie donc le comportement, pas juste le décorateur."""
    fake_lf = pl.LazyFrame(
        [
            {
                "siret": "11111111111",
                "codeCommuneEtablissement": "1053",
                "activitePrincipaleEtablissement": "11.1A",
                "nomenclatureActivitePrincipaleEtablissement": "NAFv2",
                "enseigne1Etablissement": None,
                "denominationUsuelleEtablissement": "Dénom usuelle",
                "libelleVoieEtablissement": "Rue Test",
                "typeVoieEtablissement": "RUE",
                "numeroVoieEtablissement": "1",
                "indiceRepetitionEtablissement": None,
                "codePostalEtablissement": "01000",
                "libelleCommuneEtablissement": "Bourg-en-Bresse",
            }
        ]
    )
    monkeypatch.setattr("src.tasks.get.scan_etablissements", lambda: fake_lf)

    lf_siret_latlong = pl.LazyFrame(
        {
            "siret": ["00011111111111"],
            "latitude": [45.75],
            "longitude": [4.85],
        }
    )

    out = tmp_path / "etablissements.parquet"
    assert not out.exists()

    get_etablissements(out, lf_siret_latlong)

    assert out.exists(), (
        "get_etablissements n'a rien matérialisé : le sink_parquet doit être "
        "dans la fonction décorée, pas dans l'appelant."
    )
    df = pl.read_parquet(out)
    assert df.height == 1
    assert df["siret"].to_list() == ["00011111111111"]


def _fake_etablissements(tmp_path, sirets_bio, sirets_rge, sirets_nus=()):
    """Un parquet au format de la ressource « établissements » de l'annuaire."""
    sirets = list(dict.fromkeys([*sirets_bio, *sirets_rge, *sirets_nus]))
    path = tmp_path / "annuaire_etablissements.parquet"
    pl.DataFrame(
        {
            "siret": sirets,
            "liste_id_bio": [["B"] if s in sirets_bio else [] for s in sirets],
            "liste_rge": [["R"] if s in sirets_rge else [] for s in sirets],
        },
        schema={
            "siret": pl.String,
            "liste_id_bio": pl.List(pl.String),
            "liste_rge": pl.List(pl.String),
        },
    ).write_parquet(path)
    return path


def _fake_unites_legales(tmp_path, sirens_labellises, sirens_nus=()):
    """Un parquet au format de la ressource « unités légales » de l'annuaire."""
    sirens = [*sirens_labellises, *sirens_nus]
    path = tmp_path / "annuaire_unites_legales.parquet"
    pl.DataFrame(
        {
            "siren": sirens,
            "est_ess": [s in sirens_labellises for s in sirens],
            "est_association": [False] * len(sirens),
            "est_qualiopi": [False] * len(sirens),
            "est_siae": [False] * len(sirens),
            "est_avocat": [False] * len(sirens),
            "est_achats_responsables": [False] * len(sirens),
        }
    ).write_parquet(path)
    return path


def test_get_labels_etablissements_materialise_dans_la_fonction_decoree(
    tmp_path, monkeypatch
):
    """Même raisonnement que pour get_etablissements : l'écriture du parquet
    doit être DANS la fonction décorée, pas dans l'appelant, pour que le retry
    protège effectivement le téléchargement. On fait pointer l'URL de la source
    (la constante lue par get_labels_etablissements) vers un fichier local
    plutôt que de monkeypatcher pl.scan_parquet lui-même, qui est utilisé en
    interne par le moteur d'exécution."""
    source = _fake_etablissements(
        tmp_path,
        sirets_bio=["11111111111111", "22222222222222"],
        sirets_rge=["11111111111111", "33333333333333"],
        sirets_nus=["44444444444444"],
    )
    monkeypatch.setattr("src.tasks.get.ANNUAIRE_ETABLISSEMENTS_URL", str(source))
    # La fixture fait 4 lignes : le garde-fou de volume n'est pas le sujet ici.
    monkeypatch.setattr("src.tasks.get.LABELS_SIRET_MIN_ROWS", 1)

    out = tmp_path / "labels_siret.parquet"
    assert not out.exists()

    get_labels_etablissements(out)

    assert out.exists(), (
        "get_labels_etablissements n'a rien matérialisé : l'écriture du parquet "
        "doit être dans la fonction décorée, pas dans l'appelant."
    )
    df = pl.read_parquet(out)
    assert set(df.columns) == {"siret", "label_bio", "label_rge"}
    # Le SIRET sans aucun label n'est pas conservé
    assert df.height == 3
    assert df["label_bio"].null_count() == 0
    assert df["label_rge"].null_count() == 0
    row = df.filter(pl.col("siret") == "11111111111111")
    assert row["label_bio"].item() is True
    assert row["label_rge"].item() is True
    assert df.filter(pl.col("siret") == "22222222222222")["label_rge"].item() is False
    assert df.filter(pl.col("siret") == "33333333333333")["label_bio"].item() is False


def test_get_labels_unites_legales_materialise_dans_la_fonction_decoree(
    tmp_path, monkeypatch
):
    source = _fake_unites_legales(
        tmp_path, sirens_labellises=["111111111"], sirens_nus=["222222222"]
    )
    monkeypatch.setattr("src.tasks.get.ANNUAIRE_UNITES_LEGALES_URL", str(source))
    monkeypatch.setattr("src.tasks.get.LABELS_SIREN_MIN_ROWS", 1)

    out = tmp_path / "labels_siren.parquet"
    assert not out.exists()

    get_labels_unites_legales(out)

    assert out.exists()
    df = pl.read_parquet(out)
    assert set(df.columns) == {
        "siren",
        "label_ess",
        "label_association",
        "label_qualiopi",
        "label_siae",
        "label_avocat",
        "label_achats_responsables",
    }
    # Le SIREN sans aucun label n'est pas conservé
    assert df["siren"].to_list() == ["111111111"]
    assert df["label_ess"].item() is True


@pytest.mark.parametrize(
    ("tache", "constante_url", "constante_seuil", "fabrique_source"),
    [
        (
            get_labels_etablissements,
            "ANNUAIRE_ETABLISSEMENTS_URL",
            "LABELS_SIRET_MIN_ROWS",
            lambda tmp_path: _fake_etablissements(
                tmp_path, sirets_bio=["11111111111111"], sirets_rge=[]
            ),
        ),
        (
            get_labels_unites_legales,
            "ANNUAIRE_UNITES_LEGALES_URL",
            "LABELS_SIREN_MIN_ROWS",
            lambda tmp_path: _fake_unites_legales(
                tmp_path, sirens_labellises=["111111111"]
            ),
        ),
    ],
)
def test_get_labels_leve_si_volume_anormalement_bas(
    tmp_path, monkeypatch, tache, constante_url, constante_seuil, fabrique_source
):
    """Une source tronquée produit un parquet valide et des labels nuls partout,
    indistinguables de « aucune entreprise n'est labellisée ». Le garde-fou
    transforme ce mode de défaillance silencieux en échec franc, et n'écrit
    rien sur le disque — un fichier écrit passerait ensuite pour valide auprès
    de la garde de sirene_preprocess."""
    source = fabrique_source(tmp_path)
    monkeypatch.setattr(f"src.tasks.get.{constante_url}", str(source))
    monkeypatch.setattr(f"src.tasks.get.{constante_seuil}", 50_000)

    out = tmp_path / "labels.parquet"

    with pytest.raises(ValueError, match="anormalement bas"):
        tache(out)

    assert not out.exists(), (
        "le parquet ne doit pas être écrit quand le garde-fou se déclenche"
    )


def test_xml_stream_to_parquet_small_file_is_not_empty(tmp_path):
    """Régression : un petit XML (ndjson < taille du buffer d'écriture) doit
    quand même produire des lignes. Sans flush() avant scan_ndjson, le buffer
    n'était jamais vidé sur le disque et le parquet ressortait vide (0 ligne)."""
    xml = (
        '<?xml version="1.0" encoding="ISO-8859-15"?>\n'
        "<marches>\n"
        " <marche>\n"
        "  <id>20261115446200</id>\n"
        "  <acheteur><id>21130112200084</id></acheteur>\n"
        "  <nature>March\xe9</nature>\n"
        "  <montant>262578.28</montant>\n"
        "  <dateNotification>2026-02-11</dateNotification>\n"
        " </marche>\n"
        "</marches>\n"
    ).encode("ISO-8859-15")
    xml_path = tmp_path / "decp_small.xml"
    xml_path.write_bytes(xml)

    output_path = tmp_path / "out"
    xml_stream_to_parquet(str(xml_path), output_path, fix_chars=False)

    df = pl.read_parquet(output_path.with_suffix(".parquet"))
    assert df.height == 1
    assert df["acheteur_id"].to_list() == ["21130112200084"]
    # L'ISO-8859-15 doit être décodé correctement
    assert df["nature"].to_list() == ["Marché"]


def test_json_stream_to_parquet_small_file_detects_format(tmp_path):
    """Régression : un petit JSON DECP 2019 (< chunk_size) doit être détecté même si
    le premier chunk est tronqué par le buffering de stream_replace_bytestring (BOM
    + NaN->null), qui garde en réserve les derniers octets au cas où un motif serait
    coupé entre deux chunks. Avant le fix, la détection de format n'essayait que ce
    premier chunk tronqué et abandonnait avec le warning "Pas de match trouvé"."""
    content = (
        '{"$schema":"https://raw.githubusercontent.com/etalab/format-commande-publique'
        '/master/sch%C3%A9mas/json/paquet.json","marches":[{"id":"2019031700",'
        '"acheteur":{"id":"20005340300057","nom":"Région Normandie"},'
        '"nature":"Marché","objet":"Maintenance et assistance du logiciel CINDOC",'
        '"codeCPV":"72267100","procedure":"Marché négocié sans '
        'publicité ni mise en concurrence préalable","lieuExecution":'
        '{"code":"28000","typeCode":"Code postal","nom":"REGION NORMANDIE"},'
        '"dureeMois":48,"dateNotification":"2019-09-09",'
        '"datePublicationDonnees":"2019-10-04","montant":200000,'
        '"formePrix":"Révisable","titulaires":[{"typeIdentifiant":"SIRET",'
        '"id":"44882586900028","denominationSociale":"TECHNODOC"}],'
        '"modifications":[],"_type":"Marché"}]}'
    ).encode("utf-8")

    json_path = tmp_path / "decp_small.json"
    json_path.write_bytes(content)

    output_path = tmp_path / "out"
    resource = {
        "dataset_code": "decp_minef",
        "ori_filename": "decp_small.json",
        "dataset_name": "test",
    }

    fields, decp_format = json_stream_to_parquet(str(json_path), output_path, resource)

    assert decp_format is not None
    assert decp_format.label == "DECP 2019"
    df = pl.read_parquet(output_path.with_suffix(".parquet"))
    assert df.height == 1
    assert df["id"].to_list() == ["2019031700"]


def test_bootstrap_siret_latlong_produces_extended_schema(tmp_path, monkeypatch):
    fake_decp = tmp_path / "decp_fake.parquet"
    pl.DataFrame(
        {
            "acheteur_id": ["12345678901234", None, "00000000000000"],
            "acheteur_latitude": [48.85, None, 0.0],
            "acheteur_longitude": [2.35, None, 0.0],
            "titulaire_id": ["98765432109876", "98765432109876", None],
            "titulaire_latitude": [45.75, 45.75, None],
            "titulaire_longitude": [4.85, 4.85, None],
        }
    ).write_parquet(fake_decp)

    _original_scan = pl.scan_parquet

    def fake_scan(url, **kwargs):
        if str(url).startswith("https://"):
            return _original_scan(fake_decp)
        return _original_scan(url, **kwargs)

    monkeypatch.setattr("src.tasks.get.pl.scan_parquet", fake_scan)
    monkeypatch.setattr("src.tasks.get.publish_to_s3", lambda *a, **kw: None)
    monkeypatch.setattr("src.tasks.get.DATA_DIR", tmp_path)

    lf = bootstrap_siret_latlong()
    df = lf.collect()

    assert set(df.columns) == set(SIRET_LATLONG_SCHEMA.keys())
    assert df["source"].unique().to_list() == ["decp"]
    assert df["status"].unique().to_list() == ["success"]
    assert df["score"].is_null().all()
    assert df["geocoded_at"].is_null().all()
    assert df["siret"].n_unique() == df.height
