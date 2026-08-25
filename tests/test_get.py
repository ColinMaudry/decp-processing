import inspect
from datetime import date

import polars as pl

from src.config import SIRET_LATLONG_SCHEMA
from src.tasks.get import (
    bootstrap_siret_latlong,
    get_etablissements,
    get_labels_entreprises,
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


def test_get_labels_entreprises_materialise_dans_la_fonction_decoree(
    tmp_path, monkeypatch
):
    """Même raisonnement que pour get_etablissements : le sink_parquet doit être
    DANS la fonction décorée, pas dans l'appelant, pour que le retry protège
    effectivement le téléchargement. On fait pointer LABELS_BIO_URL/LABELS_RGE_URL
    (les constantes lues par get_labels_entreprises) vers des fichiers locaux
    plutôt que de monkeypatcher pl.read_csv/pl.read_parquet eux-mêmes : ces deux
    fonctions sont utilisées en interne par le moteur d'exécution de sink_parquet
    (constaté empiriquement — un monkeypatch global de pl.read_parquet corrompt
    silencieusement le résultat écrit, alors que collect() sur le même plan reste
    correct). Rediriger les URL est sans ce risque et exerce le vrai code de
    lecture (CSV ';' + parquet)."""
    fake_bio_df = pl.DataFrame({"SIRET": ["11111111111111", "22222222222222"]})
    fake_rge_df = pl.DataFrame(
        {
            "siret": ["11111111111111", "33333333333333"],
            "lien_date_debut": ["2000-01-01", "2000-01-01"],
            "lien_date_fin": ["2100-01-01", "2100-01-01"],
        }
    )
    bio_path = tmp_path / "bio.csv"
    rge_path = tmp_path / "rge.parquet"
    fake_bio_df.write_csv(bio_path, separator=";")
    fake_rge_df.write_parquet(rge_path)

    monkeypatch.setattr("src.tasks.get.LABELS_BIO_URL", str(bio_path))
    monkeypatch.setattr("src.tasks.get.LABELS_RGE_URL", str(rge_path))

    out = tmp_path / "labels.parquet"
    assert not out.exists()

    get_labels_entreprises(out, reference_date=date(2026, 1, 1))

    assert out.exists(), (
        "get_labels_entreprises n'a rien matérialisé : le sink_parquet doit "
        "être dans la fonction décorée, pas dans l'appelant."
    )
    df = pl.read_parquet(out)
    assert set(df.columns) == {"siret", "label_bio", "label_rge"}
    assert df.height == 3
    assert df["label_bio"].null_count() == 0
    assert df["label_rge"].null_count() == 0
    row = df.filter(pl.col("siret") == "11111111111111")
    assert row["label_bio"].item() is True
    assert row["label_rge"].item() is True
    assert df.filter(pl.col("siret") == "22222222222222")["label_rge"].item() is False
    assert df.filter(pl.col("siret") == "33333333333333")["label_bio"].item() is False


def test_get_labels_entreprises_reference_date_resolue_a_l_appel():
    """Piège classique : remettre `reference_date: date = date.today()` dans la
    signature ne serait évalué qu'une seule fois, à l'import du module, et
    figerait ensuite le pipeline sur cette date-là indéfiniment. Le default
    doit rester None dans la signature ; la résolution à date.today() doit
    se faire dans le corps de la fonction, à chaque appel."""
    default = (
        inspect.signature(get_labels_entreprises).parameters["reference_date"].default
    )
    assert default is None, (
        "reference_date doit valoir None par défaut dans la signature : la "
        "résolution à date.today() doit être faite dans le corps de la "
        "fonction, pas dans la signature (sinon elle n'est évaluée qu'une "
        "fois, à l'import du module)."
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
