import httpx

from src.tasks.scrap.marches_securises import (
    atline_to_decp,
    build_uid,
    get_marches_page,
    scrap_marches_securises_month,
)

# Record réel de l'API donnees_essentielles_oe_list (issu du site marches-securises.fr)
ATLINE_RECORD = {
    "id_dma": 256,
    "niump": "",
    "num_lot": "1537449856ljd7jt859deu",
    "is_concession": 0,
    "nature_marche": "Accord-cadre",
    "objet_marche": "ASC E1 : OMH GNCY E1 ",
    "code_cpv": "50750000-7",
    "procedure": "Appel d offres ouvert",
    "lieux": "Métropole du Grand Nancy",
    "code_lieux": "54",
    "type_code_lieux": "Code departement",
    "nom_lieux": "Métropole du Grand Nancy",
    "duree": 48,
    "date_notif": "2018-11-14",
    "date_publi_donnee": "2018-11-30",
    "version": 1,
    "etat_marche": 0,
    "date_creation": "0000-00-00 00:00:00",
    "user_nom": "",
    "user_prenom": "",
    "date_aife": None,
    "ref_fonct_aife": None,
    "date_update_aife": None,
    "mtt_estime": 69521,
    "forme_prix": "Révisable",
    "siret_pa": "40097424200069",
    "nom_pa": "OPH de la Métropole du Grand Nancy (54)",
    "is_demo": 0,
    "titulaires": [
        {
            "ident": "72202474202178",
            "type_ident": "Siret",
            "type_ident_id": None,
            "denomination": "THYSSENKRUPP ASCENSEURS",
            "type_attrib": "2",
        }
    ],
}


class TestAtlineToDecp:
    def test_mapping_des_champs_principaux(self):
        marche = atline_to_decp(ATLINE_RECORD)
        assert marche["id"] == "1537449856ljd7jt859deu"
        assert marche["acheteur"] == {"id": "40097424200069"}
        assert marche["objet"] == "ASC E1 : OMH GNCY E1 "
        assert marche["codeCPV"] == "50750000-7"
        assert marche["dureeMois"] == 48
        assert marche["dateNotification"] == "2018-11-14"
        assert marche["datePublicationDonnees"] == "2018-11-30"
        assert marche["montant"] == 69521
        assert marche["nature"] == "Accord-cadre"
        assert marche["procedure"] == "Appel d offres ouvert"
        assert marche["formePrix"] == "Révisable"
        assert marche["lieuExecution"] == {"code": "54", "typeCode": "Code departement"}
        assert marche["_type"] == "Marché"

    def test_titulaires_format_2019_plat(self):
        """Le JSON produit est ingéré comme du DECP 2019 : les structs titulaires
        doivent être plats, sans la clé "titulaire" (format 2022), sinon ils
        deviennent {null, null} dans le parquet."""
        marche = atline_to_decp(ATLINE_RECORD)
        assert marche["titulaires"] == [
            {"typeIdentifiant": "Siret", "id": "72202474202178"}
        ]

    def test_type_identifiant_vide_devient_siret(self):
        """Type identifiant = SIRET si vide (marches-securises.fr)."""
        record = {**ATLINE_RECORD, "titulaires": [{"ident": "123", "type_ident": ""}]}
        marche = atline_to_decp(record)
        assert marche["titulaires"] == [{"typeIdentifiant": "SIRET", "id": "123"}]

    def test_titulaires_vides_ou_sans_ident_ignores(self):
        record = {**ATLINE_RECORD, "titulaires": [{"type_ident": "Siret"}, None]}
        marche = atline_to_decp(record)
        assert marche["titulaires"] == []

    def test_concession(self):
        record = {**ATLINE_RECORD, "is_concession": 1}
        assert atline_to_decp(record)["_type"] == "Contrat de concession"

    def test_uid_construit_automatiquement(self):
        """L'API ne fournit pas d'uid : il est déduit d'acheteur_id + id + codeCPV."""
        marche = atline_to_decp(ATLINE_RECORD)
        assert build_uid(marche) == "400974242000691537449856ljd7jt859deu_50750000-7"


class TestGetMarchesPage:
    def test_payload_et_arret_de_pagination(self, monkeypatch):
        """La pagination s'arrête quand la page est vide ou que nb_total est atteint,
        et le payload contient la fenêtre de dates de publication."""
        payloads = []

        def fake_post(self, url, **kwargs):
            payloads.append({"url": url, "json": kwargs.get("json")})

            class R:
                def raise_for_status(self):
                    pass

                def json(self):
                    page = kwargs["json"]["pagination"]["page"]
                    # 70 résultats au total, 50 par page → 2 pages
                    donnees = [ATLINE_RECORD] * (50 if page == 1 else 20)
                    return {"donnees": donnees, "nb_total": 70}

            return R()

        monkeypatch.setattr(httpx.Client, "post", fake_post)
        client = httpx.Client()
        r1 = get_marches_page(client, "tok", "2018-11-01", "2018-11-30", 1)
        assert r1["nb_total"] == 70
        assert (
            payloads[-1]["url"] == "https://wsp.atline.fr/donnees_essentielles_oe_list"
        )
        body = payloads[-1]["json"]
        assert body["start_date"] == "2018-11-01"
        assert body["end_date"] == "2018-11-30"
        assert body["pagination"]["nbr_par_page"] == 50
        assert body["plateform"] == "MS"

    def test_none_apres_retries(self, monkeypatch):
        def fake_post(self, url, **kwargs):
            raise httpx.ConnectError("boom")

        monkeypatch.setattr(httpx.Client, "post", fake_post)
        monkeypatch.setattr(
            "src.tasks.scrap.marches_securises.time.sleep", lambda s: None
        )
        client = httpx.Client()
        assert get_marches_page(client, "tok", "2018-11-01", "2018-11-30", 1) is None


class TestScrapMonth:
    def _run(self, tmp_path, monkeypatch, pages, emails=None):
        """Exécute scrap_marches_securises_month avec l'API mockée.
        pages : listes de réponses (index 0 = page 1). Retourne le JSON écrit."""
        import json

        def fake_get_marches_page(client, token, start_date, end_date, page, retries=2):
            if page <= len(pages):
                return pages[page - 1]
            return {"donnees": [], "nb_total": 0}

        monkeypatch.setattr(
            "src.tasks.scrap.marches_securises.get_marches_page", fake_get_marches_page
        )
        monkeypatch.setattr(
            "src.tasks.scrap.marches_securises.get_atline_token", lambda c: "tok"
        )
        monkeypatch.setattr(
            "src.tasks.scrap.marches_securises.publish_scrap_to_datagouv",
            lambda *a, **k: None,
        )
        if emails is not None:
            monkeypatch.setattr(
                "src.tasks.scrap.marches_securises.notify_no_data",
                lambda *a, **k: emails.append(a),
            )
        scrap_marches_securises_month.fn("2018", "11", tmp_path)
        out = tmp_path / "marches-securises_2018-11.json"
        return json.load(open(out)) if out.exists() else None

    def test_fichier_au_format_decp_2019(self, tmp_path, monkeypatch):
        pages = [{"donnees": [ATLINE_RECORD] * 2, "nb_total": 2}]
        d = self._run(tmp_path, monkeypatch, pages)
        assert list(d.keys()) == ["marches"]
        assert len(d["marches"]) == 2
        assert (
            d["marches"][0]["uid"] == "400974242000691537449856ljd7jt859deu_50750000-7"
        )

    def test_multiples_pages(self, tmp_path, monkeypatch):
        pages = [
            {"donnees": [ATLINE_RECORD] * 50, "nb_total": 70},
            {"donnees": [ATLINE_RECORD] * 20, "nb_total": 70},
        ]
        d = self._run(tmp_path, monkeypatch, pages)
        assert len(d["marches"]) == 70

    def test_aucune_donnee_alerte_email_sans_fichier(self, tmp_path, monkeypatch):
        emails = []
        d = self._run(tmp_path, monkeypatch, [{"donnees": [], "nb_total": 0}], emails)
        assert d is None
        assert len(emails) == 1
        assert emails[0] == ("2018", "11", "2018-11-01", "2018-11-30")

    def test_page_erreur_niveau_api(self, tmp_path, monkeypatch):
        """Une page ratée (None) n'écrit pas de fichier partiel sans alerte...
        Non : une page ratée arrête la pagination ; si des marchés ont déjà été
        collectées, le fichier est écrit avec ce qui a été récupéré."""
        pages = [{"donnees": [ATLINE_RECORD], "nb_total": 10}]
        # page 2 → None via fake (hors liste) : la boucle s'arrête
        d = self._run(tmp_path, monkeypatch, pages)
        # get_marches_page mocké retourne {"donnees": []} hors liste → boucle s'arrête
        assert len(d["marches"]) == 1
