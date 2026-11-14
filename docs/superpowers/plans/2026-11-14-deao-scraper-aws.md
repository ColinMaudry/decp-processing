# Scraper DEAO www.marches-publics.info (AWS) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Ajouter à `scrap_closed` un scraper de `www.marches-publics.info` (AWS) qui publie une ligne par consultation/lot dans le dataset b, avec archivage S3 des pages d'avis (privé) et des RC (public), et 5 nouvelles colonnes de schéma.

**Architecture:** Nouveau paquet `src/tasks/scrap/deao/aws/` (parse, client, lignes, archive, scraper) branché par `plateforme: aws` dans le registre. Les briques communes avec Atexo (fichier précédent, dictionnaire SIRET, référentiels, accumulation) sont extraites dans `src/tasks/scrap/deao/plateformes.py` ; les aides S3 publiques restent dans `atexo/dce.py` et sont importées.

**Tech Stack:** Python 3.14, polars, httpx (+ `MockTransport` en test), lxml, tenacity, boto3, Prefect. Tests : `uv run pytest`.

**Spec:** `docs/superpowers/specs/2026-11-14-deao-scraper-marches-publics-info-design.md`

## Global Constraints

- Code de source : `www.marches-publics.info` ; `url_base: https://www.marches-publics.info` ; `plateforme: aws` ; `open_data: false` ; `min_rows: 2000`.
- `id` = `IDM` ; `idDernierAvis` = n° d'avis `MPI-pub-{n°}` ; `uid` = `{code}_{IDM}_{lotId}`, `lotId` = `"0"` si non allotie.
- `urlDce` toujours nul (DCE derrière captcha : aucune requête vers `dematEnt.login`, `dce.avertissement` ni aucun formulaire de retrait).
- Relecture B : page d'avis lue seulement si `IDM` nouveau, date limite ou nombre de lots changés par rapport au fichier précédent.
- S3 : page d'avis privée `{DEAO_AWS_S3_PREFIX}/{IDM}.html` (défaut `www.marches-publics.info/raw`, métadonnée `date-limite`) ; RC public `{DEAO_RC_S3_PREFIX}/www.marches-publics.info/{IDM}.pdf` (défaut `rc`, `ACL=public-read`) ; dernière version seulement.
- `urlReglementConsultation` = URL publique S3 du RC, sinon `https://www.marches-publics.info/mpiaws/index.cfm?fuseaction=dce.dRC&IDM={IDM}`.
- Requêtes : `CLOSED_USER_AGENT`, `Limiteur(CLOSED_INTERVALLE_REQUETES_S)`, retry sur transport/429/5xx, un client = une session, séquentiel.
- Nouvelles colonnes : `considerationsSociales`, `considerationsEnvironnementales`, `marcheReserve`, `groupementCommandes` (booléens), `variantes` (texte : `exigées`, `autorisées`, `interdites`) ; pas `closed_only`.
- Messages, logs, docstrings en français ; imports absolus `from src....`.

## Review Focus

1. Un avis présent dans plusieurs départements → une seule consultation (dédoublonnage sur `IDM`) et complétude comptée en `IDM` distincts — test en Task 4.
2. Dates courtes « 10/11/26 » et « 02/12/26 à 12h00 », date limite absente ou illisible → `None`, jamais d'exception sur tout le passage — tests en Task 3.
3. Pagination désynchronisée (onglet dont le nombre d'avis lus ≠ badge) → segment en erreur, recherche incomplète, aucune annulation — test en Task 4.
4. Page archivée illisible avec le parseur courant (`DEAO_AWS_RELIRE_ARCHIVE`) → la consultation est relue sur le site, pas d'arrêt — test en Task 7.
5. Entités HTML et apostrophes (`MAIRIE D&#039;AUDINCOURT`) → texte décodé, sans « (25400) » dans `acheteur_nom` — test en Task 3.

---

### Task 0: robots.txt et essai de cadence (contrôleur, avant toute implémentation)

Étape avec effets externes, faite par le contrôleur, pas par un sous-agent. But :
vérifier que le site tolère la cadence prévue, pas chercher sa limite.

- [ ] **Step 1:** Lire `https://www.marches-publics.info/robots.txt`. Si `/Annonces/`, `/mpiaws/` ou les téléchargements de RC sont interdits, ou si un `Crawl-delay` est fixé, s'arrêter et en parler à l'utilisateur (un `Crawl-delay` remplace 0,2 s).
- [ ] **Step 2:** Script jetable dans `$CLAUDE_JOB_DIR/tmp` (pas dans le dépôt), un seul client httpx avec `CLOSED_USER_AGENT` et `Limiteur(0.2)`, strictement séquentiel :
  1. quelques recherches (3 départements, pagination comprise) pour réunir environ 300 chemins de pages d'avis ;
  2. 300 GET de pages d'avis (le gros du trafic d'un premier passage), puis 20 GET de RC (`dce.dRC`, corps lu puis jeté).
     Arrêt immédiat au premier 429, 403 ou 5xx, à une page de captcha ou de blocage, ou si la médiane des 20 dernières latences dépasse 5 s.
- [ ] **Step 3:** Rapport : nombre de requêtes, codes HTTP, latences (médiane, p90, max) par tranche de 50 requêtes, pour voir si elles se dégradent au fil de l'essai, et `Retry-After` éventuel.
- [ ] **Step 4:** Décision :

  - aucun signe de gêne → 0,2 s gardé ;
  - sinon → nouvel essai de 100 requêtes à 0,5 s, puis proposer à l'utilisateur un intervalle propre à la source : champ `intervalle_s` dans le registre, lu par `ClientAws`, défaut `CLOSED_INTERVALLE_REQUETES_S`, ajouté à la Task 4.

  Consigner le résultat dans le ledger et dans `tests/data/aws/README.md` (Task 3).

### Task 1: Colonnes de schéma et transition

**Files:**

- Modify: `reference/schema_deao.json` (5 champs ajoutés après `urlAvis`)
- Modify: `src/flows/deao_processing.py:29` (`COLONNES_TRANSITOIRES`)
- Test: `tests/test_deao_schema.py` (ou le fichier de tests de schéma existant), `tests/test_deao_processing.py`

**Interfaces:**

- Produces: `published_schema()` et `open_schema()` contiennent les 5 colonnes (Boolean ×4, String pour `variantes`).

- [ ] **Step 1: Tests**

```python
def test_nouvelles_colonnes_publiees():
    s, o = published_schema(), open_schema()
    for c in ("considerationsSociales", "considerationsEnvironnementales",
              "marcheReserve", "groupementCommandes"):
        assert s[c] == pl.Boolean and c in o
    assert s["variantes"] == pl.String and "variantes" in o
```

Dans `tests/test_deao_processing.py`, étendre `test_ancien_fichier_accepte_et_renomme` : le fichier joue est aussi privé des 5 colonnes, et `dict(closed.schema) == published_schema()`.

- [ ] **Step 2: Lancer** `uv run pytest tests/test_deao_processing.py -k ancien -q` → FAIL.
- [ ] **Step 3: Implémenter** : champs JSON avec `title` « Considérations sociales », « Considérations environnementales », « Marché réservé », « Groupement de commandes », « Variantes » ; `variantes` avec `"enum": ["exigées", "autorisées", "interdites"]` ; descriptions d'une phrase disant que la valeur est nulle si la source ne la donne pas. Ajouter les 5 noms à `COLONNES_TRANSITOIRES`.
- [ ] **Step 4: Lancer** `uv run pytest -q -k "deao"` → PASS.
- [ ] **Step 5: Commit** `DEAO : 5 colonnes de schéma (considérations, marché réservé, variantes, groupement)`.

### Task 2: Briques communes des plateformes

**Files:**

- Create: `src/tasks/scrap/deao/plateformes.py`
- Modify: `src/tasks/scrap/deao/atexo/scraper.py`, `src/tasks/scrap/deao/atexo/accumulation.py`
- Modify (cibles de monkeypatch) : `tests/test_deao_atexo_scraper.py`

**Interfaces:**

- Produces, dans `src/tasks/scrap/deao/plateformes.py` (déplacés tels quels, renommés publics) :
  - `lire_precedent(entree: SourceDeao, dossier: Path) -> pl.DataFrame` (ex-`_lire_precedent`)
  - `dictionnaire_partage(today: date) -> pl.DataFrame | None` (ex-`_dictionnaire_partage`, avec `_dictionnaire`, le verrou et le cache)
  - `charger_referentiels()`, `referentiels_enrichissement() -> tuple[LazyFrame, LazyFrame, DataFrame]` (ex-`_referentiels`, avec `_VERROU_SIRENE`)
  - `instant_du_passage`, `fusionner`, `annulations_du_passage` (déplacés depuis `atexo/accumulation.py`, avec `_conformer`, `_fin_de_journee`) et `SEUIL_ANNULATIONS = 0.1`
  - `geocoder_adresses` importé ici et utilisé par les deux scrapers.
- `atexo/accumulation.py` garde `lots_a_relire`, `dce_a_telecharger`, `recherche_complete` et importe le reste depuis `plateformes`.

- [ ] **Step 1:** Déplacer, mettre à jour les imports d'Atexo et les cibles `monkeypatch.setattr(scraper, "charger_referentiels" | "geocoder_adresses", …)` vers le module `plateformes`.
- [ ] **Step 2: Lancer** `uv run pytest -q` → 707+ passed, aucun test supprimé (comparer le nombre de `def test_` par fichier avant/après).
- [ ] **Step 3: Commit** `DEAO : briques communes des scrapers de plateformes`.

### Task 3: Parseur AWS et fixtures

**Files:**

- Create: `src/tasks/scrap/deao/aws/__init__.py`, `src/tasks/scrap/deao/aws/parse.py`
- Create: `tests/data/aws/*.html`, `tests/test_deao_aws_parse.py`

**Interfaces:**

- Produces (`aws/parse.py`) :

  - `@dataclass(frozen=True) AvisListe`: `idm: str`, `numero_avis: str`, `chemin_avis: str` (ex. `/Annonces/MPI-pub-2026314252.htm`), `acheteur_nom: str`, `code_postal: str | None`, `reference: str | None`, `objet: str`, `nombre_lots: int`, `date_publication: date`, `date_limite: datetime | None`, `nature: str` (`Travaux`, `Services`, `Fournitures`), `considerations_sociales: bool`, `considerations_environnementales: bool`
  - `@dataclass(frozen=True) LotAvis`: `numero: str`, `libelle: str`, `description: str | None`, `lieu: str | None`, `code_postal_lieu: str | None`, `montant_estime: float | None`, `code_cpv: str | None`
  - `@dataclass(frozen=True) Avis`: `siret: str | None`, `acheteur_nom: str`, `adresse_voie: str | None`, `adresse_cp: str | None`, `adresse_ville: str | None`, `objet: str`, `reference: str | None`, `type_marche: str | None`, `mode: str | None`, `technique: str | None`, `nuts: str | None`, `duree_mois: int | None`, `description: str | None`, `cpv_principal: str | None`, `lots: list[LotAvis]`, `date_limite: datetime | None`, `date_envoi: date | None`, `marche_reserve: bool | None`, `variantes: str | None`, `groupement: bool | None`, `considerations_sociales: bool | None`, `considerations_environnementales: bool | None`
  - `date_courte(texte: str) -> date` (« 10/11/26 » → 2026-11-10) ; `date_heure_courte(texte: str) -> datetime | None` (« 02/12/26 à 12h00 », fuseau Europe/Paris ; illisible → `None`)
  - `trop_de_resultats(html: str) -> int | None` (« Votre recherche trouve 4093 résultats » → 4093)
  - `badges(html: str) -> dict[str, int]` (clés `"t"`, `"s"`, `"f"` depuis `badge_travaux|services|fournitures`)
  - `parse_liste(html: str, onglet: str | None = None) -> list[AvisListe]` (`onglet` `"t"|"s"|"f"` = panneaux `id="1"|"2"|"3"` ; `None` = tous)
  - `departements(html: str) -> list[str]` (valeurs d'un seul département du `<select name="IDR">`, sans `X` ni valeurs contenant une virgule)
  - `parse_avis(html: str) -> Avis`

- [ ] **Step 1: Capturer les fixtures** (seule tâche autorisée à interroger le site, au plus ~15 requêtes espacées d'1 s, `User-Agent` de `CLOSED_USER_AGENT`) : `rechercher.html` (GET `/Annonces/rechercher`) ; `liste_trop_large.html` (POST national, `IDR=X`) ; `liste_13_S_p1.html` (POST `IDR=13&IDN=S`) et `liste_13_S_p2.html` (GET `/Annonces/lister?pager_s=2` dans la même session) ; `liste_25.html` (POST `IDR=25`, plusieurs onglets) ; deux pages d'avis (une allotie, une non allotie, dont une avec « Marché réservé : Oui » ou des variantes autorisées si possible) en `avis_{n°}.html`. Corps du POST : celui de la spec (§ Recherche). Ne jamais suivre de lien DCE. Noter dans un `tests/data/aws/README.md` la date et la provenance.
- [ ] **Step 2: Tests** (valeurs à lire dans les fixtures capturées) :

```python
def test_trop_de_resultats():
    assert trop_de_resultats(lire("liste_trop_large.html")) > 1000
    assert trop_de_resultats(lire("liste_25.html")) is None

def test_badges_et_liste_par_onglet():
    html = lire("liste_13_S_p1.html")
    assert badges(html)["s"] > 10 and badges(html)["t"] == 0
    avis = parse_liste(html, "s")
    assert len(avis) == 10 and all(a.nature == "Services" for a in avis)
    assert all(a.idm.isdigit() and a.chemin_avis.startswith("/Annonces/MPI-pub-") for a in avis)

def test_nom_sans_code_postal_et_entites():
    a = parse_liste(lire("liste_25.html"))[0]
    assert "(" not in a.acheteur_nom and "&#039;" not in a.acheteur_nom
    assert a.code_postal is not None and len(a.code_postal) == 5

def test_dates_courtes():
    assert date_courte("10/11/26") == date(2026, 11, 10)
    assert date_heure_courte("02/12/26 à 12h00") == datetime(2026, 12, 2, 12, 0, tzinfo=ZoneInfo("Europe/Paris"))
    assert date_heure_courte("") is None and date_heure_courte("à préciser") is None

def test_departements():
    d = departements(lire("rechercher.html"))  # valeurs du formulaire : "1" pour l'Ain
    assert {"1", "13", "75"} <= set(d) and {"2a", "2b"} <= {x.lower() for x in d}
    assert "X" not in d and not any("," in x for x in d) and len(d) >= 96

def test_avis_alloti():
    a = parse_avis(lire("avis_<n° alloti>.html"))
    assert a.siret and len(a.siret) == 14 and a.lots and all(l.code_cpv for l in a.lots)
    assert a.date_limite is not None and a.cpv_principal
```

Plus un test `parse_avis` non alloti (`lots == []`), et un test des booléens (`marche_reserve`, `groupement`, `variantes`) avec les valeurs lues dans les fixtures. Le pictogramme `ico-critere-environnement` donne `considerations_environnementales=True` dans `AvisListe` ; repérer le pictogramme social dans les fixtures (s'il n'y en a pas, test sur un bloc modifié en remplaçant le nom du pictogramme environnement par celui que le formulaire `rechercher.html` associe aux considérations sociales).

- [ ] **Step 3: Lancer** → FAIL. **Step 4:** Implémenter avec lxml. Une ligne de la page d'avis absente → champ `None` (jamais d'exception sauf page sans objet ni acheteur → `ValueError`). Dans `Avis`, `considerations_*` = `True`/`False` seulement si la page d'avis le dit explicitement, sinon `None`. **Step 5: Lancer** → PASS.
- [ ] **Step 6: Commit** `DEAO AWS : parseur de liste et de page d'avis`.

### Task 4: Client AWS

**Files:**

- Create: `src/tasks/scrap/deao/aws/client.py`, `tests/test_deao_aws_client.py`

**Interfaces:**

- Consumes: `parse.*` (Task 3), `Limiteur`, `config.CLOSED_USER_AGENT`.
- Produces:

  - `RESULTATS_PAR_PAGE = 10`
  - `@dataclass ResultatRecherche`: `avis: list[AvisListe]` (dédoublonnés sur `idm`, premier vu gardé), `total_avant: int | None`, `total_apres: int | None`, `segments_en_erreur: list[str]` ; propriété `complete: bool` = pas de segment en erreur, totaux connus et `min(total_avant, total_apres) <= len(avis) <= max(...)`
  - `class ClientAws(url_base: str, http: httpx.Client | None = None, limiteur: Limiteur | None = None)` : `rechercher() -> ResultatRecherche`, `page_avis(chemin: str) -> str`, `telecharger_rc(idm: str, dest_dir: Path) -> Path` (fichier `{idm}.pdf`, `ValueError` si la réponse est du HTML), `close()`
  - `url_rc_plateforme(url_base: str, idm: str) -> str`

- [ ] **Step 1: Tests** (`httpx.MockTransport` servant les fixtures de Task 3 selon méthode, chemin et corps) :
  - `test_decoupage_recursif` : POST `IDR=13` → `liste_trop_large.html` ; POST `IDR=13&IDN=S` → `liste_13_S_p1.html` ; `IDN=T`/`F` → pages à 0 avis ; vérifier que les requêtes POST faites sont `IDR=13` puis `IDN` ∈ {T, S, F} et que `IDP` n'est utilisé que si une nature reste trop large.
  - `test_pagination_onglet` : badge services = 12 → GET `/Annonces/lister?pager_s=2` ; 12 avis lus.
  - `test_onglet_incomplet_segment_en_erreur` : badge 12, page 2 vide → `segments_en_erreur == ["13/S"]` (format `{dep}[/{nature}[/{procédure}]]`), `complete is False`.
  - `test_avis_dans_deux_departements` : même `IDM` servi pour `IDR=25` et `IDR=90` → un seul avis.
  - `test_completude_tolerance` : totaux national 100 avant / 103 après, 101 lus → `complete` ; 98 lus → non complet.
  - `test_aucune_requete_dce` : sur un passage complet simulé, aucune URL ne contient `dematEnt`, `avertissement` ni `type=DCE`.
- [ ] **Step 2: Lancer** → FAIL. **Step 3: Implémenter** : départements lus par GET `/Annonces/rechercher` ; total national lu avant et après (POST `IDR=X`, `trop_de_resultats`, ou somme des badges si accepté) ; découpage département → nature → procédure (`PA`, `AOO`, `AOE`, `MN`, `CO`, `CR`, `DC`, `AU`, `DSP`) ; un segment en échec HTTP ou trop large en bout de chaîne est noté et la collecte continue. Retry tenacity comme `ClientAtexo._requete`. **Step 4: Lancer** → PASS.
- [ ] **Step 5: Commit** `DEAO AWS : client de recherche (découpage, pagination, complétude)`.

### Task 5: Lignes AWS

**Files:**

- Create: `src/tasks/scrap/deao/aws/lignes.py`, `tests/test_deao_aws_lignes.py`

**Interfaces:**

- Produces: `lignes_avis(code: str, url_base: str, liste: AvisListe, avis: Avis, url_rc: str, stats: Counter | None = None) -> list[dict]` ; `libelle_procedure_aws(mode: str | None, stats: Counter | None) -> str`.

- [ ] **Step 1: Tests** : avis alloti de 3 lots → 3 lignes `uid` `www.marches-publics.info_{idm}_1..3`, `codeCPV` du lot ; non alloti → 1 ligne `lotId == "0"`, `codeCPV` = CPV principal ; `id == idm`, `idDernierAvis == numero_avis`, `urlConsultation == url_base + chemin_avis`, `urlDce is None`, `urlReglementConsultation == url_rc` ; `acheteur_id == siret` ; `procedure` : « Procédure adaptée ouverte » → « Procédure adaptée », mode inconnu → « Autre procédure » et `stats["procedure_inconnue:<mode>"] == 1` ; nouvelles colonnes : liste `considerations_environnementales=True` et avis `None` → `True` ; avis `False` et liste `False` → `False` ; les deux muets (`False` liste, `None` avis) → `None` ; `lieuExecution_code` = département du code postal du lieu du lot (« 25400 » → « 25 », « 97411 » → « 974 »), à défaut du code postal de l'acheteur, `lieuExecution_typeCode == "Code département"` ; `dureeMois`, `techniques`, `montantEstime` repris.
- [ ] **Step 2: Lancer** → FAIL. **Step 3: Implémenter** sur le modèle de `atexo/lignes.py` (mêmes clés transitoires `acheteur_adresse_*`, `acheteur_pays`, `rectifie=False`, `infosRectificatifs=[]`, `avisLies=[]`) ; libellés de procédure pris dans les valeurs de `atexo.lignes.PROCEDURES`. **Step 4: Lancer** → PASS.
- [ ] **Step 5: Commit** `DEAO AWS : lignes par consultation et lot`.

### Task 6: Archive S3 AWS

**Files:**

- Modify: `src/config.py` (`DEAO_AWS_S3_PREFIX`, `DEAO_RC_S3_PREFIX`, `DEAO_AWS_RELIRE_ARCHIVE` ; `PLATEFORMES_DEAO = {"atexo", "aws"}`)
- Create: `src/tasks/scrap/deao/aws/archive.py`, `tests/test_deao_aws_archive.py`

**Interfaces:**

- Consumes: `ErreurDepotS3`, `url_publique` depuis `src.tasks.scrap.deao.atexo.dce`.
- Produces:
  - `cle_avis(idm: str) -> str` → `f"{DEAO_AWS_S3_PREFIX}/{idm}.html"` ; `cle_rc(code: str, idm: str) -> str` → `f"{DEAO_RC_S3_PREFIX}/{code}/{idm}.pdf"`
  - `marque_date_limite(d: datetime | None) -> str` (ISO minute, `""` si `None`)
  - `lire_avis_archive(idm: str, date_limite: str | None) -> str | None` : `None` si absent ; si `date_limite` n'est pas `None`, `None` aussi quand la métadonnée `date-limite` diffère
  - `deposer_avis(idm: str, html: str, date_limite: str) -> None` (privé : sans `ACL`, `ContentType: text/html; charset=utf-8`, `Metadata={"date-limite": …}`)
  - `deposer_rc(chemin: Path, cle: str) -> str` (`ACL=public-read`, `ContentType=application/pdf`, supprime le fichier local, renvoie `url_publique(cle)`)
  - Erreur S3 autre qu'objet absent → `ErreurDepotS3`.
- `config.DEAO_AWS_RELIRE_ARCHIVE` : vrai si la variable vaut `true` (insensible à la casse).

- [ ] **Step 1: Tests** avec un faux client S3 (sur le modèle de `FakeS3` de `tests/test_deao_atexo_scraper.py`, `monkeypatch.setattr(archive, "s3_client", …)`) : clés exactes (`rc/www.marches-publics.info/1902107.pdf`, `www.marches-publics.info/raw/1902107.html`) ; `deposer_avis` sans ACL et avec métadonnée ; `deposer_rc` en `public-read` ; `lire_avis_archive` : même date → HTML, date différente → `None`, absent → `None`, `date_limite=None` → HTML quelle que soit la métadonnée ; panne S3 → `ErreurDepotS3`.
- [ ] **Step 2: Lancer** → FAIL. **Step 3: Implémenter.** **Step 4: Lancer** → PASS.
- [ ] **Step 5: Commit** `DEAO AWS : archive S3 des pages d'avis (privée) et des RC (publique)`.

### Task 7: Passage du scraper AWS et branchement

**Files:**

- Create: `src/tasks/scrap/deao/aws/scraper.py`, `tests/test_deao_aws_scraper.py`
- Modify: `src/tasks/scrap/deao/contrat.py` (`scrapers_du_registre` : `plateforme == "aws"` → `scraper_aws(code)`)
- Modify: `src/tasks/deao/dedoublonnage.py` (`_ID_CONSULTATION`), `tests/test_deao_dedoublonnage.py`

**Interfaces:**

- Consumes: Tasks 2 à 6.
- Produces: `scraper_aws(code: str, client_factory: Callable[[str], ClientAws] = ClientAws) -> Callable[[date], pl.DataFrame]`.

Passage (`_passage(code, today, client_factory)`), dans l'ordre : `lire_precedent`, `dictionnaire_partage(today)`, `referentiels_enrichissement()`, `client.rechercher()` ; IDM à relire = nouveaux, ou `dateLimiteReponse` (à la minute) / nombre de lots (lignes `lotId != "0"`) différents du précédent. Pour chacun : `lire_avis_archive(idm, marque)` sinon `page_avis` puis `deposer_avis` ; puis `telecharger_rc` + `deposer_rc` (en échec : lien plateforme, compteur `rc_en_erreur`) ; puis `lignes_avis`. Si `DEAO_AWS_RELIRE_ARCHIVE` : les autres consultations de la liste sont reconstruites depuis `lire_avis_archive(idm, None)` (page absente ou `parse_avis` en erreur → relue sur le site). Archivage seulement si `publication_autorisee() and config.S3_BUCKET` (sinon : pages lues sur le site sans dépôt, `urlReglementConsultation` = lien plateforme, ou URL S3 du fichier précédent si elle existe) ; `verifier_depot_s3(code)` en début de passage (il passe par `atexo.dce.s3_client` : les tests patchent `dce.s3_client` et `archive.s3_client`) ; `ErreurDepotS3` coupe l'archivage pour le reste du passage puis `ScrapPartiel` en fin. Enrichissement, `fusionner(..., complete=resultat.complete, instant)`, alerte d'annulations > `SEUIL_ANNULATIONS` et log de synthèse comme Atexo.

- [ ] **Step 1: Tests** (faux client renvoyant `ResultatRecherche` et HTML de fixtures ; `FakeS3` ; `fake_datagouv` pour le fichier précédent) :
  - `test_premier_passage` : 2 avis → lignes publiées au schéma, `deposer_avis` ×2, RC ×2 (`urlReglementConsultation` en `https://…/rc/www.marches-publics.info/{idm}.pdf`).
  - `test_relecture_b` : second passage même liste → aucun `page_avis` ; date limite d'un avis changée → un seul `page_avis`.
  - `test_cache_s3_reprise` : page déjà archivée avec la même date limite → aucun `page_avis`, lignes produites.
  - `test_relire_archive` : `DEAO_AWS_RELIRE_ARCHIVE=True`, liste inchangée → lignes reconstruites, aucune requête site ; page archivée illisible → `page_avis` appelé pour elle seule.
  - `test_rc_en_erreur_lien_plateforme`.
  - `test_s3_en_panne_scrap_partiel` : `ScrapPartiel` levé, lignes présentes dans `e.lignes`.
  - `test_recherche_incomplete_pas_d_annulation` : consultation ouverte absente d'une recherche `complete=False` → statut inchangé.
  - `test_scrapers_du_registre_aws` (dans `tests/test_deao_scrap_closed.py`) : une entrée `plateforme: aws` donne un scraper.
  - Dédoublonnage : URL BOAMP `https://www.marches-publics.info/mpiaws/index.cfm?fuseaction=dce.dRC&IDM=1902107` appariée exactement à la consultation `id == "1902107"` de `www.marches-publics.info`.
- [ ] **Step 2: Lancer** → FAIL. **Step 3: Implémenter** ; `_ID_CONSULTATION = r"(?:[?&](?:id|IDM)=|/consultation/)(\d+)"`. **Step 4:** `uv run pytest -q` → tout PASS.
- [ ] **Step 5: Commit** `DEAO AWS : passage du scraper (relecture B, cache S3, RC) et branchement`.

### Task 8: Mise en registre et essai réel (contrôleur)

Étapes avec effets externes, faites par le contrôleur de la session, pas par un sous-agent.

- [ ] **Step 1:** (robots.txt déjà vérifié en Task 0.)
- [ ] **Step 2:** Créer dans le dataset b la ressource `www.marches-publics.info.parquet` (parquet vide au `published_schema()`, description « Données d'appels d'offres du profil d'acheteur marches-publics.info (AWS) (https://www.marches-publics.info) au format DEAO. »).
- [ ] **Step 3:** Ajouter l'entrée au registre (`plateforme: aws`, `min_rows: 2000`, `resource_id` créé) ; étendre `test_registre_reference` (38 sources, 36 plateformes).
- [ ] **Step 4:** Essai réel sans publication ni S3 (`DECP_PROCESSING_PUBLISH` vide) : `ClientAws.rechercher()` complet + `parse_avis` et `lignes_avis` sur 20 avis tirés au hasard ; vérifier `complete`, le nombre d'IDM ≈ total national, et le taux de SIRET, CPV, date limite renseignés.
- [ ] **Step 5:** `uv run pytest -q`, pre-commit, commit `DEAO : source www.marches-publics.info (AWS)` ; mettre à jour la mémoire `deao-deploiement-prerequis.md` (ressource, premier passage long, `DEAO_AWS_RELIRE_ARCHIVE`).
