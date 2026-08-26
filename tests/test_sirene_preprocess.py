import polars as pl

from src.flows.sirene_preprocess import _parquet_est_a_jour


def test_parquet_absent_est_a_regenerer(tmp_path):
    assert not _parquet_est_a_jour(tmp_path / "absent.parquet", {"label_ess"})


def test_parquet_sans_les_colonnes_attendues_est_a_regenerer(tmp_path):
    """Régression : une garde sur la seule existence du fichier laisse passer
    les fichiers produits par une version antérieure du code. Après l'ajout de
    label_ess, l'unites_legales.parquet du mois en cours existe toujours, n'est
    donc pas régénéré, et l'enrichissement échoue plus loin sur une
    ColumnNotFoundError."""
    path = tmp_path / "unites_legales.parquet"
    pl.DataFrame({"siren": ["111111111"]}).write_parquet(path)

    assert not _parquet_est_a_jour(path, {"label_ess", "label_association"})


def test_parquet_complet_est_a_jour(tmp_path):
    path = tmp_path / "unites_legales.parquet"
    pl.DataFrame(
        {"siren": ["111111111"], "label_ess": [True], "label_association": [False]}
    ).write_parquet(path)

    assert _parquet_est_a_jour(path, {"label_ess", "label_association"})


def test_colonnes_supplementaires_ne_genent_pas(tmp_path):
    """La garde vérifie une inclusion, pas une égalité : un fichier portant plus
    de colonnes que demandé reste valide."""
    path = tmp_path / "labels_siret.parquet"
    pl.DataFrame(
        {
            "siret": ["11111111111111"],
            "label_bio": [True],
            "label_rge": [False],
            "label_futur": [True],
        }
    ).write_parquet(path)

    assert _parquet_est_a_jour(path, {"label_bio", "label_rge"})


def test_parquet_portant_une_colonne_retiree_est_a_regenerer(tmp_path):
    """Cas symétrique de la colonne ajoutée : label_ess a quitté
    prepare_unites_legales pour la table de labels. Un unites_legales.parquet
    d'une version antérieure le porte encore, et la jointure verrait alors deux
    colonnes homonymes, dont l'une serait silencieusement suffixée
    label_ess_right — des labels faux plutôt qu'une erreur."""
    path = tmp_path / "unites_legales.parquet"
    pl.DataFrame(
        {
            "siren": ["111111111"],
            "denominationUniteLegale": ["Org 1"],
            "categorieEntreprise": ["ETI"],
            "label_ess": [True],
        }
    ).write_parquet(path)

    assert not _parquet_est_a_jour(
        path,
        {"denominationUniteLegale", "categorieEntreprise"},
        colonnes_interdites={"label_ess", "label_association"},
    )


def test_parquet_sans_les_colonnes_interdites_est_a_jour(tmp_path):
    path = tmp_path / "unites_legales.parquet"
    pl.DataFrame(
        {
            "siren": ["111111111"],
            "denominationUniteLegale": ["Org 1"],
            "categorieEntreprise": ["ETI"],
        }
    ).write_parquet(path)

    assert _parquet_est_a_jour(
        path,
        {"denominationUniteLegale", "categorieEntreprise"},
        colonnes_interdites={"label_ess", "label_association"},
    )
