from pathlib import Path

import polars as pl
from prefect import flow
from prefect.transactions import transaction

from src.config import LOG_LEVEL, SIRENE_DATA_DIR
from src.flows.get_cog import get_cog
from src.tasks.get import (
    bootstrap_siret_latlong,
    get_etablissements,
    get_from_s3,
    get_labels_etablissements,
    get_labels_unites_legales,
    get_unite_legales,
)
from src.tasks.utils import create_sirene_data_dir, get_logger


def _parquet_est_a_jour(
    path: Path,
    colonnes_requises: set[str],
    colonnes_interdites: set[str] = frozenset(),
) -> bool:
    """Le parquet existe-t-il ET porte-t-il exactement les colonnes attendues ?

    Une garde sur la seule existence du fichier laisse passer les fichiers
    produits par une version antérieure du code : après l'ajout d'une colonne,
    le fichier du mois en cours existe toujours, n'est donc pas régénéré, et
    l'enrichissement échoue plus loin sur une ColumnNotFoundError. Vérifier le
    contenu rend le prétraitement auto-réparant.

    colonnes_interdites couvre le cas symétrique, celui d'une colonne *retirée* :
    un unites_legales.parquet d'une version antérieure porte encore label_ess,
    que la table de labels apporte désormais de son côté. La jointure verrait
    alors deux colonnes homonymes et en suffixerait une silencieusement en
    label_ess_right, produisant des labels faux plutôt qu'une erreur.

    collect_schema() ne lit que les métadonnées du parquet : instantané, même
    sur un fichier de plusieurs centaines de Mo.
    """
    if not path.exists():
        return False
    colonnes = set(pl.scan_parquet(path).collect_schema().names())
    return colonnes_requises <= colonnes and not (colonnes_interdites & colonnes)


@flow(log_prints=True)
def sirene_preprocess():
    """Prétraitement mensuel des données SIRENE afin d'économiser du temps lors du traitement quotidien des DECP.
    Pour chaque ressource (unités légales, établissements), un fichier parquet est produit.
    """

    logger = get_logger(level=LOG_LEVEL)

    logger.info("🚀  Pré-traitement des données SIRENE")
    # Soit les tâches de ce flow vont au bout (success), soit le dossier SIRENE_DATA_DIR est supprimé (voir remove_sirene_data_dir())
    with transaction():
        create_sirene_data_dir()

        # Récupération et préparation des données du Code Officiel Géographique
        get_cog()

        # Récupération du cache de géolocalisations
        lf_siret_latlong = get_from_s3(key="siret_latlong.parquet", prefix="")

        if not isinstance(lf_siret_latlong, pl.LazyFrame):
            lf_siret_latlong = bootstrap_siret_latlong()

        # Les labels sont préparés EN PREMIER, bien qu'ils ne dépendent de rien :
        # un échec ici déclenche le rollback de la transaction, donc la
        # suppression de SIRENE_DATA_DIR. Placé en dernier, le moindre incident
        # réseau de quelques minutes ferait jeter les stocks SIRENE téléchargés
        # pendant des heures juste avant. Ne pas le remettre à sa place
        # « logique ».
        # Les labels d'établissement (Bio, RGE) et d'unité légale (ESS,
        # association, Qualiopi, SIAE, avocat, achats responsables) proviennent
        # de deux ressources distinctes du même jeu de données, à deux
        # granularités : deux fichiers, deux clés de jointure.
        labels_siret_path = SIRENE_DATA_DIR / "labels_siret.parquet"
        if not _parquet_est_a_jour(labels_siret_path, {"label_bio", "label_rge"}):
            logger.info("Téléchargement et préparation des labels d'établissement...")
            get_labels_etablissements(labels_siret_path)
        else:
            logger.info(str(labels_siret_path) + " existe, skipping.")

        labels_siren_path = SIRENE_DATA_DIR / "labels_siren.parquet"
        if not _parquet_est_a_jour(
            labels_siren_path,
            {
                "label_ess",
                "label_association",
                "label_qualiopi",
                "label_siae",
                "label_avocat",
                "label_achats_responsables",
            },
        ):
            logger.info("Téléchargement et préparation des labels d'unité légale...")
            get_labels_unites_legales(labels_siren_path)
        else:
            logger.info(str(labels_siren_path) + " existe, skipping.")

        # préparer les données unités légales
        processed_ul_parquet_path = SIRENE_DATA_DIR / "unites_legales.parquet"
        if not _parquet_est_a_jour(
            processed_ul_parquet_path,
            {"denominationUniteLegale", "categorieEntreprise"},
            colonnes_interdites={"label_ess", "label_association"},
        ):
            logger.info("Téléchargement et préparation des unités légales...")
            get_unite_legales(processed_ul_parquet_path)
        else:
            logger.info(str(processed_ul_parquet_path) + " existe, skipping.")

        # préparer les données établissements
        # Aucune colonne n'a été ajoutée à ce fichier depuis sa création : la
        # simple existence suffit comme garde.
        processed_etab_parquet_path = SIRENE_DATA_DIR / "etablissements.parquet"
        if not processed_etab_parquet_path.exists():
            logger.info("Téléchargement et préparation des établissements...")
            get_etablissements(processed_etab_parquet_path, lf_siret_latlong)
        else:
            logger.info(str(processed_etab_parquet_path) + " existe, skipping.")

    logger.info("☑️  Fin du flow sirene_preprocess.")
