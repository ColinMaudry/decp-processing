import polars as pl
import os
from tasks.setup import create_table_artifact
from datetime import datetime
# from config import DIST_DIR
from tasks.get import get_stats
from collections import Counter
import json
DATE_NOW = datetime.now().isoformat()[0:10]  # YYYY-MM-DD
DIST_DIR = f"dist/" + DATE_NOW

def list_data_issues(df: pl.LazyFrame):
    df = df.collect()

    # Dates impossibles

    date_columns = [
        "dateNotification",
        "dateNotificationActeSousTraitance",
        "dateNotificationModificationModification",
        "dateNotificationModificationSousTraitanceModificationActeSousTraitance",
        "datePublicationDonnees",
        "datePublicationDonneesActeSousTraitance",
        "datePublicationDonneesModificationActeSousTraitance",
        "datePublicationDonneesModificationModification",
    ]

    for column in date_columns:
        print(
            "Dates impossibles dans la colonne ",
            column,
            ":",
            df.filter(
                (pl.col(column) < pl.date(2015, 1, 1))
                | (pl.col(column) > datetime.now())
            ).height,
        )


def generate_stats(df: pl.DataFrame):
    now = datetime.now()

    df_titulaires = pl.DataFrame(columns=["titulaire_id", "titulaire_typeIdentifiant"])

    for i in range(1, 4):
        df_temp = df[[f"titulaire_id_{i}", f"titulaire_typeIdentifiant_{i}"]]
        df_temp = df_temp.rename(
            columns={
                f"titulaire_id_{i}": "titulaire_id",
                f"titulaire_typeIdentifiant_{i}": "titulaire_typeIdentifiant",
            }
        )
        df_titulaires = pl.concat([df_titulaires, df_temp], ignore_index=True)

    df.to_pickle("data/decp_before_stats.pkl")

    stats = [
        {
            "datetime": now.isoformat()[:-7],  # jusqu'aux secondes
            "année": str(now.year),
            "mois": str(now.month).zfill(2),
            "dataset": "decp-augmente-minef",
            "nb_lignes": df.index.size,
            "nb_colonnes": len(df.columns),
            "nb_marches": df[["id", "acheteur_id"]].drop_duplicates().index.size,
            "nb_acheteurs_uniques": df[["acheteur_id"]].drop_duplicates().index.size
            - 1,  # -1 pour ne pas compter la valeur "acheteur vide"
            "nb_titulaires_uniques": df_titulaires.drop_duplicates().index.size
            - 1,  # -1 pour ne pas compter la valeur "titulaire vide"
            # "2024_nb_notifications": df.loc[
            #     df["dateNotification"].str.startswith("2024")
            # ].index.size,
            "2024_nb_publications": df.loc[
                df["datePublicationDonnees"].dt.year == 2024
            ].index.size,
            "2023_nb_notifications": df.loc[
                df["dateNotification"].dt.year == 2023
            ].index.size,
            "2023_nb_publications": df.loc[
                df["datePublicationDonnees"].dt.year == 2023
            ].index.size,
            "nb_marches_format_arrete_2022": df.loc[df["ccag"] != ""].index.size,
        }
    ]

    # df_per_source = (
    #     df[["id", "acheteur_id", "source"]].drop_duplicates().groupby(by="source").count()
    # )

    # for idx in df_per_source.index:
    #     if idx == "":
    #         source = "source-manquante"
    #     else:
    #         source = idx

    #     stats[source] = df_per_source.loc[idx, "id"]

    df_stats_dgfr: pl.DataFrame = get_stats()
    df_stats_dgfr = pl.concat([df_stats_dgfr, pl.DataFrame(stats)], ignore_index=True)
    df_stats_dgfr.to_csv(f"{DIST_DIR}/statistiques.csv")





def count_and_print_modifications(json_path, i_modif=None, i_marche_modifie=None):
    """
    si i_modif = i_marche_modifie = None, le code print le nombre de marchés modifiés (1 marché avec 17 modif, 4 avec 18 modifs, etc)
    si i_modif = 13 et i_marche_modifie = None, le code print tout les marchés avec 13 modification et leur contenu.
    si i_modif = 13 et i_marche_modifie = 3, le code print le contenu du 3ème marché avec 13 modifications uniquement.
    """
    print(f"📥 Lecture du fichier JSON : {json_path}")
    with open(json_path, "r", encoding="utf-8") as f:
        data = json.load(f)

    if isinstance(data, dict) and "marches" in data:
        data = data["marches"]
    elif not isinstance(data, list):
        print("❌ Format inattendu.")
        return

    compteur = Counter()
    total = 0
    marches_to_print = []

    for idx, marche in enumerate(data):
        mods = marche.get("modifications", [])
        if not isinstance(mods, list):
            mods = []
        nb_mods = len(mods)
        compteur[nb_mods] += 1
        total += 1

        if i_modif is not None and nb_mods == i_modif:
            marches_to_print.append((idx, marche))

    print(f"\n📊 Stats sur les modifications ({total} marchés) :\n")
    for nb_mods in sorted(compteur):
        print(f"🔹 {nb_mods} modification(s) : {compteur[nb_mods]:,} marché(s)")

    if i_modif is not None:
        if not marches_to_print:
            print(f"\n❌ Aucun marché avec {i_modif} modification(s) trouvé.")
        else:
            print(f"\n📋 Marchés avec {i_modif} modification(s) :")
            selected_marches = [marches_to_print[i_marche_modifie]] if i_marche_modifie is not None else marches_to_print
            for idx, marche in selected_marches:
                mods = marche.get("modifications", [])
                print(f"\n✅ Marché à l'index {idx}")
                print(f"🔑 ID : {marche.get('id', 'N/A')}")
                print(f"📝 Objet : {marche.get('objet', 'N/A')}")
                print(f"📅 Date de notification : {marche.get('dateNotification', 'N/A')}")
                print(f"🛠️ Nombre de modifications : {len(mods)}\n")
                for i, mod in enumerate(mods, 1):
                    print(f"   ✏️ Modification {i}: {json.dumps(mod, indent=2, ensure_ascii=False)}")

    return compteur


count_and_print_modifications("data/decp-2022.json",i_modif=11,i_marche_modifie=2)



