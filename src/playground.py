
import pandas as pd
import json
import ast
import numpy as np
import polars as pl


def safe_eval(value):
    """
    Gère les cas où actesSousTraitance est une string JSON, None, ou liste.
    """
    if isinstance(value, str):
        try:
            return ast.literal_eval(value)
        except (ValueError, SyntaxError):
            return []
    elif value is None:
        return []
    elif isinstance(value, list):
        return value
    else:
        return []

def test_extract_actes_sous_traitance_polars(json_path):
    print(f"📥 Lecture du fichier JSON : {json_path}")

    with open(json_path, "r", encoding="utf-8") as f:
        raw = json.load(f)

    if isinstance(raw, dict) and "marches" in raw:
        raw = raw["marches"]
    elif not isinstance(raw, list):
        print("❌ Format inattendu.")
        return

    print(f"🔍 {len(raw)} marchés chargés.")

    # Filtrer ceux avec actesSousTraitance non vides
    with_actes = [
        m for m in raw
        if isinstance(m.get("actesSousTraitance"), (list, str)) and m["actesSousTraitance"]
    ]
    print(f"🔎 Nombre de marchés avec actesSousTraitance non vides : {len(with_actes)}")

    if not with_actes:
        print("❌ Aucun marché avec actesSousTraitance trouvé.")
        return

    # Ajouter un champ uid
    for m in with_actes:
        m["uid"] = m.get("id")

    # Parser proprement les actes
    rows = []
    for m in with_actes:
        actes = safe_eval(m["actesSousTraitance"])
        if not isinstance(actes, list):
            actes = []
        for acte in actes:
            if isinstance(acte, dict):
                flat = acte.copy()
                flat["uid"] = m["uid"]
                rows.append(flat)

    if not rows:
        print("❌ Aucun acte exploitable trouvé.")
        return

    df = pl.from_dicts(rows)

    # Extraction explicite des sous-traitants si présents
    if "sousTraitant" in df.columns and df["sousTraitant"].dtype == pl.Struct:
        print("✅ Champ 'sousTraitant' trouvé, extraction en cours...")

        df = df.with_columns([
            pl.col("sousTraitant").struct.field("id").alias("siretSousTraitant"),
            pl.col("sousTraitant").struct.field("typeIdentifiant").alias("typeIdentifiantSousTraitant")
        ]).drop("sousTraitant")


    elif "sousTraitant.id" in df.columns and "sousTraitant.typeIdentifiant" in df.columns:
        print("✅ Champs sousTraitant.* détectés en colonnes, renommage en cours...")
        df = df.rename({
            "sousTraitant.id": "siretSousTraitant",
            "sousTraitant.typeIdentifiant": "typeIdentifiantSousTraitant"
        })
    else:
        print("⚠️ Aucun champ 'sousTraitant' exploitable trouvé.")
        print("🔎 Colonnes disponibles :", df.columns)

    print(f"✅ Nombre total d’actes extraits : {df.height}")
    print(df.head(20))

    # Export si besoin :
    # df.write_csv("actes_sous_traitance_polars.csv")

    return df


if __name__ == "__main__":

    test_extract_actes_sous_traitance_pandas(r"C:\Users\elyot\OneDrive\Documents\GitHub\decp-processing\data\decp-2019.json")
