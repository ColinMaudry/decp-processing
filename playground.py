
import pandas as pd
import json
import ast
import numpy as np

def safe_eval(value):
    """
    Gère les cas où actesSousTraitance est une string JSON, None, ou NaN.
    """
    if isinstance(value, str):
        try:
            return ast.literal_eval(value)
        except (ValueError, SyntaxError):
            return []
    elif value is np.nan or value is None:
        return []
    elif isinstance(value, list):
        return value
    else:
        return []

def test_extract_actes_sous_traitance_pandas(json_path):
    print(f"📥 Lecture du fichier JSON : {json_path}")

    # Charger le JSON
    with open(json_path, "r", encoding="utf-8") as f:
        raw = json.load(f)

    if isinstance(raw, dict) and "marches" in raw:
        raw = raw["marches"]
    elif not isinstance(raw, list):
        print("❌ Format de fichier inattendu.")
        return

    print(f"🔍 {len(raw)} marchés chargés.")

    # Sélectionner ceux qui contiennent des actes non vides
    with_actes = [
        marche for marche in raw
        if isinstance(marche.get("actesSousTraitance"), (list, str)) and marche["actesSousTraitance"]
    ]
    print(f"🔎 Nombre de marchés avec actesSousTraitance non vides : {len(with_actes)}")

    if not with_actes:
        print("❌ Aucun marché avec actesSousTraitance trouvé.")
        return

    # Ajouter un champ uid
    for marche in with_actes:
        marche["uid"] = marche.get("id")

    # Création du DataFrame
    df = pd.DataFrame([
        {
            "uid": marche["uid"],
            "actesSousTraitance": marche["actesSousTraitance"]
        }
        for marche in with_actes
    ])

    # 💡 Parser les strings JSON → listes valides
    df["actesSousTraitance"] = df["actesSousTraitance"].apply(safe_eval)

    # 💣 Explosion : 1 ligne par acte
    df = df.explode("actesSousTraitance").reset_index(drop=True)

    # Conversion en dict au cas où certaines valeurs seraient corrompues
    df["actesSousTraitance"] = df["actesSousTraitance"].apply(lambda x: x if isinstance(x, dict) else {})

    # Extraction dans des colonnes à part
    actes_df = pd.json_normalize(df["actesSousTraitance"].tolist())
    actes_df["uid"] = df["uid"].values

    # Extraction du sous-traitant si présent
    if any(col.startswith("sousTraitant.") for col in actes_df.columns):
        print("✅ Champ 'sousTraitant' trouvé, extraction en cours...")
        sous_traitant_df = actes_df[["sousTraitant.id", "sousTraitant.typeIdentifiant"]].copy()
        sous_traitant_df.columns = ["siretSousTraitant", "typeIdentifiantSousTraitant"]
        final_df = pd.concat([actes_df.drop(columns=["sousTraitant.id", "sousTraitant.typeIdentifiant"]), sous_traitant_df], axis=1)
    else:
        print("⚠️ Aucun champ 'sousTraitant' trouvé dans les actes.")
        print("🔎 Voici les colonnes trouvées :", actes_df.columns.tolist())
        final_df = actes_df

    print(f"✅ Nombre total d’actes extraits : {len(final_df)}")
    print(final_df.head(20))

    # Optionnel : export
    # final_df.to_csv("actes_sous_traitance_extraits.csv", index=False)


# fonctions test/debug
def inspect_actes_sous_traitance(file_path=r"C:\Users\elyot\OneDrive\Documents\GitHub\decp-processing\data\decp-2019.json", max_lignes=100_000):
    print(f"🔍 Lecture du fichier {file_path}")

    with open(file_path, encoding="utf-8") as f:
        for i, line in enumerate(f):
            if i > max_lignes:
                print("❌ Aucun acte trouvé dans les", max_lignes, "premières lignes.")
                return

            try:
                entry = json.loads(line)
                actes = entry.get("actesSousTraitance")

                if isinstance(actes, str):
                    actes = json.loads(actes)  # si c’est du texte JSON

                if isinstance(actes, list) and len(actes) > 0:
                    print("✅ Actes de sous-traitance trouvés à la ligne", i)
                    print("Exemple brut :", json.dumps(actes, indent=2, ensure_ascii=False))
                    print("🗝️ Clés utilisées :", list(actes[0].keys()))
                    return
            except Exception as e:
                continue  # on ignore les erreurs JSON

    print("❌ Aucun acte de sous-traitance trouvé.")

def extract_first_valid_actes_subset(input_path, output_path, max_found=3):
    """
    Parcourt un fichier JSON (A), extrait les marchés dont actesSousTraitance est non vide,
    et écrit les 3 premiers dans un fichier JSON standard (B).
    """
    import json
    import os

    print(f"📂 Lecture de {input_path}")
    try:
        with open(input_path, encoding="utf-8") as f:
            data = json.load(f)
            # 🧠 Si data est un dict contenant un tableau de marchés, extraire ce tableau
            if isinstance(data, dict):
                if "marches" in data:
                    data = data["marches"]
                else:
                    print("❌ Ce JSON est un dict mais ne contient pas de clé 'marches'.")
                    return

            # ✅ Si data est déjà une liste, on la garde telle quelle
            elif not isinstance(data, list):
                print("❌ Format inattendu : data doit être une liste ou un dict avec clé 'marches'.")
                return


    except Exception as e:
        print(f"❌ Erreur lors du chargement du JSON : {e}")
        return

    matches = []
    for i, entry_raw in enumerate(data):
        try:
            entry = json.loads(entry_raw) if isinstance(entry_raw, str) else entry
        except Exception:
            continue

        actes = entry.get("actesSousTraitance", [])
        if isinstance(actes, str):
            try:
                actes = json.loads(actes)
            except Exception:
                actes = []
        if isinstance(actes, list) and len(actes) > 0:
            entry["uid"] = entry.get("uid", entry.get("id", f"market_{i}"))
            matches.append(entry)
            print(f"✅ Marché avec acte trouvé à l’index {i}")
            if len(matches) >= max_found:
                break

    if not matches:
        print("❌ Aucun marché avec actesSousTraitance non vide trouvé.")
        return

    os.makedirs(os.path.dirname(output_path), exist_ok=True)

    with open(output_path, "w", encoding="utf-8") as out:
        json.dump(matches, out, ensure_ascii=False, indent=2)
        print(f"✅ {len(matches)} marché(s) écrit(s) dans {output_path}")

def debug_print_actes_sous_traitance_non_vides(input_path, max_affichage=20):
    """
    Affiche les marchés contenant un champ 'actesSousTraitance' non vide,
    et imprime un aperçu du premier élément de la liste.
    """
    import json

    print(f"🔍 Lecture du fichier : {input_path}")
    try:
        with open(input_path, encoding="utf-8") as f:
            data = json.load(f)
    except Exception as e:
        print(f"❌ Erreur de lecture JSON : {e}")
        return

    # Si le fichier a une clé 'marches'
    if isinstance(data, dict) and "marches" in data:
        data = data["marches"]
    elif not isinstance(data, list):
        print("❌ Format inattendu : ni liste, ni dict avec clé 'marches'")
        return

    print("🔍 Recherche de champs 'actesSousTraitance' NON VIDES...")
    found = 0
    for i, entry in enumerate(data):
        actes = entry.get("actesSousTraitance", None)
        if isinstance(actes, list) and len(actes) > 0:
            found += 1
            print(f"✅ Ligne {i} : {len(actes)} acte(s)")
            print("    ➤ Premier acte :", json.dumps(actes[0], ensure_ascii=False, indent=2))
            if found >= max_affichage:
                print("... (limite d'affichage atteinte)")
                break

    if found == 0:
        print("❌ Aucun champ 'actesSousTraitance' non vide trouvé.")
    else:
        print(f"✅ Total trouvé : {found} marché(s) avec actesSousTraitance non vide.")
def save_markets_with_actes_sous_traitance(input_path, output_path):
    """
    Extrait tous les marchés du fichier A (input_path) qui contiennent
    un champ 'actesSousTraitance' non vide, et les sauvegarde tels quels
    dans un fichier JSON standard (output_path).
    """
    import json
    import os

    print(f"📂 Lecture du fichier : {input_path}")
    try:
        with open(input_path, encoding="utf-8") as f:
            data = json.load(f)
    except Exception as e:
        print(f"❌ Erreur de lecture JSON : {e}")
        return

    if isinstance(data, dict) and "marches" in data:
        data = data["marches"]
    elif not isinstance(data, list):
        print("❌ Format inattendu : ni liste, ni dict avec clé 'marches'")
        return

    matches = []
    for i, entry in enumerate(data):
        actes = entry.get("actesSousTraitance", None)
        if isinstance(actes, list) and len(actes) > 0:
            matches.append(entry)

    if not matches:
        print("❌ Aucun marché avec actesSousTraitance non vide trouvé.")
        return

    os.makedirs(os.path.dirname(output_path), exist_ok=True)

    with open(output_path, "w", encoding="utf-8") as out:
        json.dump(matches, out, ensure_ascii=False, indent=2)

    print(f"✅ {len(matches)} marché(s) exporté(s) dans {output_path}")


# Tu peux l'appeler ici :
if __name__ == "__main__":

    test_extract_actes_sous_traitance_pandas(r"C:\Users\elyot\OneDrive\Documents\GitHub\decp-processing\data\decp-2019.json")
