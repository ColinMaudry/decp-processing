"""Script one-shot de calibration des seuils d'anomalie de montant.

Lit le parquet de sortie le plus récent et produit un CSV listant les marchés
flaggés par configuration de seuils. Permet de valider visuellement avant de
figer les valeurs définitives dans src/config.py.

Usage : python script/calibrate_anomaly_thresholds.py [--parquet PATH]
"""

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import polars as pl

from src.config import BASE_DIR, DIST_DIR
from src.tasks.anomaly import (
    detect_montant_anomalies,
)

OUT_DIR = BASE_DIR / "data" / "calibration"


def calibrate(parquet_path: Path, pairs_grid: list[float]) -> None:
    OUT_DIR.mkdir(parents=True, exist_ok=True)

    lf = pl.scan_parquet(parquet_path)

    lf = detect_montant_anomalies(lf, calibrating=True)

    df = lf.collect(engine="streaming")
    print(f"Marchés analysés : {len(df):,}")

    # Vérification du nombre de marchés par groupe impliquant le code CPV pour
    # calibrer la bonne taille de code CPV (make_short_cpv_code)
    df_cpv_groupes = (
        df.select("codeCPV_2", "n_groupe", "niveau_groupe")
        .filter(
            pl.col("niveau_groupe").is_in(["L3", "L4"])
        )  # on ne veut ques les stats sur des groupes basés sur le code CPV
        .drop("niveau_groupe")
        .group_by("codeCPV_2")
        .agg(
            count=pl.len(),
            min=pl.col("n_groupe").min(),
            median=pl.col("n_groupe").median(),
            average=pl.col("n_groupe").mean(),
        )
    ).sort(by="count", descending=True)

    pl.Config(fmt_str_lengths=80, fmt_table_cell_list_len=50, set_tbl_rows=50)
    median_n_groupe = df["n_groupe"].median()
    average_n_groupe = df["n_groupe"].mean()

    print(df_cpv_groupes)
    print("Somme count: ", df_cpv_groupes["count"].sum())

    print("n_groupe médian :", median_n_groupe, "n_groupe moyen :", average_n_groupe)

    # Échantillon de marchés pour vérfier la cohérence des montants
    df_sample = df.select(
        "objet",
        "montant_normalise",
        "acheteur_nom",
        "acheteur_categorie",
        "acheteur_population",
        "codeCPV_2",
        "n_groupe",
        "niveau_groupe",
        "montant_anomalie",
    ).filter(pl.col("montant_anomalie").is_not_null())
    df_sample = df.sample(30)
    print(df_sample)

    rows = []
    for suspect_thr in pairs_grid:
        for aberrant_thr in pairs_grid:
            if aberrant_thr <= suspect_thr:
                continue
            n_suspect = int(
                (
                    (df["ecart_pairs"] > suspect_thr)
                    & (df["ecart_pairs"] <= aberrant_thr)
                ).sum()
            )
            n_aberrant = int((df["ecart_pairs"] > aberrant_thr).sum())
            rows.append(
                {
                    "pairs_suspect": suspect_thr,
                    "pairs_aberrant": aberrant_thr,
                    "n_suspect": n_suspect,
                    "n_aberrant": n_aberrant,
                }
            )

    out_df = pl.DataFrame(rows)
    out_path = OUT_DIR / "grille_seuils_pairs.csv"
    out_df.write_csv(out_path)
    print(f"Grille écrite : {out_path}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--parquet", type=Path, default=DIST_DIR / "decp.parquet")
    args = parser.parse_args()

    pairs_grid = [3.0, 3.5, 4.0, 4.5, 5.0, 5.5, 6.0, 6.5, 7.0]
    calibrate(args.parquet, pairs_grid)
