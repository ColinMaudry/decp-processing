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
    compute_peer_group_stats,
    compute_signals,
    compute_tranche_population_expr,
    join_population,
    montant_normalise_expr,
)

OUT_DIR = BASE_DIR / "data" / "calibration"


def calibrate(parquet_path: Path, pairs_grid: list[float]) -> None:
    OUT_DIR.mkdir(parents=True, exist_ok=True)

    lf = pl.scan_parquet(parquet_path)

    lf = join_population(lf, BASE_DIR / "data" / "identifiants-communes.csv")
    lf = lf.with_columns(
        compute_tranche_population_expr(),
        montant_normalise_expr(),
        pl.col("codeCPV").str.slice(0, 2).alias("codeCPV_2"),
    )
    lf = lf.with_columns(
        log_montant_normalise=(pl.col("montant_normalise") + 1).log10()
    )
    lf = compute_peer_group_stats(lf, min_size=30)
    lf = compute_signals(lf)

    df = lf.collect()
    print(f"Marchés analysés : {len(df):,}")

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

    top_aberrants = (
        df.filter(pl.col("ecart_pairs") > 6.0)
        .sort("montant", descending=True)
        .head(50)
        .select(
            [
                "uid",
                "acheteur_nom",
                "titulaire_nom",
                "objet",
                "montant",
                "type",
                "codeCPV",
                "ecart_pairs",
            ]
        )
    )
    top_path = OUT_DIR / "top_aberrants.csv"
    top_aberrants.write_csv(top_path)
    print(f"Top aberrants écrit : {top_path}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--parquet", type=Path, default=DIST_DIR / "decp.parquet")
    args = parser.parse_args()

    pairs_grid = [3.0, 3.5, 4.0, 4.5, 5.0, 5.5, 6.0, 6.5, 7.0]
    calibrate(args.parquet, pairs_grid)
