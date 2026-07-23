from unittest.mock import patch

import polars as pl
import pytest

from src.tasks import utils
from src.tasks.utils import calculate_duplicates_across_source, log_column_stats

# Assuming your function is named analyze_overlaps_safe
# or imported from your module


def test_calculate_duplicates_across_source():
    # 1. Create dummy data
    data = {
        "uid": [1, 2, 3, 2, 3, 4, 3, 5],
        "sourceDataset": [
            "ds1",
            "ds1",
            "ds1",  # ds1 has 1, 2, 3
            "ds2",
            "ds2",
            "ds2",  # ds2 has 2, 3, 4
            "ds3",
            "ds3",  # ds3 has 3, 5
        ],
    }
    lf = pl.LazyFrame(data)

    # 2. Run the analysis
    result = calculate_duplicates_across_source(lf)

    # 3. Validation Logic
    # Let's check ds1 row specifically:
    # Total UIDs in ds1 = 3 (1, 2, 3)
    # Unique = 1 (UID 1) -> 1/3 = 0.333...
    # Overlap with ds2 = 2 (UID 2, 3) -> 2/3 = 0.666...
    # Overlap with ds3 = 1 (UID 3) -> 1/3 = 0.333...

    ds1_row = result.filter(pl.col("sourceDataset") == "ds1")

    assert ds1_row.get_column("unique")[0] == pytest.approx(1 / 3)
    assert ds1_row.get_column("ds2")[0] == pytest.approx(2 / 3)
    assert ds1_row.get_column("ds3")[0] == pytest.approx(1 / 3)

    # Let's check ds3 row:
    # Total UIDs in ds3 = 2 (3, 5)
    # Unique = 1 (UID 5) -> 1/2 = 0.5
    # Overlap with ds1 = 1 (UID 3) -> 1/2 = 0.5
    # Overlap with ds2 = 1 (UID 3) -> 1/2 = 0.5

    ds3_row = result.filter(pl.col("sourceDataset") == "ds3")

    assert ds3_row.get_column("unique")[0] == pytest.approx(0.5)
    assert ds3_row.get_column("ds1")[0] == pytest.approx(0.5)
    assert ds3_row.get_column("ds2")[0] == pytest.approx(0.5)

    # Check for consistency (Shape)
    # 3 sources = 3 rows and 5 columns (sourceDataset, unique, ds1, ds2, ds3)
    assert result.height == 3
    assert len(result.columns) == 5


def test_log_column_stats_reports_distinct_and_null_percentage():
    titulaire_schema = pl.Struct({"a": pl.String, "b": pl.Int64})
    lf = pl.LazyFrame(
        {
            "montant": [100, 200, None],
            "nom": ["a", "b", "a"],
            "titulaires": [[{"a": "x", "b": 1}], [{"a": "y", "b": 2}], None],
        },
        schema={
            "montant": pl.Int64,
            "nom": pl.String,
            "titulaires": pl.List(titulaire_schema),
        },
    )

    with patch.object(utils, "logger") as mock_logger:
        log_column_stats(lf, nb_lignes=3)

    mock_logger.info.assert_called_once()
    message = mock_logger.info.call_args[0][0]

    # montant : 2 valeurs distinctes hors null (100, 200), 1/3 null
    assert "montant" in message
    lines = {line.split()[0]: line for line in message.splitlines() if line.strip()}
    assert lines["montant"].split()[1] == "2"
    assert "33.33%" in lines["montant"]

    # nom : 2 valeurs distinctes (a, b), 0% null
    assert lines["nom"].split()[1] == "2"
    assert "0.00%" in lines["nom"]

    # titulaires (List(Struct)) : 2 valeurs distinctes hors null, 1/3 null
    assert lines["titulaires"].split()[1] == "2"
    assert "33.33%" in lines["titulaires"]


def test_log_column_stats_handles_column_without_nulls():
    lf = pl.LazyFrame({"id": [1, 2, 3, 4]}, schema={"id": pl.Int64})

    with patch.object(utils, "logger") as mock_logger:
        log_column_stats(lf, nb_lignes=4)

    message = mock_logger.info.call_args[0][0]
    line = next(line for line in message.splitlines() if line.startswith("id"))
    assert line.split()[1] == "4"
    assert "0.00%" in line
