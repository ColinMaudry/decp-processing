import polars as pl
import pytest

from src.tasks.utils import calculate_duplicates_across_source

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
