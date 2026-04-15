from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def manifest_conflicts_from_aggregated_groups(groups_df: DataFrame) -> DataFrame:
    """
    Returns groups with conflicting manifest information.

    Expected columns:
      - group_key
      - manifest_distinct_count
      - manifest_expected_values
      - manifest_expected_parts
    """
    return groups_df.where(
        (F.col("manifest_distinct_count") > 1)
        | (
            F.col("manifest_expected_parts").isNotNull()
            & F.col("expected_parts").isNotNull()
            & (F.col("manifest_expected_parts") != F.col("expected_parts"))
        )
    )
