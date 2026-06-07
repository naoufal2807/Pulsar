# pulsar/core/intelligence/small_world/isolator.py
"""
Isolator: Extract bounded sample for deep understanding.

The Isolator is the first step in the Small World Framework.
It extracts a small but representative sample (10% or min 1000 rows)
for deep pattern discovery.

Key strategies:
- Random: Uniformly random sampling
- Stratified: Balanced by column values
- Temporal: Time-distributed sampling
- First-N: First N rows (baseline)
"""

from typing import Any, Dict, Optional, List
import logging
from enum import Enum

import polars as pl

logger = logging.getLogger(__name__)


class SamplingStrategy(Enum):
    """Sampling strategies."""
    RANDOM = "random"
    STRATIFIED = "stratified"
    TEMPORAL = "temporal"
    FIRST_N = "first_n"


class Isolator:
    """
    Extract bounded sample for deep understanding.

    Sampling parameters are configurable:
    - Percentage: Extract ~10% of data
    - Minimum: Ensure at least 1000 rows
    - Strategy: How to sample (random, stratified, temporal, first-N)
    """

    def __init__(self, config: Optional[Dict[str, Any]] = None):
        """
        Initialize Isolator.

        Args:
            config: Optional configuration with keys:
                - sample_percentage: 0.0-1.0 (default: 0.10)
                - min_size: Minimum rows (default: 1000)
                - seed: Random seed (default: 42)
                - strategy: 'random', 'stratified', 'temporal', 'first_n'
        """
        self.config = config or {}
        self.sample_percentage = self.config.get('sample_percentage', 0.10)
        self.min_size = self.config.get('min_size', 1000)
        self.seed = self.config.get('seed', 42)
        self.strategy = self.config.get('strategy', 'random')

        logger.debug(
            f"Initialized Isolator: percentage={self.sample_percentage}, "
            f"min_size={self.min_size}, strategy={self.strategy}"
        )

    def extract(
        self,
        df: pl.DataFrame,
        strategy: Optional[str] = None,
        stratify_col: Optional[str] = None,
        temporal_col: Optional[str] = None,
    ) -> pl.DataFrame:
        """
        Extract sample from dataset.

        Args:
            df: Input DataFrame
            strategy: Sampling strategy ('random', 'stratified', 'temporal', 'first_n')
                     Defaults to config strategy
            stratify_col: Column to stratify by (for stratified sampling)
                         Auto-detected if None
            temporal_col: Column with temporal data (for temporal sampling)
                         Auto-detected if None

        Returns:
            Sample DataFrame
        """
        if df.is_empty():
            logger.warning("Empty DataFrame provided to Isolator")
            return df

        strategy = strategy or self.strategy
        sample_size = max(int(df.shape[0] * self.sample_percentage), self.min_size)
        sample_size = min(sample_size, df.shape[0])

        logger.info(
            f"Extracting sample: {df.shape[0]} rows → {sample_size} rows "
            f"({100 * sample_size / df.shape[0]:.1f}%) using {strategy}"
        )

        if strategy == 'stratified':
            return self._stratified_sample(df, sample_size, stratify_col)
        elif strategy == 'temporal':
            return self._temporal_sample(df, sample_size, temporal_col)
        elif strategy == 'first_n':
            return self._first_n_sample(df, sample_size)
        else:  # default: random
            return self._random_sample(df, sample_size)

    def _random_sample(self, df: pl.DataFrame, n: int) -> pl.DataFrame:
        """Random sampling: uniformly random rows."""
        return df.sample(n=n, seed=self.seed)

    def _stratified_sample(
        self,
        df: pl.DataFrame,
        n: int,
        stratify_col: Optional[str] = None
    ) -> pl.DataFrame:
        """
        Stratified sampling: balanced by column values.

        If stratify_col not specified, auto-detect categorical column.
        """
        if stratify_col is None:
            # Auto-detect a good stratification column
            stratify_col = self._find_stratification_column(df)

        if stratify_col is None:
            logger.debug("No stratification column found, falling back to random")
            return self._random_sample(df, n)

        logger.debug(f"Stratifying by column: {stratify_col}")

        try:
            # Get unique values in stratify column
            unique_values = df[stratify_col].unique().to_list()

            if not unique_values:
                return self._random_sample(df, n)

            # Proportional sampling per strata
            samples = []
            sample_per_strata = max(1, n // len(unique_values))

            for value in unique_values:
                strata_df = df.filter(pl.col(stratify_col) == value)
                strata_size = min(sample_per_strata, strata_df.shape[0])

                if strata_size > 0:
                    sample = strata_df.sample(n=strata_size, seed=self.seed)
                    samples.append(sample)

            # Combine all strata samples
            if samples:
                result = pl.concat(samples)
                # Trim to exact size if needed
                if result.shape[0] > n:
                    result = result.sample(n=n, seed=self.seed)
                return result

            return self._random_sample(df, n)

        except Exception as e:
            logger.debug(f"Error in stratified sampling: {e}, falling back to random")
            return self._random_sample(df, n)

    def _temporal_sample(
        self,
        df: pl.DataFrame,
        n: int,
        temporal_col: Optional[str] = None
    ) -> pl.DataFrame:
        """
        Temporal sampling: time-distributed rows.

        If temporal_col not specified, auto-detect date column.
        """
        if temporal_col is None:
            temporal_col = self._find_temporal_column(df)

        if temporal_col is None:
            logger.debug("No temporal column found, falling back to random")
            return self._random_sample(df, n)

        logger.debug(f"Sampling temporally using column: {temporal_col}")

        try:
            # Sort by temporal column
            sorted_df = df.sort(temporal_col)

            # Calculate step to distribute sample across time
            total_rows = sorted_df.shape[0]
            step = max(1, total_rows // n)

            # Sample every nth row
            sample_indices = list(range(0, total_rows, step))[:n]

            if sample_indices:
                return sorted_df[sample_indices]

            return self._random_sample(df, n)

        except Exception as e:
            logger.debug(f"Error in temporal sampling: {e}, falling back to random")
            return self._random_sample(df, n)

    def _first_n_sample(self, df: pl.DataFrame, n: int) -> pl.DataFrame:
        """First-N sampling: first N rows (baseline/deterministic)."""
        return df.head(n)

    def _find_stratification_column(self, df: pl.DataFrame) -> Optional[str]:
        """
        Auto-detect good stratification column.

        Look for columns with:
        - Categorical type
        - Moderate cardinality (5-100 unique values)
        """
        for col_name in df.columns:
            col = df[col_name]
            dtype_str = str(col.dtype)

            # Look for categorical/string columns
            if dtype_str not in ('Utf8', 'String', 'Categorical'):
                continue

            # Check cardinality
            n_unique = col.n_unique()
            if 5 <= n_unique <= 100:
                logger.debug(f"Auto-detected stratification column: {col_name} ({n_unique} unique)")
                return col_name

        return None

    def _find_temporal_column(self, df: pl.DataFrame) -> Optional[str]:
        """
        Auto-detect temporal column.

        Look for columns with Date or Datetime type.
        Prefer column with 'date', 'time', 'created', 'timestamp' in name.
        """
        temporal_cols = []

        for col_name in df.columns:
            dtype_str = str(df[col_name].dtype)
            if dtype_str in ('Date', 'Datetime') or 'Datetime' in dtype_str:
                temporal_cols.append(col_name)

        if not temporal_cols:
            return None

        # Prefer columns with temporal keywords
        keywords = ['date', 'time', 'created', 'timestamp', 'when']
        for col_name in temporal_cols:
            if any(kw in col_name.lower() for kw in keywords):
                logger.debug(f"Auto-detected temporal column: {col_name}")
                return col_name

        # Fall back to first temporal column
        if temporal_cols:
            logger.debug(f"Auto-detected temporal column: {temporal_cols[0]}")
            return temporal_cols[0]

        return None
