# pulsar/core/quality/drift/detectors/completeness.py
"""
Completeness Drift Detector.

Detects changes in null/missing value counts between baseline and current data.
This is often the most important drift metric as increasing nulls usually indicates
upstream data quality issues.

Example:
    baseline: 50 nulls (5% of 1000 rows)
    current: 200 nulls (20% of 1000 rows)
    
    Result: HIGH drift - 150 more nulls detected
    Insight: "🔴 150 more nulls in 'email' - data source degraded?"
"""

from typing import Any, Dict, Optional
import logging

from ..base import DriftDetector, DriftResult, Severity

logger = logging.getLogger(__name__)


class CompletenessDriftDetector(DriftDetector):
    """
    Detect drift in data completeness (null counts).
    
    Configuration:
        threshold: Minimum completeness change to trigger (default: 0.01 = 1%)
        weight: Importance weight (default: 2.0, high importance)
    
    Metrics Used:
        - completeness: Percentage of non-null values (0.0 to 1.0)
        - null_count: Absolute number of null values
    
    Returns:
        DriftResult with:
        - score: Absolute change in completeness ratio
        - severity: Based on magnitude of change
        - insight: Human-readable explanation with null count delta
    """
    
    def is_applicable(
        self, 
        baseline_col: Dict[str, Any], 
        current_col: Dict[str, Any]
    ) -> bool:
        """
        Check if completeness metrics are available.
        
        All columns should have completeness, but check to be safe.
        """
        return (
            'completeness' in baseline_col and 
            'completeness' in current_col and
            'null_count' in baseline_col and
            'null_count' in current_col
        )
    
    def detect(
        self,
        column_name: str,
        baseline_col: Dict[str, Any],
        current_col: Dict[str, Any]
    ) -> Optional[DriftResult]:
        """
        Detect completeness drift.
        
        Args:
            column_name: Name of column being analyzed
            baseline_col: Baseline column profile
            current_col: Current column profile
        
        Returns:
            DriftResult if drift exceeds threshold, None otherwise
        """
        # Check if detector should run
        if not self.should_detect(baseline_col, current_col):
            return None
        
        # Extract metrics
        baseline_completeness = baseline_col['completeness']
        current_completeness = current_col['completeness']
        baseline_nulls = baseline_col['null_count']
        current_nulls = current_col['null_count']
        
        # Calculate drift
        delta = current_completeness - baseline_completeness
        delta_pct = (delta / baseline_completeness * 100) if baseline_completeness > 0 else 0
        null_change = current_nulls - baseline_nulls
        
        # Check threshold
        if abs(delta) < self.threshold:
            logger.debug(
                f"{column_name}: Completeness drift below threshold "
                f"({abs(delta):.3f} < {self.threshold})"
            )
            return None
        
        # Calculate severity
        score = abs(delta)
        severity = self.calculate_severity(score)
        
        # Generate insight
        insight = self._generate_insight(column_name, null_change, delta)
        
        logger.info(
            f"{column_name}: Completeness drift detected - "
            f"{baseline_completeness:.3f} → {current_completeness:.3f} "
            f"({null_change:+d} nulls, {severity.name})"
        )
        
        return DriftResult(
            metric_name='completeness',
            column_name=column_name,
            drift_detected=True,
            score=score,
            severity=severity,
            baseline_value=round(baseline_completeness, 3),
            current_value=round(current_completeness, 3),
            delta=round(delta, 3),
            delta_percentage=round(delta_pct, 1),
            insight=insight,
            metadata={
                'baseline_nulls': baseline_nulls,
                'current_nulls': current_nulls,
                'null_change': null_change,
            }
        )
    
    def _generate_insight(
        self, 
        column_name: str, 
        null_change: int, 
        delta: float
    ) -> str:
        """
        Generate actionable insight message.
        
        Args:
            column_name: Column name
            null_change: Change in null count (positive = more nulls)
            delta: Change in completeness ratio (negative = more nulls)
        
        Returns:
            Human-readable insight string
        """
        if delta < 0:  # Completeness decreased = more nulls
            return (
                f"🔴 {abs(null_change):,} more nulls in '{column_name}' - "
                f"data source degraded?"
            )
        else:  # Completeness increased = fewer nulls
            return (
                f"🟢 {abs(null_change):,} fewer nulls in '{column_name}' - "
                f"data quality improved"
            )


# Example usage (for testing)
if __name__ == '__main__':
    # Set up logging
    logging.basicConfig(level=logging.DEBUG)
    
    # Example baseline column
    baseline = {
        'completeness': 0.95,
        'null_count': 50,
        'dtype': 'Utf8',
    }
    
    # Example current column (degraded quality)
    current = {
        'completeness': 0.80,
        'null_count': 200,
        'dtype': 'Utf8',
    }
    
    # Create detector with config
    config = {
        'enabled': True,
        'threshold': 0.01,
        'weight': 2.0,
    }
    
    detector = CompletenessDriftDetector(config)
    
    # Detect drift
    result = detector.detect('email', baseline, current)
    
    if result:
        print(f"\n{'='*80}")
        print(f"DRIFT DETECTED")
        print(f"{'='*80}")
        print(f"Column: {result.column_name}")
        print(f"Metric: {result.metric_name}")
        print(f"Severity: {result.severity}")
        print(f"Score: {result.score:.3f}")
        print(f"Baseline: {result.baseline_value}")
        print(f"Current: {result.current_value}")
        print(f"Delta: {result.delta} ({result.delta_percentage}%)")
        print(f"\nInsight: {result.insight}")
        print(f"{'='*80}\n")
    else:
        print("No drift detected")