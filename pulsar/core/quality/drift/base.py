# pulsar/core/quality/drift/base.py
"""
Core drift detection interfaces.

This module defines the base classes and data structures for the drift detection system.
All drift detectors inherit from DriftDetector and return DriftResult objects.
"""

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Dict, Optional
import logging

logger = logging.getLogger(__name__)


class Severity(Enum):
    """
    Drift severity levels.
    
    Used to categorize the magnitude of detected drift.
    """
    NONE = 0
    LOW = 1
    MODERATE = 2
    HIGH = 3
    CRITICAL = 4
    
    def __str__(self):
        return self.name
    
    def __lt__(self, other):
        if isinstance(other, Severity):
            return self.value < other.value
        return NotImplemented


@dataclass
class DriftResult:
    """
    Type-safe drift detection result.
    
    Attributes:
        metric_name: Name of the metric being tracked (e.g., 'completeness', 'mean_shift')
        column_name: Name of the column being analyzed
        drift_detected: Whether drift was detected
        score: Drift score (0.0 to 1.0+)
        severity: Severity level (NONE, LOW, MODERATE, HIGH, CRITICAL)
        baseline_value: Value from baseline
        current_value: Value from current dataset
        delta: Absolute change (current - baseline)
        delta_percentage: Percentage change (optional)
        insight: Human-readable actionable insight
        metadata: Additional context (detector-specific)
    """
    metric_name: str
    column_name: str
    drift_detected: bool
    score: float
    severity: Severity
    baseline_value: Any
    current_value: Any
    delta: Any
    delta_percentage: Optional[float] = None
    insight: str = ""
    metadata: Dict[str, Any] = field(default_factory=dict)
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for JSON serialization."""
        return {
            'metric_name': self.metric_name,
            'column_name': self.column_name,
            'drift_detected': self.drift_detected,
            'score': round(self.score, 3),
            'severity': self.severity.name,
            'baseline_value': self.baseline_value,
            'current_value': self.current_value,
            'delta': self.delta,
            'delta_percentage': round(self.delta_percentage, 2) if self.delta_percentage is not None else None,
            'insight': self.insight,
            'metadata': self.metadata,
        }


class DriftDetector(ABC):
    """
    Base class for all drift detectors.
    
    Drift detectors analyze specific metrics (completeness, mean, outliers, etc.)
    and determine if drift has occurred between baseline and current data.
    
    Each detector:
    - Checks if it's applicable to the column type
    - Calculates drift if applicable
    - Returns DriftResult or None
    
    Detectors are configured via YAML and registered in the DetectorRegistry.
    """
    
    def __init__(self, config: Dict[str, Any]):
        """
        Initialize detector with configuration.
        
        Args:
            config: Detector configuration from YAML
                   Expected keys:
                   - enabled (bool): Whether detector is active
                   - threshold (float): Minimum change to trigger drift
                   - weight (float): Importance weight (0.0 to 1.0+)
                   - severity_levels (dict): Score thresholds for severity
        """
        self.config = config
        self.enabled = config.get('enabled', True)
        self.threshold = config.get('threshold', 0.05)
        self.weight = config.get('weight', 1.0)
        self.severity_levels = config.get('severity_levels', {
            'critical': 0.5,
            'high': 0.2,
            'moderate': 0.1,
            'low': 0.05,
        })
        
        logger.debug(f"Initialized {self.__class__.__name__}: threshold={self.threshold}, weight={self.weight}")
    
    @abstractmethod
    def detect(
        self, 
        column_name: str,
        baseline_col: Dict[str, Any], 
        current_col: Dict[str, Any]
    ) -> Optional[DriftResult]:
        """
        Detect drift between baseline and current column profiles.
        
        Args:
            column_name: Name of the column
            baseline_col: Baseline column profile (from profiler)
            current_col: Current column profile (from profiler)
        
        Returns:
            DriftResult if drift detected and above threshold, None otherwise
        
        Example:
            baseline_col = {
                'completeness': 0.95,
                'null_count': 50,
                'dtype': 'Utf8',
                ...
            }
            
            current_col = {
                'completeness': 0.80,
                'null_count': 200,
                'dtype': 'Utf8',
                ...
            }
            
            result = detector.detect('email', baseline_col, current_col)
            # Returns DriftResult with insight: "🔴 150 more nulls in 'email' - data source degraded?"
        """
        pass
    
    @abstractmethod
    def is_applicable(
        self, 
        baseline_col: Dict[str, Any], 
        current_col: Dict[str, Any]
    ) -> bool:
        """
        Check if this detector applies to this column type.
        
        Args:
            baseline_col: Baseline column profile
            current_col: Current column profile
        
        Returns:
            True if detector can analyze this column, False otherwise
        
        Example:
            # MeanShiftDetector only works on numeric columns
            def is_applicable(self, baseline_col, current_col):
                return (
                    'numeric_stats' in baseline_col and 
                    'numeric_stats' in current_col and
                    'mean' in baseline_col['numeric_stats']
                )
        """
        pass
    
    def calculate_severity(self, score: float) -> Severity:
        """
        Map drift score to severity level.
        
        Args:
            score: Drift score (0.0 to 1.0+)
        
        Returns:
            Severity enum value
        
        Example:
            score = 0.15
            severity = detector.calculate_severity(score)
            # Returns Severity.MODERATE
        """
        if score >= self.severity_levels['critical']:
            return Severity.CRITICAL
        elif score >= self.severity_levels['high']:
            return Severity.HIGH
        elif score >= self.severity_levels['moderate']:
            return Severity.MODERATE
        elif score >= self.severity_levels['low']:
            return Severity.LOW
        else:
            return Severity.NONE
    
    def should_detect(self, baseline_col: Dict[str, Any], current_col: Dict[str, Any]) -> bool:
        """
        Check if detector should run (enabled + applicable).
        
        Args:
            baseline_col: Baseline column profile
            current_col: Current column profile
        
        Returns:
            True if detector should run, False otherwise
        """
        return self.enabled and self.is_applicable(baseline_col, current_col)


class DatasetLevelDetector(ABC):
    """
    Base class for dataset-level drift detectors.
    
    These detect drift at the dataset level (not column level):
    - Row count changes
    - Column count changes
    - Schema changes
    """
    
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.enabled = config.get('enabled', True)
        self.threshold = config.get('threshold', 0.05)
    
    @abstractmethod
    def detect(
        self, 
        baseline_profile: Dict[str, Any], 
        current_profile: Dict[str, Any]
    ) -> Optional[DriftResult]:
        """
        Detect dataset-level drift.
        
        Args:
            baseline_profile: Full baseline dataset profile
            current_profile: Full current dataset profile
        
        Returns:
            DriftResult if drift detected, None otherwise
        """
        pass


# Utility functions for insight generation

def format_number(value: Any) -> str:
    """Format number for display."""
    if isinstance(value, float):
        return f"{value:.2f}"
    elif isinstance(value, int):
        return f"{value:,}"
    else:
        return str(value)


def format_percentage(value: float) -> str:
    """Format percentage for display."""
    return f"{value:+.1f}%" if value != 0 else "0%"


def get_emoji_for_change(delta: float, higher_is_better: bool = True) -> str:
    """
    Get emoji for change direction.
    
    Args:
        delta: Change value (positive or negative)
        higher_is_better: Whether increasing values are good
    
    Returns:
        Emoji string
    """
    if delta > 0:
        return "🟢" if higher_is_better else "🔴"
    elif delta < 0:
        return "🔴" if higher_is_better else "🟢"
    else:
        return "⚪"