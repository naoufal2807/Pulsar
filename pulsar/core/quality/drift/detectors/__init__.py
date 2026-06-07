# pulsar/core/quality/drift/__init__.py
"""
Pulsar Drift Detection System.

This package provides a pluggable architecture for detecting data drift
between baseline and current datasets.

Components:
- base: Core interfaces (DriftDetector, DriftResult, Severity)
- registry: Detector registration and management
- detectors/: Built-in detector implementations

Usage:
    from pulsar.core.quality.drift import get_registry, Severity, DriftResult
    
    # Get registry and load detectors
    registry = get_registry()
    detectors = registry.load_from_yaml('config/drift_detection.yaml')
    
    # Run detectors
    for detector in detectors:
        result = detector.detect(col_name, baseline_col, current_col)
        if result:
            print(f"{result.severity}: {result.insight}")
"""

from .base import (
    DriftDetector,
    DatasetLevelDetector,
    DriftResult,
    Severity,
    format_number,
    format_percentage,
    get_emoji_for_change,
)

from .registry import (
    DriftDetectorRegistry,
    get_registry,
    register_builtin_detectors,
)

# Auto-register built-in detectors on import
register_builtin_detectors()

__all__ = [
    # Base classes
    'DriftDetector',
    'DatasetLevelDetector',
    'DriftResult',
    'Severity',
    
    # Registry
    'DriftDetectorRegistry',
    'get_registry',
    'register_builtin_detectors',
    
    # Utilities
    'format_number',
    'format_percentage',
    'get_emoji_for_change',
]

__version__ = '0.2.0'