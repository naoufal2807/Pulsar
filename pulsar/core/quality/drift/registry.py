# pulsar/core/quality/drift/registry.py
"""
Drift Detector Registry.

Central registry for all drift detectors. Supports:
- Registering built-in detectors
- Registering custom user detectors
- Creating detector instances from configuration
- Loading detectors dynamically
"""

from typing import Dict, List, Type, Optional
import logging
from pathlib import Path
import yaml

from .base import DriftDetector, DatasetLevelDetector

logger = logging.getLogger(__name__)


class DriftDetectorRegistry:
    """
    Registry for drift detectors.
    
    This is a singleton that manages all available drift detectors.
    Detectors can be:
    - Built-in (shipped with Pulsar)
    - Custom (user-defined)
    
    Example:
        # Register a detector
        registry = DriftDetectorRegistry()
        registry.register('completeness', CompletenessDriftDetector)
        
        # Create detector instances from config
        config = {'completeness': {'enabled': True, 'threshold': 0.01}}
        detectors = registry.create_detectors(config)
    """
    
    def __init__(self):
        """Initialize empty registry."""
        self._detectors: Dict[str, Type[DriftDetector]] = {}
        self._dataset_detectors: Dict[str, Type[DatasetLevelDetector]] = {}
        logger.debug("DriftDetectorRegistry initialized")
    
    def register(
        self, 
        name: str, 
        detector_class: Type[DriftDetector],
        dataset_level: bool = False
    ) -> None:
        """
        Register a drift detector.
        
        Args:
            name: Detector identifier (must match config key)
            detector_class: Detector class (must inherit from DriftDetector)
            dataset_level: Whether this is a dataset-level detector
        
        Raises:
            ValueError: If detector_class doesn't inherit from DriftDetector
            
        Example:
            registry.register('completeness', CompletenessDriftDetector)
        """
        # Validate detector class
        if dataset_level:
            if not issubclass(detector_class, DatasetLevelDetector):
                raise ValueError(
                    f"{detector_class.__name__} must inherit from DatasetLevelDetector"
                )
            self._dataset_detectors[name] = detector_class
            logger.info(f"Registered dataset-level detector: {name} ({detector_class.__name__})")
        else:
            if not issubclass(detector_class, DriftDetector):
                raise ValueError(
                    f"{detector_class.__name__} must inherit from DriftDetector"
                )
            self._detectors[name] = detector_class
            logger.info(f"Registered detector: {name} ({detector_class.__name__})")
    
    def unregister(self, name: str) -> None:
        """
        Unregister a detector.
        
        Args:
            name: Detector identifier
        """
        if name in self._detectors:
            del self._detectors[name]
            logger.info(f"Unregistered detector: {name}")
        elif name in self._dataset_detectors:
            del self._dataset_detectors[name]
            logger.info(f"Unregistered dataset-level detector: {name}")
        else:
            logger.warning(f"Attempted to unregister unknown detector: {name}")
    
    def list_detectors(self) -> Dict[str, str]:
        """
        List all registered detectors.
        
        Returns:
            Dict mapping detector names to class names
        """
        result = {}
        for name, cls in self._detectors.items():
            result[name] = cls.__name__
        for name, cls in self._dataset_detectors.items():
            result[f"{name} (dataset)"] = cls.__name__
        return result
    
    def create_detectors(
        self, 
        config: Dict[str, Dict]
    ) -> List[DriftDetector]:
        """
        Create detector instances from configuration.
        
        Args:
            config: Configuration dict (from YAML)
                   Keys = detector names
                   Values = detector configs
        
        Returns:
            List of initialized detector instances
        
        Example:
            config = {
                'completeness': {'enabled': True, 'threshold': 0.01},
                'mean_shift': {'enabled': True, 'threshold': 0.05},
            }
            detectors = registry.create_detectors(config)
            # Returns [CompletenessDriftDetector(...), MeanShiftDetector(...)]
        """
        detectors = []
        
        for name, detector_config in config.items():
            # Check if detector is registered
            if name not in self._detectors:
                logger.warning(f"Detector '{name}' not registered, skipping")
                continue
            
            # Check if enabled
            if not detector_config.get('enabled', True):
                logger.debug(f"Detector '{name}' disabled in config, skipping")
                continue
            
            # Create instance
            try:
                detector_class = self._detectors[name]
                detector = detector_class(detector_config)
                detectors.append(detector)
                logger.debug(f"Created detector: {name}")
            except Exception as e:
                logger.error(f"Failed to create detector '{name}': {e}")
                continue
        
        logger.info(f"Created {len(detectors)} detectors from config")
        return detectors
    
    def create_dataset_detectors(
        self, 
        config: Dict[str, Dict]
    ) -> List[DatasetLevelDetector]:
        """
        Create dataset-level detector instances.
        
        Args:
            config: Configuration dict
        
        Returns:
            List of initialized dataset-level detectors
        """
        detectors = []
        
        for name, detector_config in config.items():
            if name not in self._dataset_detectors:
                continue
            
            if not detector_config.get('enabled', True):
                logger.debug(f"Dataset detector '{name}' disabled, skipping")
                continue
            
            try:
                detector_class = self._dataset_detectors[name]
                detector = detector_class(detector_config)
                detectors.append(detector)
                logger.debug(f"Created dataset detector: {name}")
            except Exception as e:
                logger.error(f"Failed to create dataset detector '{name}': {e}")
                continue
        
        logger.info(f"Created {len(detectors)} dataset detectors")
        return detectors
    
    def load_from_yaml(self, config_path: Path) -> List[DriftDetector]:
        """
        Load detectors from YAML config file.
        
        Args:
            config_path: Path to drift_detection.yaml
        
        Returns:
            List of initialized detectors
        
        Example:
            detectors = registry.load_from_yaml('config/drift_detection.yaml')
        """
        logger.info(f"Loading detectors from: {config_path}")
        
        try:
            with open(config_path, 'r') as f:
                config = yaml.safe_load(f)
        except FileNotFoundError:
            logger.error(f"Config file not found: {config_path}")
            return []
        except yaml.YAMLError as e:
            logger.error(f"Failed to parse YAML config: {e}")
            return []
        
        # Extract detector configuration
        detector_config = config.get('detectors', {})
        
        # Create detectors
        detectors = self.create_detectors(detector_config)
        
        logger.info(f"Loaded {len(detectors)} detectors from config")
        return detectors
    
    def register_custom_detector(
        self, 
        name: str, 
        module_path: str, 
        class_name: str
    ) -> None:
        """
        Register a custom user-defined detector.
        
        Args:
            name: Detector identifier
            module_path: Python module path (e.g., 'mycompany.detectors')
            class_name: Class name (e.g., 'CustomDetector')
        
        Example:
            registry.register_custom_detector(
                'price_range',
                'mycompany.detectors',
                'PriceRangeDetector'
            )
        """
        try:
            # Dynamic import
            import importlib
            module = importlib.import_module(module_path)
            detector_class = getattr(module, class_name)
            
            # Register
            self.register(name, detector_class)
            logger.info(f"Registered custom detector: {name} from {module_path}.{class_name}")
        except ImportError as e:
            logger.error(f"Failed to import module '{module_path}': {e}")
        except AttributeError as e:
            logger.error(f"Class '{class_name}' not found in '{module_path}': {e}")
        except Exception as e:
            logger.error(f"Failed to register custom detector '{name}': {e}")


# Global registry instance (singleton)
_global_registry: Optional[DriftDetectorRegistry] = None


def get_registry() -> DriftDetectorRegistry:
    """
    Get the global drift detector registry.
    
    Returns:
        Singleton DriftDetectorRegistry instance
    
    Example:
        registry = get_registry()
        registry.register('completeness', CompletenessDriftDetector)
    """
    global _global_registry
    if _global_registry is None:
        _global_registry = DriftDetectorRegistry()
    return _global_registry


def register_builtin_detectors() -> None:
    """
    Register all built-in detectors.
    
    This is called automatically when the drift module is imported.
    """
    registry = get_registry()
    
    # Import built-in detectors (will add as we create them)
    try:
        from .detectors.completeness import CompletenessDriftDetector
        registry.register('completeness', CompletenessDriftDetector)
    except ImportError:
        logger.debug("CompletenessDriftDetector not available yet")
    
    try:
        from .detectors.mean_shift import MeanShiftDetector
        registry.register('mean_shift', MeanShiftDetector)
    except ImportError:
        logger.debug("MeanShiftDetector not available yet")
    
    try:
        from .detectors.outlier_drift import OutlierDriftDetector
        registry.register('outlier_drift', OutlierDriftDetector)
    except ImportError:
        logger.debug("OutlierDriftDetector not available yet")
    
    try:
        from .detectors.format_validation import FormatValidationDetector
        registry.register('format_validation', FormatValidationDetector)
    except ImportError:
        logger.debug("FormatValidationDetector not available yet")
    
    try:
        from .detectors.cardinality import CardinalityDetector
        registry.register('cardinality', CardinalityDetector)
    except ImportError:
        logger.debug("CardinalityDetector not available yet")
    
    try:
        from .detectors.uniqueness import UniquenessDetector
        registry.register('uniqueness', UniquenessDetector)
    except ImportError:
        logger.debug("UniquenessDetector not available yet")
    
    try:
        from .detectors.row_count import RowCountDetector
        registry.register('row_count', RowCountDetector, dataset_level=True)
    except ImportError:
        logger.debug("RowCountDetector not available yet")
    
    logger.info("Built-in detectors registered")