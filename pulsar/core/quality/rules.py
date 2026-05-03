# pulsar/core/quality/rules.py

import logging
import re
from typing import Any, Dict, Optional
from pulsar.logging_config import get_logger


logger = get_logger("pulsar.quality.rules")


RULE_TYPES = {
    "not_null": "Column has no nulls",
    "regex": "Values match pattern",
    "range": "Values between min/max",
    "unique": "All values are unique",
    "in_list": "Values in allowed list",
}


class Rule:
    """A single validation rule."""
    
    def __init__(
        self,
        name: str,
        column: str,
        rule_type: str,
        threshold: float = 1.0,
        params: Optional[Dict[str, Any]] = None,
        **kwargs
    ):
        self.name = name
        self.column = column
        self.rule_type = rule_type
        self.threshold = threshold
        self.params = params or {}
        self.params.update(kwargs)
        self.__post_init__()
    
    def __post_init__(self):
        """Validate rule after initialization"""
        try:
            if not self.name or not isinstance(self.name, str):
                raise ValueError(f"Rule name must be non-empty string")
            if not self.column or not isinstance(self.column, str):
                raise ValueError(f"Column must be non-empty string")
            
            if self.rule_type not in RULE_TYPES:
                raise ValueError(f"Invalid rule_type: {self.rule_type}")
            
            threshold_float = float(self.threshold)
            if not (0.0 <= threshold_float <= 1.0):
                raise ValueError(f"Threshold must be between 0.0 and 1.0")
            self.threshold = threshold_float
            
            if not isinstance(self.params, dict):
                raise TypeError(f"Params must be dict")
            
            self._validate_params()
            logger.info(f"Rule created: {self.name}")
        
        except (ValueError, TypeError) as e:
            logger.error(f"Failed to create rule: {e}")
            raise
    
    def _validate_params(self):
        """Validate rule parameters"""
        if self.rule_type == "regex":
            if "pattern" not in self.params:
                raise ValueError("regex rule requires 'pattern'")
            try:
                re.compile(self.params["pattern"])
            except re.error as e:
                raise ValueError(f"Invalid regex pattern: {e}")
        
        elif self.rule_type == "range":
            if "min" not in self.params or "max" not in self.params:
                raise ValueError("range rule requires 'min' and 'max'")
            min_val = float(self.params["min"])
            max_val = float(self.params["max"])
            if min_val > max_val:
                raise ValueError(f"min ({min_val}) cannot be > max ({max_val})")
        
        elif self.rule_type == "in_list":
            if "values" not in self.params:
                raise ValueError("in_list rule requires 'values'")
            if not isinstance(self.params["values"], (list, tuple, set)):
                raise TypeError("values must be list/tuple/set")
    
    def __repr__(self) -> str:
        return f"Rule(name={self.name}, column={self.column}, type={self.rule_type})"
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "name": self.name,
            "column": self.column,
            "rule_type": self.rule_type,
            "threshold": self.threshold,
            "params": self.params,
        }
