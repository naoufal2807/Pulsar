# pulsar/core/quality/loader.py

import yaml
from typing import List, Dict, Any
from pathlib import Path
from pulsar.logging_config import get_logger
from pulsar.core.quality.rules import Rule


logger = get_logger("pulsar.quality.loader")


def load_rules_yaml(path: str) -> List[Rule]:
    """Load validation rules from YAML file."""
    try:
        logger.info(f"Loading rules from: {path}")
        
        file_path = Path(path)
        if not file_path.exists():
            logger.error(f"File not found: {path}")
            raise FileNotFoundError(f"Rules file not found: {path}")
        
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                config = yaml.safe_load(f)
            logger.debug(f"YAML loaded successfully")
        except yaml.YAMLError as e:
            logger.error(f"Invalid YAML syntax: {e}")
            raise ValueError(f"Invalid YAML syntax: {e}") from e
        
        if not config:
            logger.warning("YAML file is empty")
            return []
        
        if not isinstance(config, dict):
            logger.error("YAML root must be a dict")
            raise ValueError("YAML root must be a dict")
        
        rules_list = config.get("rules", [])
        if not isinstance(rules_list, list):
            logger.error("'rules' key must be a list")
            raise ValueError("'rules' key must be a list")
        
        if not rules_list:
            logger.warning("No rules defined in YAML")
            return []
        
        rules = []
        for idx, rule_dict in enumerate(rules_list):
            try:
                rule = _parse_rule(rule_dict, idx)
                rules.append(rule)
                logger.debug(f"Rule {idx+1} parsed: {rule.name}")
            except (ValueError, TypeError) as e:
                logger.error(f"Error parsing rule #{idx+1}: {e}")
                raise ValueError(f"Error in rule #{idx+1}: {e}") from e
        
        logger.info(f"Successfully loaded {len(rules)} rule(s)")
        return rules
    
    except Exception as e:
        logger.error(f"Failed to load rules: {e}", exc_info=True)
        raise


def _parse_rule(rule_dict: Dict[str, Any], index: int) -> Rule:
    """Parse a single rule from dict."""
    try:
        if not isinstance(rule_dict, dict):
            raise ValueError(f"Rule #{index+1} must be a dict, got {type(rule_dict)}")
        
        name = rule_dict.get("name")
        if not name:
            raise ValueError(f"Rule #{index+1} missing 'name'")
        
        column = rule_dict.get("column")
        if not column:
            raise ValueError(f"Rule '{name}' missing 'column'")
        
        rule_type = rule_dict.get("type")
        if not rule_type:
            raise ValueError(f"Rule '{name}' missing 'type'")
        
        threshold = float(rule_dict.get("threshold", 1.0))
        
        standard_keys = {"name", "column", "type", "threshold"}
        params = {k: v for k, v in rule_dict.items() if k not in standard_keys}
        
        logger.debug(f"Parsing rule '{name}': type={rule_type}, params={list(params.keys())}")
        
        rule = Rule(
            name=name,
            column=column,
            rule_type=rule_type,
            threshold=threshold,
            params=params
        )
        
        return rule
    
    except Exception as e:
        logger.error(f"Error parsing rule: {e}")
        raise


def validate_yaml_syntax(path: str) -> bool:
    """Validate YAML syntax without parsing rules."""
    try:
        file_path = Path(path)
        if not file_path.exists():
            logger.warning(f"File not found: {path}")
            return False
        
        with open(file_path, 'r', encoding='utf-8') as f:
            yaml.safe_load(f)
        
        logger.debug(f"YAML syntax is valid")
        return True
    
    except yaml.YAMLError as e:
        logger.error(f"Invalid YAML syntax: {e}")
        return False
    except Exception as e:
        logger.error(f"Error validating YAML: {e}")
        return False


def create_example_rules_file(path: str = "rules.example.yaml") -> str:
    """Create an example rules YAML file."""
    example_yaml = """# Pulsar Validation Rules
# Define data quality rules for your dataset

rules:
  # Email format validation (using single quotes to avoid YAML escaping)
  - name: "email_valid"
    column: "email"
    type: "regex"
    pattern: '^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
    threshold: 0.95

  # Age range validation
  - name: "age_in_range"
    column: "age"
    type: "range"
    min: 0
    max: 150
    threshold: 1.0

  # Country validation
  - name: "country_valid"
    column: "country"
    type: "in_list"
    values:
      - US
      - CA
      - MX
      - UK
    threshold: 0.99

  # User ID uniqueness
  - name: "user_id_unique"
    column: "user_id"
    type: "unique"
    threshold: 1.0

  # No null emails
  - name: "email_not_null"
    column: "email"
    type: "not_null"
    threshold: 0.95
"""
    
    try:
        with open(path, 'w', encoding='utf-8') as f:
            f.write(example_yaml)
        logger.info(f"Example rules file created: {path}")
        return path
    except Exception as e:
        logger.error(f"Failed to create example file: {e}")
        raise
