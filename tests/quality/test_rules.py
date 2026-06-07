# tests/quality/test_rules.py

"""Unit tests for pulsar.core.quality.rules"""

import pytest
from pulsar.logging_config import get_logger
from pulsar.core.quality.rules import Rule, RULE_TYPES


logger = get_logger("test.rules")


class TestRuleCreation:
    """Test Rule class creation and validation"""
    
    def test_create_valid_regex_rule(self):
        """Test creating a valid regex rule"""
        rule = Rule(
            name="email_valid",
            column="email",
            rule_type="regex",
            threshold=0.95,
            pattern=r"^[\w\.-]+@[\w\.-]+\.\w+$"
        )
        assert rule.name == "email_valid"
        assert rule.column == "email"
        assert rule.rule_type == "regex"
        assert rule.threshold == 0.95
        logger.info(f"✅ Valid regex rule created: {rule}")
    
    def test_create_valid_range_rule(self):
        """Test creating a valid range rule"""
        rule = Rule(
            name="age_valid",
            column="age",
            rule_type="range",
            threshold=1.0,
            min=0,
            max=150
        )
        assert rule.rule_type == "range"
        assert rule.params["min"] == 0
        assert rule.params["max"] == 150
        logger.info(f"✅ Valid range rule created: {rule}")
    
    def test_create_valid_in_list_rule(self):
        """Test creating a valid in_list rule"""
        rule = Rule(
            name="country_valid",
            column="country",
            rule_type="in_list",
            threshold=0.99,
            values=["US", "CA", "MX", "UK"]
        )
        assert rule.rule_type == "in_list"
        assert len(rule.params["values"]) == 4
        logger.info(f"✅ Valid in_list rule created: {rule}")
    
    def test_create_valid_unique_rule(self):
        """Test creating a unique rule"""
        rule = Rule(
            name="id_unique",
            column="id",
            rule_type="unique"
        )
        assert rule.rule_type == "unique"
        logger.info(f"✅ Valid unique rule created: {rule}")
    
    def test_create_valid_not_null_rule(self):
        """Test creating a not_null rule"""
        rule = Rule(
            name="no_nulls",
            column="email",
            rule_type="not_null"
        )
        assert rule.rule_type == "not_null"
        logger.info(f"✅ Valid not_null rule created: {rule}")


class TestRuleValidation:
    """Test Rule validation and error handling"""
    
    def test_invalid_rule_type(self):
        """Test that invalid rule type raises error"""
        with pytest.raises(ValueError, match="Invalid rule_type"):
            Rule(
                name="bad",
                column="col",
                rule_type="invalid_type"
            )
        logger.info("✅ Invalid rule type caught")
    
    def test_invalid_threshold_too_high(self):
        """Test that threshold > 1.0 raises error"""
        with pytest.raises(ValueError, match="Threshold must be between"):
            Rule(
                name="bad",
                column="col",
                rule_type="not_null",
                threshold=1.5
            )
        logger.info("✅ Invalid threshold (>1.0) caught")
    
    def test_invalid_threshold_negative(self):
        """Test that negative threshold raises error"""
        with pytest.raises(ValueError, match="Threshold must be between"):
            Rule(
                name="bad",
                column="col",
                rule_type="not_null",
                threshold=-0.5
            )
        logger.info("✅ Invalid threshold (<0.0) caught")
    
    def test_regex_missing_pattern(self):
        """Test that regex rule without pattern raises error"""
        with pytest.raises(ValueError, match="regex rule requires 'pattern'"):
            Rule(
                name="bad",
                column="email",
                rule_type="regex"
            )
        logger.info("✅ Missing regex pattern caught")
    
    def test_regex_invalid_pattern(self):
        """Test that invalid regex pattern raises error"""
        with pytest.raises(ValueError, match="Invalid regex pattern"):
            Rule(
                name="bad",
                column="email",
                rule_type="regex",
                pattern="[invalid("
            )
        logger.info("✅ Invalid regex pattern caught")
    
    def test_range_missing_params(self):
        """Test that range rule without min/max raises error"""
        with pytest.raises(ValueError, match="range rule requires 'min' and 'max'"):
            Rule(
                name="bad",
                column="age",
                rule_type="range",
                min=0
            )
        logger.info("✅ Missing range params caught")
    
    def test_range_min_greater_than_max(self):
        """Test that min > max raises error"""
        with pytest.raises(ValueError, match="min.*cannot be > max"):
            Rule(
                name="bad",
                column="age",
                rule_type="range",
                min=150,
                max=0
            )
        logger.info("✅ Range min > max caught")
    
    def test_in_list_missing_values(self):
        """Test that in_list rule without values raises error"""
        with pytest.raises(ValueError, match="in_list rule requires 'values'"):
            Rule(
                name="bad",
                column="country",
                rule_type="in_list"
            )
        logger.info("✅ Missing in_list values caught")
    
    def test_in_list_empty_values(self):
        """Test that in_list rule with empty values raises warning"""
        rule = Rule(
            name="bad",
            column="country",
            rule_type="in_list",
            values=[]
        )
        # Should create but log warning
        assert rule.name == "bad"
        logger.info("✅ Empty in_list values warning logged")
    
    def test_invalid_name(self):
        """Test that empty name raises error"""
        with pytest.raises(ValueError, match="Rule name must be non-empty"):
            Rule(
                name="",
                column="col",
                rule_type="not_null"
            )
        logger.info("✅ Empty name caught")
    
    def test_invalid_column(self):
        """Test that empty column raises error"""
        with pytest.raises(ValueError, match="Column must be non-empty"):
            Rule(
                name="test",
                column="",
                rule_type="not_null"
            )
        logger.info("✅ Empty column caught")


class TestRuleConversion:
    """Test Rule to_dict conversion"""
    
    def test_rule_to_dict(self):
        """Test converting rule to dictionary"""
        rule = Rule(
            name="test_rule",
            column="test_col",
            rule_type="not_null",
            threshold=0.95
        )
        
        rule_dict = rule.to_dict()
        
        assert rule_dict["name"] == "test_rule"
        assert rule_dict["column"] == "test_col"
        assert rule_dict["rule_type"] == "not_null"
        assert rule_dict["threshold"] == 0.95
        logger.info(f"✅ Rule converted to dict: {rule_dict}")
    
    def test_rule_repr(self):
        """Test rule string representation"""
        rule = Rule(
            name="test",
            column="col",
            rule_type="not_null"
        )
        
        repr_str = repr(rule)
        assert "test" in repr_str
        assert "col" in repr_str
        assert "not_null" in repr_str
        logger.info(f"✅ Rule repr: {repr_str}")