# tests/quality/test_validator.py

"""Unit tests for pulsar.core.quality.validator"""

import pytest
import polars as pl
from pulsar.logging_config import get_logger
from pulsar.core.quality.rules import Rule
from pulsar.core.quality.validator import Validator


logger = get_logger("test.validator")


class TestValidatorNotNull:
    """Test not_null rule validation"""
    
    def test_all_not_null(self):
        """Test column with no nulls"""
        data = {'email': ['a@x.com', 'b@x.com', 'c@x.com']}
        lf = pl.LazyFrame(data)
        
        rule = Rule(name="test", column="email", rule_type="not_null")
        validator = Validator()
        results = validator.validate(lf, [rule])
        
        assert results['test']['status'] == "PASS"
        assert results['test']['percentage'] == 100.0
        logger.info("✅ All not_null: PASS")
    
    def test_some_nulls(self):
        """Test column with some nulls"""
        data = {'email': ['a@x.com', None, 'c@x.com']}
        lf = pl.LazyFrame(data)
        
        rule = Rule(name="test", column="email", rule_type="not_null", threshold=0.66)
        validator = Validator()
        results = validator.validate(lf, [rule])
        
        assert results['test']['status'] == "PASS"
        assert results['test']['percentage'] == pytest.approx(66.67, 0.1)
        logger.info("✅ Some nulls: PASS")
    
    def test_all_nulls(self):
        """Test column with all nulls"""
        data = {'email': [None, None, None]}
        lf = pl.LazyFrame(data)
        
        rule = Rule(name="test", column="email", rule_type="not_null")
        validator = Validator()
        results = validator.validate(lf, [rule])
        
        assert results['test']['status'] == "FAIL"
        assert results['test']['percentage'] == 0.0
        logger.info("✅ All nulls: FAIL")


class TestValidatorRegex:
    """Test regex rule validation"""
    
    def test_all_match_pattern(self):
        """Test all values match pattern"""
        data = {'email': ['a@x.com', 'b@x.com', 'c@x.com']}
        lf = pl.LazyFrame(data)
        
        rule = Rule(
            name="test",
            column="email",
            rule_type="regex",
            pattern=r"^.+@.+\..+$"
        )
        validator = Validator()
        results = validator.validate(lf, [rule])
        
        assert results['test']['status'] == "PASS"
        assert results['test']['percentage'] == 100.0
        logger.info("✅ All match regex: PASS")
    
    def test_some_match_pattern(self):
        """Test some values match pattern"""
        data = {'email': ['a@x.com', 'invalid', 'c@x.com']}
        lf = pl.LazyFrame(data)
        
        rule = Rule(
            name="test",
            column="email",
            rule_type="regex",
            threshold=0.66,
            pattern=r"^.+@.+\..+$"
        )
        validator = Validator()
        results = validator.validate(lf, [rule])
        
        assert results['test']['status'] == "PASS"
        assert results['test']['percentage'] == pytest.approx(66.67, 0.1)
        logger.info("✅ Some match regex: PASS")


class TestValidatorRange:
    """Test range rule validation"""
    
    def test_all_in_range(self):
        """Test all values in range"""
        data = {'age': [10, 20, 30, 40]}
        lf = pl.LazyFrame(data)
        
        rule = Rule(name="test", column="age", rule_type="range", min=0, max=100)
        validator = Validator()
        results = validator.validate(lf, [rule])
        
        assert results['test']['status'] == "PASS"
        assert results['test']['percentage'] == 100.0
        logger.info("✅ All in range: PASS")
    
    def test_some_out_of_range(self):
        """Test some values out of range"""
        data = {'age': [10, 150, 30, 200]}
        lf = pl.LazyFrame(data)
        
        rule = Rule(
            name="test",
            column="age",
            rule_type="range",
            threshold=0.75,
            min=0,
            max=100
        )
        validator = Validator()
        results = validator.validate(lf, [rule])
        
        assert results['test']['status'] == "FAIL"
        assert results['test']['percentage'] == 50.0
        logger.info("✅ Some out of range: FAIL")


class TestValidatorUnique:
    """Test unique rule validation"""
    
    def test_all_unique(self):
        """Test all values are unique"""
        data = {'id': [1, 2, 3, 4, 5]}
        lf = pl.LazyFrame(data)
        
        rule = Rule(name="test", column="id", rule_type="unique")
        validator = Validator()
        results = validator.validate(lf, [rule])
        
        assert results['test']['status'] == "PASS"
        assert results['test']['percentage'] == 100.0
        logger.info("✅ All unique: PASS")
    
    def test_has_duplicates(self):
        """Test values with duplicates"""
        data = {'id': [1, 2, 3, 2, 1]}
        lf = pl.LazyFrame(data)
        
        rule = Rule(name="test", column="id", rule_type="unique")
        validator = Validator()
        results = validator.validate(lf, [rule])
        
        assert results['test']['status'] == "FAIL"
        assert results['test']['percentage'] == 60.0
        logger.info("✅ Has duplicates: FAIL")


class TestValidatorInList:
    """Test in_list rule validation"""
    
    def test_all_in_list(self):
        """Test all values in list"""
        data = {'country': ['US', 'CA', 'MX', 'US']}
        lf = pl.LazyFrame(data)
        
        rule = Rule(
            name="test",
            column="country",
            rule_type="in_list",
            values=['US', 'CA', 'MX']
        )
        validator = Validator()
        results = validator.validate(lf, [rule])
        
        assert results['test']['status'] == "PASS"
        assert results['test']['percentage'] == 100.0
        logger.info("✅ All in list: PASS")
    
    def test_some_not_in_list(self):
        """Test some values not in list"""
        data = {'country': ['US', 'CA', 'MX', 'XX']}
        lf = pl.LazyFrame(data)
        
        rule = Rule(
            name="test",
            column="country",
            rule_type="in_list",
            threshold=0.9,
            values=['US', 'CA', 'MX']
        )
        validator = Validator()
        results = validator.validate(lf, [rule])
        
        assert results['test']['status'] == "FAIL"
        assert results['test']['percentage'] == 75.0
        logger.info("✅ Some not in list: FAIL")


class TestValidatorEdgeCases:
    """Test edge cases"""
    
    def test_empty_dataframe(self):
        """Test empty dataframe"""
        data = {'col': []}
        lf = pl.LazyFrame(data)
        
        rule = Rule(name="test", column="col", rule_type="not_null")
        validator = Validator()
        results = validator.validate(lf, [rule])
        
        assert results['test']['status'] == "SKIP"
        logger.info("✅ Empty dataframe: SKIP")
    
    def test_nonexistent_column(self):
        """Test nonexistent column"""
        data = {'email': ['a@x.com']}
        lf = pl.LazyFrame(data)
        
        rule = Rule(name="test", column="nonexistent", rule_type="not_null")
        validator = Validator()
        results = validator.validate(lf, [rule])
        
        assert results['test']['status'] == "ERROR"
        assert 'error' in results['test']
        logger.info("✅ Nonexistent column: ERROR")
    
    def test_multiple_rules(self):
        """Test multiple rules at once"""
        data = {
            'email': ['a@x.com', 'b@x.com', None],
            'age': [25, 150, 35]
        }
        lf = pl.LazyFrame(data)
        
        rules = [
            Rule(name="email_not_null", column="email", rule_type="not_null", threshold=0.66),
            Rule(name="age_range", column="age", rule_type="range", min=0, max=120)
        ]
        
        validator = Validator()
        results = validator.validate(lf, rules)
        
        assert len(results) == 2
        assert 'email_not_null' in results
        assert 'age_range' in results
        logger.info("✅ Multiple rules: OK")