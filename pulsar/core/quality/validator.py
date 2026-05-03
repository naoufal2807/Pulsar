# pulsar/core/quality/validator.py

import re
from typing import Dict, Any, List, Optional
import polars as pl
from pulsar.logging_config import get_logger
from pulsar.core.quality.rules import Rule


logger = get_logger("pulsar.quality.validator")


class Validator:
    """
    Validates data against rules using Polars expressions.
    
    Works with LazyFrame (streaming) to handle large files efficiently.
    """
    
    def __init__(self):
        logger.info("Validator initialized")
    
    def validate(
        self,
        lf: pl.LazyFrame,
        rules: List[Rule]
    ) -> Dict[str, Dict[str, Any]]:
        """
        Run all rules against LazyFrame
        
        Args:
            lf: Polars LazyFrame (not collected yet)
            rules: List of Rule objects
        
        Returns:
            Dict with results for each rule
        
        Example:
            results = validator.validate(lf, [rule1, rule2])
            # {
            #   "email_valid": {
            #     "status": "PASS",
            #     "passed": 950,
            #     "total": 1000,
            #     "percentage": 95.0
            #   }
            # }
        """
        try:
            logger.info(f"Starting validation with {len(rules)} rules")
            
            if not rules:
                logger.warning("No rules provided for validation")
                return {}
            
            results = {}
            
            for rule in rules:
                try:
                    logger.debug(f"Validating rule: {rule.name}")
                    result = self._check_rule(lf, rule)
                    results[rule.name] = result
                    logger.info(
                        f"Rule '{rule.name}': {result['status']} "
                        f"({result['percentage']:.1f}%)"
                    )
                
                except Exception as e:
                    logger.error(
                        f"Error validating rule '{rule.name}': {e}",
                        exc_info=True
                    )
                    results[rule.name] = {
                        "status": "ERROR",
                        "error": str(e),
                        "passed": 0,
                        "total": 0,
                        "percentage": 0.0
                    }
            
            logger.info(f"Validation complete. {len(results)} rules processed")
            return results
        
        except Exception as e:
            logger.error(f"Fatal error during validation: {e}", exc_info=True)
            raise
    
    def _check_rule(self, lf: pl.LazyFrame, rule: Rule) -> Dict[str, Any]:
        """
        Check a single rule against data
        
        Args:
            lf: Polars LazyFrame
            rule: Rule object
        
        Returns:
            Result dict with status, passed count, percentage
        """
        try:
            # Validate column exists
            schema = lf.collect_schema()
            if rule.column not in schema:
                raise ValueError(f"Column '{rule.column}' not found in data")
            
            # Get total row count
            total = lf.select(pl.len()).collect().item()
            logger.debug(f"Total rows: {total}")
            
            if total == 0:
                logger.warning(f"No data to validate for rule '{rule.name}'")
                return {
                    "status": "SKIP",
                    "reason": "Empty dataset",
                    "passed": 0,
                    "total": 0,
                    "percentage": 0.0
                }
            
            # Run rule-specific validation
            if rule.rule_type == "not_null":
                return self._check_not_null(lf, rule, total)
            
            elif rule.rule_type == "regex":
                return self._check_regex(lf, rule, total)
            
            elif rule.rule_type == "range":
                return self._check_range(lf, rule, total)
            
            elif rule.rule_type == "unique":
                return self._check_unique(lf, rule, total)
            
            elif rule.rule_type == "in_list":
                return self._check_in_list(lf, rule, total)
            
            else:
                raise ValueError(f"Unknown rule type: {rule.rule_type}")
        
        except Exception as e:
            logger.error(
                f"Error checking rule '{rule.name}': {e}",
                exc_info=True
            )
            raise
    
    def _check_not_null(
        self,
        lf: pl.LazyFrame,
        rule: Rule,
        total: int
    ) -> Dict[str, Any]:
        """Check that column has no nulls"""
        try:
            logger.debug(f"Checking not_null for column '{rule.column}'")
            
            # Count non-null values
            passed = lf.select(
                pl.col(rule.column).is_not_null().sum()
            ).collect().item()
            
            percentage = (passed / total) * 100 if total > 0 else 0
            status = "PASS" if percentage >= (rule.threshold * 100) else "FAIL"
            
            return {
                "status": status,
                "passed": int(passed),
                "total": total,
                "percentage": percentage,
                "nulls_found": total - int(passed)
            }
        
        except Exception as e:
            logger.error(f"Error in not_null check: {e}", exc_info=True)
            raise
    
    def _check_regex(
        self,
        lf: pl.LazyFrame,
        rule: Rule,
        total: int
    ) -> Dict[str, Any]:
        """Check that column values match regex pattern"""
        try:
            pattern = rule.params.get("pattern")
            if not pattern:
                raise ValueError("regex rule missing 'pattern' parameter")
            
            logger.debug(
                f"Checking regex for column '{rule.column}' "
                f"with pattern: {pattern}"
            )
            
            # Validate regex first
            try:
                re.compile(pattern)
            except re.error as e:
                logger.error(f"Invalid regex pattern: {e}")
                raise ValueError(f"Invalid regex pattern: {e}") from e
            
            # Count matches using Polars regex
            passed = lf.select(
                pl.col(rule.column)
                .str.contains(pattern)
                .sum()
            ).collect().item()
            
            percentage = (passed / total) * 100 if total > 0 else 0
            status = "PASS" if percentage >= (rule.threshold * 100) else "FAIL"
            
            return {
                "status": status,
                "passed": int(passed),
                "total": total,
                "percentage": percentage,
                "pattern": pattern
            }
        
        except Exception as e:
            logger.error(f"Error in regex check: {e}", exc_info=True)
            raise
    
    def _check_range(
        self,
        lf: pl.LazyFrame,
        rule: Rule,
        total: int
    ) -> Dict[str, Any]:
        """Check that column values are within min/max range"""
        try:
            min_val = rule.params.get("min")
            max_val = rule.params.get("max")
            
            if min_val is None or max_val is None:
                raise ValueError("range rule missing 'min' or 'max'")
            
            logger.debug(
                f"Checking range for column '{rule.column}' "
                f"({min_val} to {max_val})"
            )
            
            # Count values within range
            passed = lf.select(
                ((pl.col(rule.column) >= min_val) & 
                 (pl.col(rule.column) <= max_val))
                .sum()
            ).collect().item()
            
            percentage = (passed / total) * 100 if total > 0 else 0
            status = "PASS" if percentage >= (rule.threshold * 100) else "FAIL"
            
            return {
                "status": status,
                "passed": int(passed),
                "total": total,
                "percentage": percentage,
                "min": min_val,
                "max": max_val,
                "out_of_range": total - int(passed)
            }
        
        except Exception as e:
            logger.error(f"Error in range check: {e}", exc_info=True)
            raise
    
    def _check_unique(
        self,
        lf: pl.LazyFrame,
        rule: Rule,
        total: int
    ) -> Dict[str, Any]:
        """Check that all column values are unique"""
        try:
            logger.debug(f"Checking unique for column '{rule.column}'")
            
            # Count distinct values
            distinct = lf.select(
                pl.col(rule.column).n_unique()
            ).collect().item()
            
            passed = distinct  # All unique = distinct == total
            percentage = (passed / total) * 100 if total > 0 else 0
            status = "PASS" if percentage >= (rule.threshold * 100) else "FAIL"
            
            duplicates = total - distinct
            
            return {
                "status": status,
                "passed": passed,
                "total": total,
                "percentage": percentage,
                "distinct_values": distinct,
                "duplicates": duplicates
            }
        
        except Exception as e:
            logger.error(f"Error in unique check: {e}", exc_info=True)
            raise
    
    def _check_in_list(
        self,
        lf: pl.LazyFrame,
        rule: Rule,
        total: int
    ) -> Dict[str, Any]:
        """Check that column values are in allowed list"""
        try:
            values = rule.params.get("values")
            if not values:
                raise ValueError("in_list rule missing 'values' parameter")
            
            values_list = list(values) if not isinstance(values, list) else values
            logger.debug(
                f"Checking in_list for column '{rule.column}' "
                f"with {len(values_list)} allowed values"
            )
            
            # Count values in list
            passed = lf.select(
                pl.col(rule.column).is_in(values_list).sum()
            ).collect().item()
            
            percentage = (passed / total) * 100 if total > 0 else 0
            status = "PASS" if percentage >= (rule.threshold * 100) else "FAIL"
            
            return {
                "status": status,
                "passed": int(passed),
                "total": total,
                "percentage": percentage,
                "allowed_values": values_list,
                "not_in_list": total - int(passed)
            }
        
        except Exception as e:
            logger.error(f"Error in in_list check: {e}", exc_info=True)
            raise