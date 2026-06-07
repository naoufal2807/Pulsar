#!/usr/bin/env python
# test_ticket_1_1.py

"""
Complete integration test for Ticket 1.1: Rule System & Validator

Tests:
1. Rule creation and validation
2. Error handling (invalid rules)
3. Validator with all rule types
4. Logging to file with session ID
5. Edge cases (empty data, nulls, etc.)
"""

import sys
import json
import logging
from pathlib import Path

import polars as pl

from pulsar.logging_config import setup_logging, get_logger, get_session_id
from pulsar.core.quality.rules import Rule, RULE_TYPES
from pulsar.core.quality.validator import Validator


def test_rule_creation():
    """Test Rule class creation and validation"""
    print("\n" + "="*60)
    print("TEST 1: Rule Creation & Validation")
    print("="*60)
    
    logger = get_logger("test.rules")
    
    # Test 1.1: Valid rules
    print("\n✓ Creating valid rules...")
    try:
        rules = [
            Rule(
                name="email_valid",
                column="email",
                rule_type="regex",
                threshold=0.95,
                pattern=r"^[\w\.-]+@[\w\.-]+\.\w+$"
            ),
            Rule(
                name="age_in_range",
                column="age",
                rule_type="range",
                threshold=1.0,
                min=0,
                max=150
            ),
            Rule(
                name="country_valid",
                column="country",
                rule_type="in_list",
                threshold=0.99,
                values=["US", "CA", "MX", "UK"]
            ),
            Rule(
                name="user_id_unique",
                column="user_id",
                rule_type="unique",
                threshold=1.0
            ),
            Rule(
                name="no_null_emails",
                column="email",
                rule_type="not_null",
                threshold=1.0
            )
        ]
        
        for rule in rules:
            print(f"  ✅ {rule.name}: {rule.rule_type}")
            logger.info(f"Rule created: {rule}")
    
    except Exception as e:
        print(f"  ❌ Error: {e}")
        logger.error(f"Failed to create valid rules: {e}", exc_info=True)
        return False
    
    # Test 1.2: Invalid rule type
    print("\n✓ Testing invalid rule type (should error)...")
    try:
        bad_rule = Rule(
            name="bad",
            column="col",
            rule_type="invalid_type"
        )
        print("  ❌ Should have raised error!")
        return False
    except ValueError as e:
        print(f"  ✅ Caught error: {e}")
        logger.info(f"Invalid rule type caught: {e}")
    
    # Test 1.3: Invalid regex pattern
    print("\n✓ Testing invalid regex pattern (should error)...")
    try:
        bad_rule = Rule(
            name="bad_regex",
            column="email",
            rule_type="regex",
            pattern="[invalid("
        )
        print("  ❌ Should have raised error!")
        return False
    except ValueError as e:
        print(f"  ✅ Caught error: {e}")
        logger.info(f"Invalid regex caught: {e}")
    
    # Test 1.4: Range min > max
    print("\n✓ Testing invalid range (min > max, should error)...")
    try:
        bad_rule = Rule(
            name="bad_range",
            column="age",
            rule_type="range",
            min=150,
            max=0
        )
        print("  ❌ Should have raised error!")
        return False
    except ValueError as e:
        print(f"  ✅ Caught error: {e}")
        logger.info(f"Invalid range caught: {e}")
    
    # Test 1.5: Missing required params
    print("\n✓ Testing missing required params (should error)...")
    try:
        bad_rule = Rule(
            name="bad_regex",
            column="email",
            rule_type="regex"
            # Missing 'pattern'
        )
        print("  ❌ Should have raised error!")
        return False
    except ValueError as e:
        print(f"  ✅ Caught error: {e}")
        logger.info(f"Missing params caught: {e}")
    
    print("\n✅ RULES TEST PASSED\n")
    return True


def test_validator_all_rules():
    """Test Validator with all rule types"""
    print("\n" + "="*60)
    print("TEST 2: Validator - All Rule Types")
    print("="*60)
    
    logger = get_logger("test.validator")
    
    # Create test data
    print("\n✓ Creating test data...")
    data = {
        'user_id': [1, 2, 3, 4, 5],
        'email': [
            'user1@example.com',
            'user2@example.com',
            'invalid-email',
            None,
            'user5@example.com'
        ],
        'age': [25, 150, 35, 40, 200],  # 2 out of range
        'country': ['US', 'CA', 'MX', 'US', 'XX'],  # 1 invalid
    }
    
    lf = pl.LazyFrame(data)
    print(f"  ✅ Created LazyFrame with {len(data)} columns and 5 rows")
    logger.info(f"Test data created: {list(data.keys())}")
    
    # Create rules
    print("\n✓ Creating validation rules...")
    rules = [
        Rule(
            name="email_format",
            column="email",
            rule_type="regex",
            threshold=0.8,
            pattern=r"^[\w\.-]+@[\w\.-]+\.\w+$"
        ),
        Rule(
            name="age_valid",
            column="age",
            rule_type="range",
            threshold=1.0,
            min=0,
            max=120
        ),
        Rule(
            name="country_valid",
            column="country",
            rule_type="in_list",
            threshold=0.9,
            values=["US", "CA", "MX", "UK"]
        ),
        Rule(
            name="user_id_unique",
            column="user_id",
            rule_type="unique",
            threshold=1.0
        ),
        Rule(
            name="email_not_null",
            column="email",
            rule_type="not_null",
            threshold=0.8
        )
    ]
    
    for rule in rules:
        print(f"  ✅ {rule.name}")
    
    # Run validation
    print("\n✓ Running validator...")
    try:
        validator = Validator()
        results = validator.validate(lf, rules)
        print(f"  ✅ Validation complete: {len(results)} rules processed")
    except Exception as e:
        print(f"  ❌ Validation error: {e}")
        logger.error(f"Validation failed: {e}", exc_info=True)
        return False
    
    # Display results
    print("\n✓ Validation Results:")
    print("-" * 60)
    
    passed_count = 0
    for rule_name, result in results.items():
        status = result['status']
        percentage = result['percentage']
        passed = result['passed']
        total = result['total']
        
        status_icon = "✅" if status == "PASS" else "❌" if status == "FAIL" else "⚠️"
        
        print(f"  {status_icon} {rule_name:20} {status:6} {percentage:6.1f}% ({passed}/{total})")
        
        if status == "PASS":
            passed_count += 1
        
        logger.info(f"Rule '{rule_name}': {status} ({percentage:.1f}%)")
    
    print("-" * 60)
    print(f"\nSummary: {passed_count}/{len(results)} rules passed")
    
    # Verify expected results
    print("\n✓ Verifying expected outcomes...")
    expected = {
        "email_format": "PASS",      # 4/5 match pattern (80% >= threshold)
        "age_valid": "FAIL",         # 3/5 in range (60% < 100%)
        "country_valid": "FAIL",     # 4/5 in list (80% < 90%)
        "user_id_unique": "PASS",    # All unique
        "email_not_null": "PASS"     # 4/5 not null (80% >= 80%)
    }
    
    all_correct = True
    for rule_name, expected_status in expected.items():
        actual_status = results[rule_name]['status']
        match = "✅" if actual_status == expected_status else "❌"
        print(f"  {match} {rule_name}: {actual_status} (expected {expected_status})")
        if actual_status != expected_status:
            all_correct = False
            logger.warning(
                f"Unexpected result for {rule_name}: "
                f"got {actual_status}, expected {expected_status}"
            )
    
    if all_correct:
        print("\n✅ VALIDATOR TEST PASSED\n")
        return True
    else:
        print("\n❌ VALIDATOR TEST FAILED\n")
        return False


def test_edge_cases():
    """Test edge cases"""
    print("\n" + "="*60)
    print("TEST 3: Edge Cases")
    print("="*60)
    
    logger = get_logger("test.edge_cases")
    
    # Test 3.1: Empty dataset
    print("\n✓ Testing empty dataset...")
    try:
        empty_data = {'email': [], 'age': []}
        empty_lf = pl.LazyFrame(empty_data)
        
        validator = Validator()
        rule = Rule(
            name="test_empty",
            column="email",
            rule_type="not_null"
        )
        
        results = validator.validate(empty_lf, [rule])
        status = results['test_empty']['status']
        print(f"  ✅ Empty dataset handled: {status}")
        logger.info(f"Empty dataset result: {status}")
    
    except Exception as e:
        print(f"  ❌ Error: {e}")
        logger.error(f"Empty dataset error: {e}", exc_info=True)
        return False
    
    # Test 3.2: All nulls
    print("\n✓ Testing all nulls...")
    try:
        null_data = {'email': [None, None, None]}
        null_lf = pl.LazyFrame(null_data)
        
        validator = Validator()
        rule = Rule(
            name="test_nulls",
            column="email",
            rule_type="not_null",
            threshold=0.5
        )
        
        results = validator.validate(null_lf, [rule])
        percentage = results['test_nulls']['percentage']
        print(f"  ✅ All nulls handled: {percentage:.1f}%")
        logger.info(f"All nulls result: {percentage}%")
    
    except Exception as e:
        print(f"  ❌ Error: {e}")
        logger.error(f"All nulls error: {e}", exc_info=True)
        return False
    
    # Test 3.3: Invalid column
    print("\n✓ Testing invalid column...")
    try:
        data = {'email': ['test@example.com']}
        lf = pl.LazyFrame(data)
        
        validator = Validator()
        rule = Rule(
            name="invalid_col",
            column="nonexistent",
            rule_type="not_null"
        )
        
        results = validator.validate(lf, [rule])
        if results['invalid_col']['status'] == "ERROR":
            print(f"  ✅ Invalid column caught")
            logger.info("Invalid column properly caught")
        else:
            print(f"  ❌ Should have errored")
            return False
    
    except Exception as e:
        print(f"  ❌ Unexpected error: {e}")
        return False
    
    print("\n✅ EDGE CASES TEST PASSED\n")
    return True


def test_logging():
    """Test logging to file with session ID"""
    print("\n" + "="*60)
    print("TEST 4: Logging System")
    print("="*60)
    
    session_id = get_session_id()
    print(f"\n✓ Session ID: {session_id}")
    
    # Check log file exists
    log_dir = Path("logs")
    if log_dir.exists():
        log_files = list(log_dir.glob(f"pulsar_*_{session_id}.log"))
        if log_files:
            log_file = log_files[0]
            print(f"✅ Log file created: {log_file}")
            
            # Read log file
            with open(log_file) as f:
                lines = f.readlines()
            
            print(f"✅ Log file contains {len(lines)} entries")
            
            # Check for session ID in logs
            session_in_logs = sum(1 for line in lines if session_id in line)
            print(f"✅ Session ID appears in {session_in_logs} log entries")
            
            # Show last 5 log entries
            print("\n✓ Last 5 log entries:")
            for line in lines[-5:]:
                print(f"  {line.rstrip()}")
            
            print("\n✅ LOGGING TEST PASSED\n")
            return True
        else:
            print(f"❌ No log file found for session {session_id}")
            return False
    else:
        print(f"❌ Logs directory not found")
        return False


def main():
    """Run all tests"""
    print("\n" + "="*60)
    print("PULSAR TICKET 1.1 - COMPLETE SYSTEM TEST")
    print("="*60)
    
    # Setup logging
    log_file = setup_logging(level=logging.INFO)
    root_logger = get_logger("test")
    root_logger.info("Starting Ticket 1.1 integration tests")
    
    # Run tests
    tests = [
        ("Rule Creation", test_rule_creation),
        ("Validator", test_validator_all_rules),
        ("Edge Cases", test_edge_cases),
        ("Logging", test_logging),
    ]
    
    results = {}
    for test_name, test_func in tests:
        try:
            results[test_name] = test_func()
        except Exception as e:
            print(f"\n❌ {test_name} failed with exception: {e}")
            root_logger.error(f"{test_name} failed: {e}", exc_info=True)
            results[test_name] = False
    
    # Summary
    print("\n" + "="*60)
    print("TEST SUMMARY")
    print("="*60)
    
    passed = sum(1 for v in results.values() if v)
    total = len(results)
    
    for test_name, passed_bool in results.items():
        status = "✅ PASS" if passed_bool else "❌ FAIL"
        print(f"  {status} - {test_name}")
    
    print(f"\nTotal: {passed}/{total} tests passed")
    
    if passed == total:
        print("\n🎉 ALL TESTS PASSED! Ticket 1.1 is complete!")
        root_logger.info("✅ All tests passed - Ticket 1.1 complete")
        return 0
    else:
        print(f"\n❌ {total - passed} test(s) failed")
        root_logger.error(f"❌ {total - passed} test(s) failed")
        return 1


if __name__ == "__main__":
    sys.exit(main())