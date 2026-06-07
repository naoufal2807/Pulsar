# tests/quality/test_loader.py

import pytest
import tempfile
from pathlib import Path
from pulsar.core.quality.loader import load_rules_yaml, validate_yaml_syntax, create_example_rules_file
from pulsar.core.quality.rules import Rule


class TestLoadRulesYaml:
    """Test YAML rule loading"""
    
    def test_load_valid_rules(self):
        """Test loading valid rules from YAML"""
        yaml_content = """rules:
  - name: "email_valid"
    column: "email"
    type: "regex"
    pattern: '^[^@]+@[^@]+\\.[^@]+$'
    threshold: 0.95
  
  - name: "age_range"
    column: "age"
    type: "range"
    min: 0
    max: 150
"""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False, encoding='utf-8') as f:
            f.write(yaml_content)
            temp_path = f.name
        
        try:
            rules = load_rules_yaml(temp_path)
            assert len(rules) == 2
            assert rules[0].name == "email_valid"
            assert rules[0].rule_type == "regex"
            assert rules[1].name == "age_range"
            assert rules[1].rule_type == "range"
        finally:
            Path(temp_path).unlink(missing_ok=True)
    
    def test_load_file_not_found(self):
        """Test error when file doesn't exist"""
        with pytest.raises(FileNotFoundError):
            load_rules_yaml("nonexistent.yaml")
    
    def test_load_invalid_yaml(self):
        """Test error on invalid YAML syntax"""
        yaml_content = "rules:\n  - name: test\n    column: [invalid"
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False, encoding='utf-8') as f:
            f.write(yaml_content)
            temp_path = f.name
        
        try:
            with pytest.raises(ValueError, match="Invalid YAML"):
                load_rules_yaml(temp_path)
        finally:
            Path(temp_path).unlink(missing_ok=True)
    
    def test_load_empty_rules(self):
        """Test loading empty rules list"""
        yaml_content = "rules: []"
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False, encoding='utf-8') as f:
            f.write(yaml_content)
            temp_path = f.name
        
        try:
            rules = load_rules_yaml(temp_path)
            assert len(rules) == 0
        finally:
            Path(temp_path).unlink(missing_ok=True)
    
    def test_load_all_rule_types(self):
        """Test loading all 5 rule types"""
        yaml_content = """rules:
  - name: "not_null"
    column: "col1"
    type: "not_null"
  
  - name: "regex"
    column: "col2"
    type: "regex"
    pattern: '^test'
  
  - name: "range"
    column: "col3"
    type: "range"
    min: 0
    max: 100
  
  - name: "unique"
    column: "col4"
    type: "unique"
  
  - name: "in_list"
    column: "col5"
    type: "in_list"
    values: [a, b, c]
"""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False, encoding='utf-8') as f:
            f.write(yaml_content)
            temp_path = f.name
        
        try:
            rules = load_rules_yaml(temp_path)
            assert len(rules) == 5
            types = [r.rule_type for r in rules]
            assert "not_null" in types
            assert "regex" in types
            assert "range" in types
            assert "unique" in types
            assert "in_list" in types
        finally:
            Path(temp_path).unlink(missing_ok=True)
    
    def test_load_missing_required_field(self):
        """Test error on missing required field"""
        yaml_content = """rules:
  - column: "email"
    type: "regex"
"""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False, encoding='utf-8') as f:
            f.write(yaml_content)
            temp_path = f.name
        
        try:
            with pytest.raises(ValueError, match="missing 'name'"):
                load_rules_yaml(temp_path)
        finally:
            Path(temp_path).unlink(missing_ok=True)


class TestValidateYamlSyntax:
    """Test YAML syntax validation"""
    
    def test_valid_yaml_syntax(self):
        """Test valid YAML syntax"""
        yaml_content = "rules:\n  - name: test\n    column: col\n    type: not_null"
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False, encoding='utf-8') as f:
            f.write(yaml_content)
            temp_path = f.name
        
        try:
            assert validate_yaml_syntax(temp_path) == True
        finally:
            Path(temp_path).unlink(missing_ok=True)
    
    def test_invalid_yaml_syntax(self):
        """Test invalid YAML syntax"""
        yaml_content = "rules: [invalid"
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False, encoding='utf-8') as f:
            f.write(yaml_content)
            temp_path = f.name
        
        try:
            assert validate_yaml_syntax(temp_path) == False
        finally:
            Path(temp_path).unlink(missing_ok=True)
    
    def test_nonexistent_file(self):
        """Test nonexistent file"""
        assert validate_yaml_syntax("nonexistent.yaml") == False


class TestCreateExampleFile:
    """Test example file creation"""
    
    def test_create_example_file(self):
        """Test creating example rules file"""
        with tempfile.TemporaryDirectory() as tmpdir:
            path = Path(tmpdir) / "rules.example.yaml"
            created_path = create_example_rules_file(str(path))
            
            assert Path(created_path).exists()
            
            rules = load_rules_yaml(created_path)
            assert len(rules) > 0
