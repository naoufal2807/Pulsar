# Pulsar 2.0 - Project Context & Continuation Guide

**Last Updated:** June 7, 2026  
**Project Status:** Week 1 - Small World Framework Foundation  
**Current Phase:** Building Intelligence Layer

---

## 🎯 Project Vision

### Old Vision (Commodity)
```
"Great Expectations but faster"
- Profile datasets
- Detect drift
- Validate quality
- Report issues
```

### New Vision (Category Creator)
```
"Data understands itself"
- Understand WHAT data IS
- Auto-fix broken data
- Learn to prevent problems
- Context-aware quality
```

**Catchphrase:** "Data understands itself. Pulsar listens."

---

## 🏗️ Architecture - 6 Tiers

```
TIER 1: UNDERSTANDING (Small World Framework)
├── Isolator (sample 10% deeply)
├── Learner (discover patterns)
├── Expander (validate & expand)
├── Contextualizer (build rules)
└── Analyzer (understand what IS)
    ↓
TIER 2: DETECTION (Context-Aware Anomalies)
├── Anomaly Detector
├── Contextual Outlier Detector
├── Behavioral Shift Detector
└── Intelligent Drift Detector
    ↓
TIER 3: DIAGNOSIS (Root Cause Analysis)
├── Root Cause Analyzer
├── Impact Assessor
├── Trend Analyzer
└── LLM Explainer (Gemma)
    ↓
TIER 4: REMEDIATION (Auto-Fix)
├── Auto-Fixer
├── Strategy Generator
├── Rule Builder
└── Fix Validator
    ↓
TIER 5: PREVENTION (Continuous Learning)
├── Rule Learner
├── Anomaly Gating
├── Pattern Monitoring
└── Auto-Rule Refinement
    ↓
TIER 6: INTEGRATION (Data Stack)
├── dbt Integration
├── Airflow Integration
├── Kafka Streaming
└── Data Lake Connectors
```

---

## 📁 Directory Structure (Current)

```
pulsar/
├── core/
│   ├── profiling/
│   │   ├── profiler.py          ✅ PHASE 1 (KEEP)
│   │   └── metrics.py           ✅ PHASE 1 (KEEP)
│   │
│   ├── quality/
│   │   ├── validators.py        ✅ PHASE 1 (KEEP)
│   │   ├── rules.py             ✅ PHASE 1 (KEEP)
│   │   ├── loader.py            ✅ PHASE 1 (KEEP)
│   │   └── drift/               ✅ PHASE 2.1 FOUNDATION
│   │       ├── base.py          (DriftDetector, DriftResult, Severity)
│   │       ├── registry.py      (Plugin system)
│   │       ├── __init__.py
│   │       └── detectors/
│   │           ├── completeness.py  (Example detector - 162 lines)
│   │           └── __init__.py
│   │
│   ├── intelligence/            ✅ WEEK 1: SMALL WORLD
│   │   ├── __init__.py
│   │   └── small_world/
│   │       ├── __init__.py
│   │       ├── isolator.py      (Sampling - 270 lines) ✅ DONE
│   │       ├── learner.py       (Pattern discovery - TO BUILD)
│   │       ├── expander.py      (Validation/expansion - TO BUILD)
│   │       ├── contextualizer.py (Rule building - TO BUILD)
│   │       └── analyzer.py      (Data understanding - TO BUILD)
│   │
│   ├── diagnosis/               ⏳ WEEK 2
│   │   ├── root_cause.py
│   │   ├── impact_assessment.py
│   │   └── llm_explainer.py
│   │
│   ├── remediation/             ⏳ WEEK 3
│   │   ├── auto_fixer.py
│   │   ├── strategy_generator.py
│   │   └── rule_builder.py
│   │
│   └── prevention/              ⏳ WEEK 4
│       ├── rule_learner.py
│       └── anomaly_gating.py
│
├── config/
│   ├── drift_detection.yaml     ✅ (9 detectors configured)
│   ├── remediation.yaml         ⏳ TO CREATE
│   └── prevention.yaml          ⏳ TO CREATE
│
├── cli.py                       ✅ PHASE 1 (KEEP & UPDATE)
│
└── integrations/                ⏳ WEEKS 5-6
    ├── dbt/
    ├── airflow/
    ├── kafka/
    └── data_lake/

config/
├── drift_detection.yaml         ✅ (detectors + thresholds)
├── remediation.yaml             ⏳
└── prevention.yaml              ⏳

tests/
├── quality/                     ✅ PHASE 1 (46 tests)
│
├── intelligence/                ✅ WEEK 1
│   ├── test_isolator.py         (11 tests) ✅ DONE
│   ├── test_learner.py          ⏳
│   ├── test_expander.py         ⏳
│   ├── test_contextualizer.py   ⏳
│   └── test_analyzer.py         ⏳
│
├── diagnosis/                   ⏳ WEEK 2
├── remediation/                 ⏳ WEEK 3
└── prevention/                  ⏳ WEEK 4

requirements.txt                 ✅ (65 dependencies)
```

---

## 📚 Key Classes & Interfaces

### Tier 1: Small World Framework

#### Isolator (DONE ✅ - 270 lines)
```python
class Isolator:
    """Extract bounded subset for deep understanding"""
    
    def extract(df: pl.DataFrame) -> pl.DataFrame:
        """Sample 10% (or min 1000 rows)"""
        
    def random_sample(df, n): """Random rows"""
    def stratified_sample(df, n): """Balanced by column"""
    def temporal_sample(df, n): """Time-distributed"""
    def first_n_sample(df, n): """First N rows"""
```

**Tests:** 11 tests in `test_isolator.py`
- ✅ Percentage sampling
- ✅ Absolute count sampling
- ✅ Strategy selection (random, stratified, temporal, first_n)
- ✅ Min size enforcement
- ✅ Schema preservation
- ✅ Reproducibility

#### Learner (TO BUILD - Week 1 Day 3)
```python
class Learner:
    """Discover patterns from sample"""
    
    def discover_formats(sample): 
        """Email, phone, URL, IP, date, UUID, credit card"""
    
    def discover_ranges(sample): 
        """Min, max, distribution"""
    
    def discover_distributions(sample): 
        """Skewness, kurtosis, outliers"""
    
    def discover_relationships(sample): 
        """Correlations, dependencies"""
    
    def discover_patterns(sample): 
        """Recurring structures"""
```

**Expected Output:**
```python
{
    'formats': {'email': 'regex', 'phone': 'pattern', ...},
    'ranges': {'min': 0, 'max': 1000, 'mean': 500},
    'distributions': {'skewness': 0.5, 'kurtosis': 2.1},
    'relationships': {('col_a', 'col_b'): 0.85},
    'patterns': {'seasonal': True, 'trend': 'increasing'},
}
```

#### Expander (TO BUILD - Week 1 Day 4)
```python
class Expander:
    """Validate & expand to full dataset"""
    
    def expand_layer(df, patterns, from_row, to_row):
        """Process next batch of rows"""
    
    def validate_layer(df, patterns):
        """Check rows match patterns"""
    
    def filter_contradictions(df, patterns):
        """Remove rows that violate patterns"""
    
    def update_patterns(old, new_data):
        """Learn from expanded data"""
```

**Logic:**
1. Layer 1: Sample 10K rows → Learn patterns
2. Layer 2: Validate 50K rows → Update patterns
3. Layer 3: Validate 200K rows → Refine understanding
4. Layer 4: Validate 500K rows → Lock in rules
5. Layer 5: Full dataset → Final validation

#### Contextualizer (TO BUILD - Week 1 Day 5)
```python
class Contextualizer:
    """Build contextual understanding"""
    
    def infer_data_type(patterns):
        """customer, transaction, event, log, etc."""
    
    def infer_data_purpose(patterns):
        """acquisition, ml, analytics, etc."""
    
    def build_validation_rules(patterns):
        """What makes this data valid?"""
    
    def extract_quality_requirements(patterns):
        """What does good look like?"""
```

#### Analyzer (TO BUILD - Week 1 Day 5)
```python
class Analyzer:
    """Understand what data IS"""
    
    def understand(df, patterns, rules, end_goal):
        """Return: 'This is B2B lead data. Quality: 92%'"""
    
    def quality_score(df, patterns):
        """0-100 score"""
    
    def recommendation_engine(analysis):
        """What to do next?"""
    
    def compare(baseline, current):
        """Contextual comparison"""
```

### Tier 2: Detection (Drift Detection)

#### DriftDetector (Base Class - DONE ✅)
```python
class DriftDetector(ABC):
    """All detectors inherit from this"""
    
    @abstractmethod
    def detect(col_name, baseline, current) -> DriftResult:
        """Return DriftResult or None"""
    
    @abstractmethod
    def is_applicable(baseline, current) -> bool:
        """Can this detector handle this column?"""
    
    def calculate_severity(score) -> Severity:
        """Map score to NONE/LOW/MODERATE/HIGH/CRITICAL"""
```

#### DriftResult (DONE ✅)
```python
@dataclass
class DriftResult:
    metric_name: str
    column_name: str
    drift_detected: bool
    score: float
    severity: Severity
    baseline_value: Any
    current_value: Any
    delta: Any
    delta_percentage: Optional[float]
    insight: str  # Human-readable!
    metadata: Dict[str, Any]
```

#### Severity (DONE ✅)
```python
class Severity(Enum):
    NONE = 0
    LOW = 1
    MODERATE = 2
    HIGH = 3
    CRITICAL = 4
```

#### Detectors (Implemented: 1/9)

**Completeness (DONE ✅ - 162 lines)**
- Tracks: Null/missing values
- Config: `threshold: 0.01` (1% change triggers)
- Insight: "🔴 150 more nulls in 'email' - data source degraded?"

**TO BUILD (Week 2):**
- MeanShiftDetector (numeric averages)
- OutlierDriftDetector (IQR outliers)
- FormatValidationDetector (email/phone/URL formats)
- UniquenessDetector (duplicate counts)
- CardinalityDetector (unique value counts)
- SkewnessDetector (distribution shape)
- RowCountDetector (dataset-level)
- StdDeviationDetector (spread/volatility)

---

## 🔧 Configuration System

### drift_detection.yaml Structure

```yaml
detectors:
  completeness:
    enabled: true
    threshold: 0.01        # Trigger if 1% change
    weight: 2.0            # Importance weight
    severity_levels:
      critical: 0.5        # ≥50% change
      high: 0.2
      moderate: 0.1
      low: 0.05

  # ... 8 more detectors

global_settings:
  parallel_processing: true
  max_workers: 4
  cache_enabled: true
  cache_ttl_seconds: 3600
```

### Registry System (DONE ✅)

```python
registry = get_registry()

# Register detectors
registry.register('completeness', CompletenessDriftDetector)

# Load from YAML
detectors = registry.load_from_yaml('config/drift_detection.yaml')

# Create instances
detectors = registry.create_detectors(config)
```

---

## 📊 Data Profiles (From Phase 1 Profiler)

### Column Profile Structure
```python
{
    'name': 'email',
    'dtype': 'Utf8',
    'count': 10000,
    'null_count': 50,
    'completeness': 0.995,
    'uniqueness_ratio': 0.98,
    'string_stats': {
        'min_length': 5,
        'max_length': 50,
        'avg_length': 25,
        'formats': {
            'email': 0.98,  # 98% match email regex
            'phone': 0.02,
        }
    },
    'numeric_stats': None,
    'temporal_stats': None,
    'categorical_stats': {
        'n_unique': 9800,
        'top_values': [('alice@example.com', 5), ...],
    }
}
```

---

## 🚀 Weekly Roadmap

### ✅ WEEK 1: Small World Foundation (CURRENT)

**Day 1-2: Isolator** ✅ DONE
- ✅ Extract sample (10%, stratified, temporal, first-N)
- ✅ 11 tests passing
- ✅ Auto-detect stratification column
- ✅ Auto-detect date column
- ✅ Reproducible (seed=42)

**Day 3-5: Learner, Expander, Contextualizer, Analyzer** ⏳ TO BUILD
- [ ] Learner: Pattern discovery from sample
- [ ] Expander: Layer-by-layer validation
- [ ] Contextualizer: Rule extraction
- [ ] Analyzer: Data understanding
- [ ] 40+ tests total
- [ ] Integration tests (end-to-end)

### ⏳ WEEK 2: Detection + Diagnosis

**Day 1-3: Detection Layer**
- [ ] AnomalyDetector (context-aware)
- [ ] ContextualOutlierDetector
- [ ] BehavioralShiftDetector
- [ ] Tests for each

**Day 4-5: Diagnosis Layer**
- [ ] RootCauseAnalyzer
- [ ] ImpactAssessor
- [ ] TrendAnalyzer
- [ ] LLMExplainer (Gemma integration)

### ⏳ WEEK 3: Remediation

- [ ] AutoFixer (nulls, duplicates, formats, types)
- [ ] StrategyGenerator
- [ ] RuleBuilder
- [ ] 80% of problems fixable automatically

### ⏳ WEEK 4: Prevention

- [ ] RuleLearner
- [ ] ContinuousMonitor
- [ ] EffectivenessTracker
- [ ] System learns over time

### ⏳ WEEKS 5-6: Integration

- [ ] dbt integration
- [ ] Airflow integration
- [ ] Kafka streaming
- [ ] Data lake support

### ⏳ WEEKS 7-8: Testing + Optimization

- [ ] 80%+ coverage
- [ ] Performance tests
- [ ] Parallel processing
- [ ] Caching layer

### ⏳ WEEKS 9-10: Launch Prep

- [ ] Documentation
- [ ] Beta program
- [ ] First 100 users

---

## 🧪 Testing Strategy

### Phase 1 Tests (KEEP - 46 tests)
```
tests/quality/
├── test_profiler.py      ✅ Working
├── test_validators.py    ✅ Working
├── test_loader.py        ✅ Working
└── test_rules.py         ✅ Working
```

### Week 1 Tests (NEW - 11 tests)
```
tests/intelligence/
├── test_isolator.py      ✅ DONE (11 tests)
├── test_learner.py       ⏳ TO BUILD
├── test_expander.py      ⏳ TO BUILD
├── test_contextualizer.py ⏳ TO BUILD
└── test_analyzer.py      ⏳ TO BUILD
```

### Test Each Detector
```
tests/quality/drift/
├── test_completeness.py  ✅ DONE (8 tests)
├── test_mean_shift.py    ⏳ TO BUILD
├── test_outlier_drift.py ⏳ TO BUILD
└── ... (6 more)
```

### Running Tests
```bash
# Phase 1 only (verify nothing broke)
pytest tests/quality/ -v

# Week 1 only
pytest tests/intelligence/ -v

# All tests
pytest tests/ -v --cov=pulsar

# Specific test
pytest tests/intelligence/test_isolator.py::TestIsolator::test_random_sampling -v
```

---

## 💻 Code Patterns

### Pattern 1: Detectors (All detectors follow this)

```python
from typing import Any, Dict, Optional
from ..base import DriftDetector, DriftResult, Severity

class MyDetector(DriftDetector):
    """Detect my specific metric"""
    
    def is_applicable(self, baseline, current) -> bool:
        """Can we use this detector?"""
        return 'my_metric' in baseline and 'my_metric' in current
    
    def detect(self, col_name, baseline, current) -> Optional[DriftResult]:
        """Detect drift"""
        if not self.should_detect(baseline, current):
            return None
        
        baseline_val = baseline['my_metric']
        current_val = current['my_metric']
        delta = current_val - baseline_val
        
        if abs(delta) < self.threshold:
            return None
        
        score = abs(delta)
        severity = self.calculate_severity(score)
        insight = self._generate_insight(col_name, delta)
        
        return DriftResult(
            metric_name='my_metric',
            column_name=col_name,
            drift_detected=True,
            score=score,
            severity=severity,
            baseline_value=baseline_val,
            current_value=current_val,
            delta=delta,
            insight=insight,
        )
    
    def _generate_insight(self, col_name, delta) -> str:
        """Human-readable insight"""
        if delta > 0:
            return f"📈 '{col_name}' increased by {delta:.2f}"
        else:
            return f"📉 '{col_name}' decreased by {abs(delta):.2f}"
```

### Pattern 2: Tests (All modules follow this)

```python
import pytest
import polars as pl
from pulsar.core.intelligence.small_world.my_module import MyClass

class TestMyClass:
    """Test MyClass"""
    
    @pytest.fixture
    def sample_data(self):
        """Create test data"""
        return pl.DataFrame({'x': range(1000)})
    
    def test_basic_functionality(self, sample_data):
        """Test basic case"""
        obj = MyClass()
        result = obj.process(sample_data)
        assert result is not None
    
    def test_edge_case(self, sample_data):
        """Test edge case"""
        obj = MyClass({'config': 'value'})
        result = obj.process(sample_data)
        assert len(result) > 0
```

### Pattern 3: Configuration (YAML)

```yaml
# Always has this structure
detectors/modules:
  my_detector:
    enabled: true
    threshold: 0.05
    weight: 1.0
    severity_levels:
      critical: 0.5
      high: 0.2
      moderate: 0.1
      low: 0.05
```

---

## 📝 Important Files to Know

### Phase 1 (DO NOT MODIFY)
- `pulsar/core/profiling/profiler.py` - Creates profiles
- `pulsar/core/profiling/metrics.py` - Calculates metrics
- `pulsar/core/quality/validators.py` - Validates data
- `tests/quality/*.py` - 46 passing tests

### Phase 2.1 (Foundation - Read Only)
- `pulsar/core/quality/drift/base.py` - Base classes
- `pulsar/core/quality/drift/registry.py` - Plugin system
- `pulsar/config/drift_detection.yaml` - Detector config

### Week 1 (Building Now)
- `pulsar/core/intelligence/small_world/isolator.py` ✅ DONE
- `pulsar/core/intelligence/small_world/learner.py` ⏳ TO BUILD
- `pulsar/core/intelligence/small_world/expander.py` ⏳ TO BUILD
- `pulsar/core/intelligence/small_world/contextualizer.py` ⏳ TO BUILD
- `pulsar/core/intelligence/small_world/analyzer.py` ⏳ TO BUILD

---

## 🔑 Key Concepts

### Small World Framework
1. **Isolate:** Sample 10% deeply (remove noise)
2. **Learn:** Discover patterns from sample
3. **Expand:** Validate against full dataset layer-by-layer
4. **Filter:** Remove contradictions progressively
5. **Output:** Clean, coherent dataset with context

### Why It's Better Than Traditional Quality Tools
```
Traditional: Try to understand 1M rows at once
            → Slow, noisy, missed patterns

Pulsar:     Understand 10K rows deeply first
           → Fast, clear, accurate patterns
           → Then expand with confidence
```

### Context-Aware Quality
```
Not: "Is email valid?" (yes/no)
But: "For this B2B lead database, is email valid?" (context matters)

Different goals = Different quality standards:
- Acquisition: 95%+ confidence (high bar)
- Segmentation: 80%+ confidence (medium bar)
- ML: 99%+ confidence (very high bar)
```

---

## 🎯 This Week's Goal

**By end of Week 1:**
- ✅ Isolator working (DONE)
- ⏳ Learner working
- ⏳ Expander working
- ⏳ Contextualizer working
- ⏳ Analyzer working
- ⏳ 50+ tests passing
- ⏳ Small World end-to-end working

**Then can answer:** "What IS this data?"

---

## 🚀 How to Continue

### For Learner Module (Next: Week 1 Day 3)

```python
# Location: pulsar/core/intelligence/small_world/learner.py

class Learner:
    """Discover patterns from sample"""
    
    def __init__(self, config: Optional[Dict[str, Any]] = None):
        self.config = config or {}
    
    def discover_formats(self, sample: pl.DataFrame) -> Dict[str, Dict]:
        """
        Discover string formats in sample
        
        Returns:
        {
            'email': {
                'format': 'regex',
                'pattern': r'^[^@]+@[^@]+\.[^@]+$',
                'match_ratio': 0.98,
                'examples': ['user@example.com', ...],
            },
            'phone': {...},
            ...
        }
        """
        pass
    
    def discover_ranges(self, sample: pl.DataFrame) -> Dict[str, Dict]:
        """
        Discover numeric ranges
        
        Returns:
        {
            'age': {
                'min': 18,
                'max': 95,
                'mean': 42.5,
                'median': 40,
                'std': 15.2,
            },
            ...
        }
        """
        pass
    
    # ... more methods

# Tests: tests/intelligence/test_learner.py
```

---

## 🎓 Claude Code Can Help With

### Immediate (Week 1)
- [ ] Complete Learner (use Pulsar's metrics.py)
- [ ] Complete Expander (layer-by-layer validation)
- [ ] Complete Contextualizer (build rules)
- [ ] Complete Analyzer (understand data)
- [ ] Write tests for each
- [ ] Integration tests

### Short-term (Week 2)
- [ ] Build 8 more detectors (follow Completeness pattern)
- [ ] Build Diagnosis layer
- [ ] LLM integration (Gemma)

### Medium-term (Week 3-4)
- [ ] Remediation layer
- [ ] Prevention layer
- [ ] Full end-to-end testing

---

## 📞 How to Ask Claude Code

**Good:**
```
"Build Learner class following the Isolator pattern.
Use pulsar.core.profiling.metrics to calculate statistics.
Follow test pattern in test_isolator.py.
Docstrings should explain what patterns are discovered."
```

**Better:**
```
"Complete learner.py for Week 1 Day 3.

Module: pulsar/core/intelligence/small_world/learner.py
Pattern: Follow Isolator class structure
Use: metrics.py for calculations
Output: {
  'formats': {...},
  'ranges': {...},
  'distributions': {...},
  'relationships': {...},
}
Tests: 12+ tests in test_learner.py
Reference: See Isolator for code style and patterns"
```

---

## ✅ Checklist for Week 1 Completion

- [x] Isolator module (DONE)
- [x] Isolator tests (11 tests)
- [ ] Learner module
- [ ] Learner tests
- [ ] Expander module
- [ ] Expander tests
- [ ] Contextualizer module
- [ ] Contextualizer tests
- [ ] Analyzer module
- [ ] Analyzer tests
- [ ] Integration tests
- [ ] All 50+ tests passing
- [ ] Phase 1 (46 tests) still passing
- [ ] Ready for Week 2

---

## 🔗 Key Files Reference

| File | Lines | Status | Purpose |
|------|-------|--------|---------|
| isolator.py | 270 | ✅ DONE | Sample extraction |
| learner.py | ⏳ | TO BUILD | Pattern discovery |
| expander.py | ⏳ | TO BUILD | Layer validation |
| contextualizer.py | ⏳ | TO BUILD | Rule extraction |
| analyzer.py | ⏳ | TO BUILD | Data understanding |
| drift/base.py | 299 | ✅ DONE | Base interfaces |
| drift/registry.py | 342 | ✅ DONE | Plugin system |
| completeness.py | 162 | ✅ DONE | Example detector |
| test_isolator.py | 250 | ✅ DONE | 11 tests |
| test_learner.py | ⏳ | TO BUILD | 12+ tests |
| drift_detection.yaml | 187 | ✅ DONE | Config |

---

**This context file should be enough for Claude Code to continue building Pulsar 2.0 intelligently.** 🚀

Use it as reference when asking Claude Code to build modules, and it will understand:
- Architecture
- Patterns
- Testing approach
- Week-by-week goals
- Code style
- What's done vs. what needs building