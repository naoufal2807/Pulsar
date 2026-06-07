#!/usr/bin/env python
"""Quick analysis of 08_EWC2025_Country_Results.csv using all 4 Pulsar layers."""

import polars as pl
import json
from pathlib import Path

# ============================================================================
# LAYER 1: INTELLIGENCE (Understanding)
# ============================================================================
print("\n" + "="*80)
print("LAYER 1: UNDERSTANDING (Intelligence)")
print("="*80)

from pulsar.core.intelligence.small_world.isolator import Isolator
from pulsar.core.intelligence.small_world.learner import Learner
from pulsar.core.intelligence.small_world.expander import Expander
from pulsar.core.intelligence.small_world.contextualizer import Contextualizer
from pulsar.core.intelligence.small_world.analyzer import Analyzer

# Load data
file_path = "08_EWC2025_Country_Results.csv"
df = pl.read_csv(file_path)

print(f"\n[Dataset] {Path(file_path).stem}")
print(f"   Rows: {df.shape[0]:,} | Columns: {df.shape[1]}")
print(f"   Columns: {', '.join(df.columns)}")

# Sample data (10%)
isolator = Isolator()
sample = isolator.sample(df, strategy='random', sample_size=0.1)
print(f"\n[OK] Sampled {len(sample):,} rows ({100*len(sample)/len(df):.1f}%)")

# Learn patterns
learner = Learner()
patterns = learner.learn(sample)
print(f"[OK] Discovered patterns:")
print(f"  - Formats: {list(patterns.get('formats', {}).keys())}")
print(f"  - Ranges: {list(patterns.get('ranges', {}).keys())}")
print(f"  - Distributions: {list(patterns.get('distributions', {}).keys())}")

# Expand validation to full dataset
expander = Expander()
expanded = expander.expand(df, patterns)
print(f"[OK] Validated full dataset: {expanded.shape[0]:,} rows")

# Contextualize
contextualizer = Contextualizer()
context = contextualizer.infer_context(df, patterns)
print(f"[OK] Inferred context: {context}")

# Analyze quality
analyzer = Analyzer()
analysis = analyzer.analyze(df, patterns)
print(f"\n[QUALITY SCORE] {analysis['quality_score']:.2%}")
print(f"   Strengths: {analysis['strengths'][:2] if analysis['strengths'] else 'N/A'}")
print(f"   Weaknesses: {analysis['weaknesses'][:2] if analysis['weaknesses'] else 'N/A'}")

# ============================================================================
# LAYER 2: DIAGNOSIS (Root Cause Analysis)
# ============================================================================
print("\n" + "="*80)
print("LAYER 2: DIAGNOSIS (Root Cause Analysis)")
print("="*80)

from pulsar.core.diagnosis.anomaly_detector import AnomalyDetector
from pulsar.core.diagnosis.root_cause_analyzer import RootCauseAnalyzer
from pulsar.core.diagnosis.impact_assessor import ImpactAssessor

# Detect anomalies
detector = AnomalyDetector()
anomalies = detector.detect(df, patterns)
print(f"\n[ANOMALIES] Detected: {len(anomalies)}")
for i, anomaly in enumerate(anomalies[:5], 1):
    print(f"   {i}. {anomaly['type']} in {anomaly['column']} (severity: {anomaly['severity']}/4)")

# Analyze root causes
if anomalies:
    root_cause_analyzer = RootCauseAnalyzer()
    print(f"\n[ROOT CAUSES]")
    for anomaly in anomalies[:3]:
        cause = root_cause_analyzer.analyze(df, anomaly)
        print(f"   - {anomaly['column']}: {cause['root_cause']}")

# Assess impact
if anomalies:
    impact_assessor = ImpactAssessor()
    impacts = impact_assessor.assess(anomalies, df)
    print(f"\n[IMPACT ASSESSMENT]")
    for i, impact in enumerate(impacts[:3], 1):
        print(f"   {i}. {impact['name']}: {impact['impact']:.1%} (Score: {impact['impact_score']:.2f})")

# ============================================================================
# LAYER 3: REMEDIATION (Auto-Fix)
# ============================================================================
print("\n" + "="*80)
print("LAYER 3: REMEDIATION (Auto-Fix)")
print("="*80)

from pulsar.core.remediation.strategy_generator import StrategyGenerator
from pulsar.core.remediation.auto_fixer import AutoFixer
from pulsar.core.remediation.rule_builder import RuleBuilder

if anomalies:
    generator = StrategyGenerator()
    fixer = AutoFixer()

    print(f"\n[FIX STRATEGIES]")
    fixed_df = df.clone()

    for anomaly in anomalies[:3]:
        strategies = generator.generate_strategy(
            anomaly['type'],
            anomaly['column'],
            {'dtype': str(df[anomaly['column']].dtype)},
            context
        )

        if strategies:
            best_strategy = strategies[0]
            print(f"   - {anomaly['column']}: {best_strategy.action.value} "
                  f"(confidence: {best_strategy.confidence:.1%})")

            # Apply fix
            fixed_df, result = fixer.apply_fix(fixed_df, best_strategy)
            if result.success:
                print(f"     [OK] Applied (affected {result.rows_affected} rows)")

# Build reusable rules
print(f"\n[REUSABLE RULES]")
rule_builder = RuleBuilder()
for col_name in df.columns[:3]:
    rule = rule_builder.build_null_rule(col_name, 'DELETE_ROWS', {})
    print(f"   [OK] Built rule: {rule.name}")

# ============================================================================
# LAYER 4: PREVENTION (Continuous Learning)
# ============================================================================
print("\n" + "="*80)
print("LAYER 4: PREVENTION (Continuous Learning)")
print("="*80)

from pulsar.core.prevention.rule_learner import RuleLearner
from pulsar.core.prevention.pattern_monitor import PatternMonitor
from pulsar.core.prevention.rule_refiner import RuleRefiner

# Learn preventive rules
learner_prevention = RuleLearner()
preventive_rules = learner_prevention.learn_rules(df, patterns, context)
print(f"\n📚 Learned {len(preventive_rules)} preventive rules:")
for rule in preventive_rules[:5]:
    print(f"   - {rule.name}: {rule.rule_type} (confidence: {rule.confidence:.1%})")

# Monitor patterns
monitor = PatternMonitor()
for col_name in df.columns[:3]:
    col_data = patterns.get('ranges', {}).get(col_name, {})
    if col_data.get('type') == 'numeric':
        monitor.record_pattern(col_name, 'mean', col_data.get('mean', 0))

print(f"\n👁️  Pattern monitoring: {len(monitor.pattern_history)} patterns tracked")

# Rule refinement
refiner = RuleRefiner()
for rule in preventive_rules[:3]:
    refiner.track_rule_performance(
        rule.name,
        rule.rule_type,
        triggered=True,
        prevented_issue=True
    )

metrics = refiner.get_all_metrics()
print(f"📊 Rule effectiveness metrics: {len(metrics)} rules tracked")

# ============================================================================
# SUMMARY
# ============================================================================
print("\n" + "="*80)
print("SUMMARY")
print("="*80)
print(f"""
✅ Analysis Complete!

Findings:
  • Quality Score: {analysis['quality_score']:.2%}
  • Anomalies Found: {len(anomalies)}
  • Strategies Generated: {min(len(anomalies), 3)}
  • Preventive Rules Learned: {len(preventive_rules)}
  • Patterns Monitored: {len(monitor.pattern_history)}

Next Steps:
  1. Review detailed profiles: pulsar profile {file_path} --verbose
  2. Create baseline: pulsar baseline {file_path} --save
  3. Schedule drift detection: Compare against baseline periodically
  4. Deploy learned rules: Use rule_builder.export_rules() to save
""")
print("="*80 + "\n")
