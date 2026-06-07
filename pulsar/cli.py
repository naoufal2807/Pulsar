# pulsar/cli.py

import typer
from pathlib import Path
import json
from typing import Optional, Dict, Any
import uuid
import polars as pl

from pulsar.logging_config import setup_logging, get_logger
from pulsar.core.ingestion.loader import load
from pulsar.core.quality.loader import load_rules_yaml
from pulsar.core.quality.validator import Validator
from pulsar.output.formatter import format_validation_output

app = typer.Typer(help="Pulsar - Data Quality CLI")
logger = get_logger("pulsar.cli")


def _format_profile_text(profile: Dict[str, Any], verbose: bool = False) -> str:
    """Format profile as text table."""
    lines = []
    lines.append("\n" + "="*80)
    lines.append(f"📊 Dataset: {profile['dataset_name']}")
    lines.append(f"   {profile['row_count']:,} rows | {profile['column_count']} columns")
    lines.append("="*80)
    
    for col_name, col_data in profile["columns"].items():
        lines.append(f"\nColumn: {col_name} ({col_data['dtype']})")
        lines.append(f"├─ Completeness: {col_data['completeness']*100:.1f}% ({col_data['non_null_count']}/{profile['row_count']})")
        lines.append(f"├─ Uniqueness: {col_data['uniqueness']*100:.1f}% ({col_data['distinct_count']} distinct)")
        
        # Numeric stats
        if "numeric_stats" in col_data:
            stats = col_data["numeric_stats"]
            if not stats.get("error"):
                lines.append(f"├─ Distribution: Min {stats.get('min')} | Max {stats.get('max')} | Mean {stats.get('mean'):.2f}")
                if verbose and "skewness" in stats:
                    lines.append(f"├─ Skewness: {stats['skewness']:.2f}")
                    lines.append(f"├─ Kurtosis: {stats['kurtosis']:.2f}")
                    if "outliers" in stats:
                        lines.append(f"├─ Outliers (IQR): {stats['outliers'].get('iqr_method', 0)}")
                lines.append(f"└─ Percentiles: P25: {stats.get('p25')} | P50: {stats.get('p50')} | P75: {stats.get('p75')}")
        
        # DateTime stats
        elif "datetime_stats" in col_data:
            stats = col_data["datetime_stats"]
            lines.append(f"├─ Min: {stats.get('min')}")
            lines.append(f"└─ Max: {stats.get('max')}")
        
        # Categorical stats
        elif "categorical_stats" in col_data:
            stats = col_data["categorical_stats"]
            if stats.get("top_k"):
                lines.append(f"├─ Top values:")
                for item in stats["top_k"][:5]:
                    lines.append(f"│  • {item['value']}: {item['count']}")
            if verbose and "string_patterns" in stats:
                patterns = stats["string_patterns"]
                lines.append(f"├─ Patterns:")
                for pattern, count in list(patterns.items())[:3]:
                    lines.append(f"│  • {pattern}: {count}")
            lines.append(f"└─ Samples: {', '.join(col_data['sample_values'][:3])}")
    
    lines.append("\n" + "="*80 + "\n")
    return "\n".join(lines)


@app.command()
def profile(
    file: str = typer.Argument(..., help="Path to data file (CSV/Parquet)"),
    columns: Optional[str] = typer.Option(None, help="Columns to profile (comma-separated)"),
    output: str = typer.Option("text", help="Output format: text, json, csv"),
    verbose: bool = typer.Option(False, help="Show advanced metrics"),
    log_file: Optional[str] = typer.Option(None, help="Log file path"),
):
    """Profile a dataset."""
    log_path = setup_logging(log_file or "logs")
    logger.info(f"Profile command: file={file}, output={output}, verbose={verbose}")
    
    try:
        from pulsar.core.profiling.profiler import profile_dataset
        
        lf = load(file)
        logger.info(f"File loaded: {file}")
        
        # Profile the dataset
        profile_data = profile_dataset(lf, path=file, detailed=verbose)
        logger.info(f"Dataset profiled: {profile_data['row_count']} rows, {profile_data['column_count']} columns")
        
        # Filter columns if specified
        if columns:
            col_list = [c.strip() for c in columns.split(",")]
            profile_data["columns"] = {k: v for k, v in profile_data["columns"].items() if k in col_list}
            logger.info(f"Filtered to {len(profile_data['columns'])} columns")
        
        # Format output
        if output == "json":
            print(json.dumps(profile_data, indent=2, default=str))
        else:  # text
            formatted = _format_profile_text(profile_data, verbose=verbose)
            print(formatted)
        
    except FileNotFoundError as e:
        logger.error(f"File not found: {e}")
        typer.echo(f"❌ Error: {e}", err=True)
        raise typer.Exit(code=1)
    except ValueError as e:
        logger.error(f"Profile error: {e}")
        typer.echo(f"❌ Error: {e}", err=True)
        raise typer.Exit(code=1)
    except Exception as e:
        logger.error(f"Unexpected error: {e}", exc_info=True)
        typer.echo(f"❌ Error: {e}", err=True)
        raise typer.Exit(code=1)


@app.command()
def validate(
    file: str = typer.Argument(..., help="Path to data file (CSV/Parquet)"),
    rules: str = typer.Option(..., help="Path to rules YAML file"),
    output: str = typer.Option("text", help="Output format: text, json"),
    verbose: bool = typer.Option(False, help="Verbose output with details"),
    columns: Optional[str] = typer.Option(None, help="Columns to validate (comma-separated)"),
    log_file: Optional[str] = typer.Option(None, help="Log file path"),
):
    """Validate a dataset against rules."""
    log_path = setup_logging(log_file or "logs")
    logger.info(f"Validate command: file={file}, rules={rules}, output={output}")
    
    try:
        # Load data
        logger.debug(f"Loading file: {file}")
        lf = load(file)
        logger.info(f"File loaded: {file}")
        
        # Load rules
        logger.debug(f"Loading rules: {rules}")
        rules_list = load_rules_yaml(rules)
        logger.info(f"Loaded {len(rules_list)} rules")
        
        if not rules_list:
            logger.warning("No rules to validate")
            print("⚠️  No rules defined")
            return
        
        # Filter columns if specified
        if columns:
            col_list = [c.strip() for c in columns.split(",")]
            rules_list = [r for r in rules_list if r.column in col_list]
            logger.info(f"Filtered to {len(rules_list)} rules for columns: {col_list}")
        
        # Run validation
        logger.debug("Starting validation")
        validator = Validator()
        results = validator.validate(lf, rules_list)
        logger.info("Validation complete")
        
        # Format output
        formatted = format_validation_output(results, output=output, verbose=verbose)
        print(formatted)
        
        # Log summary
        passed = sum(1 for r in results.values() if r.get("status") == "PASS")
        total = len(results)
        logger.info(f"Validation summary: {passed}/{total} rules passed")
        
    except FileNotFoundError as e:
        logger.error(f"File not found: {e}")
        typer.echo(f"❌ Error: {e}", err=True)
        raise typer.Exit(code=1)
    except ValueError as e:
        logger.error(f"Validation error: {e}")
        typer.echo(f"❌ Error: {e}", err=True)
        raise typer.Exit(code=1)
    except Exception as e:
        logger.error(f"Unexpected error: {e}", exc_info=True)
        typer.echo(f"❌ Error: {e}", err=True)
        raise typer.Exit(code=1)

# Baeline command for drift detection (03-05-2026)
@app.command()
def baseline(
    file: str = typer.Argument(..., help="Path to data file (CSV/Parquet)"),
    save: bool = typer.Option(False, "--save", help="Create baseline snapshot"),
    compare: Optional[str] = typer.Option(None, "--compare", help="Compare to baseline file"),
    output: Optional[str] = typer.Option(None, "--output", help="Output path for baseline/drift JSON"),
    log_file: Optional[str] = typer.Option(None, help="Log file path"),
):
    """Create baseline or detect drift."""
    log_path = setup_logging(log_file or "logs")
    session_id = str(uuid.uuid4())[:8]
    
    logger.info(f"Baseline command: file={file}, save={save}, compare={compare}")
    
    try:
        from pulsar.core.quality.baseline import BaselineManager
        
        lf = load(file)
        dataset_name = Path(file).stem
        logger.info(f"File loaded: {file}")
        
        manager = BaselineManager()
        
        if save:
            # CREATE BASELINE
            baseline_path = manager.create_baseline(
                lf=lf,
                dataset_name=dataset_name,
                file_path=file,
                session_id=session_id,
                output_path=output,
            )
            print(f"\n✅ Baseline created: {baseline_path}")
            print(f"   Session: {session_id}\n")
        
        elif compare:
            # COMPARE TO BASELINE
            drift_report = manager.compare_to_baseline(
                lf=lf,
                dataset_name=dataset_name,
                file_path=file,
                baseline_path=compare,
                session_id=session_id,
                output_path=output,
            )
            
            # Display summary
            summary = drift_report["summary"]
            print(f"\n{'='*80}")
            print(f"DRIFT DETECTION REPORT")
            print(f"{'='*80}")
            print(f"Dataset: {dataset_name}")
            print(f"Baseline: {compare}")
            print(f"Drift Score: {summary['overall_drift_score']:.3f} ({summary['severity']})")
            print(f"Columns with Drift: {summary['columns_with_drift']}/{summary['total_columns']} ({summary['drift_percentage']:.1f}%)")
            
            if summary['new_columns']:
                print(f"New Columns: {', '.join(summary['new_columns'])}")
            if summary['deleted_columns']:
                print(f"Deleted Columns: {', '.join(summary['deleted_columns'])}")
            if summary['type_changes']:
                print(f"Type Changes: {len(summary['type_changes'])} column(s)")
            
            print(f"{'='*80}")
            
            # Show high drift columns
            high_drift_cols = [
                (name, data) for name, data in drift_report["column_drift"].items()
                if data["drift_detected"] and data.get("severity") in ["HIGH", "CRITICAL"]
            ]
            
            if high_drift_cols:
                print(f"\n🚨 HIGH/CRITICAL DRIFT COLUMNS:")
                for col_name, col_data in high_drift_cols:
                    print(f"\n  {col_name} ({col_data['severity']}) - Score: {col_data['drift_score']:.3f}")
                    for alert in col_data.get("alerts", []):
                        print(f"    • {alert}")
            
            print(f"\n📄 Full report: {drift_report['metadata']['comparison_timestamp'].split('T')[0]}")
            print(f"   Saved to drift_reports/")
            print(f"   Session: {session_id}\n")
        
        else:
            print("❌ Error: Must specify --save or --compare")
            logger.error("No action specified (--save or --compare required)")
            raise typer.Exit(code=1)
    
    except FileNotFoundError as e:
        logger.error(f"File not found: {e}")
        typer.echo(f"❌ Error: {e}", err=True)
        raise typer.Exit(code=1)
    except ValueError as e:
        logger.error(f"Baseline error: {e}")
        typer.echo(f"❌ Error: {e}", err=True)
        raise typer.Exit(code=1)
    except Exception as e:
        logger.error(f"Unexpected error: {e}", exc_info=True)
        typer.echo(f"❌ Error: {e}", err=True)
        raise typer.Exit(code=1)


@app.command()
def infer(
    file: str = typer.Argument(..., help="Path to data file (CSV/Parquet)"),
    output: str = typer.Option("markdown", help="Output format: markdown, json"),
    save: bool = typer.Option(False, "--save", help="Save report to file"),
    log_file: Optional[str] = typer.Option(None, help="Log file path"),
):
    """Infer intelligence about your data using Small World Framework + Agent reasoning."""
    log_path = setup_logging(log_file or "logs")
    logger.info(f"Infer command: file={file}, output={output}")

    try:
        from pulsar.core.intelligence.small_world.isolator import Isolator
        from pulsar.core.intelligence.small_world.learner import Learner
        from pulsar.core.intelligence.small_world.intelligence_generator import IntelligenceGenerator
        from pulsar.core.intelligence.small_world.report_generator import IntelligenceReportGenerator
        from pulsar.core.intelligence.agent import Agent

        # Load data
        lf = load(file)
        df = lf.collect()
        dataset_name = Path(file).stem
        logger.info(f"File loaded: {file}")

        # Initialize Agent for LLM-powered reasoning
        logger.debug("Initializing reasoning agent")
        agent = Agent()
        logger.info(f"Agent initialized - Provider available: {agent.provider_available}")

        # Small World Framework: Learn in isolation
        logger.debug("Starting Small World Framework")
        isolator = Isolator()
        sample = isolator.extract(df, strategy='random')
        logger.debug(f"Isolated sample: {len(sample)} rows")

        learner = Learner()
        patterns = learner.learn(sample)
        logger.debug("Patterns discovered")

        # Generate intelligence
        intel_gen = IntelligenceGenerator(df, patterns)
        intelligence = intel_gen.generate_intelligence()
        logger.info("Intelligence generated")

        # Use Agent for deep reasoning
        logger.debug("Requesting agent reasoning about patterns")

        # Format entities and metrics for the agent
        entities_list = []
        for col, values in intelligence['entities'].items():
            if values:
                entities_list.append(f"{col}: {', '.join(str(v) for v in values[:2])}")
        entities_str = "; ".join(entities_list[:3]) if entities_list else "various"

        metrics_list = list(intelligence['key_metrics'].keys())[:3] if intelligence.get('key_metrics') else []
        metrics_str = ", ".join(metrics_list) if metrics_list else "various metrics"

        # Let agent reason about domain and key patterns
        domain_analysis = agent.think(
            f"Analyze this data: Domain={intelligence['domain']}, "
            f"Key entities={entities_str}, "
            f"Key metrics={metrics_str}. "
            f"What does this data represent and what insights can we draw?"
        )
        logger.info(f"Agent reasoning complete - Response: {domain_analysis[:200] if domain_analysis else 'empty'}")

        # Generate deep analysis with agent insights
        from pulsar.core.intelligence.small_world.deep_analyzer import DeepAnalyzer

        deep_analyzer = DeepAnalyzer(df, patterns, intelligence)
        deep_analysis = {
            'concentration': deep_analyzer.analyze_concentration(),
            'distribution': deep_analyzer.analyze_distribution_skewness(),
            'variability': deep_analyzer.analyze_variability(),
            'relationships': deep_analyzer.analyze_relationships(),
            'risks': deep_analyzer.analyze_data_quality_risks(),
            'agent_insights': domain_analysis,
        }
        logger.info("Deep analysis + agent reasoning complete")

        # Export session before clearing memory
        session_export = agent.export_session()
        agent.clear_memory()
        logger.info(f"Agent session exported and memory cleared")

        # Format output
        if output == "json":
            # Convert to JSON-serializable format
            intel_json = {
                'domain': intelligence['domain'],
                'entities': intelligence['entities'],
                'key_metrics': intelligence['key_metrics'],
                'top_performers': intelligence['top_performers'],
                'patterns_discovered': intelligence['patterns_discovered'],
                'outliers_meaning': intelligence['outliers_meaning'],
                'concentration': intelligence['concentration'],
                'summary': intelligence['summary_statement'],
                'agent_insights': domain_analysis,
                'agent_message_count': session_export['total_messages'],
            }
            report_text = json.dumps(intel_json, indent=2, default=str)
        else:  # markdown
            report_gen = IntelligenceReportGenerator(
                dataset_name, intelligence, df=df, patterns=patterns, deep_analysis=deep_analysis
            )
            report_text = report_gen.generate_report()

        # Output
        if save:
            report_path = Path(f"intelligence_reports/{dataset_name}_intelligence.md")
            report_path.parent.mkdir(parents=True, exist_ok=True)
            report_path.write_text(report_text)
            print(f"\n[OK] Intelligence report saved: {report_path}\n")
            logger.info(f"Report saved: {report_path}")
        else:
            print(report_text)

    except FileNotFoundError as e:
        logger.error(f"File not found: {e}")
        typer.echo(f"[ERROR] {e}", err=True)
        raise typer.Exit(code=1)
    except Exception as e:
        logger.error(f"Inference error: {e}", exc_info=True)
        typer.echo(f"[ERROR] {e}", err=True)
        raise typer.Exit(code=1)


if __name__ == "__main__":
    app()