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
from pulsar.core.presentation import JourneyPresenter
from pulsar.core.intelligence.run_persistence import RunPersistence
from pulsar.core.journey_orchestrator import Journey
from pulsar.core.intelligence.shared_state_store import SharedStateStore
from pulsar.core.intelligence.stage_agents import AceAgent

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


@app.command()
def journey(
    file: Optional[str] = typer.Argument(None, help="Path to data file (CSV/Parquet)"),
    stage: Optional[str] = typer.Option(None, help="Start at specific stage: scout, explorer, detective, analyst, ace"),
    full: bool = typer.Option(False, "--full", help="Run complete journey from SCOUT to ACE"),
    save: bool = typer.Option(False, "--save", help="Save journey report to file"),
    resume: Optional[str] = typer.Option(None, "--resume", help="Resume from checkpoint (run_id)"),
    list_runs: bool = typer.Option(False, "--list-runs", help="List all saved journey runs"),
    log_file: Optional[str] = typer.Option(None, help="Log file path"),
):
    """Explore your data on a guided journey from Zero to Ace understanding."""

    # Handle --list-runs flag
    if list_runs:
        persistence = RunPersistence()
        runs = persistence.list_runs()

        if not runs:
            print("\n[INFO] No saved runs found\n")
            return

        print("\n" + "="*100)
        print("SAVED JOURNEY RUNS")
        print("="*100 + "\n")
        print(f"{'Dataset':<15} {'Run ID':<20} {'Status':<15} {'Progress':<12} {'Last Updated':<25} {'Stages':<30}")
        print("-"*100)

        for run in runs:
            stages_str = ", ".join(run['completed_stages']).upper()
            print(f"{run['dataset']:<15} {run['run_id']:<20} {run['status']:<15} {run['progress']}%{'':<9} {run['start_time']:<25} {stages_str:<30}")

        print("\n" + "="*100)
        print("To resume a run: uv run pulsar.cli journey DATA.csv --resume <run_id>\n")
        return
    log_path = setup_logging(log_file or "logs")
    logger.info(f"Journey command: file={file}, stage={stage}, full={full}, resume={resume}")

    try:
        from pulsar.core.intelligence.journey import DataExplorationJourney, Stage

        # Handle resume mode
        if resume:
            if not file:
                typer.echo("[ERROR] Must provide data file when resuming", err=True)
                raise typer.Exit(code=1)

            # Verify run exists
            persistence = RunPersistence()
            dataset_name = Path(file).stem
            metadata = persistence.get_run_metadata(dataset_name, resume)

            if not metadata:
                typer.echo(f"[ERROR] Run not found: {dataset_name}/{resume}", err=True)
                raise typer.Exit(code=1)

            logger.info(f"Resuming run: {dataset_name}/{resume}, completed stages: {metadata.completed_stages}")
            print(f"\n[RESUME] Resuming journey '{dataset_name}' from run {resume}")
            print(f"[RESUME] Completed stages: {', '.join(metadata.completed_stages).upper()}")
            print(f"[RESUME] Progress: {len(metadata.completed_stages) * 20}%\n")

        # Load data
        lf = load(file)
        df = lf.collect()
        dataset_name = Path(file).stem
        logger.info(f"File loaded: {file}")

        # Initialize journey (with resume support)
        journey_obj = DataExplorationJourney(df, dataset_name, run_id=resume)
        logger.info(f"Journey initialized for {dataset_name}")

        # Run appropriate stage
        if full:
            # Run complete journey
            logger.info("Running complete journey SCOUT → ACE")
            result = journey_obj.run_complete_journey()
        elif stage:
            # Run specific stage
            stage_map = {
                'scout': journey_obj.start_journey,
                'explorer': journey_obj.explore_stage,
                'detective': journey_obj.detective_stage,
                'analyst': journey_obj.analyst_stage,
                'ace': journey_obj.ace_stage,
            }

            if stage.lower() not in stage_map:
                typer.echo(
                    f"[ERROR] Unknown stage: {stage}. "
                    f"Use: scout, explorer, detective, analyst, or ace",
                    err=True
                )
                raise typer.Exit(code=1)

            logger.info(f"Running stage: {stage}")
            result = stage_map[stage.lower()]()
        else:
            # Default: start at SCOUT
            logger.info("Starting journey at SCOUT stage")
            result = journey_obj.start_journey()

        # Output result
        print(result)

        # Save if requested
        if save:
            report_path = Path(f"journey_reports/{dataset_name}_journey.md")
            report_path.parent.mkdir(parents=True, exist_ok=True)
            report_path.write_text(result)
            print(f"\n[OK] Journey report saved: {report_path}")
            logger.info(f"Report saved: {report_path}")

        # Show summary using presenter
        summary = journey_obj.get_journey_summary()
        presenter = JourneyPresenter(dataset_name)
        summary_output = presenter.present_summary(
            dataset_name=summary['dataset'],
            rows=summary['total_rows'],
            cols=summary['total_columns'],
            current_stage=summary['current_stage'],
            progress=summary['progress_percent'],
            insights_count=summary['total_insights'],
        )
        print(summary_output)

    except FileNotFoundError as e:
        logger.error(f"File not found: {e}")
        typer.echo(f"[ERROR] {e}", err=True)
        raise typer.Exit(code=1)
    except Exception as e:
        logger.error(f"Journey error: {e}", exc_info=True)
        typer.echo(f"[ERROR] {e}", err=True)
        raise typer.Exit(code=1)


@app.command()
def chat(
    file: Optional[str] = typer.Option(None, help="Path to data file (CSV/Parquet)"),
    skip_journey: bool = typer.Option(False, help="Skip journey and go directly to chat"),
    log_file: Optional[str] = typer.Option(None, help="Log file path"),
):
    """Start interactive chat with agents for data analysis."""
    log_path = setup_logging(log_file or "logs")
    logger.info(f"Chat command: file={file}, skip_journey={skip_journey}")

    df = None
    shared_state = None

    try:
        if not skip_journey and file:
            # Load dataset
            logger.info(f"Loading dataset: {file}")
            try:
                lf = load(file)
                df = lf.collect()
                logger.info(f"Dataset loaded: {df.shape[0]} rows, {df.shape[1]} columns")
            except Exception as e:
                logger.error(f"Failed to load dataset: {e}")
                typer.echo(f"❌ Error loading dataset: {e}", err=True)
                raise typer.Exit(code=1)

            # Run journey
            dataset_name = Path(file).stem
            journey_obj = Journey(
                dataset_name=dataset_name,
                df=df,
            )

            logger.info("Starting journey...")
            try:
                results = journey_obj.execute()
                shared_state = journey_obj.shared_state
                logger.info("Journey completed successfully!")
                print("\n✓ Journey completed successfully!")
                print(f"  Stages: {len(results['results'])} completed")
                print(f"  Memory: {results['shared_state_stats']['total_mb']:.2f} MB")
            except Exception as e:
                logger.error(f"Journey failed: {e}")
                typer.echo(f"❌ Journey error: {e}", err=True)
                raise typer.Exit(code=1)
        else:
            # Create empty shared state for chat-only mode
            shared_state = SharedStateStore()
            logger.info("Chat mode: no journey. Using empty shared state.")

        # Start interactive chat
        logger.info("Starting interactive chat...")
        _start_interactive_chat(shared_state)

    except Exception as e:
        logger.error(f"Chat error: {e}", exc_info=True)
        typer.echo(f"❌ Error: {e}", err=True)
        raise typer.Exit(code=1)


def _start_interactive_chat(shared_state: SharedStateStore):
    """Start the interactive chat loop."""
    agent = AceAgent(
        shared_state=shared_state,
        conversation_id="chat_session",
    )

    print("\n" + "=" * 60)
    print("INTERACTIVE CHAT MODE")
    print("=" * 60)
    print("Type your questions below. Commands:")
    print("  /context - Show current context")
    print("  /keys - List all available keys")
    print("  /help - Show this help")
    print("  /quit - Exit chat")
    print("=" * 60 + "\n")

    while True:
        try:
            user_input = input("You: ").strip()

            if not user_input:
                continue

            if user_input.lower() == "/quit":
                print("\nGoodbye!")
                break
            elif user_input.lower() == "/help":
                print("\nCommands:")
                print("  /context - Show current context")
                print("  /keys - List all available keys")
                print("  /help - Show this help")
                print("  /quit - Exit chat\n")
            elif user_input.lower() == "/context":
                stats = shared_state.get_stats()
                print("\n" + "=" * 60)
                print("SHARED STATE CONTEXT")
                print("=" * 60)
                print(f"Total keys: {stats['total_keys']}")
                print(f"Memory used: {stats['total_mb']:.2f} MB")
                print(f"Total accesses: {stats['total_accesses']}")
                print(f"\nKeys by stage:")
                for stage, count in stats["writes_by_stage"].items():
                    print(f"  {stage}: {count} keys")
                print("=" * 60 + "\n")
            elif user_input.lower() == "/keys":
                keys = shared_state.keys()
                print("\nAvailable keys:")
                for key in sorted(keys):
                    print(f"  - {key}")
                print()
            else:
                # Send message to agent
                print("\nAgent: Thinking...", end="", flush=True)
                response = agent.think(user_input)
                print("\r" + " " * 30, end="\r")  # Clear "Thinking..."
                print(f"Agent: {response}\n")

        except KeyboardInterrupt:
            print("\n\nGoodbye!")
            break
        except Exception as e:
            logger.error(f"Chat error: {e}")
            print(f"Error: {e}\n")


@app.command()
def analyze(
    file: str = typer.Argument(..., help="Path to data file (CSV/Parquet)"),
    mode: str = typer.Option(
        "scout",
        help="Analysis mode: scout, schema, quality, stats, narrator",
    ),
    save: bool = typer.Option(False, "--save", help="Save verdict to file"),
    log_file: Optional[str] = typer.Option(None, help="Log file path"),
):
    """
    Run multi-agent analysis on a dataset.

    --mode scout    : full onboarding (schema+quality+stats → narrator)
    --mode schema   : structural identity only
    --mode quality  : data-quality audit only
    --mode stats    : statistical profiling only
    """
    import asyncio
    import sys

    log_path = setup_logging(log_file or "logs")
    logger.info(f"Analyze command: file={file}, mode={mode}")

    if sys.platform == "win32":
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

    try:
        asyncio.run(_run_scout(file, mode, save))
    except Exception as e:
        logger.error(f"Analyze error: {e}", exc_info=True)
        typer.echo(f"[ERROR] {e}", err=True)
        raise typer.Exit(code=1)


@app.command()
def scout(
    file: str = typer.Argument(..., help="Path to data file (CSV/Parquet)"),
    mode: str = typer.Option("scout", help="Analysis mode: scout, schema, quality, stats"),
    save: bool = typer.Option(False, "--save", help="Save verdict to file"),
    log_file: Optional[str] = typer.Option(None, help="Log file path"),
):
    """
    Run multi-agent dataset onboarding intelligence.

    Concurrently runs SchemaAgent + QualityAgent + StatsAgent, then
    NarratorAgent synthesizes a 5-sentence verdict.
    """
    import asyncio
    import sys

    log_path = setup_logging(log_file or "logs")
    logger.info(f"Scout command: file={file}, mode={mode}")

    if sys.platform == "win32":
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

    try:
        asyncio.run(_run_scout(file, mode, save))
    except Exception as e:
        logger.error(f"Scout error: {e}", exc_info=True)
        typer.echo(f"[ERROR] {e}", err=True)
        raise typer.Exit(code=1)


async def _run_scout(file: str, mode: str, save: bool) -> None:
    """Async implementation: concurrent tier-1 agents then tier-2 narrator."""
    import asyncio
    from pulsar.core.intelligence.agent_registry import AgentRegistry
    from pulsar.core.intelligence.question_router import get_tiers, TIER1_QUESTION
    from pulsar.core.intelligence.narrator_agent import NarratorAgent

    lf = load(file)
    df = lf.collect()
    dataset_name = Path(file).stem

    shared_state = SharedStateStore()

    tier1_keys, tier2_keys = get_tiers(mode)

    typer.echo(f"\n[SCOUT] Dataset: {dataset_name}  ({df.shape[0]:,} rows × {df.shape[1]} cols)")
    typer.echo(f"[SCOUT] Running agents: {', '.join(tier1_keys)}" + (
        f" → {', '.join(tier2_keys)}" if tier2_keys else ""
    ))

    # ── Tier 1: concurrent ───────────────────────────────────────────────────
    tier1_agents = [
        AgentRegistry.get_agent(key, df=df, shared_state=shared_state)
        for key in tier1_keys
    ]

    typer.echo(f"[SCOUT] Tier 1: {len(tier1_agents)} agents running concurrently…")

    tier1_results = await asyncio.gather(
        *[
            asyncio.to_thread(
                agent.think,
                TIER1_QUESTION.get(key, f"Analyze the dataset for {key} insights."),
            )
            for key, agent in zip(tier1_keys, tier1_agents)
        ],
        return_exceptions=True,
    )

    for key, result in zip(tier1_keys, tier1_results):
        if isinstance(result, Exception):
            logger.warning(f"[SCOUT] {key} agent failed: {result}")
            typer.echo(f"  [WARN] {key}: {result}", err=True)
        else:
            typer.echo(f"  [OK] {key} analysis complete")

    # ── Tier 2: sequential (narrator reads tier-1 shared state) ─────────────
    verdict = ""
    if tier2_keys:
        typer.echo(f"[SCOUT] Tier 2: narrator synthesizing verdict…")
        narrator = NarratorAgent(df=df, shared_state=shared_state)
        try:
            verdict = narrator.synthesize()
        except Exception as e:
            logger.error(f"[SCOUT] Narrator failed: {e}")
            verdict = "[Narrator unavailable — check LLM connection]"

    # ── Output ────────────────────────────────────────────────────────────────
    separator = "=" * 70
    typer.echo(f"\n{separator}")
    typer.echo(f"  SCOUT VERDICT — {dataset_name}")
    typer.echo(f"{separator}\n")
    typer.echo(verdict or "\n".join(
        f"[{k.upper()}]\n{r}" for k, r in zip(tier1_keys, tier1_results)
        if not isinstance(r, Exception)
    ))
    typer.echo(f"\n{separator}\n")

    if save:
        report_path = Path(f"journey_reports/{dataset_name}_scout.md")
        report_path.parent.mkdir(parents=True, exist_ok=True)
        report_path.write_text(verdict or "")
        typer.echo(f"[OK] Saved: {report_path}")
        logger.info(f"Scout report saved: {report_path}")


if __name__ == "__main__":
    app()