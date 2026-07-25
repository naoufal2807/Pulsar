# pulsar/core/intelligence/journey.py
"""
Data Exploration Journey: Guided path from Zero to Ace understanding a dataset.

Stages:
1. SCOUT (Zero) - Basic dataset overview
2. EXPLORER (Discovery) - Pattern exploration
3. DETECTIVE (Investigation) - Deep analysis and anomalies
4. ANALYST (Mastery) - Advanced insights and relationships
5. ACE (Expert) - Complete understanding with recommendations
"""

from enum import Enum
from typing import Any, Dict, List, Optional
import logging
from dataclasses import dataclass

import polars as pl

from pulsar.core.intelligence.agent import Agent
from pulsar.core.intelligence.tools import create_default_registry
from pulsar.core.presentation import JourneyPresenter, LogSuppressor
from pulsar.core.intelligence.run_persistence import RunPersistence

logger = logging.getLogger(__name__)


class Stage(Enum):
    """Journey stages from zero to ace."""
    SCOUT = "scout"          # 0% - Basic overview
    EXPLORER = "explorer"    # 25% - Discovery mode
    DETECTIVE = "detective"  # 50% - Deep investigation
    ANALYST = "analyst"      # 75% - Advanced insights
    ACE = "ace"              # 100% - Expert mastery


@dataclass
class JourneyCheckpoint:
    """Checkpoint in the learning journey."""
    stage: Stage
    title: str
    description: str
    questions_explored: List[str]
    insights_gained: List[str]
    next_stage_hint: Optional[str] = None


class DataExplorationJourney:
    """Guide user through structured data exploration."""

    def __init__(self, df: pl.DataFrame, dataset_name: str, run_id: Optional[str] = None):
        """
        Initialize journey for a dataset.

        Args:
            df: The DataFrame to explore
            dataset_name: Name of the dataset
            run_id: Optional run ID for resumption
        """
        self.df = df
        self.dataset_name = dataset_name
        self.agent = Agent(df=df, tools_enabled=True)
        self.presenter = JourneyPresenter(dataset_name)
        self.persistence = RunPersistence()

        self.current_stage = Stage.SCOUT
        self.checkpoints: List[JourneyCheckpoint] = []
        self.total_insights = 0
        self.run_id = run_id

        # Create new run if not resuming
        if not run_id:
            self.run_id = self.persistence.create_run(
                dataset_name=dataset_name,
                total_rows=df.height,
                total_columns=df.width,
            )
        else:
            logger.info(f"Resuming run: {dataset_name}/{run_id}")

        logger.info(
            f"Starting data exploration journey for '{dataset_name}' "
            f"({df.height} rows, {df.width} cols) [Run: {self.run_id}]"
        )

    def start_journey(self) -> str:
        """Start the data exploration journey at SCOUT stage."""
        # Check if already completed
        if 'scout' in self._get_completed_stages():
            logger.info("SCOUT stage already completed, skipping...")
            checkpoint = self._load_stage_checkpoint('scout')
            if checkpoint:
                return ""  # Skip output, will be shown in summary

        with LogSuppressor():
            checkpoint = self._scout_stage()
        self.checkpoints.append(checkpoint)
        self.current_stage = Stage.SCOUT

        # Save to persistence
        self.persistence.save_stage(
            dataset_name=self.dataset_name,
            run_id=self.run_id,
            stage='scout',
            progress=0,
            analysis=checkpoint.description,
            insights=checkpoint.insights_gained,
        )

        # Use presenter for clean output
        intro = self.presenter.present_journey_intro()
        stage_output = self.presenter.present_stage(
            stage_name='SCOUT',
            progress=0,
            analysis=checkpoint.description,
            insights=checkpoint.insights_gained,
            hint=checkpoint.next_stage_hint,
        )
        return intro + stage_output

    def _scout_stage(self) -> JourneyCheckpoint:
        """Stage 1: SCOUT - Get basic dataset overview."""
        logger.info("Stage 1: SCOUT - Dataset overview")

        # Get basic info
        questions = [
            "What is the shape and size of the dataset?",
            "What are the columns and their data types?",
            "What does the data represent?",
        ]

        # Get sample data to provide concrete context
        sample = self.df.head(1)
        first_row = sample.row(0, named=True)

        prompt = (
            f"SCOUT STAGE: Provide a CONCRETE, SPECIFIC overview of '{self.dataset_name}'.\n\n"
            f"Dataset Size: {self.df.height:,} rows × {self.df.width} columns\n"
            f"Columns: {', '.join(self.df.columns)}\n\n"
            f"Sample row: {first_row}\n\n"
            f"Your task:\n"
            f"1. What IS this dataset? (Not generic - be SPECIFIC)\n"
            f"2. What DOMAIN is it? (Entertainment? Sports? Finance?)\n"
            f"3. What are the 3 most important columns and WHY?\n"
            f"4. What business questions could be answered?\n\n"
            f"Do NOT say 'I'm ready to analyze'. Give SPECIFIC insights about this data RIGHT NOW."
        )

        # Allow multiple iterations for tool calling
        overview = self.agent.think(prompt, max_iterations=5)

        insights = [
            f"Dataset contains {self.df.height:,} rows and {self.df.width} columns",
            f"Identified {self.df.width} distinct data fields",
        ]

        return JourneyCheckpoint(
            stage=Stage.SCOUT,
            title="[SCOUT] Dataset Overview",
            description=overview,
            questions_explored=questions,
            insights_gained=insights,
            next_stage_hint="Next: EXPLORE the patterns and distributions in the data.",
        )

    def explore_stage(self) -> str:
        """Stage 2: EXPLORER - Discover patterns and distributions."""
        logger.info("Stage 2: EXPLORER - Pattern exploration")

        questions = [
            "What are the most important columns?",
            "What are the distributions like?",
            "Are there obvious patterns or groupings?",
        ]

        # Identify key columns to analyze
        numeric_cols = [
            col for col in self.df.columns
            if self.df[col].dtype in [pl.Int32, pl.Int64, pl.Float32, pl.Float64]
        ]

        categorical_cols = [
            col for col in self.df.columns
            if self.df[col].dtype == pl.Utf8
        ]

        # Build specific analysis requests
        analyses = []
        if numeric_cols:
            analyses.extend([f"[TOOL: compute_statistics(column={col})]" for col in numeric_cols[:2]])
        if categorical_cols:
            analyses.extend([f"[TOOL: get_top_values(column={col}, limit=5)]" for col in categorical_cols[:3]])

        prompt = (
            f"EXPLORER STAGE: Deep pattern discovery in '{self.dataset_name}'.\n\n"
            f"EXECUTE these tools and report SPECIFIC numbers:\n"
            + "\n".join(analyses) + f"\n\n"
            f"Then provide CONCRETE findings:\n"
            f"1. Key statistics (means, ranges, distributions)\n"
            f"2. Top categories and their counts\n"
            f"3. What patterns JUMP OUT from the data?\n"
            f"4. Which columns are MOST IMPORTANT for analysis?\n\n"
            f"Be SPECIFIC with numbers, percentages, rankings. "
            f"Avoid vague statements. This is DISCOVERY - find interesting facts!"
        )

        with LogSuppressor():
            exploration = self.agent.think(prompt, max_iterations=5)

        insights = [
            f"Identified {len(numeric_cols)} numeric columns",
            f"Found {len(categorical_cols)} categorical columns",
            "Discovered main data distributions",
        ]

        self.current_stage = Stage.EXPLORER

        checkpoint = JourneyCheckpoint(
            stage=Stage.EXPLORER,
            title="[EXPLORER] Pattern Discovery",
            description=exploration,
            questions_explored=questions,
            insights_gained=insights,
            next_stage_hint="Next: DETECT anomalies and deep patterns in the data.",
        )

        self.checkpoints.append(checkpoint)

        # Save to persistence
        self.persistence.save_stage(
            dataset_name=self.dataset_name,
            run_id=self.run_id,
            stage='explorer',
            progress=25,
            analysis=exploration,
            insights=insights,
        )

        return self.presenter.present_stage(
            stage_name='EXPLORER',
            progress=25,
            analysis=exploration,
            insights=insights,
            hint="Next: DETECT anomalies and deep patterns in the data.",
        )

        self.checkpoints.append(checkpoint)
        return self._format_stage_transition(Stage.SCOUT, Stage.EXPLORER)

    def detective_stage(self) -> str:
        """Stage 3: DETECTIVE - Find anomalies and deep issues."""
        logger.info("Stage 3: DETECTIVE - Anomaly detection")

        questions = [
            "Are there outliers or anomalies?",
            "What data quality issues exist?",
            "Are there unexpected relationships?",
        ]

        # Get columns to check for quality
        numeric_cols = [
            col for col in self.df.columns
            if self.df[col].dtype in [pl.Int32, pl.Int64, pl.Float32, pl.Float64]
        ]

        categorical_cols = [
            col for col in self.df.columns
            if self.df[col].dtype == pl.Utf8
        ][:3]

        quality_checks = []
        quality_checks.extend([f"[TOOL: check_data_quality(column={col})]" for col in categorical_cols])
        if numeric_cols:
            quality_checks.extend([f"[TOOL: detect_outliers(column={col})]" for col in numeric_cols[:2]])

        prompt = (
            f"DETECTIVE STAGE: Find REAL problems in '{self.dataset_name}'.\n\n"
            f"EXECUTE quality checks:\n"
            + "\n".join(quality_checks) + f"\n\n"
            f"Report SPECIFIC issues found:\n"
            f"1. NULL values - which columns and how many?\n"
            f"2. Duplicates - any exact duplicates?\n"
            f"3. Outliers - what's unusual or extreme?\n"
            f"4. Data type mismatches - any suspicious values?\n"
            f"5. Missing domains - any incomplete data?\n\n"
            f"This is DETECTIVE work: Find the red flags that need investigation. "
            f"Give SPECIFIC column names and percentages."
        )

        with LogSuppressor():
            investigation = self.agent.think(prompt, max_iterations=5)

        insights = [
            "Detected anomalies and outliers",
            "Identified data quality concerns",
            "Found unexpected relationships",
        ]

        self.current_stage = Stage.DETECTIVE

        checkpoint = JourneyCheckpoint(
            stage=Stage.DETECTIVE,
            title="[DETECTIVE] Deep Investigation",
            description=investigation,
            questions_explored=questions,
            insights_gained=insights,
            next_stage_hint="Next: ANALYZE correlations and advanced insights.",
        )

        self.checkpoints.append(checkpoint)

        # Save to persistence
        self.persistence.save_stage(
            dataset_name=self.dataset_name,
            run_id=self.run_id,
            stage='detective',
            progress=50,
            analysis=investigation,
            insights=insights,
        )

        return self.presenter.present_stage(
            stage_name='DETECTIVE',
            progress=50,
            analysis=investigation,
            insights=insights,
            hint="Next: ANALYZE correlations and advanced insights.",
        )

    def analyst_stage(self) -> str:
        """Stage 4: ANALYST - Advanced insights and relationships."""
        logger.info("Stage 4: ANALYST - Advanced analysis")

        questions = [
            "What are the key relationships?",
            "What correlations exist?",
            "What drives the most important metrics?",
        ]

        numeric_cols = [
            col for col in self.df.columns
            if self.df[col].dtype in [pl.Int32, pl.Int64, pl.Float32, pl.Float64]
        ]

        correlations = []
        if len(numeric_cols) >= 2:
            for i in range(min(3, len(numeric_cols)-1)):
                correlations.append(f"[TOOL: analyze_correlation(col1={numeric_cols[i]}, col2={numeric_cols[i+1]})]")

        prompt = (
            f"ANALYST STAGE: Strategic insights from '{self.dataset_name}'.\n\n"
            f"EXECUTE correlation analysis:\n"
            + "\n".join(correlations) + f"\n\n"
            f"Provide STRATEGIC insights:\n"
            f"1. Key relationships - what variables drive each other?\n"
            f"2. Correlation findings - are there strong relationships? Which ones?\n"
            f"3. Business drivers - what factors matter most?\n"
            f"4. Opportunities - what insights create competitive advantage?\n"
            f"5. Risks - what should be monitored?\n"
            f"6. Recommendations - what should be optimized?\n\n"
            f"Think like a strategist. What patterns would matter to executives?"
        )

        with LogSuppressor():
            analysis = self.agent.think(prompt, max_iterations=5)

        insights = [
            "Analyzed key correlations",
            "Identified metric drivers",
            "Found strategic insights",
        ]

        self.current_stage = Stage.ANALYST

        checkpoint = JourneyCheckpoint(
            stage=Stage.ANALYST,
            title="[ANALYST] Advanced Insights",
            description=analysis,
            questions_explored=questions,
            insights_gained=insights,
            next_stage_hint="Finally: Reach ACE mastery with expert recommendations.",
        )

        self.checkpoints.append(checkpoint)

        # Save to persistence
        self.persistence.save_stage(
            dataset_name=self.dataset_name,
            run_id=self.run_id,
            stage='analyst',
            progress=75,
            analysis=analysis,
            insights=insights,
        )

        return self.presenter.present_stage(
            stage_name='ANALYST',
            progress=75,
            analysis=analysis,
            insights=insights,
            hint="Finally: Reach ACE mastery with expert recommendations.",
        )

    def ace_stage(self) -> str:
        """Stage 5: ACE - Complete mastery and expert recommendations."""
        logger.info("Stage 5: ACE - Expert mastery")

        questions = [
            "What are the top 3 critical findings?",
            "What actions should be taken?",
            "What are the risks to monitor?",
        ]

        prompt = (
            f"ACE STAGE: You are now an EXPERT on '{self.dataset_name}'.\n\n"
            f"Synthesize EVERYTHING from SCOUT through ANALYST stages.\n\n"
            f"Provide an EXECUTIVE SUMMARY with:\n\n"
            f"1. TOP 3 CRITICAL FINDINGS (not vague - SPECIFIC facts)\n"
            f"   - Example: '78% of content is from 5 countries'\n"
            f"   - Example: 'Average release year is 2015, shows aging dataset'\n\n"
            f"2. ACTIONABLE RECOMMENDATIONS (specific, not generic)\n"
            f"   - What should be done based on findings?\n"
            f"   - What metrics should be optimized?\n"
            f"   - Where is the biggest opportunity?\n\n"
            f"3. RISKS & MITIGATION (real concerns)\n"
            f"   - Data quality issues to fix\n"
            f"   - Business risks from patterns\n"
            f"   - What needs monitoring\n\n"
            f"4. NEXT STEPS\n"
            f"   - Deeper analyses needed\n"
            f"   - Questions to investigate\n\n"
            f"This is the EXECUTIVE BRIEF you'd present to leadership. "
            f"Be SPECIFIC, ACTIONABLE, and INSIGHTFUL."
        )

        with LogSuppressor():
            expert_synthesis = self.agent.think(prompt, max_iterations=3)

        insights = [
            "Synthesized all learnings",
            "Generated expert recommendations",
            "Identified strategic risks",
        ]

        self.current_stage = Stage.ACE

        checkpoint = JourneyCheckpoint(
            stage=Stage.ACE,
            title="[ACE] Expert Mastery & Recommendations",
            description=expert_synthesis,
            questions_explored=questions,
            insights_gained=insights,
            next_stage_hint=None,
        )

        self.checkpoints.append(checkpoint)

        # Save to persistence
        self.persistence.save_stage(
            dataset_name=self.dataset_name,
            run_id=self.run_id,
            stage='ace',
            progress=100,
            analysis=expert_synthesis,
            insights=insights,
        )

        # Mark run as completed
        self.persistence.complete_run(self.dataset_name, self.run_id)

        return self.presenter.present_stage(
            stage_name='ACE',
            progress=100,
            analysis=expert_synthesis,
            insights=insights,
        )

    def run_complete_journey(self) -> str:
        """Run the complete journey from SCOUT to ACE."""
        logger.info("Running complete journey...")

        results = []

        # Stage 1: Scout
        results.append(self.start_journey())

        # Stage 2: Explorer
        results.append(self.explore_stage())

        # Stage 3: Detective
        results.append(self.detective_stage())

        # Stage 4: Analyst
        results.append(self.analyst_stage())

        # Stage 5: Ace
        results.append(self.ace_stage())

        return "".join(results)


    def _get_completed_stages(self) -> List[str]:
        """Get list of completed stages from persistence."""
        metadata = self.persistence.get_run_metadata(self.dataset_name, self.run_id)
        return metadata.completed_stages if metadata else []

    def _load_stage_checkpoint(self, stage: str) -> Optional[JourneyCheckpoint]:
        """Load a saved stage checkpoint."""
        cp = self.persistence.get_stage_checkpoint(self.dataset_name, self.run_id, stage)
        if cp:
            return JourneyCheckpoint(
                stage=Stage[stage.upper()],
                title=f"[{stage.upper()}]",
                description=cp.analysis,
                questions_explored=[],
                insights_gained=cp.insights,
            )
        return None

    def get_journey_summary(self) -> Dict[str, Any]:
        """Get a summary of the journey completed."""
        return {
            'dataset': self.dataset_name,
            'total_rows': self.df.height,
            'total_columns': self.df.width,
            'current_stage': self.current_stage.value,
            'progress_percent': {
                Stage.SCOUT: 0,
                Stage.EXPLORER: 25,
                Stage.DETECTIVE: 50,
                Stage.ANALYST: 75,
                Stage.ACE: 100,
            }[self.current_stage],
            'checkpoints_completed': len(self.checkpoints),
            'total_insights': sum(len(cp.insights_gained) for cp in self.checkpoints),
        }
