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

    def __init__(self, df: pl.DataFrame, dataset_name: str):
        """
        Initialize journey for a dataset.

        Args:
            df: The DataFrame to explore
            dataset_name: Name of the dataset
        """
        self.df = df
        self.dataset_name = dataset_name
        self.agent = Agent(df=df, tools_enabled=True)

        self.current_stage = Stage.SCOUT
        self.checkpoints: List[JourneyCheckpoint] = []
        self.total_insights = 0

        logger.info(
            f"Starting data exploration journey for '{dataset_name}' "
            f"({df.height} rows, {df.width} cols)"
        )

    def start_journey(self) -> str:
        """Start the data exploration journey at SCOUT stage."""
        checkpoint = self._scout_stage()
        self.checkpoints.append(checkpoint)
        self.current_stage = Stage.SCOUT

        return self._format_stage_introduction()

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
        return self._format_stage_transition(Stage.EXPLORER, Stage.DETECTIVE)

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
        return self._format_stage_transition(Stage.DETECTIVE, Stage.ANALYST)

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
        return self._format_stage_transition(Stage.ANALYST, Stage.ACE)

    def run_complete_journey(self) -> str:
        """Run the complete journey from SCOUT to ACE."""
        logger.info("Running complete journey...")

        results = []

        # Stage 1: Scout
        results.append(self.start_journey())
        results.append("\n" + "="*80 + "\n")

        # Stage 2: Explorer
        results.append(self.explore_stage())
        results.append("\n" + "="*80 + "\n")

        # Stage 3: Detective
        results.append(self.detective_stage())
        results.append("\n" + "="*80 + "\n")

        # Stage 4: Analyst
        results.append(self.analyst_stage())
        results.append("\n" + "="*80 + "\n")

        # Stage 5: Ace
        results.append(self.ace_stage())

        return "".join(results)

    def _format_stage_introduction(self) -> str:
        """Format the introduction to the journey."""
        return (
            f"\n{'='*80}\n"
            f"[JOURNEY] DATA EXPLORATION JOURNEY: {self.dataset_name}\n"
            f"{'='*80}\n"
            f"Your guide: Intelligent Agent\n"
            f"Goal: Become an 'ACE' at understanding your data\n"
            f"Progress: SCOUT (0%) > EXPLORER > DETECTIVE > ANALYST > ACE (100%)\n"
            f"{'='*80}\n\n"
            f"Starting at SCOUT stage...\n\n"
            + self.checkpoints[-1].description
        )

    def _format_stage_transition(self, from_stage: Stage, to_stage: Stage) -> str:
        """Format the transition between stages."""
        percent = {
            Stage.SCOUT: 0,
            Stage.EXPLORER: 25,
            Stage.DETECTIVE: 50,
            Stage.ANALYST: 75,
            Stage.ACE: 100,
        }

        stage_labels = {
            Stage.SCOUT: "[SCOUT]",
            Stage.EXPLORER: "[EXPLORER]",
            Stage.DETECTIVE: "[DETECTIVE]",
            Stage.ANALYST: "[ANALYST]",
            Stage.ACE: "[ACE]",
        }

        checkpoint = self.checkpoints[-1]

        transition = (
            f"\n{'='*80}\n"
            f"{stage_labels[to_stage]} {checkpoint.title}\n"
            f"Progress: {percent[to_stage]}% Complete\n"
            f"{'='*80}\n\n"
            f"{checkpoint.description}\n\n"
            f"[INSIGHTS] Key Insights This Stage:\n"
        )

        for insight in checkpoint.insights_gained:
            transition += f"  • {insight}\n"

        if checkpoint.next_stage_hint:
            transition += f"\n[NEXT] {checkpoint.next_stage_hint}\n"

        return transition

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
