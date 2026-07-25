"""
Journey Orchestrator with Shared State Architecture.

Orchestrates the complete journey from SCOUT to ACE using shared state
for efficient agent communication and context sharing.
"""

import logging
from datetime import datetime
from typing import Dict, Any, Optional
from pathlib import Path

import polars as pl

from pulsar.core.intelligence.shared_state_store import SharedStateStore
from pulsar.core.intelligence.stage_agents import (
    ScoutAgent,
    ExplorerAgent,
    DetectiveAgent,
    AnalystAgent,
    AceAgent,
)
from pulsar.core.intelligence.run_persistence import RunPersistence
from pulsar.core.llm_connectors import LLMConfig

logger = logging.getLogger(__name__)


class Journey:
    """Execute a complete data discovery journey with shared state."""

    STAGES = [
        ("scout", ScoutAgent, "Initial data discovery"),
        ("explorer", ExplorerAgent, "Detailed exploration"),
        ("detective", DetectiveAgent, "Quality validation"),
        ("analyst", AnalystAgent, "Strategic analysis"),
        ("ace", AceAgent, "Executive synthesis"),
    ]

    def __init__(
        self,
        dataset_name: str,
        df: Optional[pl.DataFrame] = None,
        llm_config: Optional[LLMConfig] = None,
    ):
        """
        Initialize journey.

        Args:
            dataset_name: Name of dataset being analyzed
            df: Optional DataFrame to analyze
            llm_config: Optional LLM configuration
        """
        self.dataset_name = dataset_name
        self.df = df
        self.llm_config = llm_config
        self.run_id = datetime.now().strftime("%Y%m%d_%H%M%S")
        self.conversation_id = f"{dataset_name}_{self.run_id}"

        # Initialize shared state (central communication hub)
        self.shared_state = SharedStateStore()

        # Initialize run persistence
        self.run_persistence = RunPersistence()
        self.run_persistence.create_run(
            dataset_name=dataset_name,
            total_rows=len(df) if df is not None else 0,
            total_columns=df.shape[1] if df is not None else 0,
        )

        logger.info(f"Journey initialized: {self.dataset_name} (run: {self.run_id})")

    def execute(self) -> Dict[str, Any]:
        """
        Execute complete journey through all stages.

        Returns:
            Dictionary with results and shared state stats
        """
        results = {}

        logger.info("=" * 60)
        logger.info("JOURNEY EXECUTION STARTED")
        logger.info("=" * 60)

        for stage_idx, (stage_name, AgentClass, description) in enumerate(self.STAGES, 1):
            logger.info(f"\n[{stage_idx}/5] {stage_name.upper()}: {description}")
            logger.info("-" * 60)

            try:
                # Create stage-specific agent with shared state
                agent = AgentClass(
                    llm_config=self.llm_config,
                    df=self.df,
                    conversation_id=self.conversation_id,
                    dataset_name=self.dataset_name,
                    shared_state=self.shared_state,
                )

                # Get stage-specific question
                question = self._get_stage_question(stage_name)

                # Execute stage
                logger.info(f"Executing {stage_name} agent...")
                response = agent.think(question)

                # Store result
                results[stage_name] = {
                    "response": response[:500],  # Truncate for display
                    "full_response": response,
                    "timestamp": datetime.now().isoformat(),
                }

                # Log what was shared
                stage_keys = self.shared_state.get_keys_for_stage(stage_name)
                logger.info(f"✓ {stage_name}: wrote {len(stage_keys['writes'])} keys, read {len(stage_keys['reads'])} keys")

                # Persist stage output
                self.run_persistence.save_stage(
                    dataset_name=self.dataset_name,
                    run_id=self.run_id,
                    stage=stage_name,
                    progress=stage_idx * 20,  # 20% per stage
                    analysis=response[:1000],
                    insights=stage_keys["writes"],
                )

                logger.info(f"✓ Stage {stage_name} completed")

            except Exception as e:
                logger.error(f"✗ Stage {stage_name} failed: {e}")
                raise

        # Mark run as complete
        self.run_persistence.complete_run(
            dataset_name=self.dataset_name,
            run_id=self.run_id,
        )

        # Get final statistics
        stats = self.shared_state.get_stats()

        logger.info("\n" + "=" * 60)
        logger.info("JOURNEY COMPLETED SUCCESSFULLY")
        logger.info("=" * 60)
        logger.info(f"Total keys in shared state: {stats['total_keys']}")
        logger.info(f"Total memory: {stats['total_mb']:.2f} MB")
        logger.info(f"Total accesses: {stats['total_accesses']}")
        logger.info(f"Run ID: {self.run_id}")

        return {
            "results": results,
            "shared_state_stats": stats,
            "run_id": self.run_id,
            "dataset_name": self.dataset_name,
        }

    def _get_stage_question(self, stage_name: str) -> str:
        """Get the question/prompt for each stage."""
        questions = {
            "scout": (
                "Please analyze this dataset: What is the data structure, quality, and key statistics? "
                "Identify any blocking issues that need resolution."
            ),
            "explorer": (
                "Explore the data in detail: What are the column types and distributions? "
                "Discover key patterns and relationships. What stands out?"
            ),
            "detective": (
                "Validate data quality: Check for outliers, anomalies, and data integrity issues. "
                "Provide a quality verdict for this dataset. Is it ready for analysis?"
            ),
            "analyst": (
                "Analyze strategically: Find correlations, key drivers, and high-value segments. "
                "What are the most important insights and opportunities?"
            ),
            "ace": (
                "Synthesize everything: Provide an executive summary, top 5 recommendations, "
                "key risks, and strategic opportunities. What is the business value?"
            ),
        }
        return questions.get(stage_name, "Analyze this data.")

    def get_shared_state(self) -> SharedStateStore:
        """Get the shared state store for inspection or continuation."""
        return self.shared_state

    def get_dependency_graph(self) -> Dict[str, Any]:
        """Get the dependency graph showing which agents depend on what."""
        return self.shared_state.get_dependency_graph()


__all__ = ["Journey"]
