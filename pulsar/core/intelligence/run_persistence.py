"""
RunPersistence: Save and resume journey runs across interruptions.

Stores journey state in runs/ directory with:
- Metadata (dataset, start time, completed stages)
- Stage outputs (insights, analysis)
- Agent memory (for resumption)
"""

import json
import logging
from pathlib import Path
from datetime import datetime
from typing import Dict, Any, List, Optional
from dataclasses import dataclass, asdict

logger = logging.getLogger(__name__)


@dataclass
class RunMetadata:
    """Metadata for a journey run."""
    dataset_name: str
    run_id: str  # timestamp-based
    start_time: str
    last_updated: str
    completed_stages: List[str]  # ['scout', 'explorer', ...]
    status: str  # 'in_progress', 'completed', 'interrupted'
    total_rows: int
    total_columns: int


@dataclass
class StageCheckpoint:
    """Saved state for a completed stage."""
    stage: str
    progress: int
    analysis: str
    insights: List[str]
    timestamp: str


class RunPersistence:
    """Handle saving and loading journey runs."""

    RUNS_DIR = Path("runs")

    def __init__(self):
        """Initialize persistence system."""
        self.RUNS_DIR.mkdir(parents=True, exist_ok=True)

    def create_run(
        self,
        dataset_name: str,
        total_rows: int,
        total_columns: int,
    ) -> str:
        """
        Create a new run and return its run_id.

        Args:
            dataset_name: Name of dataset
            total_rows: Row count
            total_columns: Column count

        Returns:
            run_id (timestamp format)
        """
        run_id = datetime.now().strftime("%Y%m%d_%H%M%S")
        run_dir = self._get_run_dir(dataset_name, run_id)
        run_dir.mkdir(parents=True, exist_ok=True)

        metadata = RunMetadata(
            dataset_name=dataset_name,
            run_id=run_id,
            start_time=datetime.now().isoformat(),
            last_updated=datetime.now().isoformat(),
            completed_stages=[],
            status="in_progress",
            total_rows=total_rows,
            total_columns=total_columns,
        )

        self._save_metadata(dataset_name, run_id, metadata)
        logger.info(f"Created run: {dataset_name}/{run_id}")

        return run_id

    def save_stage(
        self,
        dataset_name: str,
        run_id: str,
        stage: str,
        progress: int,
        analysis: str,
        insights: List[str],
    ) -> None:
        """
        Save a completed stage checkpoint.

        Args:
            dataset_name: Dataset name
            run_id: Run ID
            stage: Stage name (scout, explorer, etc.)
            progress: Progress percentage
            analysis: Stage analysis text
            insights: List of insight strings
        """
        run_dir = self._get_run_dir(dataset_name, run_id)

        checkpoint = StageCheckpoint(
            stage=stage,
            progress=progress,
            analysis=analysis,
            insights=insights,
            timestamp=datetime.now().isoformat(),
        )

        checkpoint_path = run_dir / f"stage_{stage}.json"
        with open(checkpoint_path, 'w') as f:
            json.dump(asdict(checkpoint), f, indent=2)

        # Update metadata
        metadata = self._load_metadata(dataset_name, run_id)
        if stage not in metadata.completed_stages:
            metadata.completed_stages.append(stage)
        metadata.last_updated = datetime.now().isoformat()

        self._save_metadata(dataset_name, run_id, metadata)
        logger.info(f"Saved stage {stage} for run {dataset_name}/{run_id}")

    def complete_run(self, dataset_name: str, run_id: str) -> None:
        """Mark a run as completed."""
        metadata = self._load_metadata(dataset_name, run_id)
        metadata.status = "completed"
        metadata.last_updated = datetime.now().isoformat()
        self._save_metadata(dataset_name, run_id, metadata)
        logger.info(f"Completed run: {dataset_name}/{run_id}")

    def interrupt_run(self, dataset_name: str, run_id: str) -> None:
        """Mark a run as interrupted (for resume capability)."""
        metadata = self._load_metadata(dataset_name, run_id)
        metadata.status = "interrupted"
        metadata.last_updated = datetime.now().isoformat()
        self._save_metadata(dataset_name, run_id, metadata)
        logger.info(f"Interrupted run: {dataset_name}/{run_id}")

    def get_run_metadata(self, dataset_name: str, run_id: str) -> Optional[RunMetadata]:
        """Get metadata for a run."""
        try:
            return self._load_metadata(dataset_name, run_id)
        except FileNotFoundError:
            return None

    def get_stage_checkpoint(
        self, dataset_name: str, run_id: str, stage: str
    ) -> Optional[StageCheckpoint]:
        """Get a saved stage checkpoint."""
        run_dir = self._get_run_dir(dataset_name, run_id)
        checkpoint_path = run_dir / f"stage_{stage}.json"

        if not checkpoint_path.exists():
            return None

        with open(checkpoint_path, 'r') as f:
            data = json.load(f)
            return StageCheckpoint(**data)

    def list_runs(self, dataset_name: Optional[str] = None) -> List[Dict[str, Any]]:
        """
        List all saved runs.

        Args:
            dataset_name: Optional filter by dataset

        Returns:
            List of run info dicts
        """
        runs = []

        for dataset_dir in self.RUNS_DIR.iterdir():
            if not dataset_dir.is_dir():
                continue

            ds_name = dataset_dir.name

            # Filter by dataset if specified
            if dataset_name and ds_name != dataset_name:
                continue

            # Find all run directories (looking for metadata.json)
            for run_dir in dataset_dir.iterdir():
                if not run_dir.is_dir():
                    continue

                metadata_path = run_dir / "metadata.json"
                if not metadata_path.exists():
                    continue

                with open(metadata_path, 'r') as f:
                    metadata = json.load(f)

                runs.append({
                    'dataset': ds_name,
                    'run_id': metadata['run_id'],
                    'start_time': metadata['start_time'],
                    'status': metadata['status'],
                    'completed_stages': metadata['completed_stages'],
                    'progress': len(metadata['completed_stages']) * 20,  # Each stage is 20%
                })

        # Sort by start time (newest first)
        runs.sort(key=lambda x: x['start_time'], reverse=True)
        return runs

    def get_latest_run(self, dataset_name: str) -> Optional[str]:
        """Get the latest run ID for a dataset."""
        runs = self.list_runs(dataset_name)
        if runs:
            return runs[0]['run_id']
        return None

    # Private methods

    @staticmethod
    def _get_run_dir(dataset_name: str, run_id: str) -> Path:
        """Get the directory path for a run."""
        return RunPersistence.RUNS_DIR / dataset_name / run_id

    @staticmethod
    def _save_metadata(dataset_name: str, run_id: str, metadata: RunMetadata) -> None:
        """Save metadata to file."""
        run_dir = RunPersistence._get_run_dir(dataset_name, run_id)
        metadata_path = run_dir / "metadata.json"
        with open(metadata_path, 'w') as f:
            json.dump(asdict(metadata), f, indent=2)

    @staticmethod
    def _load_metadata(dataset_name: str, run_id: str) -> RunMetadata:
        """Load metadata from file."""
        run_dir = RunPersistence._get_run_dir(dataset_name, run_id)
        metadata_path = run_dir / "metadata.json"

        if not metadata_path.exists():
            raise FileNotFoundError(f"Run not found: {dataset_name}/{run_id}")

        with open(metadata_path, 'r') as f:
            data = json.load(f)
            return RunMetadata(**data)
