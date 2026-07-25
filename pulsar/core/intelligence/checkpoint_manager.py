"""
Layer 4: CheckpointManager - Pre-tool snapshots for graceful recovery.

Saves agent state before tool calls to enable recovery from timeouts/failures.
Provides "resume and retry" capability without re-executing completed stages.

Architecture:
  Before tool call → save_pre_tool_checkpoint(agent_state)
  Tool fails/timeout → restore_from_checkpoint(checkpoint_id)
  Resume tool call → exact same point, no work lost
"""

import json
import logging
from pathlib import Path
from datetime import datetime, timedelta
from typing import Dict, Any, List, Optional
from dataclasses import dataclass, asdict
import uuid
import hashlib

logger = logging.getLogger(__name__)


@dataclass
class ToolAttempt:
    """Info about a tool call attempt."""
    tool_name: str
    parameters: Dict[str, Any]
    start_time: str
    description: str = ""


@dataclass
class CheckpointMetadata:
    """Metadata for a pre-tool checkpoint."""
    checkpoint_id: str
    conversation_id: str
    dataset_name: str
    timestamp: str
    tool_attempt: ToolAttempt
    agent_state_hash: str
    status: str  # "pending_recovery", "recovered", "failed", "expired"
    recovery_attempts: int = 0
    last_recovery_attempt: Optional[str] = None


class CheckpointManager:
    """Manage pre-tool checkpoints for recovery from tool timeouts."""

    CHECKPOINTS_DIR = Path("checkpoints")
    CHECKPOINT_TTL_DAYS = 7

    def __init__(self, checkpoints_dir: Optional[str] = None):
        """Initialize checkpoint manager."""
        if checkpoints_dir:
            self.CHECKPOINTS_DIR = Path(checkpoints_dir)
        self.CHECKPOINTS_DIR.mkdir(parents=True, exist_ok=True)

    def save_pre_tool_checkpoint(
        self,
        agent_state: Dict[str, Any],
        tool_name: str,
        parameters: Dict[str, Any],
        conversation_id: str,
        dataset_name: str,
        description: str = "",
    ) -> str:
        """
        Save agent state before attempting a tool call.

        Args:
            agent_state: Full agent state (buffer, memory, config)
            tool_name: Name of tool about to execute
            parameters: Tool parameters
            conversation_id: Conversation ID
            dataset_name: Dataset name
            description: Human-readable description of what tool will do

        Returns:
            checkpoint_id for later recovery
        """
        checkpoint_id = str(uuid.uuid4())

        # Create tool attempt info
        tool_attempt = ToolAttempt(
            tool_name=tool_name,
            parameters=parameters,
            start_time=datetime.now().isoformat(),
            description=description,
        )

        # Create checkpoint metadata
        metadata = CheckpointMetadata(
            checkpoint_id=checkpoint_id,
            conversation_id=conversation_id,
            dataset_name=dataset_name,
            timestamp=datetime.now().isoformat(),
            tool_attempt=tool_attempt,
            agent_state_hash=self._hash_state(agent_state),
            status="pending_recovery",
        )

        try:
            # Save checkpoint directory
            checkpoint_dir = self.CHECKPOINTS_DIR / checkpoint_id
            checkpoint_dir.mkdir(parents=True, exist_ok=True)

            # Save metadata
            metadata_path = checkpoint_dir / "metadata.json"
            with open(metadata_path, "w") as f:
                json.dump(asdict(metadata), f, indent=2, default=str)

            # Save full agent state
            state_path = checkpoint_dir / "agent_state.json"
            with open(state_path, "w") as f:
                json.dump(agent_state, f, indent=2, default=str)

            logger.info(
                f"Checkpoint saved: {checkpoint_id} for tool {tool_name} "
                f"(conv: {conversation_id}, dataset: {dataset_name})"
            )

            return checkpoint_id

        except Exception as e:
            logger.error(f"Failed to save checkpoint {checkpoint_id}: {e}")
            raise

    def restore_from_checkpoint(self, checkpoint_id: str) -> Optional[Dict[str, Any]]:
        """
        Restore agent state from a checkpoint.

        Args:
            checkpoint_id: Checkpoint ID to restore from

        Returns:
            Agent state dict, or None if checkpoint not found/invalid
        """
        checkpoint_dir = self._get_checkpoint_dir(checkpoint_id)

        if not checkpoint_dir.exists():
            logger.warning(f"Checkpoint not found: {checkpoint_id}")
            return None

        try:
            # Load metadata
            metadata_path = checkpoint_dir / "metadata.json"
            if not metadata_path.exists():
                logger.error(f"Metadata missing for checkpoint: {checkpoint_id}")
                return None

            with open(metadata_path, "r") as f:
                metadata = json.load(f)

            # Check if checkpoint is expired
            checkpoint_time = datetime.fromisoformat(metadata["timestamp"])
            if (datetime.now() - checkpoint_time) > timedelta(days=self.CHECKPOINT_TTL_DAYS):
                logger.warning(f"Checkpoint expired: {checkpoint_id}")
                self._update_status(checkpoint_id, "expired")
                return None

            # Load agent state
            state_path = checkpoint_dir / "agent_state.json"
            if not state_path.exists():
                logger.error(f"Agent state missing for checkpoint: {checkpoint_id}")
                return None

            with open(state_path, "r") as f:
                agent_state = json.load(f)

            # Mark recovery attempt
            self._record_recovery_attempt(checkpoint_id)

            logger.info(
                f"Restored from checkpoint: {checkpoint_id} "
                f"(tool: {metadata['tool_attempt']['tool_name']}, "
                f"conv: {metadata['conversation_id']})"
            )

            return agent_state

        except Exception as e:
            logger.error(f"Failed to restore checkpoint {checkpoint_id}: {e}")
            return None

    def get_checkpoint_metadata(self, checkpoint_id: str) -> Optional[CheckpointMetadata]:
        """Get metadata for a checkpoint."""
        checkpoint_dir = self._get_checkpoint_dir(checkpoint_id)
        metadata_path = checkpoint_dir / "metadata.json"

        if not metadata_path.exists():
            return None

        try:
            with open(metadata_path, "r") as f:
                data = json.load(f)
                # Reconstruct nested dataclass from dict
                if isinstance(data.get("tool_attempt"), dict):
                    data["tool_attempt"] = ToolAttempt(**data["tool_attempt"])
                return CheckpointMetadata(**data)
        except Exception as e:
            logger.error(f"Failed to load checkpoint metadata {checkpoint_id}: {e}")
            return None

    def mark_recovery_successful(self, checkpoint_id: str) -> None:
        """Mark a checkpoint as successfully recovered from."""
        self._update_status(checkpoint_id, "recovered")
        logger.info(f"Checkpoint marked as recovered: {checkpoint_id}")

    def mark_recovery_failed(self, checkpoint_id: str) -> None:
        """Mark a checkpoint recovery as failed (gave up)."""
        self._update_status(checkpoint_id, "failed")
        logger.warning(f"Checkpoint marked as failed: {checkpoint_id}")

    def list_checkpoints(
        self,
        conversation_id: Optional[str] = None,
        status: Optional[str] = None,
    ) -> List[CheckpointMetadata]:
        """
        List all checkpoints with optional filtering.

        Args:
            conversation_id: Filter by conversation
            status: Filter by status (pending_recovery, recovered, failed, expired)

        Returns:
            List of checkpoint metadata
        """
        checkpoints = []

        for checkpoint_dir in self.CHECKPOINTS_DIR.iterdir():
            if not checkpoint_dir.is_dir():
                continue

            metadata = self.get_checkpoint_metadata(checkpoint_dir.name)
            if not metadata:
                continue

            # Apply filters
            if conversation_id and metadata.conversation_id != conversation_id:
                continue
            if status and metadata.status != status:
                continue

            checkpoints.append(metadata)

        # Sort by timestamp (newest first)
        checkpoints.sort(key=lambda x: x.timestamp, reverse=True)
        return checkpoints

    def get_recovery_candidates(self, conversation_id: str) -> List[CheckpointMetadata]:
        """
        Get all pending checkpoints for a conversation (candidates for recovery).

        Args:
            conversation_id: Conversation ID

        Returns:
            List of pending checkpoints
        """
        return self.list_checkpoints(
            conversation_id=conversation_id,
            status="pending_recovery",
        )

    def cleanup_old_checkpoints(self, days: Optional[int] = None) -> int:
        """
        Clean up expired checkpoints.

        Args:
            days: Number of days to keep (default: CHECKPOINT_TTL_DAYS)

        Returns:
            Number of checkpoints deleted
        """
        if days is None:
            days = self.CHECKPOINT_TTL_DAYS

        cutoff = datetime.now() - timedelta(days=days)
        deleted = 0

        for checkpoint_dir in self.CHECKPOINTS_DIR.iterdir():
            if not checkpoint_dir.is_dir():
                continue

            metadata = self.get_checkpoint_metadata(checkpoint_dir.name)
            if not metadata:
                continue

            checkpoint_time = datetime.fromisoformat(metadata.timestamp)
            if checkpoint_time < cutoff:
                try:
                    import shutil
                    shutil.rmtree(checkpoint_dir)
                    deleted += 1
                    logger.debug(f"Deleted checkpoint: {checkpoint_dir.name}")
                except Exception as e:
                    logger.error(f"Failed to delete checkpoint {checkpoint_dir.name}: {e}")

        logger.info(f"Cleanup complete: {deleted} checkpoints deleted")
        return deleted

    def get_checkpoint_stats(self) -> Dict[str, Any]:
        """Get statistics about all checkpoints."""
        all_checkpoints = self.list_checkpoints()

        stats = {
            "total": len(all_checkpoints),
            "by_status": {},
            "by_tool": {},
            "pending_recovery_count": 0,
        }

        for checkpoint in all_checkpoints:
            # Count by status
            status = checkpoint.status
            stats["by_status"][status] = stats["by_status"].get(status, 0) + 1

            # Count by tool
            tool = checkpoint.tool_attempt.tool_name
            stats["by_tool"][tool] = stats["by_tool"].get(tool, 0) + 1

            # Count pending
            if status == "pending_recovery":
                stats["pending_recovery_count"] += 1

        return stats

    # Private methods

    def _get_checkpoint_dir(self, checkpoint_id: str) -> Path:
        """Get directory for a checkpoint."""
        return self.CHECKPOINTS_DIR / checkpoint_id

    @staticmethod
    def _hash_state(state: Dict[str, Any]) -> str:
        """Create a hash of agent state for validation."""
        state_str = json.dumps(state, sort_keys=True, default=str)
        return hashlib.md5(state_str.encode()).hexdigest()

    def _update_status(self, checkpoint_id: str, new_status: str) -> None:
        """Update checkpoint status."""
        checkpoint_dir = self._get_checkpoint_dir(checkpoint_id)
        metadata_path = checkpoint_dir / "metadata.json"

        if not metadata_path.exists():
            return

        try:
            with open(metadata_path, "r") as f:
                data = json.load(f)

            data["status"] = new_status
            data["last_recovery_attempt"] = datetime.now().isoformat()

            with open(metadata_path, "w") as f:
                json.dump(data, f, indent=2, default=str)

        except Exception as e:
            logger.error(f"Failed to update checkpoint status: {e}")

    def _record_recovery_attempt(self, checkpoint_id: str) -> None:
        """Record that recovery was attempted."""
        checkpoint_dir = self._get_checkpoint_dir(checkpoint_id)
        metadata_path = checkpoint_dir / "metadata.json"

        if not metadata_path.exists():
            return

        try:
            with open(metadata_path, "r") as f:
                data = json.load(f)

            data["recovery_attempts"] += 1
            data["last_recovery_attempt"] = datetime.now().isoformat()

            with open(metadata_path, "w") as f:
                json.dump(data, f, indent=2, default=str)

        except Exception as e:
            logger.error(f"Failed to record recovery attempt: {e}")
