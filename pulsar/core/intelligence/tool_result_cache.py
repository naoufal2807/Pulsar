"""
Layer 3: ToolResultCache - Compress and cache tool results.

Stores large tool outputs to disk instead of memory/context:
- Execute tool, get result (50KB+ JSON)
- Write full result to disk (cache_dir/[hash].json)
- Generate 100-200 char summary
- Add only summary to agent context
- Full result available for debugging

Problem solved:
- 50KB tool results stored in-memory bloats heap
- Same results re-sent to API 5+ times wastes tokens
- No way to debug without full results

Solution:
- Content-addressable storage (MD5 of params)
- Full result on disk, summary in memory
- 90% memory reduction
- Automatic cleanup of old results

Expected savings: 90% memory reduction for tool outputs
"""

import json
import hashlib
import logging
import os
from pathlib import Path
from datetime import datetime
from typing import Dict, Any, Tuple, Optional

logger = logging.getLogger(__name__)


class ToolResultCache:
    """Compress and cache tool results."""

    def __init__(self, cache_dir: str = "tool_results"):
        """
        Initialize result cache.

        Args:
            cache_dir: Directory for storing cached results
        """
        self.cache_dir = Path(cache_dir)
        self.cache_dir.mkdir(parents=True, exist_ok=True)
        logger.info(f"ToolResultCache initialized at {cache_dir}")

    def store_result(
        self,
        tool_name: str,
        params: Dict[str, Any],
        full_result: Any,
    ) -> Tuple[str, str]:
        """
        Store full result to disk, return (result_id, summary).

        Args:
            tool_name: Name of the tool
            params: Tool parameters (dict)
            full_result: Full result from tool execution

        Returns:
            (result_id, summary_for_context) tuple
        """
        # Create deterministic cache key
        key = f"{tool_name}:{json.dumps(params, sort_keys=True)}"
        result_id = hashlib.md5(key.encode()).hexdigest()

        # Write full result to disk
        result_path = self.cache_dir / f"{result_id}.json"

        cache_entry = {
            "tool": tool_name,
            "params": params,
            "result": full_result,
            "timestamp": datetime.utcnow().isoformat(),
            "size_bytes": len(json.dumps(full_result)),
        }

        with open(result_path, 'w') as f:
            json.dump(cache_entry, f, indent=2)

        # Generate summary for model context
        summary = self._compress_result(tool_name, full_result)

        logger.debug(
            f"Cached tool result: {tool_name}, size={cache_entry['size_bytes']} bytes, "
            f"summary={len(summary)} chars, cache_id={result_id}"
        )

        return result_id, summary

    @staticmethod
    def _compress_result(tool_name: str, result: Any) -> str:
        """
        Convert large result into 100-200 char summary.

        Args:
            tool_name: Tool name for context
            result: Result to compress

        Returns:
            Summary string
        """
        # Handle dict results (queries, API responses)
        if isinstance(result, dict):
            # Error case
            if "error" in result:
                return f"[ERROR] {result['error']}"

            # Query results (rows + columns)
            if "rows" in result and isinstance(result["rows"], list):
                rows = result["rows"]
                cols = len(rows[0].keys()) if rows else 0
                return f"Query: {len(rows)} rows, {cols} cols"

            # Count results
            if "count" in result:
                return f"Result: {result.get('count', 0)} items"

            # Statistics results
            if "mean" in result or "std" in result or "min" in result:
                keys = list(result.keys())
                sample = ", ".join(keys[:3])
                return f"Stats: {sample}"

            # Generic dict summary
            keys = list(result.keys())[:3]
            return f"Dict: {', '.join(keys)}"

        # Handle list results
        if isinstance(result, list):
            if len(result) == 0:
                return "Empty list"

            # Sample first few items
            sample = ", ".join(str(item)[:15] for item in result[:3])
            return f"List: {len(result)} items [{sample}...]"

        # Handle string/int results
        text = str(result)
        if len(text) > 150:
            return text[:150] + "..."

        return text

    def get_full_result(self, result_id: str) -> Optional[Dict[str, Any]]:
        """
        Retrieve full result if needed for debugging.

        Args:
            result_id: Cache result ID

        Returns:
            Full cache entry or None
        """
        result_path = self.cache_dir / f"{result_id}.json"

        if result_path.exists():
            with open(result_path, 'r') as f:
                return json.load(f)

        logger.warning(f"Cache miss: {result_id}")
        return None

    def cleanup(self, max_age_days: int = 7) -> int:
        """
        Remove old cached results.

        Args:
            max_age_days: Delete results older than this

        Returns:
            Number of files deleted
        """
        import time

        now = time.time()
        max_age_seconds = max_age_days * 86400
        deleted_count = 0

        for filepath in self.cache_dir.glob("*.json"):
            file_age = now - filepath.stat().st_mtime

            if file_age > max_age_seconds:
                filepath.unlink()
                deleted_count += 1
                logger.debug(f"Cleaned up {filepath.name}")

        if deleted_count > 0:
            logger.info(f"Cleaned up {deleted_count} old cache files")

        return deleted_count

    def get_stats(self) -> Dict[str, Any]:
        """Get cache statistics."""
        total_size = 0
        file_count = 0

        for filepath in self.cache_dir.glob("*.json"):
            file_count += 1
            total_size += filepath.stat().st_size

        return {
            "cached_results": file_count,
            "total_size_bytes": total_size,
            "avg_result_size_bytes": total_size // max(1, file_count),
        }
