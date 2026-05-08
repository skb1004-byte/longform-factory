# -*- coding: utf-8 -*-
"""
LongForm Factory - FFmpeg Worker Job State Checkpoint

Persistent job state management using JSON checkpoint files.
Tracks pipeline progress, recoverable from interruption, and maintains audit trail.
"""

from __future__ import annotations
import json
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Optional

from config import JOBS_DIR


class JobState:
    """
    Manages persistent state for a video processing job.

    State is stored as JSON at {JOBS_DIR}/{job_id}/state.json
    Checkpoint tracks completed stages, payloads, and error conditions.

    Attributes:
        job_id: Unique job identifier
        job_dir: Path to job working directory
        state_file: Path to state.json checkpoint
        data: In-memory state dictionary
    """

    def __init__(self, job_id: str) -> None:
        """
        Initialize job state manager.

        Loads existing state.json if present; creates empty state otherwise.

        Args:
            job_id: Unique identifier for this job
        """
        self.job_id: str = job_id
        self.job_dir: Path = JOBS_DIR / job_id
        self.state_file: Path = self.job_dir / "state.json"

        # Ensure job directory exists (handle permission errors gracefully)
        try:
            self.job_dir.mkdir(parents=True, exist_ok=True)
        except (PermissionError, OSError):
            # Fail silently in development; will use in-memory state only
            pass

        # Load or initialize state
        if self.state_file.exists():
            with open(self.state_file, "r", encoding="utf-8") as f:
                self.data: Dict[str, Any] = json.load(f)
        else:
            self.data = {
                "job_id": job_id,
                "created_at": datetime.utcnow().isoformat() + "Z",
                "stages": {},
                "error": None,
                "request": None,
            }
            self.save()

    def save(self) -> None:
        """
        Persist current state to disk as JSON.

        Updates 'updated_at' timestamp before writing.
        """
        self.data["updated_at"] = datetime.utcnow().isoformat() + "Z"
        with open(self.state_file, "w", encoding="utf-8") as f:
            json.dump(self.data, f, indent=2, ensure_ascii=False)

    def mark(self, stage: str, payload: Optional[Dict[str, Any]] = None) -> None:
        """
        Record completion of a pipeline stage.

        Args:
            stage: Stage identifier (e.g., 'tts_generated', 'assets_downloaded')
            payload: Optional metadata to store with stage completion
        """
        if "stages" not in self.data:
            self.data["stages"] = {}

        self.data["stages"][stage] = {
            "completed_at": datetime.utcnow().isoformat() + "Z",
            "payload": payload or {},
        }
        self.save()

    def has(self, stage: str) -> bool:
        """
        Check if a stage has been completed.

        Args:
            stage: Stage identifier to query

        Returns:
            True if stage is marked complete, False otherwise
        """
        stages: Dict[str, Any] = self.data.get("stages", {})
        return stage in stages

    def unmark(self, stage: str) -> None:
        """
        Remove a completed stage from state (used for forced re-run).

        Args:
            stage: Stage identifier to clear
        """
        stages: Dict[str, Any] = self.data.get("stages", {})
        if stage in stages:
            del stages[stage]
            self.save()

    def get_payload(self, stage: str) -> Dict[str, Any]:
        """
        Retrieve metadata payload associated with a completed stage.

        Args:
            stage: Stage identifier

        Returns:
            Dictionary of stage metadata, or empty dict if not found
        """
        stages: Dict[str, Any] = self.data.get("stages", {})
        return stages.get(stage, {}).get("payload", {})

    def set_error(self, error_msg: str) -> None:
        """
        Record a fatal error condition.

        Args:
            error_msg: Error description or exception message
        """
        self.data["error"] = {
            "message": error_msg,
            "timestamp": datetime.utcnow().isoformat() + "Z",
        }
        self.save()

    def remember_request(self, request: Any) -> None:
        """
        Store the original API request for audit and replay.

        Serializes request object (dict or BaseModel) to JSON-compatible format.

        Args:
            request: Request object (typically dict or Pydantic BaseModel)
        """
        # Handle Pydantic models
        if hasattr(request, "model_dump"):
            request_data = request.model_dump()
        elif hasattr(request, "dict"):
            request_data = request.dict()
        elif isinstance(request, dict):
            request_data = request
        else:
            request_data = str(request)

        self.data["request"] = request_data
        self.save()

    def get_request(self) -> Optional[Dict[str, Any]]:
        """
        Retrieve stored request data.

        Returns:
            Request dictionary, or None if not set
        """
        req = self.data.get("request")
        return req if isinstance(req, dict) else None

    def get_stages_completed(self) -> list[str]:
        """
        Get list of all completed pipeline stages.

        Returns:
            List of stage identifiers in completion order
        """
        stages: Dict[str, Any] = self.data.get("stages", {})
        return list(stages.keys())

    def is_error(self) -> bool:
        """
        Check if job has encountered a fatal error.

        Returns:
            True if error state is set
        """
        return self.data.get("error") is not None

    def get_error(self) -> Optional[str]:
        """
        Retrieve error message if job failed.

        Returns:
            Error message string, or None if no error
        """
        err = self.data.get("error")
        if err and isinstance(err, dict):
            return err.get("message")
        return None

    def clear_error(self) -> None:
        """Clear error state (for retry scenarios)."""
        self.data["error"] = None
        self.save()

    def to_dict(self) -> Dict[str, Any]:
        """
        Export state as dictionary.

        Returns:
            Complete state dictionary (includes created_at, stages, error, etc.)
        """
        return dict(self.data)

    def to_json_str(self) -> str:
        """
        Export state as JSON string.

        Returns:
            Formatted JSON representation of state
        """
        return json.dumps(self.data, indent=2, ensure_ascii=False)
