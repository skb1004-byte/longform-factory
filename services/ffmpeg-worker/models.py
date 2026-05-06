# -*- coding: utf-8 -*-
"""
LongForm Factory - FFmpeg Worker Data Models

Pydantic models for request/response validation, job status tracking, and scene definitions.
Provides structured interfaces for API contracts and internal state serialization.
"""

from __future__ import annotations
from enum import Enum
from typing import Optional, List
from pydantic import BaseModel, Field


# ============================================================================
# Job Status Enumeration
# ============================================================================

class JobStatus(str, Enum):
    """Enumeration of job processing states throughout the pipeline."""

    PENDING = "pending"  # Initial state, awaiting processing
    QUEUED = "queued"  # Waiting in queue
    TTS_GENERATING = "tts_generating"  # Text-to-speech synthesis
    DOWNLOADING_ASSETS = "downloading_assets"  # Fetching video/image assets
    PROCESSING = "processing"  # Core video editing and composition
    RENDERING = "rendering"  # Final FFmpeg encoding pass
    COMPLETED = "completed"  # Successfully finished
    FAILED = "failed"  # Error occurred during processing
    CANCELLED = "cancelled"  # Manually cancelled by user


# ============================================================================
# Scene Definition
# ============================================================================

class Scene(BaseModel):
    """
    Represents a single video scene with media and timing information.

    Scene is the fundamental unit of video composition.
    Multiple scenes are sequenced together to form the complete output.
    """

    scene_id: str = Field(..., description="Unique identifier for this scene")
    keyword: str = Field(default="", description="Search keyword for asset discovery")
    search_query: Optional[str] = Field(default=None, description="Alias for keyword (search_query → keyword)")
    narration: str = Field(default="", description="Voice-over text (TTS source)")
    description: str = Field(default="", description="Scene context for asset matching")
    duration_seconds: float = Field(default=5.0, description="Target scene duration")
    asset_url: Optional[str] = Field(default=None, description="Primary video/image URL")
    alt_asset_url: Optional[str] = Field(default=None, description="Fallback asset URL")

    def model_post_init(self, __context) -> None:
        # Accept search_query as alias for keyword
        if self.search_query and not self.keyword:
            object.__setattr__(self, "keyword", self.search_query)

    class Config:
        json_schema_extra = {
            "example": {
                "scene_id": "sc_001",
                "keyword": "artificial intelligence",
                "narration": "AI is transforming the world.",
                "description": "Abstract AI technology visualization",
                "duration_seconds": 8.0,
                "asset_url": "https://pexels.com/video/12345",
                "alt_asset_url": "https://pixabay.com/video/67890",
            }
        }


# ============================================================================
# Auto Video Request (AI-generated content)
# ============================================================================

class AutoVideoRequest(BaseModel):
    """
    Request to automatically generate a video from text prompt using AI.

    The system will generate scenes, fetch assets, synthesize audio,
    and produce a complete video end-to-end.
    """

    job_id: str = Field(..., description="Unique job identifier")
    topic: str = Field(..., description="Video topic or subject")
    script: Optional[str] = Field(default=None, description="Full script text (optional)")
    video_type: str = Field(
        default="longform",
        description="Output format: 'longform' (16:9) or 'shorts' (9:16)",
    )
    duration_sec: int = Field(default=60, description="Target video duration in seconds")
    tone: str = Field(default="neutral", description="Narration tone: neutral, formal, casual")
    add_subtitles: bool = Field(default=True, description="Enable subtitle generation")
    add_bgm: bool = Field(default=True, description="Add background music")
    bgm_volume: float = Field(default=0.3, description="BGM volume (0.0-1.0)")
    image_mode: str = Field(
        default="stock",
        description="Asset source: 'stock' (Pexels/Pixabay) or 'ai' (WaveSpeed FLUX + DALL-E fallback)",
    )

    class Config:
        json_schema_extra = {
            "example": {
                "job_id": "job_20260430_001",
                "topic": "The future of quantum computing",
                "script": None,
                "video_type": "longform",
                "duration_sec": 120,
                "tone": "formal",
                "add_subtitles": True,
                "add_bgm": True,
                "bgm_volume": 0.25,
            }
        }


# ============================================================================
# Video Creation Request (Traditional)
# ============================================================================

class VideoCreateRequest(BaseModel):
    """
    Request to create a video from pre-defined scenes and assets.

    Maintains backward compatibility with existing API consumers.
    Scene data and asset URLs are provided directly; no AI generation.
    """

    job_id: str = Field(..., description="Unique job identifier")
    resolution: str = Field(default="1920x1080", description="Output resolution")
    fps: int = Field(default=30, description="Frames per second")
    add_subtitles: bool = Field(default=False, description="Generate and burn subtitles")
    add_bgm: bool = Field(default=True, description="Mix background music")
    bgm_volume: float = Field(default=0.3, description="BGM volume (0.0-1.0)")
    scenes: Optional[List[Scene]] = Field(default=None, description="List of scenes to render")
    audio_url: Optional[str] = Field(default=None, description="Pre-recorded voiceover audio")
    title: Optional[str] = Field(default=None, description="Video title for metadata")

    class Config:
        json_schema_extra = {
            "example": {
                "job_id": "job_20260430_002",
                "resolution": "1920x1080",
                "fps": 30,
                "add_subtitles": True,
                "add_bgm": True,
                "bgm_volume": 0.25,
                "scenes": [
                    {
                        "scene_id": "sc_001",
                        "narration": "Welcome to the show.",
                        "duration_seconds": 5.0,
                        "asset_url": "https://example.com/intro.mp4",
                    }
                ],
                "audio_url": "https://example.com/voiceover.mp3",
                "title": "My Amazing Video",
            }
        }


# ============================================================================
# Video Creation Response
# ============================================================================

class VideoCreateResponse(BaseModel):
    """Response confirming video creation request was accepted."""

    job_id: str = Field(..., description="Echoed job identifier")
    status: JobStatus = Field(..., description="Current processing status")
    message: Optional[str] = Field(default=None, description="Status message or error")
    output_path: Optional[str] = Field(default=None, description="Final output file path (when ready)")

    class Config:
        json_schema_extra = {
            "example": {
                "job_id": "job_20260430_001",
                "status": "queued",
                "message": "Job accepted and queued for processing",
                "output_path": None,
            }
        }


# ============================================================================
# Job Status Query Response
# ============================================================================

class JobStatusResponse(BaseModel):
    """Response to job status query."""

    job_id: str = Field(..., description="Job identifier")
    status: JobStatus = Field(..., description="Current job status")
    progress_percent: int = Field(default=0, description="Completion percentage (0-100)")
    current_stage: Optional[str] = Field(default=None, description="Current processing stage")
    error_message: Optional[str] = Field(default=None, description="Error details if failed")
    output_path: Optional[str] = Field(default=None, description="Output file path when complete")
    created_at: Optional[str] = Field(default=None, description="ISO 8601 creation timestamp")
    updated_at: Optional[str] = Field(default=None, description="ISO 8601 last-update timestamp")

    class Config:
        json_schema_extra = {
            "example": {
                "job_id": "job_20260430_001",
                "status": "rendering",
                "progress_percent": 75,
                "current_stage": "ffmpeg_encode",
                "error_message": None,
                "output_path": None,
                "created_at": "2026-04-30T10:00:00Z",
                "updated_at": "2026-04-30T10:15:30Z",
            }
        }
