"""YouTube transcript fetcher — wraps youtube-transcript-api (FREE, no quota)."""

from __future__ import annotations

import logging
from typing import Dict, List, Optional

logger = logging.getLogger(__name__)


def get_transcript(video_id: str, languages: Optional[List[str]] = None) -> str:
    """Fetch transcript for a single video. Returns empty string on failure."""
    from youtube_transcript_api import YouTubeTranscriptApi

    langs = languages or ["en", "hi", "en-IN"]
    try:
        entries = YouTubeTranscriptApi.get_transcript(video_id, languages=langs)
        return " ".join(e["text"] for e in entries)
    except Exception as e:
        logger.debug("No transcript for %s: %s", video_id, e)
        return ""


def get_transcripts_batch(video_ids: List[str]) -> Dict[str, str]:
    """Fetch transcripts for multiple videos. Returns dict of video_id → text."""
    results = {}
    for vid in video_ids:
        text = get_transcript(vid)
        if text:
            results[vid] = text
    return results
