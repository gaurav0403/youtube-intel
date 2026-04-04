"""YouTube transcript fetcher — wraps youtube-transcript-api (FREE, no quota)."""

from __future__ import annotations

import asyncio
import logging
from typing import Dict, List, Optional

from youtube_transcript_api import YouTubeTranscriptApi

logger = logging.getLogger(__name__)

_api = YouTubeTranscriptApi()


def get_transcript(video_id: str, languages: Optional[List[str]] = None) -> str:
    """Fetch transcript for a single video. Returns empty string on failure."""
    langs = languages or ["en", "hi", "en-IN"]
    try:
        result = _api.fetch(video_id, languages=langs)
        return " ".join(snippet.text for snippet in result.snippets)
    except Exception as e:
        logger.debug("No transcript for %s: %s", video_id, e)
        return ""


async def get_transcripts_batch_async(video_ids: List[str]) -> Dict[str, str]:
    """Fetch transcripts for multiple videos in a thread pool. Returns dict of video_id → text."""
    loop = asyncio.get_event_loop()

    async def _fetch_one(vid: str) -> tuple[str, str]:
        text = await loop.run_in_executor(None, get_transcript, vid)
        return vid, text

    tasks = [_fetch_one(vid) for vid in video_ids]
    pairs = await asyncio.gather(*tasks)
    return {vid: text for vid, text in pairs if text}
