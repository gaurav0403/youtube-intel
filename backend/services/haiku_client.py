"""Haiku transcript summarization — Stage 1 of monitoring reports."""

from __future__ import annotations

import asyncio
import logging
from typing import Any, Dict, List, Tuple

import httpx

from backend.config import settings

logger = logging.getLogger(__name__)

ANTHROPIC_URL = "https://api.anthropic.com/v1/messages"
HAIKU_MODEL = "claude-haiku-4-5-20251001"
# Haiku 4.5: $0.80/M input, $4.00/M output
INPUT_COST_PER_M = 0.80
OUTPUT_COST_PER_M = 4.00
MAX_CONCURRENT = 10


async def summarize_transcript(
    video_title: str,
    channel_name: str,
    transcript: str,
) -> Tuple[str, float]:
    """Summarize a single transcript via Haiku. Returns (summary_text, cost_usd)."""
    headers = {
        "x-api-key": settings.anthropic_api_key,
        "anthropic-version": "2023-06-01",
        "content-type": "application/json",
    }
    body = {
        "model": HAIKU_MODEL,
        "max_tokens": 300,
        "messages": [
            {
                "role": "user",
                "content": (
                    "Summarize this YouTube video transcript in 2-3 sentences.\n"
                    "Focus on: key claims made, editorial framing/angle, and "
                    "sentiment (pro-government/critical/neutral).\n\n"
                    f'Video: "{video_title}" by {channel_name}\n'
                    f"Transcript:\n{transcript}"
                ),
            }
        ],
    }

    async with httpx.AsyncClient(timeout=30.0) as client:
        resp = await client.post(ANTHROPIC_URL, headers=headers, json=body)
        resp.raise_for_status()
        data = resp.json()

    text = ""
    for block in data.get("content", []):
        if block.get("type") == "text":
            text += block.get("text", "")

    usage = data.get("usage", {})
    input_tokens = usage.get("input_tokens", 0)
    output_tokens = usage.get("output_tokens", 0)
    cost = (input_tokens * INPUT_COST_PER_M + output_tokens * OUTPUT_COST_PER_M) / 1_000_000

    return text, cost


async def summarize_transcripts_batch(
    videos: List[Dict[str, Any]],
    transcripts: Dict[str, str],
) -> Tuple[Dict[str, str], float]:
    """Summarize all transcripts concurrently. Returns ({video_id: summary}, total_cost)."""
    semaphore = asyncio.Semaphore(MAX_CONCURRENT)
    summaries: Dict[str, str] = {}
    total_cost = 0.0
    lock = asyncio.Lock()

    async def _process(video: Dict[str, Any]) -> None:
        nonlocal total_cost
        vid_id = video["video_id"]
        transcript = transcripts.get(vid_id, "")
        if not transcript:
            return
        async with semaphore:
            try:
                summary, cost = await summarize_transcript(
                    video_title=video["title"],
                    channel_name=video["channel_name"],
                    transcript=transcript,
                )
                async with lock:
                    summaries[vid_id] = summary
                    total_cost += cost
            except Exception:
                logger.warning("Haiku summarization failed for %s, falling back to truncation", vid_id, exc_info=True)

    await asyncio.gather(*[_process(v) for v in videos])

    logger.info("Haiku summarized %d/%d transcripts (cost: $%.4f)", len(summaries), len(transcripts), total_cost)
    return summaries, total_cost
