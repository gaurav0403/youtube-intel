"""YouTube Narrative Report — search, transcripts, comments → Gemini analysis."""

from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List

from backend.config import settings
from backend.services.gemini_client import extract_json, generate
from backend.services.youtube_client import YouTubeClient
from backend.services.youtube_transcript import get_transcripts_batch_async

logger = logging.getLogger(__name__)


async def generate_youtube_report(
    topic: str,
    hours: int = 168,
) -> Dict[str, Any]:
    """Generate a YouTube narrative intelligence report.

    Steps:
    1. Search YouTube (3 queries: relevance, date, viewCount)
    2. Batch fetch video details
    3. Fetch transcripts (free)
    4. Fetch top comments for top videos
    5. Gemini analysis

    Args:
        topic: Search topic.
        hours: Time window in hours (default 7 days).

    Returns:
        Dict with videos, analysis, costs.
    """
    if not settings.youtube_api_key:
        return {"error": "YOUTUBE_API_KEY not configured"}
    if not settings.gemini_api_key:
        return {"error": "GEMINI_API_KEY not configured"}

    yt = YouTubeClient()
    try:
        return await _run_report(yt, topic, hours)
    finally:
        await yt.close()


async def _run_report(
    yt: YouTubeClient,
    topic: str,
    hours: int,
) -> Dict[str, Any]:
    # Time filter
    after = (datetime.now(timezone.utc) - timedelta(hours=hours)).strftime("%Y-%m-%dT%H:%M:%SZ")

    # Step 1: Search (3 queries, deduplicate)
    seen_ids: set[str] = set()
    all_videos: List[Dict] = []

    for order in ["relevance", "date", "viewCount"]:
        results = await yt.search_videos(topic, max_results=10, order=order, published_after=after)
        for v in results:
            if v["video_id"] not in seen_ids:
                seen_ids.add(v["video_id"])
                all_videos.append(v)

    if not all_videos:
        return {
            "error": f"No YouTube videos found for '{topic}' in the last {hours}h",
            "videos": [],
            "analysis": None,
            "youtube_units_used": yt.units_used,
            "gemini_cost_usd": 0,
        }

    # Step 2: Batch video details
    video_ids = [v["video_id"] for v in all_videos]
    details = await yt.get_video_details(video_ids)
    details_map = {d["video_id"]: d for d in details}

    # Merge search snippets with full details
    videos = []
    for v in all_videos:
        d = details_map.get(v["video_id"], {})
        videos.append({**v, **d})

    # Sort by views descending
    videos.sort(key=lambda x: x.get("view_count", 0), reverse=True)

    # Step 3: Transcripts (free, top 15) — runs in thread pool
    top_ids = [v["video_id"] for v in videos[:15]]
    transcripts = await get_transcripts_batch_async(top_ids)

    # Step 4: Comments for top 10 videos
    comments_map: Dict[str, List[Dict]] = {}
    for v in videos[:10]:
        vid = v["video_id"]
        comments_map[vid] = await yt.get_video_comments(vid, max_results=20)

    # Step 5: Channel info (deduplicate, top 10 channels by video count)
    channel_ids: Dict[str, int] = {}
    for v in videos:
        cid = v.get("channel_id", "")
        if cid:
            channel_ids[cid] = channel_ids.get(cid, 0) + 1
    top_channel_ids = sorted(channel_ids, key=channel_ids.get, reverse=True)[:10]
    channels_map: Dict[str, Dict] = {}
    for cid in top_channel_ids:
        info = await yt.get_channel_info(cid)
        if info:
            channels_map[cid] = info

    # Step 6: Build Gemini prompt
    prompt = _build_prompt(topic, hours, videos, transcripts, comments_map, channels_map)
    result_text, gemini_cost = await generate(
        prompt=prompt,
        system_instruction="You are a YouTube narrative intelligence analyst. Analyze YouTube videos covering a topic and produce a structured intelligence report in JSON.",
    )

    analysis = extract_json(result_text)

    return {
        "topic": topic,
        "hours": hours,
        "video_count": len(videos),
        "videos": [
            {
                "video_id": v["video_id"],
                "title": v.get("title", ""),
                "channel_title": v.get("channel_title", ""),
                "channel_id": v.get("channel_id", ""),
                "published_at": v.get("published_at", ""),
                "thumbnail": v.get("thumbnail", ""),
                "view_count": v.get("view_count", 0),
                "like_count": v.get("like_count", 0),
                "comment_count": v.get("comment_count", 0),
                "duration": v.get("duration", ""),
                "has_transcript": v["video_id"] in transcripts,
            }
            for v in videos
        ],
        "analysis": analysis,
        "youtube_units_used": yt.units_used,
        "gemini_cost_usd": gemini_cost,
    }


def _build_prompt(
    topic: str,
    hours: int,
    videos: List[Dict],
    transcripts: Dict[str, str],
    comments_map: Dict[str, List[Dict]],
    channels_map: Dict[str, Dict] | None = None,
) -> str:
    """Build the Gemini analysis prompt with all collected data."""

    # Channel info block
    channel_block = ""
    if channels_map:
        channel_block = "\n=== CHANNEL INFO ===\n"
        for cid, ch in channels_map.items():
            channel_block += (
                f"- {ch.get('title', 'Unknown')} (ID: {cid}): "
                f"{ch.get('subscriber_count', 0):,} subscribers, "
                f"{ch.get('video_count', 0):,} total videos\n"
            )

    # Video summaries
    video_block = ""
    for i, v in enumerate(videos[:20], 1):
        vid = v["video_id"]
        transcript_excerpt = transcripts.get(vid, "")[:800]
        top_comments = comments_map.get(vid, [])[:5]
        comments_text = "\n".join(f"  - {c['author']}: {c['text'][:200]}" for c in top_comments)

        video_block += f"""
--- VIDEO {i} ---
Title: {v.get('title', '')}
Channel: {v.get('channel_title', '')}
Views: {v.get('view_count', 0):,} | Likes: {v.get('like_count', 0):,} | Comments: {v.get('comment_count', 0):,}
Published: {v.get('published_at', '')}
URL: https://youtube.com/watch?v={vid}
{'Transcript excerpt: ' + transcript_excerpt if transcript_excerpt else 'No transcript available'}
{'Top comments:' + chr(10) + comments_text if comments_text else 'No comments available'}
"""

    return f"""Analyze YouTube coverage of the topic: "{topic}" (last {hours} hours).

I found {len(videos)} videos. Here are the details:
{channel_block}
{video_block}

Produce a JSON intelligence report with this EXACT structure:

{{
    "executive_summary": "2-3 sentence overview of how this topic is being covered on YouTube",
    "total_views": <sum of all video views>,
    "narrative_angles": [
        {{
            "title": "Short title for this narrative angle",
            "sentiment": "positive|negative|neutral|mixed",
            "video_count": <number of videos pushing this angle>,
            "description": "What this angle claims or frames",
            "key_claims": ["claim1", "claim2"],
            "top_videos": [
                {{
                    "video_id": "...",
                    "title": "...",
                    "channel": "...",
                    "views": <number>,
                    "why": "Why this video represents this angle"
                }}
            ]
        }}
    ],
    "channel_analysis": [
        {{
            "channel_name": "...",
            "channel_id": "...",
            "subscriber_count": <number or 0 if unknown>,
            "videos_on_topic": <count>,
            "bias": "pro-government|pro-opposition|neutral|independent|sensationalist",
            "influence_score": "high|medium|low"
        }}
    ],
    "comment_sentiment": {{
        "positive_pct": <0-100>,
        "negative_pct": <0-100>,
        "neutral_pct": <0-100>,
        "top_themes": ["theme1", "theme2", "theme3"]
    }},
    "key_claims_tracked": [
        {{
            "claim": "A specific factual claim made in videos",
            "videos_making_claim": <count>,
            "assessment": "Verified|Unverified|Misleading|Partially True"
        }}
    ],
    "trending_signals": {{
        "velocity": "rising|stable|declining",
        "peak_period": "When most videos were published",
        "prediction": "What to expect in next 24-48h"
    }},
    "related_topics": ["topic1", "topic2"]
}}

REQUIREMENTS:
- At least 3 narrative angles
- At least 3 channels in channel_analysis — use REAL subscriber counts from CHANNEL INFO above
- At least 3 key claims tracked
- All video_ids must be real IDs from the data above
- Sort narrative angles by video_count descending
"""
