"""Monitoring Report — narrative analysis across all tracked channels."""

from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List

from sqlalchemy import select

from backend.database import async_session
from backend.models import ChannelVideo, WatchedChannel
from backend.services.gemini_client import extract_json, generate
from backend.services.youtube_transcript import get_transcripts_batch_async

logger = logging.getLogger(__name__)


async def generate_monitoring_report(hours: int = 24) -> Dict[str, Any]:
    """Generate a narrative monitoring report across all tracked channels.

    1. Pull all videos from monitored channels in the time window
    2. Fetch transcripts for those videos (free)
    3. Feed to Gemini for narrative analysis

    Args:
        hours: Time window (24, 48, 72).

    Returns:
        Dict with report data.
    """
    cutoff = datetime.now(timezone.utc) - timedelta(hours=hours)

    # Step 1: Get recent videos and channel info
    async with async_session() as db:
        # Get all active channels
        ch_result = await db.execute(
            select(WatchedChannel).where(WatchedChannel.is_active == True)  # noqa: E712
        )
        channels = {
            ch.channel_id: {
                "name": ch.channel_name,
                "category": ch.category or "Uncategorized",
                "subscriber_count": ch.subscriber_count,
                "thumbnail": ch.thumbnail,
            }
            for ch in ch_result.scalars().all()
        }

        # Get videos in time window
        vid_result = await db.execute(
            select(ChannelVideo)
            .where(ChannelVideo.detected_at >= cutoff)
            .order_by(ChannelVideo.published_at.desc())
        )
        videos = vid_result.scalars().all()

    if not videos:
        return {
            "hours": hours,
            "video_count": 0,
            "channel_count": 0,
            "videos": [],
            "analysis": None,
            "gemini_cost_usd": 0,
            "error": f"No videos found from monitored channels in the last {hours}h. Try polling first.",
        }

    # Build video data with channel info
    video_data = []
    channels_seen = set()
    for v in videos:
        ch_info = channels.get(v.channel_id, {})
        channels_seen.add(v.channel_id)
        video_data.append({
            "video_id": v.video_id,
            "title": v.title,
            "channel_id": v.channel_id,
            "channel_name": ch_info.get("name", "Unknown"),
            "category": ch_info.get("category", "Uncategorized"),
            "subscriber_count": ch_info.get("subscriber_count", 0),
            "published_at": v.published_at.isoformat() if v.published_at else "",
            "thumbnail": v.thumbnail or "",
            "view_count": v.view_count,
            "topic_classification": v.topic_classification or "",
            "summary": v.summary or "",
        })

    # Step 2: Fetch transcripts for up to 30 videos (free)
    transcript_ids = [v["video_id"] for v in video_data[:30]]
    transcripts = await get_transcripts_batch_async(transcript_ids)

    # Step 3: Gemini analysis
    prompt = _build_monitoring_prompt(hours, video_data, transcripts, channels)
    result_text, gemini_cost = await generate(
        prompt=prompt,
        system_instruction=(
            "You are a YouTube monitoring intelligence analyst. "
            "You analyze videos from tracked channels and produce a structured narrative report. "
            "Focus on identifying dominant narratives, how different channel categories frame stories, "
            "and emerging trends. Be specific — cite actual video titles and channels."
        ),
    )
    analysis = extract_json(result_text)

    return {
        "hours": hours,
        "video_count": len(video_data),
        "channel_count": len(channels_seen),
        "videos": video_data,
        "analysis": analysis,
        "gemini_cost_usd": gemini_cost,
    }


def _build_monitoring_prompt(
    hours: int,
    videos: List[Dict],
    transcripts: Dict[str, str],
    channels: Dict[str, Dict],
) -> str:
    """Build the Gemini prompt for monitoring report."""

    # Channel summary by category
    cat_summary: Dict[str, List[str]] = {}
    for cid, ch in channels.items():
        cat = ch.get("category", "Uncategorized")
        cat_summary.setdefault(cat, []).append(
            f"{ch['name']} ({ch.get('subscriber_count', 0):,} subs)"
        )

    channel_block = "=== MONITORED CHANNELS ===\n"
    for cat, names in sorted(cat_summary.items()):
        channel_block += f"\n{cat}:\n"
        for n in names:
            channel_block += f"  - {n}\n"

    # Video details
    video_block = ""
    for i, v in enumerate(videos[:50], 1):
        transcript_excerpt = transcripts.get(v["video_id"], "")[:600]
        video_block += f"""
--- VIDEO {i} ---
Title: {v['title']}
Channel: {v['channel_name']} [{v['category']}]
Subs: {v['subscriber_count']:,} | Views: {v['view_count']:,}
Published: {v['published_at']}
Topic tag: {v['topic_classification']}
Auto-summary: {v['summary']}
{'Transcript: ' + transcript_excerpt if transcript_excerpt else 'No transcript'}
"""

    return f"""Analyze the last {hours} hours of content from {len(channels)} monitored YouTube channels.
{len(videos)} new videos were detected. Here is the data:

{channel_block}

{video_block}

Produce a JSON monitoring intelligence report with this EXACT structure:

{{
    "headline": "One-line headline summarizing the biggest story/theme in the last {hours}h",
    "executive_summary": "3-5 sentence overview of what monitored channels are covering, dominant narratives, and notable absences",
    "dominant_narratives": [
        {{
            "title": "Narrative title",
            "description": "What this narrative is about and how it's being framed",
            "sentiment": "positive|negative|neutral|mixed",
            "video_count": <number>,
            "channels_pushing": ["channel name 1", "channel name 2"],
            "categories_involved": ["Mainstream News", "Independent Commentator"],
            "key_claims": ["claim 1", "claim 2"],
            "top_videos": [
                {{
                    "video_id": "actual video ID",
                    "title": "actual video title",
                    "channel": "channel name",
                    "views": <number>
                }}
            ]
        }}
    ],
    "category_breakdown": [
        {{
            "category": "Mainstream News",
            "video_count": <number>,
            "primary_focus": "What this category is mostly talking about",
            "notable_framing": "How their framing differs from others"
        }}
    ],
    "channel_highlights": [
        {{
            "channel_name": "...",
            "category": "...",
            "videos_published": <count>,
            "primary_topic": "What they're focused on",
            "stance": "Brief description of their angle/stance"
        }}
    ],
    "emerging_stories": [
        {{
            "topic": "Topic that's just starting to get coverage",
            "early_signals": "Why this might grow",
            "channels_covering": ["channel 1"]
        }}
    ],
    "sentiment_overview": {{
        "overall": "positive|negative|neutral|mixed",
        "pro_government_pct": <0-100>,
        "critical_pct": <0-100>,
        "neutral_pct": <0-100>,
        "most_polarizing_topic": "The topic with most divided coverage"
    }},
    "notable_absences": "Topics or events that are conspicuously NOT being covered by certain channels"
}}

REQUIREMENTS:
- At least 3 dominant narratives, sorted by video_count descending
- Category breakdown for every category that has videos
- Channel highlights for at least 5 most active channels
- All video_ids must be REAL from the data above
- Be specific — use actual video titles and channel names
- Identify framing differences between mainstream, independent, and regional channels
"""
