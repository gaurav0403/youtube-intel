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
    2. Enrich top videos with view counts via YouTube API
    3. Fetch transcripts (free)
    4. Feed to Gemini for narrative analysis
    """
    cutoff = datetime.now(timezone.utc) - timedelta(hours=hours)

    # Step 1: Get recent videos and channel info
    async with async_session() as db:
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
            "youtube_units_used": 0,
            "error": f"No videos found from monitored channels in the last {hours}h. Try polling first.",
        }

    # Build video data
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
            "like_count": 0,
            "comment_count": 0,
            "topic_classification": v.topic_classification or "",
            "summary": v.summary or "",
        })

    # Step 2: Enrich top 50 videos with view/like/comment counts via YouTube API
    from backend.services.youtube_client import YouTubeClient
    yt = YouTubeClient()
    try:
        top_ids = [v["video_id"] for v in video_data[:50]]
        details = await yt.get_video_details(top_ids)
        details_map = {d["video_id"]: d for d in details}
        for v in video_data:
            d = details_map.get(v["video_id"])
            if d:
                v["view_count"] = d.get("view_count", 0)
                v["like_count"] = d.get("like_count", 0)
                v["comment_count"] = d.get("comment_count", 0)
    finally:
        await yt.close()

    # Sort by views for display
    video_data.sort(key=lambda x: x["view_count"], reverse=True)

    # Step 3: Fetch transcripts for top 30 by views (free)
    transcript_ids = [v["video_id"] for v in video_data[:30]]
    transcripts = await get_transcripts_batch_async(transcript_ids)

    # Step 4: Gemini analysis
    prompt = _build_monitoring_prompt(hours, video_data, transcripts, channels)
    result_text, gemini_cost = await generate(
        prompt=prompt,
        system_instruction=(
            "You are an intelligence analyst monitoring Indian YouTube news channels. "
            "Produce a structured report showing how different channel groups cover the same stories. "
            "Be specific — cite real video titles, channels, and view counts. "
            "Focus on framing differences, bias patterns, and narrative divergence between groups."
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
        "youtube_units_used": yt.units_used,
    }


def _build_monitoring_prompt(
    hours: int,
    videos: List[Dict],
    transcripts: Dict[str, str],
    channels: Dict[str, Dict],
) -> str:
    """Build the Gemini prompt for monitoring report."""

    # Group channels by category
    cat_summary: Dict[str, List[str]] = {}
    for cid, ch in channels.items():
        cat = ch.get("category", "Uncategorized")
        cat_summary.setdefault(cat, []).append(ch["name"])

    channel_block = "=== CHANNEL GROUPS ===\n"
    for cat in sorted(cat_summary.keys()):
        channel_block += f"{cat}: {', '.join(cat_summary[cat])}\n"

    # Group videos by category for the prompt
    by_cat: Dict[str, List[Dict]] = {}
    for v in videos:
        by_cat.setdefault(v["category"], []).append(v)

    video_block = ""
    for cat in sorted(by_cat.keys()):
        cat_vids = by_cat[cat][:10]  # Top 10 per category
        video_block += f"\n=== {cat.upper()} ({len(by_cat[cat])} videos) ===\n"
        for v in cat_vids:
            transcript_excerpt = transcripts.get(v["video_id"], "")[:400]
            video_block += (
                f"[{v['video_id']}] \"{v['title']}\" by {v['channel_name']} "
                f"| {v['view_count']:,} views | {v['like_count']:,} likes | {v['comment_count']:,} comments\n"
            )
            if transcript_excerpt:
                video_block += f"  Transcript: {transcript_excerpt}\n"

    total_views = sum(v["view_count"] for v in videos)

    return f"""Analyze the last {hours}h of content from {len(channels)} monitored Indian YouTube channels.
{len(videos)} videos detected. Total views: {total_views:,}.

{channel_block}

{video_block}

Produce a JSON intelligence report with this EXACT structure:

{{
    "headline": "One crisp headline (under 15 words) about the dominant story",
    "executive_summary": "3-4 sentences. What's dominating? How are different groups framing it? Any notable silence?",
    "total_views": {total_views},
    "narrative_angles": [
        {{
            "title": "Narrative title (specific, not generic)",
            "sentiment": "positive|negative|neutral|mixed",
            "video_count": <number>,
            "total_views": <sum of views for videos in this narrative>,
            "description": "2-3 sentences on what this narrative claims and how it's framed",
            "key_claims": ["specific claim 1", "specific claim 2"],
            "channels_pushing": ["channel1", "channel2"],
            "categories_involved": ["Mainstream News", "Independent Commentator"],
            "top_videos": [
                {{
                    "video_id": "real ID from data",
                    "title": "real title",
                    "channel": "channel name",
                    "views": <number>,
                    "why": "Why this video matters for this narrative"
                }}
            ]
        }}
    ],
    "group_analysis": [
        {{
            "group": "Mainstream News",
            "channel_count": <number>,
            "video_count": <number>,
            "total_views": <number>,
            "dominant_topic": "What they're mostly covering",
            "framing": "How they frame the dominant stories",
            "bias_signal": "pro-government|critical|neutral|mixed",
            "notable_channels": [
                {{
                    "name": "Channel name",
                    "videos": <count>,
                    "stance": "Brief stance description"
                }}
            ]
        }}
    ],
    "key_claims_tracked": [
        {{
            "claim": "A specific factual claim being made across videos",
            "videos_making_claim": <count>,
            "channels": ["channel1", "channel2"],
            "assessment": "Verified|Unverified|Misleading|Partially True|Contested"
        }}
    ],
    "sentiment_overview": {{
        "overall": "positive|negative|neutral|mixed",
        "pro_government_pct": <0-100>,
        "critical_pct": <0-100>,
        "neutral_analytical_pct": <0-100>,
        "most_polarizing_topic": "Topic with most divided coverage"
    }},
    "trending_signals": {{
        "velocity": "rising|stable|declining",
        "peak_period": "When most videos were published",
        "prediction": "What to expect in next 24-48h"
    }},
    "emerging_stories": [
        {{
            "topic": "Story just starting to get pickup",
            "early_signals": "Why it might grow",
            "channels_covering": ["channel1"]
        }}
    ],
    "notable_absences": "What major events/topics are NOT being covered and by which groups"
}}

REQUIREMENTS:
- At least 5 narrative angles, sorted by total_views descending
- group_analysis for EVERY channel category that published videos
- At least 5 key claims tracked with real assessment
- All video_ids MUST be real IDs from the data
- Be SPECIFIC — use actual video titles, channel names, view counts
- Focus on HOW different groups frame the same story differently
- Identify when mainstream and independent channels diverge
- Note which groups amplify government narratives vs which are critical
"""
