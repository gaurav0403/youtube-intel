"""Monitoring Report — narrative analysis across all tracked channels."""

from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List

from sqlalchemy import select

from backend.config import settings
from backend.database import async_session
from backend.models import ChannelVideo, WatchedChannel
from backend.services.gemini_client import extract_json, generate
from backend.services.youtube_transcript import get_transcripts_batch_async

logger = logging.getLogger(__name__)

# ─── 4 macro groups ─────────────────────────────────────────────────────────

MACRO_GROUPS: Dict[str, str] = {
    # Mainstream Media
    "Mainstream News": "Mainstream Media",
    "Mainstream Digital": "Mainstream Media",
    "Government Official": "Mainstream Media",
    "Global News": "Mainstream Media",
    "Global / India Policy": "Mainstream Media",
    "Geopolitics / Global": "Mainstream Media",
    # Independent & Digital
    "Independent Commentator": "Independent & Digital",
    "Independent Journalism": "Independent & Digital",
    "Independent / Critical": "Independent & Digital",
    "Independent / Explainer": "Independent & Digital",
    "Independent Commentary": "Independent & Digital",
    "Independent": "Independent & Digital",
    "Digital News": "Independent & Digital",
    "Digital Journalism": "Independent & Digital",
    "Critical Commentary": "Independent & Digital",
    "Right-Leaning Commentary": "Independent & Digital",
    "Nationalist Commentary": "Independent & Digital",
    "Political Interviews": "Independent & Digital",
    "Political Satire": "Independent & Digital",
    "Fact-Checking / Social": "Independent & Digital",
    "Ground Reporting": "Independent & Digital",
    "Media Criticism": "Independent & Digital",
    "Public Opinion": "Independent & Digital",
    "Social Issues": "Independent & Digital",
    "Social/Dalit Issues": "Independent & Digital",
    "Agrarian Politics": "Independent & Digital",
    "Investigative": "Independent & Digital",
    "Legal / Political": "Independent & Digital",
    "Political Magazine": "Independent & Digital",
    # Regional
    "Regional (Tamil)": "Regional",
    "Regional (Malayalam)": "Regional",
    "Regional (Telugu)": "Regional",
    "Regional (Kannada)": "Regional",
    "Regional (Marathi)": "Regional",
    "Regional (Bengali)": "Regional",
    "Regional (Punjabi)": "Regional",
    "Regional (Hindi Belt)": "Regional",
    "Regional (Odia)": "Regional",
    "Regional (South)": "Regional",
    "Regional News": "Regional",
    "South India Focus": "Regional",
    "South-centric Policy": "Regional",
    # Specialist & Policy
    "Defense / Strategy": "Specialist & Policy",
    "Defense / Nationalism": "Specialist & Policy",
    "Geopolitics": "Specialist & Policy",
    "Geopolitics / Strategy": "Specialist & Policy",
    "Policy / Geopolitics": "Specialist & Policy",
    "Policy / Case Studies": "Specialist & Policy",
    "Economic Policy": "Specialist & Policy",
    "Finance / Policy": "Specialist & Policy",
    "Think Tank / Policy": "Specialist & Policy",
    "Data Journalism": "Specialist & Policy",
    "Electoral Data": "Specialist & Policy",
    "Deep Analysis": "Specialist & Policy",
    "Regional Strategy": "Specialist & Policy",
}


def _get_macro_group(category: str) -> str:
    """Map a fine-grained category to one of 4 macro groups."""
    return MACRO_GROUPS.get(category, "Independent & Digital")


async def generate_monitoring_report(hours: int = 24) -> Dict[str, Any]:
    """Generate a narrative monitoring report across all tracked channels."""
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
                "group": _get_macro_group(ch.category or "Uncategorized"),
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

    # Build video data with group assignment
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
            "group": ch_info.get("group", "Independent & Digital"),
            "subscriber_count": ch_info.get("subscriber_count", 0),
            "published_at": v.published_at.isoformat() if v.published_at else "",
            "thumbnail": v.thumbnail or "",
            "view_count": v.view_count,
            "like_count": 0,
            "comment_count": 0,
            "topic_classification": v.topic_classification or "",
            "summary": v.summary or "",
        })

    # Step 2: Enrich top 50 videos with view/like/comment counts
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

    # Sort by views
    video_data.sort(key=lambda x: x["view_count"], reverse=True)

    # Step 3: Fetch transcripts for top 30 by views (free)
    transcript_ids = [v["video_id"] for v in video_data[:30]]
    transcripts = await get_transcripts_batch_async(transcript_ids)

    # Stage 1: Summarize transcripts via Haiku (if API key configured)
    haiku_cost = 0.0
    if settings.anthropic_api_key:
        from backend.services.haiku_client import summarize_transcripts_batch
        summaries, haiku_cost = await summarize_transcripts_batch(video_data[:30], transcripts)
    else:
        summaries = {}

    # Step 4: Gemini analysis
    prompt = _build_monitoring_prompt(hours, video_data, transcripts, channels, summaries)
    result_text, gemini_cost = await generate(
        prompt=prompt,
        system_instruction=(
            "You are an intelligence analyst monitoring Indian YouTube news channels. "
            "Channels are organized into 4 groups: Mainstream Media, Independent & Digital, "
            "Regional, and Specialist & Policy. Produce a structured report comparing how "
            "these 4 groups cover the same stories differently. Be specific — cite real "
            "video titles, channels, and view counts. Focus on framing differences and bias."
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
        "haiku_cost_usd": haiku_cost,
        "youtube_units_used": yt.units_used,
    }


def _build_monitoring_prompt(
    hours: int,
    videos: List[Dict],
    transcripts: Dict[str, str],
    channels: Dict[str, Dict],
    summaries: Dict[str, str] | None = None,
) -> str:
    """Build the Gemini prompt for monitoring report."""

    # Build channel roster by macro group
    by_group: Dict[str, List[str]] = {}
    for cid, ch in channels.items():
        g = ch.get("group", "Independent & Digital")
        by_group.setdefault(g, []).append(
            f"{ch['name']} [{ch.get('category', '')}] ({ch.get('subscriber_count', 0):,} subs)"
        )

    channel_block = "=== CHANNEL ROSTER (4 GROUPS) ===\n"
    for g in ["Mainstream Media", "Independent & Digital", "Regional", "Specialist & Policy"]:
        names = by_group.get(g, [])
        channel_block += f"\n{g} ({len(names)} channels):\n"
        for n in names:
            channel_block += f"  - {n}\n"

    # Group videos by macro group — top 20 per group
    vid_by_group: Dict[str, List[Dict]] = {}
    for v in videos:
        vid_by_group.setdefault(v["group"], []).append(v)

    video_block = ""
    total_in_prompt = 0
    for g in ["Mainstream Media", "Independent & Digital", "Regional", "Specialist & Policy"]:
        gvids = vid_by_group.get(g, [])
        video_block += f"\n=== {g.upper()} ({len(gvids)} videos total) ===\n"
        for v in gvids[:20]:
            if total_in_prompt >= 80:
                break
            summary = (summaries or {}).get(v["video_id"], "")
            transcript_excerpt = summary or transcripts.get(v["video_id"], "")[:300]
            video_block += (
                f"[{v['video_id']}] \"{v['title']}\" by {v['channel_name']} "
                f"| {v['view_count']:,} views | {v['like_count']:,} likes | {v['comment_count']:,} comments\n"
            )
            if transcript_excerpt:
                video_block += f"  Transcript: {transcript_excerpt}\n"
            total_in_prompt += 1

    total_views = sum(v["view_count"] for v in videos)

    return f"""Analyze the last {hours}h of content from {len(channels)} monitored Indian YouTube channels.
{len(videos)} videos detected. Total views: {total_views:,}.

Channels are organized into 4 groups:
1. MAINSTREAM MEDIA — Major TV news, government channels, global English news
2. INDEPENDENT & DIGITAL — Independent commentators, digital-first outlets, political commentary
3. REGIONAL — Regional language news channels across India
4. SPECIALIST & POLICY — Defense, geopolitics, economics, think tanks, data journalism

{channel_block}

{video_block}

Produce a JSON intelligence report with this EXACT structure:

{{
    "headline": "One crisp headline (under 15 words) about the dominant story",
    "executive_summary": "3-4 sentences. What's dominating? How do the 4 groups frame it differently? Any notable silence?",
    "total_views": {total_views},
    "narrative_angles": [
        {{
            "title": "Narrative title (specific, not generic)",
            "sentiment": "positive|negative|neutral|mixed",
            "video_count": <number>,
            "total_views": <sum of views>,
            "description": "2-3 sentences on what this narrative claims and how it's framed",
            "key_claims": ["specific claim 1", "specific claim 2"],
            "channels_pushing": ["channel1", "channel2"],
            "categories_involved": ["Mainstream Media", "Independent & Digital"],
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
            "group": "Mainstream Media",
            "channel_count": <number of channels that published>,
            "video_count": <number>,
            "total_views": <number>,
            "dominant_topic": "What they're mostly covering",
            "framing": "How they frame the dominant stories — 2-3 sentences",
            "bias_signal": "pro-government|critical|neutral|mixed",
            "notable_channels": [
                {{
                    "name": "Channel name",
                    "videos": <count>,
                    "stance": "Brief stance description"
                }}
            ]
        }},
        {{
            "group": "Independent & Digital",
            ...
        }},
        {{
            "group": "Regional",
            ...
        }},
        {{
            "group": "Specialist & Policy",
            ...
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
- EXACTLY 4 entries in group_analysis: Mainstream Media, Independent & Digital, Regional, Specialist & Policy
- At least 5 key claims tracked with real assessment
- All video_ids MUST be real IDs from the data above
- Be SPECIFIC — use actual video titles, channel names, view counts
- Focus on HOW the 4 groups frame the same story differently
- Highlight when Mainstream and Independent groups diverge on framing
- Note which groups amplify government narratives vs which are critical
"""
