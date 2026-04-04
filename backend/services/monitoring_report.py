"""Monitoring Report — narrative analysis across all tracked channels."""

from __future__ import annotations

import logging
import re
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Tuple

from sqlalchemy import select

from backend.config import settings
from backend.database import async_session
from backend.models import ChannelVideo, NarrativeState, WatchedChannel
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
    # Identity mappings (for channels seeded with macro group names directly)
    "Mainstream Media": "Mainstream Media",
    "Independent & Digital": "Independent & Digital",
    "Regional": "Regional",
    "Specialist & Policy": "Specialist & Policy",
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


# ─── State-based report generation ──────────────────────────────────────────


async def generate_report_from_state(format_pass: bool = True) -> Optional[Dict[str, Any]]:
    """Generate a monitoring report from the active NarrativeState.

    Args:
        format_pass: If True, use a light Gemini call to format state into
            polished report JSON. If False, map directly in Python (free, instant).

    Returns:
        Report dict matching existing MonitoringReport schema, or None if no active state.
    """
    async with async_session() as db:
        result = await db.execute(
            select(NarrativeState).where(NarrativeState.is_active == True)  # noqa: E712
        )
        state_row = result.scalar_one_or_none()

    if not state_row:
        return None

    state = state_row.state_json
    narratives = state.get("narratives", [])
    group_summaries = state.get("group_summaries", {})

    if format_pass:
        return await _format_with_gemini(state_row, state)
    else:
        return _format_with_python(state_row, state)


async def _format_with_gemini(
    state_row: NarrativeState,
    state: Dict[str, Any],
) -> Dict[str, Any]:
    """Use a light Gemini call to format narrative state into the report JSON schema."""
    import json

    narratives = state.get("narratives", [])
    group_summaries = state.get("group_summaries", {})

    # Build a compact representation for Gemini
    compact = json.dumps({
        "narratives": [
            {
                "title": n.get("title"),
                "sentiment": n.get("sentiment"),
                "video_count": n.get("video_count"),
                "total_views": n.get("total_views"),
                "description": n.get("description", "")[:300],
                "key_claims": [
                    (c["claim"] if isinstance(c, dict) else c)
                    for c in (n.get("key_claims") or [])[:3]
                ],
                "group_coverage": n.get("group_coverage", {}),
                "top_videos": n.get("top_videos", [])[:3],
                "velocity": n.get("velocity"),
            }
            for n in narratives[:12]
        ],
        "group_summaries": group_summaries,
        "sentiment_overview": state.get("sentiment_overview", {}),
        "emerging_stories": state.get("emerging_stories", [])[:5],
        "trending_signals": state.get("trending_signals", {}),
        "notable_absences": state.get("notable_absences", ""),
    }, indent=1)

    prompt = f"""Format this narrative intelligence state into a polished monitoring report.

State ({state_row.total_videos_processed} videos, {state_row.total_channels} channels, {len(narratives)} narratives):

{compact}

Return JSON matching this EXACT schema:
{{
    "headline": "One crisp headline (under 15 words)",
    "executive_summary": "3-4 sentences comparing how 4 groups frame the dominant stories",
    "total_views": <sum of all narrative views>,
    "narrative_angles": [
        {{
            "title": "Narrative title",
            "sentiment": "positive|negative|neutral|mixed",
            "video_count": <number>,
            "total_views": <number>,
            "description": "2-3 sentences",
            "key_claims": ["claim1", "claim2"],
            "channels_pushing": ["ch1", "ch2"],
            "categories_involved": ["Mainstream Media", "Independent & Digital"],
            "top_videos": [{{"video_id": "id", "title": "title", "channel": "ch", "views": 0, "why": "reason"}}]
        }}
    ],
    "group_analysis": [
        {{
            "group": "Mainstream Media",
            "channel_count": 0, "video_count": 0, "total_views": 0,
            "dominant_topic": "topic", "framing": "2-3 sentences",
            "bias_signal": "pro-government|critical|neutral|mixed",
            "notable_channels": [{{"name": "ch", "videos": 0, "stance": "brief"}}]
        }}
    ],
    "key_claims_tracked": [
        {{"claim": "text", "videos_making_claim": 0, "channels": ["ch"], "assessment": "Verified|Unverified|Misleading"}}
    ],
    "sentiment_overview": {{
        "overall": "mixed", "pro_government_pct": 0, "critical_pct": 0,
        "neutral_analytical_pct": 0, "most_polarizing_topic": "topic"
    }},
    "trending_signals": {{"velocity": "rising|stable|declining", "peak_period": "when", "prediction": "what next"}},
    "emerging_stories": [{{"topic": "story", "early_signals": "why", "channels_covering": ["ch"]}}],
    "notable_absences": "What's NOT being covered"
}}

RULES:
- EXACTLY 4 entries in group_analysis
- Use real data from the state — do not invent
- Write polished prose for executive_summary and framing fields
"""
    text, cost = await generate(
        prompt=prompt,
        system_instruction=(
            "You format narrative intelligence data into polished JSON reports. "
            "Maintain factual accuracy from the source data."
        ),
        temperature=0.2,
    )
    analysis = extract_json(text)

    # Overlay persisted exec_summary / key_judgments from state (generated
    # once during batch build) and compute framing_divergence deterministically.
    persisted_summary = (state.get("executive_summary") or "").strip()
    if persisted_summary:
        analysis["executive_summary"] = persisted_summary
    analysis["key_judgments"] = state.get("key_judgments") or []
    analysis["framing_divergence"] = _compute_framing_divergence(narratives)

    # Stitch notable_channels from state onto each group in analysis, and
    # attach group_count to each narrative_angle for the N/4 chip.
    group_notables = {
        g_name: (g.get("notable_channels") or [])
        for g_name, g in (group_summaries or {}).items()
    }
    for g in analysis.get("group_analysis") or []:
        notable_from_state = group_notables.get(g.get("group"), [])
        if notable_from_state:
            g["notable_channels"] = [
                {
                    "name": c.get("name", ""),
                    "videos": c.get("videos", 0),
                    "views": c.get("views", 0),
                    "stance": c.get("stance", ""),
                }
                for c in notable_from_state
                if c.get("name")
            ]

    narr_by_title = {
        (n.get("title") or "").strip().lower(): n for n in narratives
    }
    for a in analysis.get("narrative_angles") or []:
        key = (a.get("title") or "").strip().lower()
        source = narr_by_title.get(key)
        if source:
            a["group_count"] = sum(
                1 for grp in _GROUP_ORDER
                if ((source.get("group_coverage") or {}).get(grp) or {}).get("video_count", 0) > 0
            )

    return {
        "hours": 24,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "video_count": state_row.total_videos_processed,
        "channel_count": state_row.total_channels,
        "videos": [],  # not included in state-based report (too large)
        "analysis": analysis,
        "gemini_cost_usd": cost,
        "haiku_cost_usd": 0,
        "youtube_units_used": 0,
        "state_id": state_row.id,
        "state_based": True,
    }


def _truncate(text: str, max_chars: int) -> str:
    """Truncate text at a word boundary, appending an ellipsis if cut."""
    text = (text or "").strip()
    if len(text) <= max_chars:
        return text
    cut = text[:max_chars]
    # Only rsplit if there is a space to split on, else hard cut
    if " " in cut:
        cut = cut.rsplit(" ", 1)[0]
    return cut + "…"


_CLAIM_NORMALIZE_RE = re.compile(r"[^\w\s]")
_CLAIM_WHITESPACE_RE = re.compile(r"\s+")


def _normalize_claim(text: str) -> str:
    """Normalize claim text for fuzzy dedup (lowercase, strip punctuation)."""
    if not text:
        return ""
    t = _CLAIM_NORMALIZE_RE.sub(" ", text.lower())
    return _CLAIM_WHITESPACE_RE.sub(" ", t).strip()


# Ordered from most to least trustworthy assessment
_ASSESS_PRIORITY: Dict[str, int] = {
    "Verified": 4,
    "Partially True": 3,
    "Contested": 2,
    "Unverified": 1,
    "Misleading": 0,
}


def _claim_rank_key(claim: Any) -> Tuple[int, int, int]:
    """Sort key for ranking claims: more sources, better assessment, shorter text first.

    Used with reverse=True so larger tuples come first. We negate length so that
    shorter (more specific) claims win ties.
    """
    if isinstance(claim, dict):
        sources = claim.get("sources") or []
        assessment = claim.get("assessment", "Unverified")
        text = claim.get("claim", "") or ""
    else:
        sources = []
        assessment = "Unverified"
        text = claim or ""
    return (
        len(sources),
        _ASSESS_PRIORITY.get(assessment, 1),
        -len(text),
    )


def _dedup_and_rank_claims(claims: List[Any]) -> List[Dict[str, Any]]:
    """Dedupe (by normalized text) and rank a list of claim entries.

    Accepts either dict-form claims or plain strings. Returns a list of dicts
    with a consistent shape: {claim, sources, assessment}.
    """
    seen: set[str] = set()
    normalized: List[Dict[str, Any]] = []
    for c in claims or []:
        if isinstance(c, dict):
            text = (c.get("claim") or "").strip()
            sources = list(c.get("sources") or [])
            assessment = c.get("assessment", "Unverified")
        else:
            text = (c or "").strip()
            sources = []
            assessment = "Unverified"
        if not text:
            continue
        key = _normalize_claim(text)
        if not key or key in seen:
            continue
        seen.add(key)
        normalized.append({
            "claim": text,
            "sources": sources,
            "assessment": assessment,
        })
    normalized.sort(key=_claim_rank_key, reverse=True)
    return normalized


_GROUP_ORDER = ["Mainstream Media", "Independent & Digital", "Regional", "Specialist & Policy"]


def _compute_framing_divergence(narratives: List[Dict]) -> Dict[str, Any]:
    """Compute convergence buckets + top divergent stories from group_coverage.

    Pure Python — no LLM. Buckets narratives by how many of the 4 macro
    groups are covering them. A "divergent" narrative is one that >= 2 groups
    cover but with conflicting bias signals; top_divergent is ranked by
    total_views so the most visible splits float up.
    """
    universal: List[Dict[str, Any]] = []
    majority: List[Dict[str, Any]] = []
    silo: List[Dict[str, Any]] = []
    divergent_candidates: List[Dict[str, Any]] = []

    for n in narratives or []:
        gc = n.get("group_coverage") or {}
        # Only count the 4 canonical macro groups so stray keys don't inflate
        active_groups = [
            g for g in _GROUP_ORDER
            if (gc.get(g) or {}).get("video_count", 0) > 0
        ]
        count = len(active_groups)
        if count == 0:
            continue

        title = n.get("title", "")
        total_views = n.get("total_views", 0) or 0
        bucket_entry = {"title": title, "total_views": total_views, "groups": count}

        if count == 4:
            universal.append(bucket_entry)
        elif count >= 2:
            majority.append(bucket_entry)
        else:
            silo.append(bucket_entry)

        # Divergence check: >=2 groups cover it AND bias signals differ
        if count >= 2:
            biases: List[str] = []
            group_cells: Dict[str, Optional[Dict[str, Any]]] = {}
            for g in _GROUP_ORDER:
                cov = gc.get(g) or {}
                if cov.get("video_count", 0) > 0:
                    bias = (cov.get("bias_signal") or "neutral").strip() or "neutral"
                    biases.append(bias)
                    group_cells[g] = {
                        "bias": bias,
                        "videos": cov.get("video_count", 0),
                    }
                else:
                    group_cells[g] = None
            if len(set(biases)) >= 2:
                divergent_candidates.append({
                    "title": title,
                    "total_views": total_views,
                    "groups": group_cells,
                })

    universal.sort(key=lambda x: x["total_views"], reverse=True)
    majority.sort(key=lambda x: x["total_views"], reverse=True)
    silo.sort(key=lambda x: x["total_views"], reverse=True)
    divergent_candidates.sort(key=lambda x: x["total_views"], reverse=True)

    return {
        "universal": universal,
        "majority": majority,
        "silo": silo,
        "top_divergent": divergent_candidates[:3],
    }


def _format_with_python(
    state_row: NarrativeState,
    state: Dict[str, Any],
) -> Dict[str, Any]:
    """Map narrative state directly to report JSON schema — free, instant."""
    narratives = state.get("narratives", [])
    group_summaries = state.get("group_summaries", {})

    # Build narrative_angles from narratives
    narrative_angles = []
    for n in narratives:
        # Collect channels from group_coverage (dedupe, preserve order)
        channels_pushing: List[str] = []
        categories_involved: List[str] = []
        seen_channels: set[str] = set()
        seen_categories: set[str] = set()
        for grp, cov in n.get("group_coverage", {}).items():
            if grp and grp not in seen_categories:
                seen_categories.add(grp)
                categories_involved.append(grp)
            for ch in cov.get("top_channels", []):
                if ch and ch not in seen_channels:
                    seen_channels.add(ch)
                    channels_pushing.append(ch)

        # Dedupe + rank + cap key_claims. Return up to 20 (frontend shows 5
        # with a "show all" expander). Each claim text truncated to 120 chars.
        ranked_claims = _dedup_and_rank_claims(n.get("key_claims") or [])
        capped_claims: List[str] = [
            _truncate(c["claim"], 120) for c in ranked_claims[:20]
        ]

        # Truncate description to ~280 chars at word boundary
        desc = _truncate(n.get("description") or "", 280)

        # Count how many of the 4 canonical macro groups actually cover this
        # narrative (not just which group keys exist on the dict).
        group_count = sum(
            1 for g in _GROUP_ORDER
            if ((n.get("group_coverage") or {}).get(g) or {}).get("video_count", 0) > 0
        )

        narrative_angles.append({
            "title": n.get("title", ""),
            "sentiment": n.get("sentiment", "mixed"),
            "video_count": n.get("video_count", 0),
            "total_views": n.get("total_views", 0),
            "description": desc,
            "key_claims": capped_claims,
            "channels_pushing": channels_pushing[:5],
            "categories_involved": categories_involved,
            "group_count": group_count,
            "top_videos": n.get("top_videos", [])[:5],
        })

    # Build group_analysis from group_summaries + live narrative data
    group_analysis = []
    for grp_name in ["Mainstream Media", "Independent & Digital", "Regional", "Specialist & Policy"]:
        gs = group_summaries.get(grp_name, {})

        # Recompute dominant topic from narrative group_coverage
        # Rank narratives by this group's video count, show top 2
        ranked = []
        for n in narratives:
            gc = n.get("group_coverage", {}).get(grp_name, {})
            gv = gc.get("video_count", 0)
            if gv > 0:
                ranked.append((n.get("title", ""), gv))
        ranked.sort(key=lambda x: x[1], reverse=True)

        if len(ranked) >= 2:
            dominant_topic = f"{ranked[0][0]}; also: {ranked[1][0]}"
        elif ranked:
            dominant_topic = ranked[0][0]
        else:
            dominant_topic = gs.get("dominant_narrative", "")

        # notable_channels is persisted on group_summaries during batch build.
        # Older states pre-dating that change may lack it — fall back to [].
        notable_from_state = gs.get("notable_channels") or []
        notable_channels = [
            {
                "name": c.get("name", ""),
                "videos": c.get("videos", 0),
                "views": c.get("views", 0),
                "stance": c.get("stance", ""),
            }
            for c in notable_from_state
            if c.get("name")
        ]

        group_analysis.append({
            "group": grp_name,
            "channel_count": gs.get("channel_count", 0),
            "video_count": gs.get("video_count", 0),
            "total_views": gs.get("views", 0),
            "dominant_topic": dominant_topic,
            "framing": gs.get("framing", ""),
            "bias_signal": gs.get("bias_signal", "neutral"),
            "notable_channels": notable_channels,
        })

    # Build key_claims_tracked — dedupe across all narratives, rank, then cap.
    # Each claim carries the parent narrative's video_count for context.
    pooled_claims: List[Dict[str, Any]] = []
    for n in narratives:
        video_count = n.get("video_count", 0)
        for c in _dedup_and_rank_claims(n.get("key_claims") or []):
            pooled_claims.append({
                **c,
                "video_count": video_count,
            })
    # Re-dedupe across narratives (the same claim may exist under multiple)
    # and re-rank by (source count, assessment, brevity).
    seen_pool: set[str] = set()
    unique_claims: List[Dict[str, Any]] = []
    for c in pooled_claims:
        key = _normalize_claim(c["claim"])
        if key in seen_pool:
            continue
        seen_pool.add(key)
        unique_claims.append(c)
    unique_claims.sort(key=_claim_rank_key, reverse=True)
    all_claims = [
        {
            "claim": _truncate(c["claim"], 140),
            "videos_making_claim": c.get("video_count", 0),
            "channels": (c.get("sources") or [])[:5],
            "assessment": c.get("assessment", "Unverified"),
        }
        for c in unique_claims[:15]
    ]

    total_views = sum(n.get("total_views", 0) for n in narratives)
    # Fallback: use group_summaries views if narrative views are all 0
    if total_views == 0:
        total_views = sum(gs.get("views", 0) for gs in group_summaries.values())

    # Build headline from top narrative
    headline = narratives[0]["title"] if narratives else "No active narratives"

    # Prefer the Gemini-written exec summary + judgments persisted in state;
    # fall back to a deterministic template if the batch didn't produce them.
    exec_summary = (state.get("executive_summary") or "").strip()
    key_judgments = state.get("key_judgments") or []
    if not exec_summary:
        top_titles = [n["title"] for n in narratives[:3]]
        exec_summary = (
            f"Analysis of {state_row.total_videos_processed} videos across "
            f"{state_row.total_channels} channels. Top narratives: {', '.join(top_titles)}."
        )

    framing_divergence = _compute_framing_divergence(narratives)

    sentiment = state.get("sentiment_overview", {})
    trending = state.get("trending_signals", {})

    analysis = {
        "headline": headline,
        "executive_summary": exec_summary,
        "key_judgments": key_judgments,
        "framing_divergence": framing_divergence,
        "total_views": total_views,
        "narrative_angles": narrative_angles,
        "group_analysis": group_analysis,
        "key_claims_tracked": all_claims,
        "sentiment_overview": {
            "overall": sentiment.get("overall", "mixed"),
            "pro_government_pct": sentiment.get("pro_government_pct", 0),
            "critical_pct": sentiment.get("critical_pct", 0),
            "neutral_analytical_pct": sentiment.get("neutral_pct", 0),
            "most_polarizing_topic": sentiment.get("most_polarizing_topic", ""),
        },
        "trending_signals": {
            "velocity": trending.get("velocity", "stable"),
            "peak_period": trending.get("peak_period", ""),
            "prediction": trending.get("prediction", ""),
        },
        "emerging_stories": [
            {
                "topic": s.get("topic", ""),
                "early_signals": s.get("first_seen", s.get("early_signals", "")),
                "channels_covering": s.get("channels_covering", []),
            }
            for s in state.get("emerging_stories", [])[:5]
        ],
        "notable_absences": state.get("notable_absences", ""),
    }

    return {
        "hours": 24,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "video_count": state_row.total_videos_processed,
        "channel_count": state_row.total_channels,
        "videos": [],
        "analysis": analysis,
        "gemini_cost_usd": 0,
        "haiku_cost_usd": 0,
        "youtube_units_used": 0,
        "state_id": state_row.id,
        "state_based": True,
    }
