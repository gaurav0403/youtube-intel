"""Narrative State — stateful batch + incremental narrative analysis.

Processes ALL ingested videos via chunked Gemini calls, maintains a living
narrative state document, and applies incremental updates after each poll.
"""

from __future__ import annotations

import json
import logging
import re
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Tuple

from sqlalchemy import select, update

from backend.config import settings
from backend.database import async_session
from backend.models import ChannelVideo, NarrativeState, WatchedChannel
from backend.services.gemini_client import extract_json, generate
from backend.services.monitoring_report import MACRO_GROUPS

logger = logging.getLogger(__name__)

CHUNK_SIZE = 100  # videos per Gemini chunk call
MAX_NARRATIVES = 15
MAX_TOP_VIDEOS = 5

BATCH_SYSTEM_INSTRUCTION = (
    "You are an intelligence analyst monitoring Indian YouTube news channels. "
    "Extract narratives, key claims, framing, and sentiment from the provided videos. "
    "Be specific — cite real video titles, channels, and view counts."
)

INCREMENTAL_SYSTEM_INSTRUCTION = (
    "You are an intelligence analyst maintaining a living narrative state for Indian YouTube news. "
    "Given the current narrative state and newly published videos, determine how narratives "
    "should be updated: assign new videos to existing narratives, create new ones if needed, "
    "update velocity signals, and flag emerging stories."
)


def _get_macro_group(category: str) -> str:
    """Map a fine-grained category to one of 4 macro groups."""
    return MACRO_GROUPS.get(category, "Independent & Digital")


def _slugify(title: str) -> str:
    """Create a URL-safe slug from a narrative title."""
    slug = title.lower().strip()
    slug = re.sub(r"[^a-z0-9\s-]", "", slug)
    slug = re.sub(r"[\s-]+", "-", slug)
    return slug[:80]


# ─── Batch Processing ──────────────────────────────────────────────────────


async def build_batch_state(hours: int = 24) -> Dict[str, Any]:
    """Build a complete narrative state from ALL videos in the time window.

    1. Query all ChannelVideo records from last N hours
    2. Partition into 4 macro groups, chunk each into batches of ~100
    3. Gemini call per chunk to extract narratives
    4. Merge chunk outputs in Python
    5. Optional deduplication pass
    6. Selective transcript enrichment for top narratives
    7. Store as new NarrativeState row
    """
    cutoff = datetime.now(timezone.utc) - timedelta(hours=hours)
    total_gemini_cost = 0.0
    total_haiku_cost = 0.0

    # Step 1: Query all videos and channels
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
        logger.warning("No videos found for batch state (last %dh)", hours)
        return {"error": f"No videos found in last {hours}h"}

    # Build video data with group assignment
    video_data: List[Dict[str, Any]] = []
    max_video_db_id = 0
    channels_seen: set[str] = set()
    for v in videos:
        ch_info = channels.get(v.channel_id, {})
        channels_seen.add(v.channel_id)
        max_video_db_id = max(max_video_db_id, v.id)
        video_data.append({
            "db_id": v.id,
            "video_id": v.video_id,
            "title": v.title,
            "channel_id": v.channel_id,
            "channel_name": ch_info.get("name", "Unknown"),
            "group": ch_info.get("group", "Independent & Digital"),
            "subscriber_count": ch_info.get("subscriber_count", 0),
            "published_at": v.published_at.isoformat() if v.published_at else "",
            "view_count": v.view_count,
            "topic_classification": v.topic_classification or "",
            "summary": v.summary or "",
        })

    # Step 1b: Enrich top 200 videos with real view counts from YouTube API
    # videos.list costs 1 unit per 50 videos = 4 units for 200 videos
    from backend.services.youtube_client import YouTubeClient
    yt = YouTubeClient()
    try:
        top_ids = [v["video_id"] for v in video_data[:200]]
        details = await yt.get_video_details(top_ids)
        details_map = {d["video_id"]: d for d in details}
        enriched = 0
        for v in video_data:
            d = details_map.get(v["video_id"])
            if d:
                v["view_count"] = d.get("view_count", 0)
                enriched += 1
        # Also update DB records so future incremental updates have views
        async with async_session() as db:
            for v in video_data:
                d = details_map.get(v["video_id"])
                if d and d.get("view_count", 0) > 0:
                    vid_result2 = await db.execute(
                        select(ChannelVideo).where(ChannelVideo.video_id == v["video_id"])
                    )
                    row = vid_result2.scalar_one_or_none()
                    if row:
                        row.view_count = d["view_count"]
            await db.commit()
        logger.info("Enriched %d/%d videos with YouTube view counts (API units: %d)", enriched, len(video_data), yt.units_used)
    except Exception:
        logger.exception("View count enrichment failed (non-fatal)")
    finally:
        await yt.close()

    # Step 2: Partition by group
    by_group: Dict[str, List[Dict]] = {}
    for v in video_data:
        by_group.setdefault(v["group"], []).append(v)

    logger.info(
        "Building batch state: %d videos across %d channels, %d groups",
        len(video_data), len(channels_seen),
        len(by_group),
    )

    # Step 3: Chunk and call Gemini per chunk
    all_chunk_results: List[Dict] = []
    chunk_count = 0

    for group_name, group_videos in by_group.items():
        for i in range(0, len(group_videos), CHUNK_SIZE):
            chunk = group_videos[i : i + CHUNK_SIZE]
            chunk_count += 1
            logger.info(
                "Processing chunk %d: %s (%d videos)",
                chunk_count, group_name, len(chunk),
            )
            try:
                result, cost = await _process_chunk(group_name, chunk)
                total_gemini_cost += cost
                all_chunk_results.append(result)
            except Exception:
                logger.exception("Chunk %d failed (%s)", chunk_count, group_name)

    if not all_chunk_results:
        return {"error": "All chunk processing failed"}

    # Step 4: Merge chunk outputs
    merged = _merge_chunk_outputs(all_chunk_results)

    # Step 5: Optional deduplication via Gemini
    if len(merged["narratives"]) > MAX_NARRATIVES:
        try:
            merged, dedup_cost = await _deduplicate_narratives(merged)
            total_gemini_cost += dedup_cost
        except Exception:
            logger.exception("Deduplication pass failed, using merged result")
            # Truncate manually
            merged["narratives"] = merged["narratives"][:MAX_NARRATIVES]

    # Step 6: Selective transcript enrichment for top 5 narratives
    if settings.anthropic_api_key:
        try:
            haiku_cost = await _enrich_top_narratives(merged, video_data)
            total_haiku_cost += haiku_cost
        except Exception:
            logger.exception("Transcript enrichment failed")

    # Step 7: Recompute group summaries
    merged["group_summaries"] = _compute_group_summaries(merged["narratives"], by_group)

    # Step 8: Store as new NarrativeState
    now = datetime.now(timezone.utc)
    state_data = {
        "narratives": merged["narratives"],
        "group_summaries": merged["group_summaries"],
        "sentiment_overview": merged.get("sentiment_overview", {}),
        "emerging_stories": merged.get("emerging_stories", []),
        "trending_signals": merged.get("trending_signals", {}),
        "notable_absences": merged.get("notable_absences", ""),
    }

    async with async_session() as db:
        # Deactivate old states
        await db.execute(
            update(NarrativeState)
            .where(NarrativeState.is_active == True)  # noqa: E712
            .values(is_active=False)
        )
        new_state = NarrativeState(
            state_type="batch",
            window_start=cutoff,
            window_end=now,
            total_videos_processed=len(video_data),
            total_channels=len(channels_seen),
            incremental_updates=0,
            state_json=state_data,
            total_gemini_cost_usd=total_gemini_cost,
            total_haiku_cost_usd=total_haiku_cost,
            last_video_id=max_video_db_id,
            is_active=True,
        )
        db.add(new_state)
        await db.commit()
        await db.refresh(new_state)

    logger.info(
        "Batch state complete: id=%d, %d videos, %d narratives, %d chunks, cost=$%.4f",
        new_state.id, len(video_data),
        len(state_data["narratives"]), chunk_count,
        total_gemini_cost + total_haiku_cost,
    )

    return {
        "state_id": new_state.id,
        "videos_processed": len(video_data),
        "narratives": len(state_data["narratives"]),
        "chunks": chunk_count,
        "gemini_cost_usd": total_gemini_cost,
        "haiku_cost_usd": total_haiku_cost,
    }


async def _process_chunk(
    group_name: str,
    videos: List[Dict],
) -> Tuple[Dict, float]:
    """Run Gemini on a single chunk of videos. Returns (result_dict, cost)."""
    video_block = ""
    for v in videos:
        summary_text = v.get("summary", "")
        video_block += (
            f'[{v["video_id"]}] "{v["title"]}" by {v["channel_name"]} '
            f'| {v["view_count"]:,} views | Group: {v["group"]}\n'
        )
        if summary_text:
            video_block += f"  Summary: {summary_text[:200]}\n"

    prompt = f"""Analyze these {len(videos)} videos from the "{group_name}" group.

{video_block}

Extract narratives from these videos. Return JSON:
{{
    "narratives": [
        {{
            "title": "Specific narrative title (max 12 words)",
            "slug": "url-safe-slug",
            "sentiment": "positive|negative|neutral|mixed",
            "importance_score": <1-10>,
            "video_count": <number of videos in this narrative>,
            "total_views": <sum of views>,
            "description": "ONE sentence, max 25 words, describing the core story and framing",
            "key_claims": [
                {{"claim": "specific claim (max 15 words)", "sources": ["channel1"], "assessment": "Verified|Unverified|Misleading"}}
            ],
            "group_coverage": {{
                "{group_name}": {{
                    "video_count": <number>,
                    "views": <total views>,
                    "framing": "How this group frames it (max 20 words)",
                    "bias_signal": "pro-government|critical|neutral|mixed",
                    "top_channels": ["channel1", "channel2"]
                }}
            }},
            "top_videos": [
                {{"video_id": "real ID", "title": "real title", "channel": "name", "views": <number>, "why": "Why it matters (max 12 words)"}}
            ],
            "velocity": "rising|stable|declining"
        }}
    ],
    "group_summary": {{
        "group": "{group_name}",
        "video_count": {len(videos)},
        "dominant_narrative": "Main story this group is covering (max 15 words)",
        "framing": "How this group frames stories (max 20 words)",
        "bias_signal": "pro-government|critical|neutral|mixed"
    }},
    "emerging_stories": [
        {{"topic": "Story just starting (max 12 words)", "first_seen": "ISO timestamp", "channels_covering": ["ch1"]}}
    ]
}}

RULES:
- Max 8 narratives per chunk
- All video_ids must be real IDs from the data
- Max 5 top_videos per narrative
- Max 3 key_claims per narrative, each under 15 words
- description MUST be exactly ONE sentence, max 25 words
- Be specific and concise — no filler phrases, no hedging
- Do NOT repeat the title inside the description
"""
    text, cost = await generate(
        prompt=prompt,
        system_instruction=BATCH_SYSTEM_INSTRUCTION,
    )
    result = extract_json(text)
    return result, cost


def _merge_chunk_outputs(chunks: List[Dict]) -> Dict:
    """Merge multiple chunk results into a unified narrative state."""
    narratives_by_slug: Dict[str, Dict] = {}
    all_emerging: List[Dict] = []

    for chunk in chunks:
        for narr in chunk.get("narratives", []):
            slug = narr.get("slug") or _slugify(narr.get("title", "unknown"))
            narr["slug"] = slug

            if slug in narratives_by_slug:
                existing = narratives_by_slug[slug]
                # Merge counts
                existing["video_count"] = existing.get("video_count", 0) + narr.get("video_count", 0)
                existing["total_views"] = existing.get("total_views", 0) + narr.get("total_views", 0)
                # Merge claims (dedupe by claim text)
                existing_claims = {c["claim"] for c in existing.get("key_claims", [])}
                for claim in narr.get("key_claims", []):
                    if claim["claim"] not in existing_claims:
                        existing.setdefault("key_claims", []).append(claim)
                # Merge group coverage
                for grp, cov in narr.get("group_coverage", {}).items():
                    if grp not in existing.get("group_coverage", {}):
                        existing.setdefault("group_coverage", {})[grp] = cov
                    else:
                        ec = existing["group_coverage"][grp]
                        ec["video_count"] = ec.get("video_count", 0) + cov.get("video_count", 0)
                        ec["views"] = ec.get("views", 0) + cov.get("views", 0)
                        # Merge top channels
                        existing_channels = set(ec.get("top_channels", []))
                        for ch in cov.get("top_channels", []):
                            existing_channels.add(ch)
                        ec["top_channels"] = list(existing_channels)
                # Merge top videos (keep top 5 by views)
                all_top = existing.get("top_videos", []) + narr.get("top_videos", [])
                seen_ids: set[str] = set()
                unique_top: List[Dict] = []
                for tv in sorted(all_top, key=lambda x: x.get("views", 0), reverse=True):
                    if tv["video_id"] not in seen_ids:
                        seen_ids.add(tv["video_id"])
                        unique_top.append(tv)
                existing["top_videos"] = unique_top[:MAX_TOP_VIDEOS]
                # Take higher importance score
                existing["importance_score"] = max(
                    existing.get("importance_score", 0),
                    narr.get("importance_score", 0),
                )
            else:
                narratives_by_slug[slug] = narr

        all_emerging.extend(chunk.get("emerging_stories", []))

    # Sort narratives by importance * views
    narratives = sorted(
        narratives_by_slug.values(),
        key=lambda n: n.get("importance_score", 0) * n.get("total_views", 0),
        reverse=True,
    )

    # Assign IDs
    for i, narr in enumerate(narratives):
        narr["id"] = f"narr-{i+1}"

    return {
        "narratives": narratives,
        "emerging_stories": all_emerging,
        "sentiment_overview": {},
        "trending_signals": {},
        "notable_absences": "",
    }


async def _deduplicate_narratives(state: Dict) -> Tuple[Dict, float]:
    """Use a lightweight Gemini call to merge similar narratives and add overview fields."""
    narrative_summaries = []
    for n in state["narratives"]:
        narrative_summaries.append({
            "id": n.get("id"),
            "title": n.get("title"),
            "slug": n.get("slug"),
            "video_count": n.get("video_count"),
            "total_views": n.get("total_views"),
            "description": n.get("description", "")[:150],
        })

    prompt = f"""You have {len(narrative_summaries)} narratives extracted from Indian YouTube news.
Some may be duplicates or near-duplicates covering the same story.

Narratives:
{json.dumps(narrative_summaries, indent=2)}

Return JSON:
{{
    "merge_groups": [
        {{
            "keep_id": "narr-X",
            "merge_ids": ["narr-Y", "narr-Z"],
            "merged_title": "Better combined title"
        }}
    ],
    "sentiment_overview": {{
        "overall": "positive|negative|neutral|mixed",
        "pro_government_pct": <0-100>,
        "critical_pct": <0-100>,
        "neutral_pct": <0-100>
    }},
    "trending_signals": {{
        "velocity": "rising|stable|declining",
        "peak_period": "description",
        "prediction": "What to expect next"
    }},
    "notable_absences": "What major topics are NOT being covered"
}}

RULES:
- Only merge truly duplicate narratives about the same event
- Keep max {MAX_NARRATIVES} narratives after merging
- Be conservative — don't merge narratives that are related but distinct
"""
    text, cost = await generate(
        prompt=prompt,
        system_instruction="You deduplicate and polish narrative lists. Be conservative with merges.",
        temperature=0.1,
    )
    result = extract_json(text)

    # Apply merges
    narr_by_id = {n["id"]: n for n in state["narratives"]}
    for group in result.get("merge_groups", []):
        keep_id = group.get("keep_id")
        if keep_id not in narr_by_id:
            continue
        keeper = narr_by_id[keep_id]
        if group.get("merged_title"):
            keeper["title"] = group["merged_title"]
            keeper["slug"] = _slugify(group["merged_title"])
        for merge_id in group.get("merge_ids", []):
            if merge_id in narr_by_id:
                victim = narr_by_id.pop(merge_id)
                keeper["video_count"] = keeper.get("video_count", 0) + victim.get("video_count", 0)
                keeper["total_views"] = keeper.get("total_views", 0) + victim.get("total_views", 0)
                # Merge group coverage
                for grp, cov in victim.get("group_coverage", {}).items():
                    if grp not in keeper.get("group_coverage", {}):
                        keeper.setdefault("group_coverage", {})[grp] = cov

    state["narratives"] = sorted(
        narr_by_id.values(),
        key=lambda n: n.get("importance_score", 0) * n.get("total_views", 0),
        reverse=True,
    )[:MAX_NARRATIVES]

    # Re-assign IDs
    for i, narr in enumerate(state["narratives"]):
        narr["id"] = f"narr-{i+1}"

    state["sentiment_overview"] = result.get("sentiment_overview", {})
    state["trending_signals"] = result.get("trending_signals", {})
    state["notable_absences"] = result.get("notable_absences", "")

    return state, cost


async def _enrich_top_narratives(
    state: Dict,
    video_data: List[Dict],
) -> float:
    """Enrich top 5 narratives with Haiku transcript summaries."""
    from backend.services.haiku_client import summarize_transcripts_batch
    from backend.services.youtube_transcript import get_transcripts_batch_async

    # Collect video IDs from top 5 narratives
    video_ids: List[str] = []
    for narr in state["narratives"][:5]:
        for tv in narr.get("top_videos", []):
            if tv.get("video_id") and tv["video_id"] not in video_ids:
                video_ids.append(tv["video_id"])

    if not video_ids:
        return 0.0

    # Fetch transcripts
    transcripts = await get_transcripts_batch_async(video_ids[:20])
    if not transcripts:
        return 0.0

    # Build video info for Haiku
    vid_lookup = {v["video_id"]: v for v in video_data}
    haiku_videos = []
    for vid_id in video_ids[:20]:
        v = vid_lookup.get(vid_id, {})
        haiku_videos.append({
            "video_id": vid_id,
            "title": v.get("title", ""),
            "channel_name": v.get("channel_name", "Unknown"),
        })

    summaries, cost = await summarize_transcripts_batch(haiku_videos, transcripts)

    # Attach summaries to top_videos
    for narr in state["narratives"][:5]:
        for tv in narr.get("top_videos", []):
            if tv.get("video_id") in summaries:
                tv["transcript_summary"] = summaries[tv["video_id"]]

    logger.info("Enriched %d videos with Haiku summaries (cost=$%.4f)", len(summaries), cost)
    return cost


def _compute_group_summaries(
    narratives: List[Dict],
    by_group: Dict[str, List[Dict]],
) -> Dict[str, Dict]:
    """Compute per-group summary statistics from narratives and raw video data.

    For dominant_narrative, picks the narrative where this group has the
    highest *share* of coverage relative to other groups. This avoids every
    group showing the same global top narrative.
    """
    from collections import Counter

    # Pre-compute total video count per narrative for share calculation
    narr_totals = {
        narr.get("id", i): narr.get("video_count", 0)
        for i, narr in enumerate(narratives)
    }

    summaries = {}
    for group_name in ["Mainstream Media", "Independent & Digital", "Regional", "Specialist & Policy"]:
        group_videos = by_group.get(group_name, [])
        total_views = sum(v.get("view_count", 0) for v in group_videos)

        # Rank narratives by this group's coverage (video count in this group)
        ranked: List[tuple[str, int, float]] = []  # (title, group_vids, share)
        for narr in narratives:
            coverage = narr.get("group_coverage", {}).get(group_name, {})
            group_vids = coverage.get("video_count", 0)
            if group_vids == 0:
                continue
            narr_total = narr.get("video_count", 1) or 1
            share = group_vids / narr_total  # what fraction of this narrative belongs to this group
            ranked.append((narr.get("title", ""), group_vids, share))

        # Sort by video count descending
        ranked.sort(key=lambda x: x[1], reverse=True)

        # Build descriptive dominant topic: show top 2 to differentiate groups
        if len(ranked) >= 2:
            dominant = f"{ranked[0][0]}; also: {ranked[1][0]}"
        elif ranked:
            dominant = ranked[0][0]
        else:
            dominant = ""

        # Aggregate framing from group_coverage across narratives
        framing_parts = []
        bias_signals = []
        for narr in narratives:
            coverage = narr.get("group_coverage", {}).get(group_name, {})
            if coverage.get("framing"):
                framing_parts.append(coverage["framing"])
            if coverage.get("bias_signal"):
                bias_signals.append(coverage["bias_signal"])

        # Determine dominant bias
        if bias_signals:
            bias_counts = Counter(bias_signals)
            dominant_bias = bias_counts.most_common(1)[0][0]
        else:
            dominant_bias = "neutral"

        summaries[group_name] = {
            "channel_count": len({v.get("channel_id") for v in group_videos}),
            "video_count": len(group_videos),
            "views": total_views,
            "dominant_narrative": dominant,
            "framing": "; ".join(framing_parts[:3]) if framing_parts else "",
            "bias_signal": dominant_bias,
        }

    return summaries


# ─── Incremental Updates ───────────────────────────────────────────────────


async def apply_incremental_update(
    state_id: int,
    new_videos: List[Dict],
) -> Dict[str, Any]:
    """Apply new videos to an existing narrative state via Gemini.

    1. Load active NarrativeState
    2. Send current state summary + new videos to Gemini
    3. Gemini returns: narrative assignments, new narratives, velocity changes
    4. Merge into state_json
    5. Update watermark
    """
    async with async_session() as db:
        state_row = await db.get(NarrativeState, state_id)
        if not state_row or not state_row.is_active:
            return {"error": "No active state found"}

        current_state = state_row.state_json

    # Build compact state summary for context
    state_summary = _build_state_summary(current_state)

    # Build new videos block
    video_block = ""
    max_db_id = 0
    for v in new_videos:
        max_db_id = max(max_db_id, v.get("db_id", 0))
        video_block += (
            f'[{v["video_id"]}] "{v["title"]}" by {v["channel_name"]} '
            f'| {v["view_count"]:,} views | Group: {v["group"]}\n'
        )
        if v.get("summary"):
            video_block += f"  Summary: {v['summary'][:200]}\n"

    prompt = f"""Current narrative state (from {current_state.get('_meta', {}).get('total_videos', '?')} videos):

{state_summary}

---

NEW VIDEOS ({len(new_videos)} just published):

{video_block}

Analyze these new videos against the current state. Return JSON:
{{
    "narrative_updates": [
        {{
            "narrative_id": "narr-X or NEW",
            "title": "narrative title (use existing title for updates, new title for new narratives)",
            "action": "update|create",
            "video_assignments": ["video_id1", "video_id2"],
            "velocity_change": "rising|stable|declining|null",
            "new_claims": [{{"claim": "...", "sources": ["ch1"], "assessment": "..."}}],
            "framing_update": "Any change in how this narrative is being framed"
        }}
    ],
    "emerging_stories": [
        {{"topic": "New emerging topic", "first_seen": "now", "channels_covering": ["ch1"]}}
    ],
    "sentiment_shift": {{
        "direction": "more_critical|more_positive|stable",
        "reason": "Why sentiment shifted"
    }}
}}

RULES:
- Assign each new video to exactly one narrative (existing or new)
- Only create a new narrative if no existing one fits
- Be specific about velocity changes
"""
    text, cost = await generate(
        prompt=prompt,
        system_instruction=INCREMENTAL_SYSTEM_INSTRUCTION,
    )
    result = extract_json(text)

    # Apply the incremental merge
    _apply_incremental_merge(current_state, result, new_videos)

    # Update the database
    async with async_session() as db:
        state_row = await db.get(NarrativeState, state_id)
        if not state_row:
            return {"error": "State disappeared during update"}
        state_row.state_json = current_state
        state_row.incremental_updates += 1
        state_row.total_videos_processed += len(new_videos)
        state_row.total_gemini_cost_usd += cost
        state_row.updated_at = datetime.now(timezone.utc)
        if max_db_id > 0:
            state_row.last_video_id = max_db_id
        await db.commit()

    logger.info(
        "Incremental update applied: state=%d, +%d videos, cost=$%.4f",
        state_id, len(new_videos), cost,
    )
    return {
        "state_id": state_id,
        "new_videos": len(new_videos),
        "gemini_cost_usd": cost,
        "updates": len(result.get("narrative_updates", [])),
    }


def _build_state_summary(state: Dict) -> str:
    """Build a compact text summary of the current state for Gemini context."""
    lines = []
    narratives = state.get("narratives", [])
    lines.append(f"ACTIVE NARRATIVES ({len(narratives)}):")
    for n in narratives:
        lines.append(
            f"  [{n.get('id')}] {n.get('title')} "
            f"| {n.get('video_count', 0)} videos | {n.get('total_views', 0):,} views "
            f"| sentiment={n.get('sentiment')} | velocity={n.get('velocity', 'stable')}"
        )
        for claim in (n.get("key_claims") or [])[:2]:
            claim_text = claim if isinstance(claim, str) else claim.get("claim", "")
            lines.append(f"    - Claim: {claim_text}")

    gs = state.get("group_summaries", {})
    if gs:
        lines.append("\nGROUP SUMMARIES:")
        for grp, info in gs.items():
            lines.append(
                f"  {grp}: {info.get('video_count', 0)} videos, "
                f"{info.get('views', 0):,} views, bias={info.get('bias_signal', '?')}"
            )

    return "\n".join(lines)


def _apply_incremental_merge(
    state: Dict,
    update_result: Dict,
    new_videos: List[Dict],
) -> None:
    """Surgically merge incremental update results into the living state."""
    narr_by_id = {n["id"]: n for n in state.get("narratives", [])}

    for upd in update_result.get("narrative_updates", []):
        narr_id = upd.get("narrative_id", "")
        action = upd.get("action", "update")

        if action == "create" or narr_id == "NEW" or narr_id not in narr_by_id:
            # Create new narrative
            new_id = f"narr-{len(narr_by_id) + 1}"
            new_narr = {
                "id": new_id,
                "title": upd.get("title", "Emerging Narrative"),
                "slug": _slugify(upd.get("title", "emerging")),
                "sentiment": "mixed",
                "importance_score": 5,
                "video_count": len(upd.get("video_assignments", [])),
                "total_views": 0,
                "description": upd.get("framing_update", ""),
                "key_claims": upd.get("new_claims", []),
                "group_coverage": {},
                "top_videos": [],
                "velocity": upd.get("velocity_change") or "rising",
            }
            # Populate top_videos from new_videos
            vid_lookup = {v["video_id"]: v for v in new_videos}
            for vid_id in upd.get("video_assignments", [])[:MAX_TOP_VIDEOS]:
                v = vid_lookup.get(vid_id, {})
                new_narr["total_views"] += v.get("view_count", 0)
                new_narr["top_videos"].append({
                    "video_id": vid_id,
                    "title": v.get("title", ""),
                    "channel": v.get("channel_name", ""),
                    "views": v.get("view_count", 0),
                    "why": "Newly published",
                })
                # Update group coverage
                grp = v.get("group", "Independent & Digital")
                if grp not in new_narr["group_coverage"]:
                    new_narr["group_coverage"][grp] = {
                        "video_count": 0, "views": 0, "framing": "", "bias_signal": "mixed", "top_channels": [],
                    }
                new_narr["group_coverage"][grp]["video_count"] += 1
                new_narr["group_coverage"][grp]["views"] += v.get("view_count", 0)

            narr_by_id[new_id] = new_narr
        else:
            # Update existing narrative
            narr = narr_by_id[narr_id]
            vid_lookup = {v["video_id"]: v for v in new_videos}
            assigned = upd.get("video_assignments", [])
            narr["video_count"] = narr.get("video_count", 0) + len(assigned)

            for vid_id in assigned:
                v = vid_lookup.get(vid_id, {})
                narr["total_views"] = narr.get("total_views", 0) + v.get("view_count", 0)
                # Update group coverage
                grp = v.get("group", "Independent & Digital")
                gc = narr.setdefault("group_coverage", {})
                if grp not in gc:
                    gc[grp] = {
                        "video_count": 0, "views": 0, "framing": "", "bias_signal": "mixed", "top_channels": [],
                    }
                gc[grp]["video_count"] += 1
                gc[grp]["views"] += v.get("view_count", 0)

            # Update velocity
            if upd.get("velocity_change"):
                narr["velocity"] = upd["velocity_change"]

            # Append new claims
            existing_claims = {
                (c["claim"] if isinstance(c, dict) else c)
                for c in narr.get("key_claims", [])
            }
            for claim in upd.get("new_claims", []):
                claim_text = claim["claim"] if isinstance(claim, dict) else claim
                if claim_text not in existing_claims:
                    narr.setdefault("key_claims", []).append(claim)

            if upd.get("framing_update"):
                narr["description"] = upd["framing_update"]

    # Replace narratives, sorted by importance * views, capped
    state["narratives"] = sorted(
        narr_by_id.values(),
        key=lambda n: n.get("importance_score", 0) * n.get("total_views", 0),
        reverse=True,
    )[:MAX_NARRATIVES]

    # Add emerging stories
    for story in update_result.get("emerging_stories", []):
        state.setdefault("emerging_stories", []).append(story)
    # Keep only latest 10 emerging stories
    state["emerging_stories"] = state.get("emerging_stories", [])[-10:]


# ─── Watermark-based helpers ───────────────────────────────────────────────


async def apply_incremental_update_from_watermark() -> Optional[Dict[str, Any]]:
    """Find active state, query videos with id > last_video_id, apply incremental update."""
    async with async_session() as db:
        result = await db.execute(
            select(NarrativeState).where(NarrativeState.is_active == True)  # noqa: E712
        )
        state_row = result.scalar_one_or_none()
        if not state_row:
            logger.debug("No active narrative state for incremental update")
            return None

        watermark = state_row.last_video_id or 0
        state_id = state_row.id

        # Query new videos since watermark
        vid_result = await db.execute(
            select(ChannelVideo)
            .where(ChannelVideo.id > watermark)
            .order_by(ChannelVideo.id.asc())
        )
        new_vids = vid_result.scalars().all()

        if not new_vids:
            logger.debug("No new videos since watermark %d", watermark)
            return None

        # Get channel info
        channel_ids = {v.channel_id for v in new_vids}
        ch_result = await db.execute(
            select(WatchedChannel).where(WatchedChannel.channel_id.in_(channel_ids))
        )
        channels = {
            ch.channel_id: {
                "name": ch.channel_name,
                "group": _get_macro_group(ch.category or "Uncategorized"),
            }
            for ch in ch_result.scalars().all()
        }

    # Build video data
    video_data = []
    for v in new_vids:
        ch_info = channels.get(v.channel_id, {})
        video_data.append({
            "db_id": v.id,
            "video_id": v.video_id,
            "title": v.title,
            "channel_id": v.channel_id,
            "channel_name": ch_info.get("name", "Unknown"),
            "group": ch_info.get("group", "Independent & Digital"),
            "view_count": v.view_count,
            "summary": v.summary or "",
        })

    logger.info(
        "Incremental update: %d new videos since watermark %d",
        len(video_data), watermark,
    )
    return await apply_incremental_update(state_id, video_data)


async def maybe_rebuild_batch_state(max_age_hours: int = 24) -> Optional[Dict[str, Any]]:
    """Check if active state exists and is fresh. If not, rebuild."""
    async with async_session() as db:
        result = await db.execute(
            select(NarrativeState).where(NarrativeState.is_active == True)  # noqa: E712
        )
        state_row = result.scalar_one_or_none()

    cutoff = datetime.now(timezone.utc) - timedelta(hours=max_age_hours)

    if state_row and state_row.created_at and state_row.created_at >= cutoff:
        logger.debug("Active state id=%d is still fresh (%s)", state_row.id, state_row.created_at)
        return None

    logger.info("No fresh active state — triggering batch rebuild")
    return await build_batch_state(hours=max_age_hours)


async def get_active_state() -> Optional[Dict[str, Any]]:
    """Get the currently active narrative state metadata."""
    async with async_session() as db:
        result = await db.execute(
            select(NarrativeState).where(NarrativeState.is_active == True)  # noqa: E712
        )
        state_row = result.scalar_one_or_none()

    if not state_row:
        return None

    return {
        "id": state_row.id,
        "state_type": state_row.state_type,
        "created_at": state_row.created_at.isoformat() if state_row.created_at else None,
        "updated_at": state_row.updated_at.isoformat() if state_row.updated_at else None,
        "window_start": state_row.window_start.isoformat() if state_row.window_start else None,
        "window_end": state_row.window_end.isoformat() if state_row.window_end else None,
        "total_videos_processed": state_row.total_videos_processed,
        "total_channels": state_row.total_channels,
        "incremental_updates": state_row.incremental_updates,
        "narrative_count": len(state_row.state_json.get("narratives", [])),
        "total_gemini_cost_usd": state_row.total_gemini_cost_usd,
        "total_haiku_cost_usd": state_row.total_haiku_cost_usd,
        "last_video_id": state_row.last_video_id,
        "is_active": state_row.is_active,
    }


async def get_state_by_id(state_id: int) -> Optional[Dict[str, Any]]:
    """Get full narrative state by ID including state_json."""
    async with async_session() as db:
        state_row = await db.get(NarrativeState, state_id)

    if not state_row:
        return None

    return {
        "id": state_row.id,
        "state_type": state_row.state_type,
        "created_at": state_row.created_at.isoformat() if state_row.created_at else None,
        "updated_at": state_row.updated_at.isoformat() if state_row.updated_at else None,
        "window_start": state_row.window_start.isoformat() if state_row.window_start else None,
        "window_end": state_row.window_end.isoformat() if state_row.window_end else None,
        "total_videos_processed": state_row.total_videos_processed,
        "total_channels": state_row.total_channels,
        "incremental_updates": state_row.incremental_updates,
        "state_json": state_row.state_json,
        "total_gemini_cost_usd": state_row.total_gemini_cost_usd,
        "total_haiku_cost_usd": state_row.total_haiku_cost_usd,
        "last_video_id": state_row.last_video_id,
        "is_active": state_row.is_active,
        "error": state_row.error,
    }
