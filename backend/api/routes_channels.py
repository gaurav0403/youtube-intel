"""Channel monitoring endpoints."""

from __future__ import annotations

import logging
import re
from datetime import datetime, timezone

from fastapi import APIRouter, Query
from sqlalchemy import func, select

from backend.database import async_session
from backend.models import ChannelVideo, WatchedChannel

router = APIRouter()
logger = logging.getLogger(__name__)


def _extract_channel_id(url_or_id: str) -> str | None:
    """Extract channel ID from URL or return as-is if it looks like an ID."""
    s = url_or_id.strip()
    # Direct channel ID (starts with UC, 24 chars)
    if re.match(r"^UC[\w-]{22}$", s):
        return s
    # URL patterns
    m = re.search(r"youtube\.com/channel/(UC[\w-]{22})", s)
    if m:
        return m.group(1)
    return None


async def _resolve_and_add_channel(
    channel_url: str,
    category: str | None = None,
) -> dict:
    """Resolve a channel URL/handle and add to watch list. Returns result dict."""
    from backend.services.youtube_client import YouTubeClient

    channel_id = _extract_channel_id(channel_url)

    # If not a direct channel ID, try resolving via handle/custom URL
    if not channel_id:
        yt = YouTubeClient()
        try:
            info = await yt.get_channel_by_handle(channel_url.strip())
            if info:
                channel_id = info["channel_id"]
        finally:
            await yt.close()

    if not channel_id:
        return {"error": f"Could not resolve: {channel_url}"}

    # Check if already exists
    async with async_session() as db:
        existing = await db.execute(
            select(WatchedChannel).where(WatchedChannel.channel_id == channel_id)
        )
        row = existing.scalar_one_or_none()
        if row:
            # Update category if provided and channel exists
            if category and row.category != category:
                row.category = category
                await db.commit()
            return {"channel_id": channel_id, "channel_name": row.channel_name, "status": "exists"}

    # Fetch channel info
    yt = YouTubeClient()
    try:
        info = await yt.get_channel_info(channel_id)
    finally:
        await yt.close()

    if not info:
        return {"error": f"Channel not found: {channel_id}"}

    async with async_session() as db:
        ch = WatchedChannel(
            channel_id=channel_id,
            channel_name=info.get("title", ""),
            subscriber_count=info.get("subscriber_count", 0),
            thumbnail=info.get("thumbnail", ""),
            category=category,
            added_at=datetime.now(timezone.utc),
        )
        db.add(ch)
        await db.commit()
        await db.refresh(ch)

    logger.info("Added channel: %s (%s) [%s]", info.get("title"), channel_id, category)
    return {
        "id": ch.id,
        "channel_id": channel_id,
        "channel_name": info.get("title", ""),
        "subscriber_count": info.get("subscriber_count", 0),
        "thumbnail": info.get("thumbnail", ""),
        "category": category,
        "status": "added",
    }


@router.post("/api/channels/add")
async def add_channel(channel_url: str, category: str | None = None):
    """Add a YouTube channel to the watch list."""
    return await _resolve_and_add_channel(channel_url, category)


@router.post("/api/channels/bulk-add")
async def bulk_add_channels(channels: list[dict]):
    """Bulk add channels. Body: [{"handle": "@foo", "name": "Foo", "category": "News"}, ...]"""
    results = []
    for ch in channels:
        handle = ch.get("handle", "")
        category = ch.get("category")
        try:
            result = await _resolve_and_add_channel(handle, category)
            result["input_handle"] = handle
            results.append(result)
        except Exception as e:
            results.append({"input_handle": handle, "error": str(e)})
    added = sum(1 for r in results if r.get("status") == "added")
    existing = sum(1 for r in results if r.get("status") == "exists")
    failed = sum(1 for r in results if "error" in r)
    return {"added": added, "existing": existing, "failed": failed, "details": results}


@router.get("/api/channels")
async def list_channels(category: str | None = None):
    """List all watched channels with video counts. Optionally filter by category."""
    async with async_session() as db:
        stmt = select(WatchedChannel).order_by(WatchedChannel.channel_name.asc())
        if category:
            stmt = stmt.where(WatchedChannel.category == category)
        result = await db.execute(stmt)
        channels = result.scalars().all()

        # Get video counts per channel
        counts_result = await db.execute(
            select(ChannelVideo.channel_id, func.count(ChannelVideo.id))
            .group_by(ChannelVideo.channel_id)
        )
        counts = dict(counts_result.all())

    return [
        {
            "id": ch.id,
            "channel_id": ch.channel_id,
            "channel_name": ch.channel_name,
            "subscriber_count": ch.subscriber_count,
            "thumbnail": ch.thumbnail,
            "is_active": ch.is_active,
            "added_at": ch.added_at.isoformat() if ch.added_at else None,
            "last_checked_at": ch.last_checked_at.isoformat() if ch.last_checked_at else None,
            "category": ch.category,
            "video_count": counts.get(ch.channel_id, 0),
        }
        for ch in channels
    ]


@router.get("/api/channels/categories")
async def list_categories():
    """List all unique channel categories with counts."""
    async with async_session() as db:
        result = await db.execute(
            select(WatchedChannel.category, func.count(WatchedChannel.id))
            .where(WatchedChannel.is_active == True)  # noqa: E712
            .group_by(WatchedChannel.category)
            .order_by(func.count(WatchedChannel.id).desc())
        )
        return [{"category": cat or "Uncategorized", "count": count} for cat, count in result.all()]


@router.delete("/api/channels/{channel_id}")
async def remove_channel(channel_id: str):
    """Remove a channel from the watch list (soft delete)."""
    async with async_session() as db:
        result = await db.execute(
            select(WatchedChannel).where(WatchedChannel.channel_id == channel_id)
        )
        ch = result.scalar_one_or_none()
        if not ch:
            return {"error": "Channel not found"}
        ch.is_active = False
        await db.commit()
    return {"status": "removed", "channel_id": channel_id}


@router.get("/api/channels/{channel_id}/videos")
async def channel_videos(
    channel_id: str,
    limit: int = Query(50, ge=1, le=200),
):
    """Get videos detected for a watched channel."""
    async with async_session() as db:
        result = await db.execute(
            select(ChannelVideo)
            .where(ChannelVideo.channel_id == channel_id)
            .order_by(ChannelVideo.published_at.desc())
            .limit(limit)
        )
        videos = result.scalars().all()

    return [
        {
            "id": v.id,
            "video_id": v.video_id,
            "channel_id": v.channel_id,
            "title": v.title,
            "published_at": v.published_at.isoformat() if v.published_at else None,
            "thumbnail": v.thumbnail,
            "view_count": v.view_count,
            "topic_classification": v.topic_classification,
            "summary": v.summary,
            "detected_at": v.detected_at.isoformat() if v.detected_at else None,
        }
        for v in videos
    ]


@router.get("/api/channels/feed")
async def activity_feed(limit: int = Query(50, ge=1, le=200)):
    """Get the latest videos across all watched channels."""
    async with async_session() as db:
        result = await db.execute(
            select(ChannelVideo)
            .order_by(ChannelVideo.detected_at.desc())
            .limit(limit)
        )
        videos = result.scalars().all()

        # Get channel names
        ch_result = await db.execute(select(WatchedChannel))
        channels = {ch.channel_id: ch.channel_name for ch in ch_result.scalars().all()}

    return [
        {
            "id": v.id,
            "video_id": v.video_id,
            "channel_id": v.channel_id,
            "channel_name": channels.get(v.channel_id, ""),
            "title": v.title,
            "published_at": v.published_at.isoformat() if v.published_at else None,
            "thumbnail": v.thumbnail,
            "topic_classification": v.topic_classification,
            "summary": v.summary,
            "detected_at": v.detected_at.isoformat() if v.detected_at else None,
        }
        for v in videos
    ]


@router.post("/api/channels/poll")
async def trigger_poll():
    """Manually trigger a poll of all watched channels."""
    from backend.services.channel_monitor import poll_all_channels

    result = await poll_all_channels()
    return result
