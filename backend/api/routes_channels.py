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


@router.post("/api/channels/add")
async def add_channel(channel_url: str):
    """Add a YouTube channel to the watch list."""
    from backend.services.youtube_client import YouTubeClient

    channel_id = _extract_channel_id(channel_url)

    # If not a direct channel ID, try resolving via handle/custom URL
    if not channel_id:
        # Try the YouTube API search for the handle
        yt = YouTubeClient()
        try:
            info = await yt.get_channel_by_handle(channel_url.strip())
            if info:
                channel_id = info["channel_id"]
        finally:
            await yt.close()

    if not channel_id:
        return {"error": "Could not resolve channel ID. Provide a channel URL or ID."}

    # Check if already exists
    async with async_session() as db:
        existing = await db.execute(
            select(WatchedChannel).where(WatchedChannel.channel_id == channel_id)
        )
        if existing.scalar_one_or_none():
            return {"error": "Channel already in watch list"}

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
            added_at=datetime.now(timezone.utc),
        )
        db.add(ch)
        await db.commit()
        await db.refresh(ch)

    logger.info("Added channel: %s (%s)", info.get("title"), channel_id)
    return {
        "id": ch.id,
        "channel_id": channel_id,
        "channel_name": info.get("title", ""),
        "subscriber_count": info.get("subscriber_count", 0),
        "thumbnail": info.get("thumbnail", ""),
    }


@router.get("/api/channels")
async def list_channels():
    """List all watched channels with video counts."""
    async with async_session() as db:
        # Get channels
        result = await db.execute(
            select(WatchedChannel).order_by(WatchedChannel.added_at.desc())
        )
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
            "video_count": counts.get(ch.channel_id, 0),
        }
        for ch in channels
    ]


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
