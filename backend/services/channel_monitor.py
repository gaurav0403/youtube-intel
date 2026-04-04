"""Channel Monitor — RSS-based polling for new YouTube videos."""

from __future__ import annotations

import asyncio
import logging
import xml.etree.ElementTree as ET
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

import httpx
from sqlalchemy import select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from backend.config import settings
from backend.database import async_session
from backend.models import ChannelVideo, WatchedChannel
from backend.services.gemini_client import generate

logger = logging.getLogger(__name__)

RSS_URL = "https://www.youtube.com/feeds/videos.xml?channel_id={channel_id}"
NS = {"atom": "http://www.w3.org/2005/Atom", "yt": "http://www.youtube.com/xml/schemas/2015"}


async def fetch_rss_videos(channel_id: str) -> List[Dict[str, Any]]:
    """Fetch recent videos from a channel's RSS feed (FREE, no quota)."""
    url = RSS_URL.format(channel_id=channel_id)
    async with httpx.AsyncClient(timeout=15.0) as client:
        resp = await client.get(url)
        resp.raise_for_status()

    root = ET.fromstring(resp.text)
    videos = []
    for entry in root.findall("atom:entry", NS):
        video_id = entry.find("yt:videoId", NS)
        title = entry.find("atom:title", NS)
        published = entry.find("atom:published", NS)

        if video_id is None or title is None or published is None:
            continue

        # Parse thumbnail from media:group
        thumbnail = ""
        media_group = entry.find("{http://search.yahoo.com/mrss/}group")
        if media_group is not None:
            media_thumb = media_group.find("{http://search.yahoo.com/mrss/}thumbnail")
            if media_thumb is not None:
                thumbnail = media_thumb.get("url", "")

        videos.append({
            "video_id": video_id.text,
            "title": title.text or "",
            "published_at": published.text,
            "thumbnail": thumbnail or f"https://i.ytimg.com/vi/{video_id.text}/hqdefault.jpg",
        })
    return videos


async def classify_video(title: str) -> Dict[str, str]:
    """Use Gemini to classify a video's topic and generate a brief summary."""
    if not settings.gemini_api_key:
        return {"topic": "unknown", "summary": ""}

    prompt = f"""Classify this YouTube video based on its title. Return JSON:
{{"topic": "short topic label (2-4 words)", "summary": "one sentence summary of what this video is about"}}

Title: "{title}"
"""
    try:
        text, _ = await generate(
            prompt=prompt,
            system_instruction="You classify YouTube videos into topics. Be concise.",
            temperature=0.1,
        )
        import json
        return json.loads(text)
    except Exception as e:
        logger.warning("Classification failed for '%s': %s", title[:50], e)
        return {"topic": "unknown", "summary": ""}


async def classify_videos_batch(titles: List[str]) -> List[Dict[str, str]]:
    """Batch classify multiple videos in a single Gemini call."""
    if not settings.gemini_api_key or not titles:
        return [{"topic": "unknown", "summary": ""} for _ in titles]

    numbered = "\n".join(f"{i+1}. {t}" for i, t in enumerate(titles))
    prompt = f"""Classify each YouTube video by its title. Return a JSON array with one object per video, in order:
[{{"topic": "short topic label (2-4 words)", "summary": "one sentence summary"}}]

Videos:
{numbered}
"""
    try:
        text, _ = await generate(
            prompt=prompt,
            system_instruction="You classify YouTube videos into topics. Be concise. Return a JSON array.",
            temperature=0.1,
        )
        import json
        results = json.loads(text)
        if isinstance(results, list) and len(results) == len(titles):
            return results
    except Exception as e:
        logger.warning("Batch classification failed: %s", e)

    return [{"topic": "unknown", "summary": ""} for _ in titles]


async def check_channel(
    session: AsyncSession,
    channel: WatchedChannel,
    yt_client: Any = None,
) -> List[ChannelVideo]:
    """Check a channel for new videos. Tries RSS first, falls back to YouTube API."""
    new_videos = []

    # Try RSS first (free)
    rss_videos: List[Dict[str, Any]] = []
    try:
        rss_videos = await fetch_rss_videos(channel.channel_id)
    except Exception as e:
        logger.debug("RSS failed for %s, trying API: %s", channel.channel_id, e)

    # Fallback to YouTube API if RSS failed and client is available
    if not rss_videos and yt_client:
        try:
            rss_videos = await yt_client.get_channel_uploads(channel.channel_id, max_results=15)
        except Exception as e:
            logger.warning("API fetch also failed for %s: %s", channel.channel_id, e)
            return []

    if not rss_videos:
        return []

    # Get existing video IDs (global check to avoid UNIQUE constraint violations)
    candidate_ids = [rv["video_id"] for rv in rss_videos]
    result = await session.execute(
        select(ChannelVideo.video_id).where(ChannelVideo.video_id.in_(candidate_ids))
    )
    existing_ids = {row[0] for row in result.fetchall()}

    # Filter to only new videos
    new_rss = [rv for rv in rss_videos if rv["video_id"] not in existing_ids]
    if not new_rss:
        channel.last_checked_at = datetime.now(timezone.utc)
        if rss_videos:
            channel.last_video_id = rss_videos[0]["video_id"]
        await session.commit()
        return []

    # Batch classify all new videos in one Gemini call
    titles = [rv["title"] for rv in new_rss]
    try:
        classifications = await classify_videos_batch(titles)
    except Exception as e:
        logger.debug("Batch classification failed for %s: %s", channel.channel_id, e)
        classifications = [{"topic": "unknown", "summary": ""} for _ in new_rss]

    for rv, cls in zip(new_rss, classifications):
        published = datetime.fromisoformat(rv["published_at"].replace("Z", "+00:00"))
        video = ChannelVideo(
            video_id=rv["video_id"],
            channel_id=channel.channel_id,
            title=rv["title"],
            published_at=published,
            thumbnail=rv["thumbnail"],
            topic_classification=cls.get("topic", "unknown"),
            summary=cls.get("summary", ""),
        )
        session.add(video)
        new_videos.append(video)

    # Update channel's last_checked_at
    channel.last_checked_at = datetime.now(timezone.utc)
    if rss_videos:
        channel.last_video_id = rss_videos[0]["video_id"]

    try:
        await session.commit()
    except IntegrityError:
        await session.rollback()
        logger.debug("Duplicate video for %s, skipping batch", channel.channel_id)
        # Still update last_checked_at after rollback
        channel.last_checked_at = datetime.now(timezone.utc)
        if rss_videos:
            channel.last_video_id = rss_videos[0]["video_id"]
        await session.commit()
        return []

    return new_videos


async def poll_all_channels() -> Dict[str, int]:
    """Poll all active watched channels for new videos."""
    from backend.services.youtube_client import YouTubeClient

    yt = YouTubeClient()
    try:
        async with async_session() as session:
            result = await session.execute(
                select(WatchedChannel).where(WatchedChannel.is_active == True)  # noqa: E712
            )
            channels = result.scalars().all()

            total_new = 0
            checked = 0
            for channel in channels:
                new_vids = await check_channel(session, channel, yt_client=yt)
                total_new += len(new_vids)
                checked += 1
                if new_vids:
                    logger.info(
                        "Found %d new videos for %s", len(new_vids), channel.channel_name
                    )
    finally:
        await yt.close()

    return {
        "channels_checked": checked,
        "new_videos": total_new,
        "youtube_units_used": yt.units_used,
    }


async def background_poller(interval_seconds: int = 1800) -> None:
    """Background task that polls channels on a schedule."""
    while True:
        try:
            result = await poll_all_channels()
            logger.info(
                "Poll complete: checked %d channels, %d new videos",
                result["channels_checked"],
                result["new_videos"],
            )
        except Exception as e:
            logger.error("Poll error: %s", e)
        await asyncio.sleep(interval_seconds)
