"""YouTube Data API v3 client — async, uses httpx directly."""

from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional

import httpx

from backend.config import settings

logger = logging.getLogger(__name__)

BASE_URL = "https://www.googleapis.com/youtube/v3"


class YouTubeClient:
    """Async client for YouTube Data API v3."""

    def __init__(self) -> None:
        self.api_key = settings.youtube_api_key
        self._client: httpx.AsyncClient | None = None
        self.units_used = 0

    async def _get_client(self) -> httpx.AsyncClient:
        if self._client is None or self._client.is_closed:
            self._client = httpx.AsyncClient(timeout=30.0)
        return self._client

    async def close(self) -> None:
        if self._client and not self._client.is_closed:
            await self._client.aclose()

    async def _request(self, endpoint: str, params: Dict[str, Any]) -> Dict:
        client = await self._get_client()
        params["key"] = self.api_key
        url = f"{BASE_URL}/{endpoint}"
        resp = await client.get(url, params=params)
        resp.raise_for_status()
        return resp.json()

    async def search_videos(
        self,
        query: str,
        max_results: int = 10,
        order: str = "relevance",
        published_after: Optional[str] = None,
    ) -> List[Dict]:
        """Search for videos. Costs 100 units per call."""
        params: Dict[str, Any] = {
            "part": "snippet",
            "q": query,
            "type": "video",
            "maxResults": min(max_results, 50),
            "order": order,
        }
        if published_after:
            params["publishedAfter"] = published_after

        data = await self._request("search", params)
        self.units_used += 100

        results = []
        for item in data.get("items", []):
            results.append({
                "video_id": item["id"]["videoId"],
                "title": item["snippet"]["title"],
                "channel_title": item["snippet"]["channelTitle"],
                "channel_id": item["snippet"]["channelId"],
                "published_at": item["snippet"]["publishedAt"],
                "description": item["snippet"].get("description", ""),
                "thumbnail": item["snippet"]["thumbnails"].get("high", {}).get("url", ""),
            })
        return results

    async def get_video_details(self, video_ids: List[str]) -> List[Dict]:
        """Batch fetch video details. Costs 1 unit per call (up to 50 IDs)."""
        if not video_ids:
            return []

        params = {
            "part": "snippet,statistics,contentDetails",
            "id": ",".join(video_ids[:50]),
        }
        data = await self._request("videos", params)
        self.units_used += 1

        results = []
        for item in data.get("items", []):
            stats = item.get("statistics", {})
            snippet = item.get("snippet", {})
            results.append({
                "video_id": item["id"],
                "title": snippet.get("title", ""),
                "channel_title": snippet.get("channelTitle", ""),
                "channel_id": snippet.get("channelId", ""),
                "published_at": snippet.get("publishedAt", ""),
                "description": snippet.get("description", ""),
                "tags": snippet.get("tags", []),
                "thumbnail": snippet.get("thumbnails", {}).get("high", {}).get("url", ""),
                "duration": item.get("contentDetails", {}).get("duration", ""),
                "view_count": int(stats.get("viewCount", 0)),
                "like_count": int(stats.get("likeCount", 0)),
                "comment_count": int(stats.get("commentCount", 0)),
            })
        return results

    async def get_video_comments(
        self,
        video_id: str,
        max_results: int = 30,
    ) -> List[Dict]:
        """Fetch top-level comments for a video. Costs 1 unit per call."""
        try:
            params: Dict[str, Any] = {
                "part": "snippet",
                "videoId": video_id,
                "maxResults": min(max_results, 100),
                "order": "relevance",
                "textFormat": "plainText",
            }
            data = await self._request("commentThreads", params)
            self.units_used += 1

            results = []
            for item in data.get("items", []):
                c = item["snippet"]["topLevelComment"]["snippet"]
                results.append({
                    "author": c.get("authorDisplayName", ""),
                    "text": c.get("textDisplay", ""),
                    "like_count": c.get("likeCount", 0),
                    "published_at": c.get("publishedAt", ""),
                })
            return results
        except httpx.HTTPStatusError as e:
            if e.response.status_code == 403:
                logger.warning("Comments disabled for video %s", video_id)
                return []
            raise

    async def get_channel_info(self, channel_id: str) -> Optional[Dict]:
        """Get channel metadata. Costs 1 unit."""
        params = {
            "part": "snippet,statistics",
            "id": channel_id,
        }
        data = await self._request("channels", params)
        self.units_used += 1

        items = data.get("items", [])
        if not items:
            return None

        item = items[0]
        stats = item.get("statistics", {})
        snippet = item.get("snippet", {})
        return {
            "channel_id": channel_id,
            "title": snippet.get("title", ""),
            "description": snippet.get("description", ""),
            "subscriber_count": int(stats.get("subscriberCount", 0)),
            "video_count": int(stats.get("videoCount", 0)),
            "view_count": int(stats.get("viewCount", 0)),
            "thumbnail": snippet.get("thumbnails", {}).get("default", {}).get("url", ""),
        }

    async def get_channel_by_handle(self, handle: str) -> Optional[Dict]:
        """Resolve a YouTube handle (@name) or custom URL to channel info. Costs 1 unit."""
        handle = handle.strip().lstrip("@")
        # Try forHandle parameter
        params = {
            "part": "snippet,statistics",
            "forHandle": handle,
        }
        try:
            data = await self._request("channels", params)
            self.units_used += 1
            items = data.get("items", [])
            if items:
                item = items[0]
                stats = item.get("statistics", {})
                snippet = item.get("snippet", {})
                return {
                    "channel_id": item["id"],
                    "title": snippet.get("title", ""),
                    "subscriber_count": int(stats.get("subscriberCount", 0)),
                    "thumbnail": snippet.get("thumbnails", {}).get("default", {}).get("url", ""),
                }
        except Exception as e:
            logger.debug("Handle lookup failed for %s: %s", handle, e)
        return None
