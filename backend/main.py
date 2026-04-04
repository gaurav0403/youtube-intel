"""YouTube Intel — FastAPI application."""

from __future__ import annotations

import json
import logging
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from pathlib import Path

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy import func, select

from backend.config import settings
from backend.database import async_session, init_db
from backend.models import WatchedChannel

logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(name)s: %(message)s")
logger = logging.getLogger(__name__)

SEED_FILE = Path(__file__).parent / "data" / "channels.json"


async def seed_channels() -> None:
    """Auto-seed channels from channels.json if the watched_channels table is empty."""
    async with async_session() as db:
        count_result = await db.execute(select(func.count(WatchedChannel.id)))
        count = count_result.scalar() or 0
        if count > 0:
            logger.info("Channels table has %d rows — skipping seed.", count)
            return

    if not SEED_FILE.exists():
        logger.warning("Seed file not found at %s — skipping seed.", SEED_FILE)
        return

    channels = json.loads(SEED_FILE.read_text(encoding="utf-8"))
    logger.info("Seeding %d channels from %s …", len(channels), SEED_FILE.name)

    from backend.services.youtube_client import YouTubeClient

    yt = YouTubeClient()
    added = 0
    failed = 0

    try:
        for entry in channels:
            handle = entry.get("handle", "")
            name = entry.get("name", handle)
            category = entry.get("category")

            try:
                info = await yt.get_channel_by_handle(handle)
                if not info:
                    logger.warning("Seed: could not resolve handle %s", handle)
                    failed += 1
                    continue

                channel_id = info["channel_id"]

                async with async_session() as db:
                    existing = await db.execute(
                        select(WatchedChannel).where(
                            WatchedChannel.channel_id == channel_id
                        )
                    )
                    if existing.scalar_one_or_none():
                        continue

                    ch = WatchedChannel(
                        channel_id=channel_id,
                        channel_name=info.get("title", name),
                        subscriber_count=info.get("subscriber_count", 0),
                        thumbnail=info.get("thumbnail", ""),
                        category=category,
                        added_at=datetime.now(timezone.utc),
                    )
                    db.add(ch)
                    await db.commit()
                    added += 1

            except Exception:
                logger.exception("Seed: failed to add %s", handle)
                failed += 1
    finally:
        await yt.close()

    logger.info(
        "Seed complete: %d added, %d failed (API units used: %d)",
        added,
        failed,
        yt.units_used,
    )


@asynccontextmanager
async def lifespan(app: FastAPI):
    import asyncio

    await init_db()
    await seed_channels()
    # Start background channel poller (every 30 minutes)
    from backend.services.channel_monitor import background_poller

    poller_task = asyncio.create_task(background_poller(interval_seconds=1800))
    yield
    poller_task.cancel()


app = FastAPI(title="YouTube Intel", lifespan=lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.cors_origins.split(","),
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

from backend.api.routes_youtube import router as youtube_router  # noqa: E402
from backend.api.routes_channels import router as channels_router  # noqa: E402
from backend.api.routes_monitoring import router as monitoring_router  # noqa: E402

app.include_router(youtube_router)
app.include_router(channels_router)
app.include_router(monitoring_router)


@app.get("/health")
async def health():
    return {"status": "ok", "service": "youtube-intel"}
