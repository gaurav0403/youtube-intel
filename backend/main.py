"""YouTube Intel — FastAPI application."""

from __future__ import annotations

from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from backend.config import settings
from backend.database import init_db


@asynccontextmanager
async def lifespan(app: FastAPI):
    import asyncio

    await init_db()
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

app.include_router(youtube_router)
app.include_router(channels_router)


@app.get("/health")
async def health():
    return {"status": "ok", "service": "youtube-intel"}
