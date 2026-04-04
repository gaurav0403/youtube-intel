"""YouTube report endpoints."""

from __future__ import annotations

import logging
from datetime import datetime, timezone

from fastapi import APIRouter, Query
from sqlalchemy import select

from backend.database import async_session
from backend.models import YouTubeReport

router = APIRouter()
logger = logging.getLogger(__name__)


@router.get("/api/youtube/report/{topic}")
async def youtube_report(
    topic: str,
    hours: int = Query(168, ge=1, le=720),
):
    """Generate a YouTube narrative intelligence report."""
    from backend.services.youtube_report import generate_youtube_report

    term = topic.strip()
    if not term:
        return {"error": "Empty topic"}

    now = datetime.now(timezone.utc)
    result = await generate_youtube_report(topic=term, hours=hours)

    analysis = result.get("analysis")
    report = {
        "topic": term,
        "hours": hours,
        "generated_at": now.isoformat(),
        "video_count": result.get("video_count", 0),
        "videos": result.get("videos", []),
        "analysis": analysis,
        "youtube_units_used": result.get("youtube_units_used", 0),
        "gemini_cost_usd": result.get("gemini_cost_usd", 0),
        "error": result.get("error"),
    }

    # Save to DB
    try:
        async with async_session() as db:
            row = YouTubeReport(
                topic=term,
                topic_normalized=term.lower().strip(),
                hours=hours,
                generated_at=now,
                video_count=result.get("video_count", 0),
                gemini_cost_usd=result.get("gemini_cost_usd", 0),
                youtube_units_used=result.get("youtube_units_used", 0),
                report_json=report,
                error=result.get("error"),
            )
            db.add(row)
            await db.commit()
            await db.refresh(row)
            report["id"] = row.id
            logger.info("YouTube report saved: id=%d, topic='%s'", row.id, term)
    except Exception:
        logger.exception("Failed to save YouTube report to DB")

    return report


@router.get("/api/youtube/reports")
async def list_youtube_reports(
    limit: int = Query(50, ge=1, le=200),
    offset: int = Query(0, ge=0),
):
    """List previously generated YouTube reports."""
    async with async_session() as db:
        stmt = (
            select(
                YouTubeReport.id,
                YouTubeReport.topic,
                YouTubeReport.hours,
                YouTubeReport.generated_at,
                YouTubeReport.video_count,
                YouTubeReport.gemini_cost_usd,
                YouTubeReport.youtube_units_used,
                YouTubeReport.error,
            )
            .order_by(YouTubeReport.generated_at.desc())
            .offset(offset)
            .limit(limit)
        )
        rows = (await db.execute(stmt)).all()

    return [
        {
            "id": r.id,
            "topic": r.topic,
            "hours": r.hours,
            "generated_at": r.generated_at.isoformat() if r.generated_at else None,
            "video_count": r.video_count,
            "gemini_cost_usd": r.gemini_cost_usd,
            "youtube_units_used": r.youtube_units_used,
            "has_error": bool(r.error),
        }
        for r in rows
    ]


@router.get("/api/youtube/reports/{report_id}")
async def get_youtube_report(report_id: int):
    """Load a saved YouTube report by ID."""
    async with async_session() as db:
        row = await db.get(YouTubeReport, report_id)
        if not row:
            return {"error": "Report not found"}
        return row.report_json
