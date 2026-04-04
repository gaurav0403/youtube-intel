"""Monitoring report endpoints — narrative analysis across tracked channels."""

from __future__ import annotations

import logging
from datetime import datetime, timezone

from fastapi import APIRouter, Query
from sqlalchemy import select

from backend.database import async_session
from backend.models import MonitoringReport

router = APIRouter()
logger = logging.getLogger(__name__)


@router.post("/api/monitoring/generate")
async def generate_report(hours: int = Query(24, ge=1, le=168)):
    """Generate a monitoring report for the last N hours."""
    from backend.services.monitoring_report import generate_monitoring_report

    now = datetime.now(timezone.utc)
    result = await generate_monitoring_report(hours=hours)

    report = {
        "hours": hours,
        "generated_at": now.isoformat(),
        "video_count": result.get("video_count", 0),
        "channel_count": result.get("channel_count", 0),
        "videos": result.get("videos", []),
        "analysis": result.get("analysis"),
        "gemini_cost_usd": result.get("gemini_cost_usd", 0),
        "error": result.get("error"),
    }

    # Save to DB
    try:
        async with async_session() as db:
            row = MonitoringReport(
                hours=hours,
                generated_at=now,
                video_count=result.get("video_count", 0),
                channel_count=result.get("channel_count", 0),
                gemini_cost_usd=result.get("gemini_cost_usd", 0),
                report_json=report,
                error=result.get("error"),
            )
            db.add(row)
            await db.commit()
            await db.refresh(row)
            report["id"] = row.id
            logger.info("Monitoring report saved: id=%d, hours=%d, videos=%d", row.id, hours, result.get("video_count", 0))
    except Exception:
        logger.exception("Failed to save monitoring report to DB")

    return report


@router.get("/api/monitoring/reports")
async def list_reports(
    limit: int = Query(20, ge=1, le=100),
    offset: int = Query(0, ge=0),
):
    """List previously generated monitoring reports."""
    async with async_session() as db:
        stmt = (
            select(
                MonitoringReport.id,
                MonitoringReport.hours,
                MonitoringReport.generated_at,
                MonitoringReport.video_count,
                MonitoringReport.channel_count,
                MonitoringReport.gemini_cost_usd,
                MonitoringReport.error,
            )
            .order_by(MonitoringReport.generated_at.desc())
            .offset(offset)
            .limit(limit)
        )
        rows = (await db.execute(stmt)).all()

    return [
        {
            "id": r.id,
            "hours": r.hours,
            "generated_at": r.generated_at.isoformat() if r.generated_at else None,
            "video_count": r.video_count,
            "channel_count": r.channel_count,
            "gemini_cost_usd": r.gemini_cost_usd,
            "has_error": bool(r.error),
        }
        for r in rows
    ]


@router.get("/api/monitoring/reports/{report_id}")
async def get_report(report_id: int):
    """Load a saved monitoring report by ID."""
    async with async_session() as db:
        row = await db.get(MonitoringReport, report_id)
        if not row:
            return {"error": "Report not found"}
        return row.report_json
