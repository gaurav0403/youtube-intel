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
async def generate_report(
    hours: int = Query(24, ge=1, le=168),
    format_pass: bool = Query(False),
):
    """Generate a monitoring report.

    Prefers state-based generation if an active NarrativeState exists.
    Falls back to legacy stateless generation otherwise.
    format_pass=False (default) uses instant Python formatting (~0s, free).
    format_pass=True uses a Gemini call for polished prose (~10s, ~$0.02).
    """
    now = datetime.now(timezone.utc)

    # Try state-based generation first
    try:
        from backend.services.monitoring_report import generate_report_from_state
        state_result = await generate_report_from_state(format_pass=format_pass)
        if state_result and state_result.get("analysis"):
            report = {
                "hours": state_result.get("hours", hours),
                "generated_at": state_result.get("generated_at", now.isoformat()),
                "video_count": state_result.get("video_count", 0),
                "short_count": state_result.get("short_count", 0),
                "long_count": state_result.get("long_count", 0),
                "channel_count": state_result.get("channel_count", 0),
                "videos": state_result.get("videos", []),
                "analysis": state_result.get("analysis"),
                "gemini_cost_usd": state_result.get("gemini_cost_usd", 0),
                "state_id": state_result.get("state_id"),
                "state_based": True,
                "error": None,
            }
            # Save to DB
            try:
                async with async_session() as db:
                    row = MonitoringReport(
                        hours=hours,
                        generated_at=now,
                        video_count=state_result.get("video_count", 0),
                        channel_count=state_result.get("channel_count", 0),
                        gemini_cost_usd=state_result.get("gemini_cost_usd", 0),
                        report_json=report,
                    )
                    db.add(row)
                    await db.commit()
                    await db.refresh(row)
                    report["id"] = row.id
                    logger.info(
                        "State-based report saved: id=%d, state=%d, videos=%d",
                        row.id, state_result.get("state_id", 0), state_result.get("video_count", 0),
                    )
            except Exception:
                logger.exception("Failed to save state-based report to DB")
            return report
    except Exception:
        logger.exception("State-based report generation failed, falling back to legacy")

    # Fallback: legacy stateless generation
    from backend.services.monitoring_report import generate_monitoring_report
    result = await generate_monitoring_report(hours=hours)

    report = {
        "hours": hours,
        "generated_at": now.isoformat(),
        "video_count": result.get("video_count", 0),
        "channel_count": result.get("channel_count", 0),
        "videos": result.get("videos", []),
        "analysis": result.get("analysis"),
        "gemini_cost_usd": result.get("gemini_cost_usd", 0),
        "state_based": False,
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
            logger.info("Legacy report saved: id=%d, hours=%d, videos=%d", row.id, hours, result.get("video_count", 0))
    except Exception:
        logger.exception("Failed to save monitoring report to DB")

    return report


@router.post("/api/monitoring/batch")
async def trigger_batch_build(hours: int = Query(24, ge=1, le=168)):
    """Trigger a fresh batch narrative state build as a background task.

    Returns immediately with status=started. Poll GET /api/monitoring/state
    to check when the build completes. If a build is already in progress,
    returns status=busy without starting a duplicate run.
    """
    import asyncio
    from backend.services.narrative_state import build_batch_state, is_batch_build_in_progress

    if is_batch_build_in_progress():
        return {
            "status": "busy",
            "message": "Batch build already in progress. Poll GET /api/monitoring/state to check progress.",
        }

    async def _run_batch() -> None:
        try:
            result = await build_batch_state(hours=hours)
            if result.get("error"):
                logger.error("Batch build error: %s", result["error"])
            else:
                logger.info(
                    "Batch build complete: state=%d, %d videos, %d narratives, cost=$%.4f",
                    result.get("state_id", 0),
                    result.get("videos_processed", 0),
                    result.get("narratives", 0),
                    result.get("gemini_cost_usd", 0) + result.get("haiku_cost_usd", 0),
                )
        except Exception:
            logger.exception("Background batch build failed")

    asyncio.create_task(_run_batch())
    return {"status": "started", "message": "Batch build started in background. Poll GET /api/monitoring/state to check progress."}


@router.get("/api/monitoring/state")
async def get_active_state():
    """Get the currently active narrative state metadata."""
    from backend.services.narrative_state import get_active_state

    state = await get_active_state()
    if not state:
        return {"active": False, "error": "No active narrative state"}
    return {"active": True, **state}


@router.get("/api/monitoring/state/{state_id}")
async def get_state_detail(state_id: int):
    """Get full narrative state by ID including state_json."""
    from backend.services.narrative_state import get_state_by_id

    state = await get_state_by_id(state_id)
    if not state:
        return {"error": "State not found"}
    return state


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
