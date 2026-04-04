"""SQLAlchemy models for YouTube Intel."""

from __future__ import annotations

from datetime import datetime
from typing import Optional

from sqlalchemy import Boolean, DateTime, Float, Index, Integer, JSON, String, Text, func
from sqlalchemy.orm import Mapped, mapped_column

from backend.database import Base


class YouTubeReport(Base):
    __tablename__ = "youtube_reports"
    __table_args__ = (
        Index("ix_youtube_reports_topic_generated", "topic", "generated_at"),
    )

    id: Mapped[int] = mapped_column(primary_key=True)
    topic: Mapped[str] = mapped_column(String(256), index=True)
    topic_normalized: Mapped[str] = mapped_column(String(256), index=True)
    hours: Mapped[int] = mapped_column(Integer, default=168)
    generated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
    video_count: Mapped[int] = mapped_column(Integer, default=0)
    gemini_cost_usd: Mapped[float] = mapped_column(Float, default=0.0)
    youtube_units_used: Mapped[int] = mapped_column(Integer, default=0)
    report_json: Mapped[dict] = mapped_column(JSON)
    error: Mapped[Optional[str]] = mapped_column(Text, nullable=True)


class WatchedChannel(Base):
    __tablename__ = "watched_channels"

    id: Mapped[int] = mapped_column(primary_key=True)
    channel_id: Mapped[str] = mapped_column(String(64), unique=True, index=True)
    channel_name: Mapped[str] = mapped_column(String(256), default="")
    subscriber_count: Mapped[int] = mapped_column(Integer, default=0)
    added_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
    last_checked_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)
    last_video_id: Mapped[Optional[str]] = mapped_column(String(64), nullable=True)
    category: Mapped[Optional[str]] = mapped_column(String(128), nullable=True)
    is_active: Mapped[bool] = mapped_column(Boolean, default=True)
    thumbnail: Mapped[Optional[str]] = mapped_column(String(512), nullable=True)


class MonitoringReport(Base):
    __tablename__ = "monitoring_reports"

    id: Mapped[int] = mapped_column(primary_key=True)
    hours: Mapped[int] = mapped_column(Integer, default=24)
    generated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
    video_count: Mapped[int] = mapped_column(Integer, default=0)
    channel_count: Mapped[int] = mapped_column(Integer, default=0)
    gemini_cost_usd: Mapped[float] = mapped_column(Float, default=0.0)
    report_json: Mapped[dict] = mapped_column(JSON)
    error: Mapped[Optional[str]] = mapped_column(Text, nullable=True)


class NarrativeState(Base):
    __tablename__ = "narrative_states"

    id: Mapped[int] = mapped_column(primary_key=True)
    state_type: Mapped[str] = mapped_column(String(32), default="batch")  # "batch" | "incremental"
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now())
    window_start: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)
    window_end: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)
    total_videos_processed: Mapped[int] = mapped_column(Integer, default=0)
    total_channels: Mapped[int] = mapped_column(Integer, default=0)
    incremental_updates: Mapped[int] = mapped_column(Integer, default=0)
    state_json: Mapped[dict] = mapped_column(JSON, default=dict)
    total_gemini_cost_usd: Mapped[float] = mapped_column(Float, default=0.0)
    total_haiku_cost_usd: Mapped[float] = mapped_column(Float, default=0.0)
    last_video_id: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)  # ChannelVideo.id watermark
    is_active: Mapped[bool] = mapped_column(Boolean, default=True, index=True)
    error: Mapped[Optional[str]] = mapped_column(Text, nullable=True)


class ChannelVideo(Base):
    __tablename__ = "channel_videos"
    __table_args__ = (
        Index("ix_channel_videos_channel_published", "channel_id", "published_at"),
    )

    id: Mapped[int] = mapped_column(primary_key=True)
    video_id: Mapped[str] = mapped_column(String(64), unique=True, index=True)
    channel_id: Mapped[str] = mapped_column(String(64), index=True)
    title: Mapped[str] = mapped_column(String(512), default="")
    published_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    thumbnail: Mapped[Optional[str]] = mapped_column(String(512), nullable=True)
    view_count: Mapped[int] = mapped_column(Integer, default=0)
    detected_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
    topic_classification: Mapped[Optional[str]] = mapped_column(String(256), nullable=True)
    summary: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
