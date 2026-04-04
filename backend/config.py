"""Configuration for YouTube Intel platform."""

from __future__ import annotations

import os
from pathlib import Path

from pydantic_settings import BaseSettings

env_path = Path(__file__).resolve().parent.parent / ".env"
if env_path.exists():
    for line in env_path.read_text().splitlines():
        line = line.strip()
        if line and not line.startswith("#") and "=" in line:
            k, v = line.split("=", 1)
            os.environ.setdefault(k.strip(), v.strip())


class Settings(BaseSettings):
    # Database
    database_url: str = "sqlite+aiosqlite:///./youtube_intel.db"

    # YouTube Data API v3
    youtube_api_key: str = ""

    # Gemini
    gemini_api_key: str = ""
    gemini_model: str = "gemini-2.5-flash"

    # Server
    cors_origins: str = "*"
    api_key: str = ""

    class Config:
        env_file = ".env"
        extra = "ignore"


settings = Settings()
