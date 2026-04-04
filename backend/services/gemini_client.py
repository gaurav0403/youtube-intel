"""Gemini API client — async REST calls via httpx."""

from __future__ import annotations

import json
import logging
from typing import Any, Dict

import httpx

from backend.config import settings

logger = logging.getLogger(__name__)

GEMINI_URL = "https://generativelanguage.googleapis.com/v1beta/models"


async def generate(
    prompt: str,
    system_instruction: str = "",
    temperature: float = 0.3,
) -> tuple[str, float]:
    """Call Gemini and return (text, cost_usd).

    Uses the model from settings (default: gemini-2.5-flash).
    """
    model = settings.gemini_model
    url = f"{GEMINI_URL}/{model}:generateContent"

    body: Dict[str, Any] = {
        "contents": [{"parts": [{"text": prompt}]}],
        "generationConfig": {
            "temperature": temperature,
            "responseMimeType": "application/json",
        },
    }
    if system_instruction:
        body["systemInstruction"] = {"parts": [{"text": system_instruction}]}

    headers = {
        "Content-Type": "application/json",
        "x-goog-api-key": settings.gemini_api_key,
    }

    async with httpx.AsyncClient(timeout=300.0) as client:
        resp = await client.post(url, headers=headers, json=body)
        resp.raise_for_status()
        data = resp.json()

    # Extract text
    text = ""
    for candidate in data.get("candidates", []):
        for part in candidate.get("content", {}).get("parts", []):
            text += part.get("text", "")

    # Calculate cost
    usage = data.get("usageMetadata", {})
    input_tokens = usage.get("promptTokenCount", 0)
    output_tokens = usage.get("candidatesTokenCount", 0)
    # Gemini 2.5 Flash: $0.30/M input, $2.50/M output
    cost = (input_tokens * 0.30 + output_tokens * 2.50) / 1_000_000

    return text, cost


def extract_json(text: str) -> Dict:
    """Robustly extract a JSON object from Gemini output."""
    clean = text.strip()
    if clean.startswith("```"):
        parts = clean.split("```")
        clean = parts[1] if len(parts) > 1 else clean
        if clean.startswith("json"):
            clean = clean[4:]
    try:
        return json.loads(clean.strip())
    except json.JSONDecodeError:
        start = text.find("{")
        end = text.rfind("}") + 1
        if start >= 0 and end > start:
            try:
                return json.loads(text[start:end])
            except json.JSONDecodeError:
                pass
    return {"error": "Could not parse JSON", "raw": text[:2000]}
