#!/usr/bin/env python3
"""Safe helper utilities for upgrading automation.py prompts and rows.

Goal:
- Improve data richness for downstream analytics pages
- Keep legacy keys unchanged so existing pipeline/page-1 dashboard does not break
"""

from __future__ import annotations

import json
import re
from typing import Any


def build_v2_prompt(
    nation_label: str,
    nation_sources: str,
    prompt_extra: str = "",
    week_ago: str = "",
    today: str = "",
) -> str:
    """Return an upgraded prompt that preserves legacy fields and adds optional fields.

    IMPORTANT:
    - Legacy keys are still required.
    - New keys are optional but requested for analytics marts.
    """
    return f"""
You are a venture capital intelligence analyst. Perform a COMPREHENSIVE search
for ALL NEW funding announcements in the {nation_label} startup ecosystem from
{week_ago} to {today}.

MANDATORY SOURCES TO CHECK: {nation_sources}.
{prompt_extra}

SEARCH STRATEGY:
1. Search each source individually.
2. Use multiple search queries per source.
3. Cross-reference announcements across sources.
4. Include both major and emerging deals.

FILTERS:
1. Category: AI, Data, Machine Learning, SaaS, or Data Infrastructure.
2. Stage: Pre-Series A, Seed, Seed-plus, debt, Series A, and above.

CRITICAL: RETURN A RAW JSON LIST ONLY.
Do not include any conversational text before or after the JSON.

Output format:
[
  {{
    "Country": "Organization country name",
    "Startup_Name": "Name",
    "Description": "2-line business summary",
    "Amount": "Amount text exactly as announced",
    "Round": "Funding stage text",
    "Investors": "Comma-separated investor list",
    "Founders": "Founder names",
    "LinkedIn_Profile": "Founder LinkedIn URL or N/A",
    "Hiring": "Yes/No/Unknown",
    "Careers_Link": "Careers URL or N/A",

    "Announcement_Date": "YYYY-MM-DD if available else empty",
    "Source_URL": "Article URL if available else empty",
    "Source_Title": "Headline/title if available else empty",
    "Sector_Primary": "Primary sector label if available else empty",
    "Sector_Secondary": "Comma-separated sector labels if available else empty",
    "AI_Domain_Tags": "Comma-separated tags if available else empty",
    "Startup_City": "City if available else empty",
    "Confidence_Score": 0.0
  }}
]

If ZERO deals found after thorough search, return: []
IMPORTANT: Be exhaustive. Missing a deal is worse than finding none.
"""


def safe_extract_json_array(raw_text: str) -> list[dict[str, Any]]:
    """Extract first JSON array from model response without throwing."""
    if not raw_text:
        return []
    match = re.search(r"\[.*\]", raw_text.strip(), re.DOTALL)
    if not match:
        return []
    try:
        payload = json.loads(match.group())
    except json.JSONDecodeError:
        return []
    if not isinstance(payload, list):
        return []
    return [row for row in payload if isinstance(row, dict)]


def normalize_row_for_legacy_compat(row: dict[str, Any]) -> dict[str, Any]:
    """Guarantee legacy keys exist so old downstream code remains stable."""
    legacy_defaults = {
        "Country": "",
        "Startup_Name": "",
        "Description": "",
        "Amount": "",
        "Round": "",
        "Investors": "",
        "Founders": "",
        "LinkedIn_Profile": "N/A",
        "Hiring": "Unknown",
        "Careers_Link": "N/A",
    }
    normalized = dict(row)
    for key, default in legacy_defaults.items():
        value = normalized.get(key)
        if value is None or (isinstance(value, str) and not value.strip()):
            normalized[key] = default

    # Ensure optional new keys exist to reduce KeyError risk in analytics builders.
    for key in (
        "Announcement_Date",
        "Source_URL",
        "Source_Title",
        "Sector_Primary",
        "Sector_Secondary",
        "AI_Domain_Tags",
        "Startup_City",
    ):
        normalized.setdefault(key, "")
    normalized.setdefault("Confidence_Score", None)

    return normalized


def dedupe_deals_by_deal_signature(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """De-duplicate by deal-level signature (not startup name only).

    This prevents losing valid follow-on rounds for the same startup.
    """
    seen: set[tuple[str, str, str, str, str]] = set()
    deduped: list[dict[str, Any]] = []
    for row in rows:
        startup = str(row.get("Startup_Name", "")).strip().lower()
        round_name = str(row.get("Round", "")).strip().lower()
        amount = str(row.get("Amount", "")).strip().lower()
        date = str(row.get("Announcement_Date") or row.get("Date_Captured") or "").strip().lower()
        source = str(row.get("Source_URL", "")).strip().lower()
        signature = (startup, round_name, amount, date, source)
        if signature in seen:
            continue
        seen.add(signature)
        deduped.append(row)
    return deduped
