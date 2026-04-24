#!/usr/bin/env python3
"""Build page-specific dashboard marts from deals_enriched.json.

Outputs:
  - analytics/page2_global.json
  - analytics/page3_country.json
  - analytics/page4_investor_startup.json
"""

from __future__ import annotations

import argparse
from collections import defaultdict
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from analytics_common import (
    ROUND_GROUP_ORDER,
    clean_string,
    has_meaningful_value,
    load_json,
    now_utc_iso,
    parse_date,
    write_json,
)


def load_enriched_records(path: Path) -> list[dict[str, Any]]:
    payload = load_json(path)
    if not isinstance(payload, dict):
        return []
    records = payload.get("records")
    return records if isinstance(records, list) else []


def round_or_zero(value: Any) -> float:
    return round(float(value), 2) if isinstance(value, (int, float)) else 0.0


def date_window(records: list[dict[str, Any]], lookback_days: int) -> tuple[date, date]:
    announcement_dates = []
    for record in records:
        parsed = parse_date(record.get("announcement_date"))
        if parsed:
            announcement_dates.append(parsed)
    if announcement_dates:
        end_date = max(announcement_dates)
    else:
        end_date = datetime.now(timezone.utc).date()
    start_date = end_date - timedelta(days=max(lookback_days - 1, 0))
    return start_date, end_date


def filter_records_by_window(records: list[dict[str, Any]], start_date: date, end_date: date) -> list[dict[str, Any]]:
    filtered: list[dict[str, Any]] = []
    for record in records:
        parsed = parse_date(record.get("announcement_date"))
        if parsed is None:
            continue
        if start_date <= parsed <= end_date:
            filtered.append(record)
    return filtered


def build_global_alerts(records: list[dict[str, Any]], end_date: date) -> list[dict[str, Any]]:
    alerts: list[dict[str, Any]] = []

    latest_day_records = []
    previous_30_day_records = []
    for record in records:
        parsed = parse_date(record.get("announcement_date"))
        if not parsed:
            continue
        if parsed == end_date:
            latest_day_records.append(record)
        if end_date - timedelta(days=30) <= parsed < end_date:
            previous_30_day_records.append(record)

    latest_day_funding = sum(record.get("amount_usd") or 0 for record in latest_day_records)
    previous_funding_values = []
    by_day: defaultdict[date, float] = defaultdict(float)
    for record in previous_30_day_records:
        parsed = parse_date(record.get("announcement_date"))
        if parsed:
            by_day[parsed] += record.get("amount_usd") or 0
    previous_funding_values = list(by_day.values())
    baseline = (sum(previous_funding_values) / len(previous_funding_values)) if previous_funding_values else 0

    if baseline > 0 and latest_day_funding >= 2 * baseline:
        alerts.append(
            {
                "severity": "high",
                "type": "funding_spike",
                "entity": "global",
                "message": "Daily funding is more than 2x the trailing 30-day average.",
                "metric_value": round_or_zero(latest_day_funding),
                "baseline": round_or_zero(baseline),
                "detected_at": now_utc_iso(),
            }
        )

    large_deals = sorted(
        [record for record in records if isinstance(record.get("amount_usd"), (int, float))],
        key=lambda record: record.get("amount_usd") or 0,
        reverse=True,
    )
    if large_deals and (large_deals[0].get("amount_usd") or 0) >= 250_000_000:
        largest = large_deals[0]
        alerts.append(
            {
                "severity": "medium",
                "type": "large_round",
                "entity": largest.get("startup_name") or "Unknown Startup",
                "message": "Large round detected (>= $250M).",
                "metric_value": round_or_zero(largest.get("amount_usd")),
                "baseline": 250_000_000,
                "detected_at": now_utc_iso(),
            }
        )

    stale_threshold = end_date - timedelta(days=7)
    by_country_last_seen: dict[str, date] = {}
    for record in records:
        country = clean_string(record.get("nation"))
        parsed = parse_date(record.get("announcement_date"))
        if not country or not parsed:
            continue
        prev = by_country_last_seen.get(country)
        if prev is None or parsed > prev:
            by_country_last_seen[country] = parsed
    stale_countries = sorted([country for country, last_seen in by_country_last_seen.items() if last_seen < stale_threshold])
    if stale_countries:
        alerts.append(
            {
                "severity": "low",
                "type": "country_stale",
                "entity": ", ".join(stale_countries[:5]),
                "message": "One or more countries have no deal updates in the last 7 days.",
                "metric_value": len(stale_countries),
                "baseline": 0,
                "detected_at": now_utc_iso(),
            }
        )

    return alerts


def build_page2_global(records: list[dict[str, Any]], lookback_days: int) -> dict[str, Any]:
    start_date, end_date = date_window(records, lookback_days)
    window_records = filter_records_by_window(records, start_date, end_date)

    total_deals = len(window_records)
    amount_values = [record.get("amount_usd") for record in window_records if isinstance(record.get("amount_usd"), (int, float))]
    total_funding = sum(amount_values)
    avg_deal = (total_funding / len(amount_values)) if amount_values else 0
    unique_countries = sorted({clean_string(record.get("nation")) for record in window_records if clean_string(record.get("nation"))})

    daily_stats: defaultdict[str, dict[str, Any]] = defaultdict(lambda: {"deal_count": 0, "funding_usd": 0.0})
    country_stats: defaultdict[str, dict[str, Any]] = defaultdict(lambda: {"deal_count": 0, "funding_usd": 0.0, "iso3": "", "region": ""})

    for record in window_records:
        day_key = clean_string(record.get("announcement_date"))
        if day_key:
            daily_stats[day_key]["deal_count"] += 1
            daily_stats[day_key]["funding_usd"] += record.get("amount_usd") or 0

        nation = clean_string(record.get("nation"))
        if nation:
            country_stats[nation]["deal_count"] += 1
            country_stats[nation]["funding_usd"] += record.get("amount_usd") or 0
            country_stats[nation]["iso3"] = clean_string(record.get("country_iso3"))
            country_stats[nation]["region"] = clean_string(record.get("region"))

    daily_trend = [
        {
            "date": day,
            "deal_count": values["deal_count"],
            "funding_usd": round_or_zero(values["funding_usd"]),
        }
        for day, values in sorted(daily_stats.items())
    ]

    leaderboard = sorted(
        [
            {
                "country": country,
                "iso3": values["iso3"],
                "region": values["region"],
                "deal_count": values["deal_count"],
                "funding_usd": round_or_zero(values["funding_usd"]),
                "avg_deal_usd": round_or_zero(values["funding_usd"] / values["deal_count"]) if values["deal_count"] else 0,
            }
            for country, values in country_stats.items()
        ],
        key=lambda item: (item["funding_usd"], item["deal_count"]),
        reverse=True,
    )

    world_map = [
        {
            "iso3": item["iso3"],
            "country": item["country"],
            "deal_count": item["deal_count"],
            "funding_usd": item["funding_usd"],
            "region": item["region"],
        }
        for item in leaderboard
    ]

    return {
        "generated_at": now_utc_iso(),
        "window_start": start_date.isoformat(),
        "window_end": end_date.isoformat(),
        "kpis": {
            "total_deals": total_deals,
            "total_funding_usd": round_or_zero(total_funding),
            "average_deal_usd": round_or_zero(avg_deal),
            "active_countries": len(unique_countries),
        },
        "daily_trend": daily_trend,
        "country_leaderboard": leaderboard,
        "world_map": world_map,
        "alerts": build_global_alerts(window_records, end_date),
    }


def build_page3_country(records: list[dict[str, Any]], lookback_days: int) -> dict[str, Any]:
    start_date, end_date = date_window(records, lookback_days)
    window_records = filter_records_by_window(records, start_date, end_date)
    by_country: defaultdict[str, list[dict[str, Any]]] = defaultdict(list)
    for record in window_records:
        nation = clean_string(record.get("nation"))
        if nation:
            by_country[nation].append(record)

    countries_payload: dict[str, Any] = {}
    for nation, nation_records in sorted(by_country.items()):
        funding_values = [record.get("amount_usd") for record in nation_records if isinstance(record.get("amount_usd"), (int, float))]
        total_funding = sum(funding_values)
        total_deals = len(nation_records)
        avg_deal = (total_funding / len(funding_values)) if funding_values else 0

        trend_by_day: defaultdict[str, dict[str, Any]] = defaultdict(lambda: {"deal_count": 0, "funding_usd": 0.0})
        round_mix: defaultdict[str, int] = defaultdict(int)
        sector_mix: defaultdict[str, float] = defaultdict(float)
        startup_agg: dict[str, dict[str, Any]] = {}

        for record in nation_records:
            day_key = clean_string(record.get("announcement_date"))
            if day_key:
                trend_by_day[day_key]["deal_count"] += 1
                trend_by_day[day_key]["funding_usd"] += record.get("amount_usd") or 0

            round_name = clean_string(record.get("round_normalized")) or "Unknown"
            round_mix[round_name] += 1

            sector_name = clean_string(record.get("sector_primary")) or "Other AI/Data"
            sector_mix[sector_name] += record.get("amount_usd") or 0

            startup_id = clean_string(record.get("startup_id"))
            startup_name = clean_string(record.get("startup_name")) or "Unknown Startup"
            startup_entry = startup_agg.setdefault(
                startup_id,
                {
                    "startup_id": startup_id,
                    "startup_name": startup_name,
                    "deal_count": 0,
                    "total_funding_usd": 0.0,
                    "latest_round": "",
                    "latest_announcement_date": "",
                },
            )
            startup_entry["deal_count"] += 1
            startup_entry["total_funding_usd"] += record.get("amount_usd") or 0
            announcement_date = clean_string(record.get("announcement_date"))
            if announcement_date > startup_entry["latest_announcement_date"]:
                startup_entry["latest_announcement_date"] = announcement_date
                startup_entry["latest_round"] = round_name

        round_stage_mix = []
        for round_name in ROUND_GROUP_ORDER:
            if round_name in round_mix:
                round_stage_mix.append({"round": round_name, "deal_count": round_mix[round_name]})
        for round_name, count in sorted(round_mix.items()):
            if round_name not in ROUND_GROUP_ORDER:
                round_stage_mix.append({"round": round_name, "deal_count": count})

        treemap = [
            {
                "name": sector_name,
                "value_funding_usd": round_or_zero(amount),
            }
            for sector_name, amount in sorted(sector_mix.items(), key=lambda item: item[1], reverse=True)
        ]

        top_startups = sorted(
            startup_agg.values(),
            key=lambda row: (row["total_funding_usd"], row["deal_count"]),
            reverse=True,
        )[:25]
        for startup in top_startups:
            startup["total_funding_usd"] = round_or_zero(startup["total_funding_usd"])

        countries_payload[nation] = {
            "country_meta": {
                "country": nation,
                "iso2": clean_string(nation_records[0].get("country_iso2")),
                "iso3": clean_string(nation_records[0].get("country_iso3")),
                "region": clean_string(nation_records[0].get("region")),
                "subregion": clean_string(nation_records[0].get("subregion")),
            },
            "kpis": {
                "total_deals": total_deals,
                "total_funding_usd": round_or_zero(total_funding),
                "average_deal_usd": round_or_zero(avg_deal),
                "active_startups": len(startup_agg),
            },
            "funding_trend": [
                {
                    "date": day,
                    "deal_count": values["deal_count"],
                    "funding_usd": round_or_zero(values["funding_usd"]),
                }
                for day, values in sorted(trend_by_day.items())
            ],
            "round_stage_mix": round_stage_mix,
            "ai_sector_treemap": treemap,
            "top_startups": top_startups,
        }

    return {
        "generated_at": now_utc_iso(),
        "window_start": start_date.isoformat(),
        "window_end": end_date.isoformat(),
        "countries": countries_payload,
    }


def build_page4_investor_startup(records: list[dict[str, Any]], lookback_days: int) -> dict[str, Any]:
    start_date, end_date = date_window(records, lookback_days)
    window_records = filter_records_by_window(records, start_date, end_date)

    investor_totals: dict[str, dict[str, Any]] = {}
    investor_country_matrix: defaultdict[tuple[str, str], dict[str, Any]] = defaultdict(
        lambda: {"deal_count": 0, "funding_usd": 0.0}
    )
    startup_points: dict[str, dict[str, Any]] = {}

    for record in window_records:
        nation = clean_string(record.get("nation"))
        amount_usd = record.get("amount_usd") or 0
        investors = record.get("investors") if isinstance(record.get("investors"), list) else []
        startup_id = clean_string(record.get("startup_id"))
        startup_name = clean_string(record.get("startup_name")) or "Unknown Startup"
        round_name = clean_string(record.get("round_normalized")) or "Unknown"
        date_value = clean_string(record.get("announcement_date"))

        startup_entry = startup_points.setdefault(
            startup_id,
            {
                "startup_id": startup_id,
                "startup_name": startup_name,
                "country": nation,
                "deal_count": 0,
                "total_funding_usd": 0.0,
                "investor_ids": set(),
                "latest_round": "",
                "latest_announcement_date": "",
            },
        )
        startup_entry["deal_count"] += 1
        startup_entry["total_funding_usd"] += amount_usd
        for investor in investors:
            investor_id = clean_string(investor.get("investor_id"))
            if investor_id:
                startup_entry["investor_ids"].add(investor_id)
        if date_value > startup_entry["latest_announcement_date"]:
            startup_entry["latest_announcement_date"] = date_value
            startup_entry["latest_round"] = round_name

        for investor in investors:
            investor_id = clean_string(investor.get("investor_id"))
            investor_name = clean_string(investor.get("investor_name"))
            if not investor_id:
                continue
            investor_entry = investor_totals.setdefault(
                investor_id,
                {
                    "investor_id": investor_id,
                    "investor_name": investor_name or "Unknown Investor",
                    "deal_count": 0,
                    "funding_exposure_usd": 0.0,
                    "countries": set(),
                },
            )
            investor_entry["deal_count"] += 1
            investor_entry["funding_exposure_usd"] += amount_usd
            if nation:
                investor_entry["countries"].add(nation)

            matrix_key = (investor_id, nation)
            investor_country_matrix[matrix_key]["deal_count"] += 1
            investor_country_matrix[matrix_key]["funding_usd"] += amount_usd

    top_investors = []
    for investor in investor_totals.values():
        top_investors.append(
            {
                "investor_id": investor["investor_id"],
                "investor_name": investor["investor_name"],
                "deal_count": investor["deal_count"],
                "funding_exposure_usd": round_or_zero(investor["funding_exposure_usd"]),
                "country_count": len(investor["countries"]),
                "countries": sorted(investor["countries"]),
            }
        )
    top_investors.sort(key=lambda row: (row["deal_count"], row["funding_exposure_usd"]), reverse=True)

    heatmap = []
    for (investor_id, nation), values in investor_country_matrix.items():
        investor_name = investor_totals.get(investor_id, {}).get("investor_name", "Unknown Investor")
        heatmap.append(
            {
                "investor_id": investor_id,
                "investor_name": investor_name,
                "country": nation,
                "deal_count": values["deal_count"],
                "funding_usd": round_or_zero(values["funding_usd"]),
            }
        )
    heatmap.sort(key=lambda row: (row["deal_count"], row["funding_usd"]), reverse=True)

    bubble = []
    for startup in startup_points.values():
        bubble.append(
            {
                "startup_id": startup["startup_id"],
                "startup_name": startup["startup_name"],
                "country": startup["country"],
                "deal_count": startup["deal_count"],
                "total_funding_usd": round_or_zero(startup["total_funding_usd"]),
                "investor_count": len(startup["investor_ids"]),
                "latest_round": startup["latest_round"],
                "latest_announcement_date": startup["latest_announcement_date"],
            }
        )
    bubble.sort(key=lambda row: (row["total_funding_usd"], row["deal_count"]), reverse=True)

    recent_deals = sorted(
        [
            {
                "deal_id": clean_string(record.get("deal_id")),
                "announcement_date": clean_string(record.get("announcement_date")),
                "startup_name": clean_string(record.get("startup_name")),
                "country": clean_string(record.get("nation")),
                "round": clean_string(record.get("round_normalized")),
                "amount_usd": round_or_zero(record.get("amount_usd")),
                "investors": record.get("investor_names") if isinstance(record.get("investor_names"), list) else [],
                "source_url": clean_string(record.get("source_url")),
                "source_domain": clean_string(record.get("source_domain")),
            }
            for record in window_records
        ],
        key=lambda row: (row["announcement_date"], row["amount_usd"]),
        reverse=True,
    )[:200]

    return {
        "generated_at": now_utc_iso(),
        "window_start": start_date.isoformat(),
        "window_end": end_date.isoformat(),
        "top_investors": top_investors[:50],
        "investor_country_heatmap": heatmap[:2000],
        "startup_bubble_chart": bubble[:1000],
        "recent_deal_feed": recent_deals,
    }


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Build page-level marts for dashboard pages 2-4.")
    parser.add_argument("--enriched", default="analytics/deals_enriched.json", help="Path to deals_enriched.json")
    parser.add_argument("--output-dir", default="analytics", help="Output directory for page marts.")
    parser.add_argument(
        "--lookback-days",
        type=int,
        default=365,
        help="Lookback window used for mart generation.",
    )
    return parser


def main() -> None:
    args = build_parser().parse_args()
    enriched_path = Path(args.enriched).resolve()
    output_dir = Path(args.output_dir).resolve()
    lookback_days = max(args.lookback_days, 1)

    if not enriched_path.exists():
        raise FileNotFoundError(
            f"Enriched dataset missing: {enriched_path}. "
            "Run build_enriched_dataset.py first."
        )

    records = load_enriched_records(enriched_path)
    page2 = build_page2_global(records, lookback_days=lookback_days)
    page3 = build_page3_country(records, lookback_days=lookback_days)
    page4 = build_page4_investor_startup(records, lookback_days=lookback_days)

    write_json(output_dir / "page2_global.json", page2)
    write_json(output_dir / "page3_country.json", page3)
    write_json(output_dir / "page4_investor_startup.json", page4)

    print(
        "Analytics marts generated successfully: "
        f"page2_global.json ({len(page2.get('daily_trend', []))} points), "
        f"page3_country.json ({len(page3.get('countries', {}))} countries), "
        f"page4_investor_startup.json ({len(page4.get('top_investors', []))} investors)."
    )


if __name__ == "__main__":
    main()
