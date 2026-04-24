#!/usr/bin/env python3
"""Build an analytics-ready enriched dataset without changing legacy files.

This script is designed for the private collection repo and intentionally keeps
the existing `data/{Nation}.json` files untouched to avoid breaking the current
pipeline or dashboard.
"""

from __future__ import annotations

import argparse
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from analytics_common import (
    build_investor_entities,
    clean_string,
    has_meaningful_value,
    infer_sectors,
    load_json,
    load_nation_reference,
    normalize_hiring,
    normalize_round,
    now_utc_iso,
    parse_amount_info,
    parse_date,
    parse_datetime,
    resolve_nation_name,
    refresh_currency_rates,
    split_multi_value_field,
    stable_hash,
    write_json,
    extract_source_domain,
)

EXCLUDED_FILENAMES = {
    "manifest.json",
    "outlier.json",
    "fx_rates.json",
    "page2_global.json",
    "page3_country.json",
    "page4_investor_startup.json",
    "deals_enriched.json",
}


def detect_data_files(data_dir: Path) -> list[Path]:
    files: list[Path] = []
    for path in sorted(data_dir.glob("*.json"), key=lambda p: p.name.lower()):
        if path.name in EXCLUDED_FILENAMES:
            continue
        files.append(path)
    return files


def get_deal_announcement_date(deal: dict[str, Any], captured_date: str) -> str:
    explicit = clean_string(
        deal.get("Announcement_Date")
        or deal.get("AnnouncementDate")
        or deal.get("Deal_Date")
        or deal.get("Date_Announced")
    )
    if explicit:
        parsed = parse_date(explicit)
        if parsed:
            return parsed.isoformat()
    return captured_date


def get_captured_at_utc(
    deal: dict[str, Any],
    nation_payload: dict[str, Any],
    fallback_date_iso: str,
) -> str:
    raw_candidates = [
        deal.get("Date_Captured"),
        deal.get("Captured_At"),
        deal.get("captured_at"),
        nation_payload.get("last_updated"),
        fallback_date_iso,
    ]
    for candidate in raw_candidates:
        parsed = parse_datetime(candidate)
        if parsed:
            return parsed.astimezone(timezone.utc).isoformat(timespec="seconds")
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def get_source_url(deal: dict[str, Any]) -> str:
    candidates = [
        deal.get("Source_URL"),
        deal.get("SourceUrl"),
        deal.get("Source"),
        deal.get("News_URL"),
    ]
    for candidate in candidates:
        value = clean_string(candidate)
        if value.startswith(("http://", "https://", "www.")):
            if value.startswith("www."):
                return f"https://{value}"
            return value
    return ""


def normalize_legacy_investors(deal: dict[str, Any]) -> Any:
    for key in ("Investors", "Investor_List", "investors"):
        if key in deal:
            return deal.get(key)
    return ""


def normalize_legacy_founders(deal: dict[str, Any]) -> Any:
    for key in ("Founders", "Founder_Names", "founders"):
        if key in deal:
            return deal.get(key)
    return ""


def enrich_deal(
    deal: dict[str, Any],
    source_file: str,
    nation_payload: dict[str, Any],
    fallback_nation_name: str,
    alias_map: dict[str, str],
    nation_reference: dict[str, dict[str, Any]],
    currency_to_usd_rate: dict[str, float],
) -> dict[str, Any]:
    nation_name = resolve_nation_name(deal.get("Nation"), fallback_nation_name, alias_map)
    if nation_name == "Unknown":
        nation_name = resolve_nation_name(deal.get("Country"), fallback_nation_name, alias_map)

    nation_meta = nation_reference.get(nation_name, {})
    startup_name = clean_string(deal.get("Startup_Name") or deal.get("Startup") or "Unknown Startup")
    startup_key = startup_name.lower()
    startup_id = stable_hash("startup", nation_name, startup_key)

    captured_date_value = parse_date(deal.get("Date_Captured") or deal.get("Date"))
    captured_date_iso = (
        captured_date_value.isoformat()
        if captured_date_value
        else datetime.now(timezone.utc).date().isoformat()
    )
    announcement_date_iso = get_deal_announcement_date(deal, captured_date_iso)
    captured_at = get_captured_at_utc(deal, nation_payload, captured_date_iso)

    amount = parse_amount_info(
        deal.get("Amount"),
        currency_to_usd_rate=currency_to_usd_rate,
        default_currency=clean_string(nation_meta.get("default_currency") or "USD"),
    )

    round_raw = clean_string(deal.get("Round"))
    round_normalized = normalize_round(round_raw)

    investors_raw = normalize_legacy_investors(deal)
    investor_entities = build_investor_entities(investors_raw)
    founders = split_multi_value_field(normalize_legacy_founders(deal))

    source_url = get_source_url(deal)
    source_domain = extract_source_domain(source_url)
    source_title = clean_string(deal.get("Source_Title") or deal.get("SourceName") or "")

    description = clean_string(deal.get("Description"))
    sector_primary_input = clean_string(deal.get("Sector_Primary"))
    sector_secondary_input = deal.get("Sector_Secondary")
    if has_meaningful_value(sector_primary_input):
        sector_primary = sector_primary_input
        sector_secondary = split_multi_value_field(sector_secondary_input) or [sector_primary_input]
    else:
        sector_primary, sector_secondary = infer_sectors(description, startup_name)

    ai_domain_tags = split_multi_value_field(deal.get("AI_Domain_Tags"))
    if not ai_domain_tags:
        ai_domain_tags = [sector_primary]

    deal_id = stable_hash(
        "deal",
        startup_id,
        round_normalized,
        amount.get("amount_currency"),
        amount.get("amount_local_value"),
        announcement_date_iso,
        source_domain,
    )

    linked_in = clean_string(deal.get("LinkedIn_Profile"))
    careers = clean_string(deal.get("Careers_Link"))

    return {
        "deal_id": deal_id,
        "source_file": source_file,
        "startup_id": startup_id,
        "startup_name": startup_name,
        "startup_city": clean_string(deal.get("Startup_City")),
        "nation": nation_name,
        "country_iso2": clean_string(nation_meta.get("iso2")),
        "country_iso3": clean_string(nation_meta.get("iso3")),
        "region": clean_string(nation_meta.get("region")),
        "subregion": clean_string(nation_meta.get("subregion")),
        "announcement_date": announcement_date_iso,
        "captured_date": captured_date_iso,
        "captured_at": captured_at,
        "round_raw": round_raw,
        "round_normalized": round_normalized,
        "amount_raw": amount.get("amount_raw"),
        "amount_currency": amount.get("amount_currency"),
        "amount_local_value": amount.get("amount_local_value"),
        "amount_usd": amount.get("amount_usd"),
        "amount_was_converted": amount.get("amount_was_converted"),
        "investors": investor_entities,
        "investor_names": [investor["investor_name"] for investor in investor_entities],
        "founders": founders,
        "description": description,
        "sector_primary": sector_primary,
        "sector_secondary": sector_secondary,
        "ai_domain_tags": ai_domain_tags,
        "hiring_status": normalize_hiring(deal.get("Hiring")),
        "linkedin_profile": linked_in,
        "careers_link": careers,
        "has_linkedin": bool(linked_in and linked_in.lower() not in {"n/a", "na", "unknown"}),
        "has_careers": bool(careers and careers.lower() not in {"n/a", "na", "unknown"}),
        "source_url": source_url,
        "source_domain": source_domain,
        "source_title": source_title,
        "tier": clean_string(deal.get("Tier")),
        "confidence_score": deal.get("Confidence_Score") if isinstance(deal.get("Confidence_Score"), (int, float)) else None,
        "raw_deal": deal,
    }


def build_enriched_dataset(
    data_dir: Path,
    output_dir: Path,
    nation_reference_path: Path,
    fx_rates_path: Path | None,
) -> dict[str, Any]:
    nation_reference, alias_map = load_nation_reference(nation_reference_path)
    fx_registry = refresh_currency_rates(fx_rates_path)
    currency_to_usd_rate = fx_registry.get("currency_to_usd_rate", {})
    if not isinstance(currency_to_usd_rate, dict):
        currency_to_usd_rate = {}

    data_files = detect_data_files(data_dir)
    records: list[dict[str, Any]] = []
    duplicates_skipped = 0
    nation_counts = defaultdict(int)
    seen_deal_ids: set[str] = set()

    for json_path in data_files:
        payload = load_json(json_path)
        deals = payload.get("deals", []) if isinstance(payload, dict) else []
        if not isinstance(deals, list):
            continue

        nation_fallback = clean_string(json_path.stem)
        for deal in deals:
            if not isinstance(deal, dict):
                continue
            enriched = enrich_deal(
                deal=deal,
                source_file=json_path.name,
                nation_payload=payload if isinstance(payload, dict) else {},
                fallback_nation_name=nation_fallback,
                alias_map=alias_map,
                nation_reference=nation_reference,
                currency_to_usd_rate=currency_to_usd_rate,
            )
            if enriched["deal_id"] in seen_deal_ids:
                duplicates_skipped += 1
                continue
            seen_deal_ids.add(enriched["deal_id"])
            records.append(enriched)
            nation_counts[enriched["nation"]] += 1

    records.sort(
        key=lambda row: (
            row.get("announcement_date") or "",
            row.get("captured_at") or "",
            row.get("nation") or "",
            row.get("startup_name") or "",
        ),
        reverse=True,
    )

    payload = {
        "generated_at": now_utc_iso(),
        "schema_version": "2.0.0",
        "record_count": len(records),
        "source_file_count": len(data_files),
        "duplicates_skipped": duplicates_skipped,
        "nation_counts": dict(sorted(nation_counts.items())),
        "currency_rates_source": str(fx_rates_path) if fx_rates_path else "in-memory-only",
        "currency_rates_meta": {
            "provider": clean_string(fx_registry.get("provider")),
            "fetched_live": bool(fx_registry.get("fetched_live")),
            "is_complete_update": bool(fx_registry.get("is_complete_update")),
            "updated_at": clean_string(fx_registry.get("updated_at")),
            "source_updated_at": clean_string(fx_registry.get("source_updated_at")),
            "missing_live_currencies": fx_registry.get("missing_live_currencies")
            if isinstance(fx_registry.get("missing_live_currencies"), list)
            else [],
        },
        "records": records,
    }

    output_path = output_dir / "deals_enriched.json"
    write_json(output_path, payload)
    return payload


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Build enriched analytics dataset from legacy nation JSON files.")
    parser.add_argument("--data-dir", default="data", help="Directory containing legacy nation JSON files.")
    parser.add_argument("--output-dir", default="analytics", help="Output directory for analytics datasets.")
    parser.add_argument(
        "--nation-reference",
        default="nation_reference.json",
        help="Nation reference JSON with ISO and region metadata.",
    )
    parser.add_argument(
        "--fx-rates",
        default="data/fx_rates.json",
        help="Optional fx_rates.json path. If missing, fallback rates are used.",
    )
    return parser


def main() -> None:
    args = build_parser().parse_args()
    data_dir = Path(args.data_dir).resolve()
    output_dir = Path(args.output_dir).resolve()
    nation_reference_path = Path(args.nation_reference).resolve()
    fx_path = Path(args.fx_rates).resolve()

    if not data_dir.exists():
        raise FileNotFoundError(f"Data directory does not exist: {data_dir}")

    payload = build_enriched_dataset(
        data_dir=data_dir,
        output_dir=output_dir,
        nation_reference_path=nation_reference_path,
        fx_rates_path=fx_path,
    )
    print(
        f"Enriched dataset generated: {payload['record_count']} records "
        f"from {payload['source_file_count']} source files."
    )


if __name__ == "__main__":
    main()
