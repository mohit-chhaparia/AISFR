#!/usr/bin/env python3
"""Shared helpers for private-repo dataset enrichment and marts."""

from __future__ import annotations

import hashlib
import json
import re
import urllib.request
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

UNKNOWN_TOKENS = {
    "",
    "n/a",
    "na",
    "none",
    "unknown",
    "undisclosed",
    "not disclosed",
    "not available",
    "nil",
    "-",
}

ROUND_GROUP_ORDER = [
    "Pre-Seed",
    "Seed",
    "Pre-Series A",
    "Series A",
    "Series B",
    "Series C",
    "Series D+",
    "Growth/Late Stage",
    "Acceleration",
    "Bridge",
    "Debt",
    "Grant",
    "Strategic",
    "Venture/Other",
    "Other",
    "Unknown",
]

FALLBACK_CURRENCY_TO_USD_RATE = {
    "USD": 1.0,
    "USDC": 1.0,
    "AED": 0.2723,
    "AUD": 0.66,
    "BRL": 0.198,
    "CAD": 0.74,
    "CHF": 1.27,
    "CNY": 0.138,
    "DKK": 0.145,
    "EUR": 1.09,
    "GBP": 1.28,
    "ILS": 0.27,
    "INR": 0.012,
    "JPY": 0.0067,
    "KRW": 0.00069,
    "RUB": 0.0129,
    "SEK": 0.094,
    "SGD": 0.74,
    "TWD": 0.031,
    "ZAR": 0.053,
}
FX_API_URL = "https://open.er-api.com/v6/latest/USD"

USD_PREFIX_PATTERN = re.compile(
    r"(?:US\$|USD|\$)\s*\d[\d,]*(?:\.\d+)?(?:\s*(?:trillion|tn|billion|bn|million|mn|thousand|k|[tmb]))?",
    re.IGNORECASE,
)
USD_SUFFIX_PATTERN = re.compile(
    r"\d[\d,]*(?:\.\d+)?(?:\s*(?:trillion|tn|billion|bn|million|mn|thousand|k|[tmb]))?\s*(?:US\$|USD)",
    re.IGNORECASE,
)
GENERIC_AMOUNT_PATTERN = re.compile(
    r"\d[\d,]*(?:\.\d+)?(?:\s*(?:trillion|tn|billion|bn|million|mn|thousand|k|[tmb]))?",
    re.IGNORECASE,
)

SECTOR_RULES: dict[str, tuple[str, set[str]]] = {
    "health": ("Health AI", {"health", "biotech", "medtech", "clinical", "drug"}),
    "finance": ("Fintech AI", {"fintech", "payments", "bank", "lending", "insurtech"}),
    "security": ("Cybersecurity AI", {"cyber", "security", "fraud", "identity"}),
    "developer": ("Developer Tooling", {"developer", "devtool", "copilot", "code"}),
    "infrastructure": ("AI Infrastructure", {"infrastructure", "gpu", "model", "llm", "compute", "cloud"}),
    "enterprise": ("Enterprise SaaS", {"saas", "workflow", "automation", "crm", "erp"}),
    "retail": ("Retail & Commerce", {"retail", "commerce", "ecommerce", "marketplace"}),
    "supply": ("Supply Chain", {"supply", "logistics", "warehouse", "freight"}),
    "education": ("Education AI", {"edtech", "learning", "education", "tutor"}),
    "climate": ("Climate & Energy", {"climate", "energy", "carbon", "battery", "grid"}),
    "robotics": ("Robotics", {"robot", "autonomous", "drone"}),
}


def now_utc_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def load_json(path: Path) -> Any:
    with path.open("r", encoding="utf-8") as handle:
        return json.load(handle)


def write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(f"{json.dumps(payload, indent=2, ensure_ascii=True)}\n", encoding="utf-8")


def clean_string(value: Any) -> str:
    return str(value or "").strip()


def normalize_key(value: Any) -> str:
    return re.sub(r"[^a-z0-9]+", " ", clean_string(value).lower()).strip()


def has_meaningful_value(value: Any) -> bool:
    return normalize_key(value) not in UNKNOWN_TOKENS


def normalize_url(value: Any) -> str:
    raw = clean_string(value)
    if not raw:
        return ""
    if raw.lower().startswith(("http://", "https://")):
        return raw
    if raw.lower().startswith("www."):
        return f"https://{raw}"
    if re.match(r"^[a-z0-9-]+\.[a-z]{2,}(/[^\s]*)?$", raw, re.IGNORECASE):
        return f"https://{raw}"
    return ""


def extract_source_domain(url: Any) -> str:
    normalized = normalize_url(url)
    if not normalized:
        return ""
    parsed = urlparse(normalized)
    return parsed.netloc.replace("www.", "").lower()


def stable_hash(*parts: Any) -> str:
    payload = "||".join(clean_string(part) for part in parts)
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def parse_date(value: Any) -> date | None:
    raw = clean_string(value)
    if not raw:
        return None
    cleaned = raw.replace("Z", "+00:00")
    try:
        parsed = datetime.fromisoformat(cleaned)
        return parsed.date()
    except ValueError:
        pass
    patterns = [
        "%Y-%m-%d",
        "%Y/%m/%d",
        "%d-%m-%Y",
        "%m/%d/%Y",
        "%b %d, %Y",
        "%B %d, %Y",
        "%d %b %Y",
        "%d %B %Y",
    ]
    for pattern in patterns:
        try:
            return datetime.strptime(raw, pattern).date()
        except ValueError:
            continue
    return None


def parse_datetime(value: Any) -> datetime | None:
    raw = clean_string(value)
    if not raw:
        return None
    cleaned = raw.replace("Z", "+00:00")
    try:
        parsed = datetime.fromisoformat(cleaned)
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=timezone.utc)
        return parsed.astimezone(timezone.utc)
    except ValueError:
        pass
    parsed_date = parse_date(raw)
    if parsed_date:
        return datetime(parsed_date.year, parsed_date.month, parsed_date.day, tzinfo=timezone.utc)
    return None


def parse_amount_candidate(value: str) -> float | None:
    normalized = clean_string(value).lower().replace(",", "").strip()
    number_match = re.search(r"(\d+(?:\.\d+)?)", normalized)
    if not number_match:
        return None
    numeric_value = float(number_match.group(1))
    if re.search(r"\btrillion\b|\btn\b|(?<![a-z])t(?![a-z])", normalized):
        numeric_value *= 1e12
    elif re.search(r"\bbillion\b|\bbn\b|(?<![a-z])b(?![a-z])", normalized):
        numeric_value *= 1e9
    elif re.search(r"\bmillion\b|\bmn\b|(?<![a-z])m(?![a-z])", normalized):
        numeric_value *= 1e6
    elif re.search(r"\bthousand\b|(?<![a-z])k(?![a-z])", normalized):
        numeric_value *= 1e3
    return numeric_value


def match_amount_candidates(value: str, pattern: re.Pattern[str]) -> list[str]:
    return [match.group(0) for match in pattern.finditer(clean_string(value))]


def detect_amount_currency(value: Any) -> str:
    cleaned = clean_string(value).upper()
    if not cleaned:
        return ""
    if re.search(r"(?:US\$|USD|\$)", cleaned):
        return "USD"
    currency_matchers = [
        ("AED", r"\bAED\b"),
        ("AUD", r"\bAUD\b"),
        ("BRL", r"\bBRL\b"),
        ("CAD", r"\bCAD\b"),
        ("CHF", r"\bCHF\b"),
        ("CNY", r"\bCNY\b"),
        ("DKK", r"\bDKK\b"),
        ("EUR", r"\bEUR\b|€"),
        ("GBP", r"\bGBP\b|£"),
        ("ILS", r"\bILS\b"),
        ("INR", r"\bINR\b|₹"),
        ("JPY", r"\bJPY\b|¥"),
        ("KRW", r"\bKRW\b|₩"),
        ("RUB", r"\bRUB\b"),
        ("SEK", r"\bSEK\b"),
        ("SGD", r"\bSGD\b"),
        ("TWD", r"\bTWD\b"),
        ("USDC", r"\bUSDC\b"),
        ("ZAR", r"\bZAR\b"),
    ]
    for currency, pattern in currency_matchers:
        if re.search(pattern, cleaned):
            return currency
    return ""


def parse_amount_info(
    value: Any,
    currency_to_usd_rate: dict[str, float] | None = None,
    default_currency: str = "USD",
) -> dict[str, Any]:
    rates = currency_to_usd_rate or FALLBACK_CURRENCY_TO_USD_RATE
    raw = clean_string(value)
    if not raw:
        return {
            "amount_raw": "",
            "amount_currency": default_currency or "USD",
            "amount_local_value": None,
            "amount_usd": None,
            "amount_was_converted": False,
        }
    if normalize_key(raw) in UNKNOWN_TOKENS:
        return {
            "amount_raw": raw,
            "amount_currency": default_currency or "USD",
            "amount_local_value": None,
            "amount_usd": None,
            "amount_was_converted": False,
        }
    explicit_usd_candidates = [
        *match_amount_candidates(raw, USD_PREFIX_PATTERN),
        *match_amount_candidates(raw, USD_SUFFIX_PATTERN),
    ]
    for candidate in explicit_usd_candidates:
        parsed = parse_amount_candidate(candidate)
        if parsed is None:
            continue
        return {
            "amount_raw": raw,
            "amount_currency": "USD",
            "amount_local_value": parsed,
            "amount_usd": parsed,
            "amount_was_converted": False,
        }
    detected_currency = detect_amount_currency(raw) or default_currency or "USD"
    for candidate in match_amount_candidates(raw, GENERIC_AMOUNT_PATTERN):
        parsed = parse_amount_candidate(candidate)
        if parsed is None:
            continue
        if detected_currency == "USD":
            amount_usd = parsed
            converted = False
        else:
            rate = rates.get(detected_currency)
            amount_usd = parsed * rate if isinstance(rate, (int, float)) else None
            converted = amount_usd is not None
        return {
            "amount_raw": raw,
            "amount_currency": detected_currency,
            "amount_local_value": parsed,
            "amount_usd": amount_usd,
            "amount_was_converted": converted,
        }
    return {
        "amount_raw": raw,
        "amount_currency": detected_currency,
        "amount_local_value": None,
        "amount_usd": None,
        "amount_was_converted": False,
    }


def normalize_round(round_raw: Any) -> str:
    lower = clean_string(round_raw).lower()
    if not lower:
        return "Unknown"
    if "acceleration" in lower:
        return "Acceleration"
    if re.search(r"\bpre[- ]?seed\b", lower) and "series a" not in lower:
        return "Pre-Seed"
    if re.search(r"\bpre[- ]?series\s*a\b", lower) or "seed/pre-series" in lower:
        return "Pre-Series A"
    if re.search(r"series\s*[d-z](?!\w)", lower):
        return "Series D+"
    if "series c" in lower:
        return "Series C"
    if "series b" in lower:
        return "Series B"
    if "series a" in lower:
        return "Series A"
    if "seed" in lower:
        return "Seed"
    if any(token in lower for token in ("growth", "late stage", "later stage", "unicorn")):
        return "Growth/Late Stage"
    if "bridge" in lower:
        return "Bridge"
    if "debt" in lower:
        return "Debt"
    if "grant" in lower:
        return "Grant"
    if "strategic" in lower:
        return "Strategic"
    if "venture" in lower:
        return "Venture/Other"
    return "Other"


def normalize_hiring(value: Any) -> str:
    normalized = normalize_key(value)
    if normalized in {"yes", "true", "hiring"}:
        return "Hiring"
    if normalized in {"no", "false", "not hiring"}:
        return "Not Hiring"
    return "Unknown"


def infer_sectors(*texts: Any) -> tuple[str, list[str]]:
    haystack = " ".join(clean_string(text).lower() for text in texts)
    if not haystack:
        return ("Other AI/Data", ["Other AI/Data"])
    matched: list[str] = []
    for _, (sector_name, keywords) in SECTOR_RULES.items():
        if any(keyword in haystack for keyword in keywords):
            matched.append(sector_name)
    if not matched:
        return ("Other AI/Data", ["Other AI/Data"])
    deduped = sorted(set(matched))
    return (deduped[0], deduped)


def split_multi_value_field(value: Any) -> list[str]:
    if isinstance(value, list):
        raw_values = [clean_string(item) for item in value]
    else:
        normalized = clean_string(value)
        if not normalized:
            return []
        normalized = re.sub(r"\s+(and|&)\s+", ",", normalized, flags=re.IGNORECASE)
        raw_values = [token.strip() for token in re.split(r"[;,]|(?:\s{2,})", normalized)]
    seen: set[str] = set()
    cleaned_values: list[str] = []
    for token in raw_values:
        normalized_token = normalize_key(token)
        if not normalized_token or normalized_token in UNKNOWN_TOKENS:
            continue
        if normalized_token in seen:
            continue
        seen.add(normalized_token)
        cleaned_values.append(token)
    return cleaned_values


def build_investor_entities(investors_raw: Any) -> list[dict[str, str]]:
    entities: list[dict[str, str]] = []
    for investor_name in split_multi_value_field(investors_raw):
        investor_key = normalize_key(investor_name)
        entities.append(
            {
                "investor_id": stable_hash("investor", investor_key),
                "investor_name": investor_name,
                "investor_type": "Unknown",
                "investor_country_iso2": "",
            }
        )
    return entities


def load_nation_reference(path: Path) -> tuple[dict[str, dict[str, Any]], dict[str, str]]:
    if not path.exists():
        return {}, {}
    payload = load_json(path)
    if not isinstance(payload, dict):
        return {}, {}
    canonical_map: dict[str, dict[str, Any]] = {}
    alias_map: dict[str, str] = {}
    for nation_name, info in payload.items():
        if not isinstance(info, dict):
            continue
        canonical_map[nation_name] = info
        alias_map[normalize_key(nation_name)] = nation_name
        for alias in info.get("aliases", []):
            alias_map[normalize_key(alias)] = nation_name
    return canonical_map, alias_map


def resolve_nation_name(raw_value: Any, fallback_value: Any, alias_map: dict[str, str]) -> str:
    candidates = [clean_string(raw_value), clean_string(fallback_value)]
    for candidate in candidates:
        key = normalize_key(candidate)
        if key in alias_map:
            return alias_map[key]
    for candidate in candidates:
        if candidate:
            return candidate
    return "Unknown"


def load_currency_rates_from_file(path: Path | None) -> dict[str, float]:
    rates = dict(FALLBACK_CURRENCY_TO_USD_RATE)
    if path is None or not path.exists():
        return rates
    payload = load_json(path)
    if isinstance(payload, dict) and isinstance(payload.get("currency_to_usd_rate"), dict):
        for currency, rate in payload["currency_to_usd_rate"].items():
            if isinstance(rate, (int, float)):
                rates[currency] = float(rate)
    return rates


def load_existing_fx_registry(path: Path | None) -> dict[str, Any]:
    if path is None or not path.exists():
        return {}
    payload = load_json(path)
    return payload if isinstance(payload, dict) else {}


def fetch_live_fx_payload(timeout_seconds: int = 30) -> dict[str, Any]:
    with urllib.request.urlopen(FX_API_URL, timeout=timeout_seconds) as response:
        return json.loads(response.read().decode("utf-8"))


def build_fx_rate_registry(path: Path | None) -> dict[str, Any]:
    existing_registry = load_existing_fx_registry(path)
    previous_rates = existing_registry.get("currency_to_usd_rate", {})
    if not isinstance(previous_rates, dict):
        previous_rates = {}

    generated_at = now_utc_iso()

    try:
        payload = fetch_live_fx_payload()
        if payload.get("result") != "success":
            raise ValueError(f"Unexpected FX API status: {payload.get('result')}")
        provider = clean_string(payload.get("provider")) or FX_API_URL
        fetched_at = clean_string(payload.get("time_last_update_utc")) or generated_at
        source_rates = payload.get("rates") or {}
        fetched_live = True
    except Exception as error:  # pragma: no cover - fallback path is intentional
        provider = f"{FX_API_URL} (fallback)"
        fetched_at = generated_at
        source_rates = {}
        fetched_live = False
        print(f"Warning: failed to fetch live FX rates, using fallback values. {error}")

    currency_rates: dict[str, float] = {}
    currency_sources: dict[str, str] = {}
    for currency, fallback_rate in FALLBACK_CURRENCY_TO_USD_RATE.items():
        if currency in {"USD", "USDC"}:
            currency_rates[currency] = 1.0
            currency_sources[currency] = "fixed"
            continue

        per_usd = source_rates.get(currency)
        if isinstance(per_usd, (int, float)) and per_usd:
            currency_rates[currency] = 1 / float(per_usd)
            currency_sources[currency] = "live"
        elif isinstance(previous_rates.get(currency), (int, float)) and previous_rates.get(currency):
            currency_rates[currency] = float(previous_rates[currency])
            currency_sources[currency] = "previous"
        else:
            currency_rates[currency] = float(fallback_rate)
            currency_sources[currency] = "fallback"

    currency_rates["USD"] = 1.0
    currency_rates["USDC"] = 1.0
    currency_sources["USD"] = "fixed"
    currency_sources["USDC"] = "fixed"

    missing_live_currencies = [
        currency
        for currency in sorted(FALLBACK_CURRENCY_TO_USD_RATE.keys())
        if currency_sources.get(currency) not in {"live", "fixed"}
    ]
    is_complete_update = not missing_live_currencies
    last_complete_update = clean_string(existing_registry.get("last_complete_update"))
    last_partial_update = clean_string(existing_registry.get("last_partial_update"))
    if is_complete_update:
        last_complete_update = generated_at
    else:
        last_partial_update = generated_at

    return {
        "updated_at": generated_at,
        "source_updated_at": fetched_at,
        "provider": provider,
        "fetched_live": fetched_live,
        "is_complete_update": is_complete_update,
        "last_complete_update": last_complete_update,
        "last_partial_update": last_partial_update,
        "missing_live_currencies": missing_live_currencies,
        "base_currency": "USD",
        "currency_to_usd_rate": currency_rates,
        "currency_sources": currency_sources,
    }


def refresh_currency_rates(path: Path | None) -> dict[str, Any]:
    registry = build_fx_rate_registry(path)
    if path is not None:
        write_json(path, registry)
    return registry

