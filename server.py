"""Acurite Weather Station MCP Server.

Exposes weather data from an Acurite Iris station via myacurite.com.
"""

import os
import sys
import json
from datetime import datetime, timedelta, timezone

from dotenv import load_dotenv
from fastmcp import FastMCP

from acurite_api import AcuriteClient
from derived import (
    enrich_conditions,
    growing_degree_days,
    pressure_trend_description,
    pressure_forecast_hint,
    f_to_c,
    beaufort_scale,
    frost_risk as calc_frost_risk,
    dew_point as calc_dew_point,
)

load_dotenv()

# Validate required env vars
required = ["ACURITE_EMAIL", "ACURITE_PASSWORD"]
missing = [k for k in required if not os.environ.get(k)]
if missing:
    sys.exit(f"Missing required env vars: {', '.join(missing)}")

client = AcuriteClient(
    email=os.environ["ACURITE_EMAIL"],
    password=os.environ["ACURITE_PASSWORD"],
    device_mac=os.environ.get("ACURITE_DEVICE_MAC"),
)

mcp = FastMCP("Acurite Weather Station")


@mcp.tool()
def get_current_conditions() -> dict:
    """Get current weather conditions from the Acurite Iris station.

    Returns temperature, humidity, wind, rain, pressure, and derived values
    including heat index, wind chill, feels-like, comfort level, frost risk,
    and Beaufort wind scale. All readings include both imperial and metric units.
    """
    try:
        readings = client.get_current_conditions()
        derived = enrich_conditions(readings)
        # Remove internal fields
        readings.pop("_raw_sensors", None)
        return {"readings": readings, "derived": derived}
    except Exception as e:
        return {"error": str(e)}


@mcp.tool()
def query_history(
    start: str,
    metric: str,
    end: str = "",
    aggregation: str = "auto",
    stat: str = "auto",
) -> dict:
    """Query historical weather data from the station (up to 31 days back).

    Args:
        start: Start time - ISO date like '2026-04-01' or relative like
               '24h ago', 'yesterday', '7 days ago', 'last week', 'this month'
        metric: One of: temperature, humidity, wind_speed, wind_gust,
                wind_direction, pressure, rainfall, dew_point
        end: End time (same formats as start). Defaults to now.
        aggregation: 'raw' (5-min), 'hourly', 'daily', or 'auto' (picks based on range)
        stat: For aggregated data: 'mean', 'min', 'max', 'sum'. Defaults to
              'mean' for most metrics, 'sum' for rainfall, 'max' for wind_gust.
    """
    try:
        # Parse time range
        now = datetime.now(timezone.utc)
        start_dt = _parse_time(start, now)
        end_dt = _parse_time(end, now) if end else now

        # For now, get current data and explain limitation
        # TODO: Discover and implement the myacurite.com history endpoint
        readings = client.get_current_conditions()
        readings.pop("_raw_sensors", None)

        return {
            "note": (
                "Historical data query is planned but the myacurite.com history "
                "API endpoint has not yet been reverse-engineered. Currently only "
                "live data is available. The myacurite.com dashboard shows up to "
                "31 days of history — the endpoint powering that will be discovered "
                "and integrated."
            ),
            "query": {
                "start": start_dt.isoformat(),
                "end": end_dt.isoformat(),
                "metric": metric,
                "aggregation": aggregation,
                "stat": stat,
            },
            "current_reading": readings.get(_metric_field(metric)),
        }
    except Exception as e:
        return {"error": str(e)}


@mcp.tool()
def get_records_and_extremes(
    period: str = "today",
    metrics: str = "all",
) -> dict:
    """Get high/low records for a time period.

    Args:
        period: 'today', 'yesterday', 'this_week', 'last_week', 'this_month',
                'last_month', or a specific date like '2026-04-01'
        metrics: Comma-separated list of: temperature, humidity, wind, pressure,
                 rainfall. Or 'all' for everything.
    """
    try:
        # Same limitation as query_history for now
        readings = client.get_current_conditions()
        readings.pop("_raw_sensors", None)
        derived = enrich_conditions(readings)

        return {
            "note": (
                "Records and extremes require the historical data API endpoint "
                "which is not yet integrated. Currently showing latest readings only."
            ),
            "period": period,
            "latest": {
                "temperature_f": readings.get("temperature_f"),
                "humidity_pct": readings.get("humidity_pct"),
                "wind_speed_mph": readings.get("wind_speed_mph"),
                "wind_gust_mph": readings.get("wind_gust_mph"),
                "pressure_inhg": readings.get("pressure_inhg"),
                "rainfall_in": readings.get("rainfall_in"),
            },
        }
    except Exception as e:
        return {"error": str(e)}


@mcp.tool()
def analyze_trends(
    metric: str = "pressure",
    window: str = "6h",
) -> dict:
    """Analyze weather trends over a time window.

    Args:
        metric: One of: temperature, humidity, wind_speed, pressure, rainfall, dew_point
        window: Time window: '1h', '3h', '6h', '12h', '24h', '48h', '7d'
    """
    try:
        readings = client.get_current_conditions()
        readings.pop("_raw_sensors", None)

        return {
            "note": (
                "Trend analysis requires the historical data API endpoint "
                "which is not yet integrated. Currently showing latest reading only."
            ),
            "metric": metric,
            "window": window,
            "current_value": readings.get(_metric_field(metric)),
        }
    except Exception as e:
        return {"error": str(e)}


@mcp.tool()
def check_thresholds(
    period: str = "today",
    checks: str = "",
) -> dict:
    """Check if weather metrics have crossed thresholds.

    Args:
        period: How far back to check: '1h', '3h', '6h', '12h', 'today', '24h', '48h', '7d'
        checks: JSON string of custom checks, e.g.
                '[{"metric": "wind_gust", "operator": "gt", "value": 30}]'
                If empty, runs standard checks (freeze, high wind, heavy rain).
    """
    try:
        readings = client.get_current_conditions()
        readings.pop("_raw_sensors", None)
        derived = enrich_conditions(readings)

        temp = readings.get("temperature_f")
        wind = readings.get("wind_speed_mph", 0)
        gust = readings.get("wind_gust_mph", 0)
        rain = readings.get("rainfall_in", 0)

        standard = {
            "current_freeze": temp is not None and temp <= 32,
            "current_hard_freeze": temp is not None and temp <= 28,
            "current_high_wind": (gust or 0) > 30 or (wind or 0) > 25,
            "current_heavy_rain": (rain or 0) > 1.0,
            "frost_risk": derived.get("frost_risk", "unknown"),
            "comfort_level": derived.get("comfort_level", "unknown"),
        }

        # Process custom checks against current values
        custom_results = []
        if checks:
            try:
                check_list = json.loads(checks)
                for check in check_list:
                    field = _metric_field(check["metric"])
                    value = readings.get(field)
                    if value is not None:
                        op = check["operator"]
                        threshold = check["value"]
                        triggered = _compare(value, op, threshold)
                        custom_results.append({
                            "metric": check["metric"],
                            "operator": op,
                            "threshold": threshold,
                            "current_value": value,
                            "triggered": triggered,
                        })
            except (json.JSONDecodeError, KeyError) as e:
                custom_results.append({"error": f"Invalid checks format: {e}"})

        return {
            "period": period,
            "note": "Threshold checks against current values only. Historical checking requires the history API endpoint.",
            "standard_checks": standard,
            "custom_checks": custom_results if custom_results else None,
        }
    except Exception as e:
        return {"error": str(e)}


@mcp.tool()
def get_agricultural_data(
    period: str = "this_year",
    gdd_base_f: float = 50,
) -> dict:
    """Get agricultural and gardening weather data.

    Args:
        period: 'this_week', 'this_month', 'this_year', 'last_year',
                or date range like '2026-03-01/2026-04-07'
        gdd_base_f: Base temperature for growing degree days (default 50F)
    """
    try:
        readings = client.get_current_conditions()
        readings.pop("_raw_sensors", None)
        derived = enrich_conditions(readings)

        temp = readings.get("temperature_f")
        humidity = readings.get("humidity_pct")
        rain = readings.get("rainfall_in", 0)

        result = {
            "note": (
                "Full agricultural data (GDD accumulation, frost date history, "
                "monthly rainfall) requires the history API endpoint. "
                "Currently showing current conditions relevant to agriculture."
            ),
            "current": {
                "temperature_f": temp,
                "humidity_pct": humidity,
                "frost_risk": derived.get("frost_risk"),
                "comfort_level": derived.get("comfort_level"),
                "rain_today_in": rain,
                "dew_point_f": readings.get("dew_point_f") or derived.get("dew_point_f"),
            },
            "gdd_base_f": gdd_base_f,
        }

        return result
    except Exception as e:
        return {"error": str(e)}


@mcp.tool()
def get_device_status() -> dict:
    """Get device health and status information.

    Returns battery level, signal strength, last check-in time,
    hub and sensor names, MAC address, and API connection status.
    """
    try:
        info = client.get_device_info()
        info["api_status"] = {
            "authenticated": client.token is not None,
            "token_age_hours": round((
                __import__("time").time() - client.token_time
            ) / 3600, 1) if client.token else None,
        }
        return info
    except Exception as e:
        return {"error": str(e)}


@mcp.tool()
def export_data(
    start: str,
    end: str = "",
    format: str = "json",
    interval: str = "raw",
    metrics: str = "all",
) -> dict:
    """Export historical weather data.

    Args:
        start: Start date (ISO format or relative)
        end: End date (defaults to now)
        format: 'json' or 'csv'
        interval: 'raw' (5-min), 'hourly', 'daily'
        metrics: Comma-separated list or 'all'
    """
    try:
        return {
            "note": (
                "Data export requires the historical data API endpoint "
                "which is not yet integrated. Once available, this tool will "
                "generate CSV or JSON exports of your weather data."
            ),
            "query": {
                "start": start,
                "end": end or "now",
                "format": format,
                "interval": interval,
                "metrics": metrics,
            },
        }
    except Exception as e:
        return {"error": str(e)}


# --- Helper functions ---

def _parse_time(s: str, now: datetime) -> datetime:
    """Parse a time string into a datetime."""
    s = s.strip().lower()
    if not s:
        return now

    # Relative patterns
    if s == "now":
        return now
    if s == "yesterday":
        return (now - timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0)
    if s == "today":
        return now.replace(hour=0, minute=0, second=0, microsecond=0)
    if s == "last week":
        return (now - timedelta(weeks=1)).replace(hour=0, minute=0, second=0, microsecond=0)
    if s == "this week":
        days_since_monday = now.weekday()
        return (now - timedelta(days=days_since_monday)).replace(hour=0, minute=0, second=0, microsecond=0)
    if s == "this month":
        return now.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    if s == "last month":
        first_of_month = now.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
        return (first_of_month - timedelta(days=1)).replace(day=1)

    # "Xh ago", "Xd ago", "X days ago", "X hours ago"
    if s.endswith(" ago"):
        parts = s[:-4].strip().split()
        if len(parts) == 1:
            val = parts[0]
            if val.endswith("h"):
                return now - timedelta(hours=int(val[:-1]))
            if val.endswith("d"):
                return now - timedelta(days=int(val[:-1]))
        elif len(parts) == 2:
            num, unit = int(parts[0]), parts[1]
            if unit.startswith("hour"):
                return now - timedelta(hours=num)
            if unit.startswith("day"):
                return now - timedelta(days=num)
            if unit.startswith("week"):
                return now - timedelta(weeks=num)
            if unit.startswith("month"):
                return now - timedelta(days=num * 30)

    # ISO date or datetime
    for fmt in ("%Y-%m-%d", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%dT%H:%M:%SZ", "%Y-%m"):
        try:
            dt = datetime.strptime(s, fmt)
            return dt.replace(tzinfo=timezone.utc)
        except ValueError:
            continue

    raise ValueError(f"Cannot parse time: '{s}'")


def _metric_field(metric: str) -> str:
    """Map metric name to readings dict field."""
    mapping = {
        "temperature": "temperature_f",
        "humidity": "humidity_pct",
        "wind_speed": "wind_speed_mph",
        "wind_gust": "wind_gust_mph",
        "wind_direction": "wind_direction_deg",
        "pressure": "pressure_inhg",
        "rainfall": "rainfall_in",
        "dew_point": "dew_point_f",
    }
    return mapping.get(metric, metric)


def _compare(value: float, op: str, threshold: float) -> bool:
    ops = {
        "gt": lambda a, b: a > b,
        "lt": lambda a, b: a < b,
        "gte": lambda a, b: a >= b,
        "lte": lambda a, b: a <= b,
        "eq": lambda a, b: a == b,
    }
    return ops.get(op, lambda a, b: False)(value, threshold)


if __name__ == "__main__":
    mcp.run()
