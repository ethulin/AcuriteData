"""Acurite Weather Station MCP Server.

Exposes weather data from an Acurite Iris station via myacurite.com.
"""

import os
import sys
import json
import statistics
from datetime import datetime, date, timedelta, timezone

from dotenv import load_dotenv
from fastmcp import FastMCP

from acurite_weather.acurite_api import AcuriteClient
from acurite_weather.derived import (
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
    """Query historical weather data from the station (up to ~365 days back).

    Args:
        start: Start date - ISO date like '2026-04-01' or relative like
               '24h ago', 'yesterday', '7 days ago', 'last week', 'this month'
        metric: One of: temperature, humidity, wind_speed, wind_gust,
                wind_direction, pressure, rainfall, dew_point, feels_like
        end: End date (same formats as start). Defaults to today.
        aggregation: 'raw' (5-min), '15m', 'hourly', 'daily', or 'auto'
        stat: For daily aggregation: 'mean', 'min', 'max', 'sum'.
              Defaults to 'mean' for most, 'sum' for rainfall, 'max' for wind.
    """
    try:
        now = datetime.now(timezone.utc)
        start_dt = _parse_time(start, now)
        end_dt = _parse_time(end, now) if end else now
        start_date = start_dt.date()
        end_date = end_dt.date()

        field = _metric_field(metric)
        res_map = {"raw": "5m-summaries", "15m": "15m-summaries", "hourly": "1h-summaries"}
        resolution = res_map.get(aggregation, "auto")

        history = client.get_history(start_date, end_date, resolution)

        # Extract the metric values
        data = []
        for r in history:
            ts = r.get("timestamp", "")
            val = r.get(field)
            if val is not None:
                # Filter to requested time range
                if ts >= start_dt.isoformat() and ts <= end_dt.isoformat() + "T23:59:59":
                    data.append({"timestamp": ts, "value": val})

        # Compute summary
        values = [d["value"] for d in data if isinstance(d["value"], (int, float))]
        summary = {}
        if values:
            summary = {
                "count": len(values),
                "min": round(min(values), 2),
                "max": round(max(values), 2),
                "mean": round(statistics.mean(values), 2),
                "first": values[0],
                "last": values[-1],
            }
            if metric == "rainfall":
                summary["sum"] = round(sum(values), 2)

        # If daily aggregation, aggregate by date
        if aggregation == "daily" and data:
            data = _aggregate_daily(data, stat or "auto", metric)

        return {
            "query": {
                "start": start_date.isoformat(),
                "end": end_date.isoformat(),
                "metric": metric,
                "field": field,
                "points": len(data),
            },
            "summary": summary,
            "data": data[:500],  # cap to avoid huge responses
        }
    except Exception as e:
        return {"error": str(e)}


@mcp.tool()
def get_records_and_extremes(
    period: str = "today",
    metrics: str = "all",
) -> dict:
    """Get high/low records for a time period (up to ~365 days back).

    Args:
        period: 'today', 'yesterday', 'this_week', 'last_week', 'this_month',
                'last_month', 'this_year', or date range like '2026-03-01/2026-03-31'
        metrics: Comma-separated: temperature, humidity, wind, pressure, rainfall.
                 Or 'all' for everything.
    """
    try:
        start_date, end_date = _parse_period(period)
        history = client.get_history(start_date, end_date)

        result = {"period": period, "range": f"{start_date} to {end_date}"}

        want = metrics.split(",") if metrics != "all" else [
            "temperature", "humidity", "wind", "pressure", "rainfall"
        ]

        for m in want:
            m = m.strip()
            if m == "temperature":
                result["temperature"] = _extremes(history, "temperature_f", "F")
            elif m == "humidity":
                result["humidity"] = _extremes(history, "humidity_pct", "%")
            elif m == "wind":
                result["wind_speed"] = _extremes(history, "wind_speed_mph", "mph")
                result["wind_gust"] = _extremes(history, "wind_speed_mph", "mph", stat="max")
            elif m == "pressure":
                result["pressure"] = _extremes(history, "pressure_inhg", "inHg")
            elif m == "rainfall":
                vals = [r.get("rainfall_daily_in", 0) for r in history
                        if r.get("rainfall_daily_in") is not None]
                if vals:
                    result["rainfall"] = {
                        "max_daily_in": round(max(vals), 2),
                    }

        return result
    except Exception as e:
        return {"error": str(e)}


@mcp.tool()
def analyze_trends(
    metric: str = "pressure",
    window: str = "6h",
) -> dict:
    """Analyze weather trends over a time window.

    Args:
        metric: temperature, humidity, wind_speed, pressure, rainfall, dew_point
        window: '1h', '3h', '6h', '12h', '24h', '48h', '7d'
    """
    try:
        hours = _window_to_hours(window)
        end_date = date.today()
        start_date = end_date - timedelta(days=max(1, hours // 24))

        resolution = "5m-summaries" if hours <= 6 else "15m-summaries" if hours <= 48 else "1h-summaries"
        history = client.get_history(start_date, end_date, resolution)

        cutoff = (datetime.now(timezone.utc) - timedelta(hours=hours)).isoformat()
        field = _metric_field(metric)

        points = []
        for r in history:
            ts = r.get("timestamp", "")
            val = r.get(field)
            if ts >= cutoff and val is not None and isinstance(val, (int, float)):
                points.append((ts, val))

        if len(points) < 2:
            return {"metric": metric, "window": window, "error": "Not enough data points"}

        first_val = points[0][1]
        last_val = points[-1][1]
        total_change = round(last_val - first_val, 2)
        rate_per_hour = round(total_change / hours, 4) if hours > 0 else 0

        values = [p[1] for p in points]
        result = {
            "metric": metric,
            "window": window,
            "points": len(points),
            "trend": {
                "direction": "rising" if total_change > 0.01 else "falling" if total_change < -0.01 else "steady",
                "total_change": total_change,
                "rate_per_hour": rate_per_hour,
                "start_value": round(first_val, 2),
                "end_value": round(last_val, 2),
                "min": round(min(values), 2),
                "max": round(max(values), 2),
            },
        }

        if metric == "pressure":
            change_3h = rate_per_hour * 3
            result["trend"]["pressure_context"] = pressure_trend_description(change_3h)
            result["trend"]["forecast_hint"] = pressure_forecast_hint(change_3h)

        return result
    except Exception as e:
        return {"error": str(e)}


@mcp.tool()
def check_thresholds(
    period: str = "today",
    checks: str = "",
) -> dict:
    """Check if weather metrics have crossed thresholds.

    Args:
        period: '1h', '3h', '6h', '12h', 'today', '24h', '48h', '7d'
        checks: JSON string of custom checks, e.g.
                '[{"metric": "wind_speed", "operator": "gt", "value": 30}]'
    """
    try:
        hours = _period_to_hours(period)
        end_date = date.today()
        start_date = end_date - timedelta(days=max(1, hours // 24))
        history = client.get_history(start_date, end_date)

        cutoff = (datetime.now(timezone.utc) - timedelta(hours=hours)).isoformat()
        recent = [r for r in history if r.get("timestamp", "") >= cutoff]

        temps = [r["temperature_f"] for r in recent if r.get("temperature_f") is not None]
        winds = [r["wind_speed_mph"] for r in recent if r.get("wind_speed_mph") is not None]
        rains = [r["rainfall_daily_in"] for r in recent if r.get("rainfall_daily_in") is not None]

        standard = {
            "freeze_occurred": bool(temps and min(temps) <= 32),
            "hard_freeze_occurred": bool(temps and min(temps) <= 28),
            "min_temp_f": round(min(temps), 1) if temps else None,
            "max_temp_f": round(max(temps), 1) if temps else None,
            "high_wind_occurred": bool(winds and max(winds) > 25),
            "max_wind_mph": round(max(winds), 1) if winds else None,
            "max_rain_daily_in": round(max(rains), 2) if rains else None,
            "data_points": len(recent),
        }

        custom_results = []
        if checks:
            try:
                for check in json.loads(checks):
                    field = _metric_field(check["metric"])
                    vals = [r[field] for r in recent if r.get(field) is not None]
                    threshold = check["value"]
                    op = check["operator"]
                    triggered = [v for v in vals if _compare(v, op, threshold)]
                    custom_results.append({
                        "metric": check["metric"],
                        "operator": op,
                        "threshold": threshold,
                        "triggered": len(triggered) > 0,
                        "occurrences": len(triggered),
                        "worst_value": round(max(triggered) if op in ("gt", "gte") else min(triggered), 2) if triggered else None,
                    })
            except (json.JSONDecodeError, KeyError) as e:
                custom_results.append({"error": f"Invalid checks: {e}"})

        return {
            "period": period,
            "standard_checks": standard,
            "custom_checks": custom_results or None,
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
        period: 'this_week', 'this_month', 'this_year', or date range '2026-03-01/2026-04-07'
        gdd_base_f: Base temperature for growing degree days (default 50F)
    """
    try:
        start_date, end_date = _parse_period(period)
        history = client.get_history(start_date, end_date, "1h-summaries")

        # Group by date for daily high/low
        by_date: dict[str, list[float]] = {}
        rain_by_date: dict[str, float] = {}
        for r in history:
            ts = r.get("timestamp", "")
            d = ts[:10]
            temp = r.get("temperature_f")
            if temp is not None:
                by_date.setdefault(d, []).append(temp)
            rain = r.get("rainfall_daily_in")
            if rain is not None:
                rain_by_date[d] = max(rain_by_date.get(d, 0), rain)

        # GDD calculation
        total_gdd = 0
        gdd_by_month: dict[str, float] = {}
        frost_dates = []
        for d in sorted(by_date.keys()):
            temps = by_date[d]
            high = max(temps)
            low = min(temps)
            gdd = growing_degree_days(high, low, gdd_base_f)
            total_gdd += gdd
            month = d[:7]
            gdd_by_month[month] = round(gdd_by_month.get(month, 0) + gdd, 1)
            if low <= 32:
                frost_dates.append(d)

        # Rain summary
        rain_by_month: dict[str, float] = {}
        for d, rain in rain_by_date.items():
            month = d[:7]
            rain_by_month[month] = round(rain_by_month.get(month, 0) + rain, 2)

        rainy_days = [d for d, r in rain_by_date.items() if r > 0.01]

        result = {
            "period": f"{start_date} to {end_date}",
            "growing_degree_days": {
                "total": round(total_gdd, 1),
                "base_f": gdd_base_f,
                "by_month": gdd_by_month,
            },
            "frost": {
                "frost_days": len(frost_dates),
                "last_frost_date": frost_dates[-1] if frost_dates else None,
                "first_frost_date": frost_dates[0] if frost_dates else None,
            },
            "rainfall": {
                "total_in": round(sum(rain_by_date.values()), 2),
                "rainy_days": len(rainy_days),
                "by_month": rain_by_month,
                "days_since_last_rain": (
                    (date.today() - date.fromisoformat(rainy_days[-1])).days
                    if rainy_days else None
                ),
            },
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
    metrics: str = "all",
) -> dict:
    """Export historical weather data as JSON (up to ~365 days).

    Args:
        start: Start date (ISO format or relative like 'yesterday', '7 days ago')
        end: End date (defaults to today)
        format: 'json' (CSV not yet supported)
        metrics: Comma-separated list or 'all'
    """
    try:
        now = datetime.now(timezone.utc)
        start_dt = _parse_time(start, now)
        end_dt = _parse_time(end, now) if end else now

        history = client.get_history(start_dt.date(), end_dt.date())

        if metrics != "all":
            wanted_fields = {_metric_field(m.strip()) for m in metrics.split(",")}
            wanted_fields.add("timestamp")
            history = [
                {k: v for k, v in r.items() if k in wanted_fields}
                for r in history
            ]

        return {
            "format": "json",
            "range": f"{start_dt.date()} to {end_dt.date()}",
            "record_count": len(history),
            "data": history[:1000],  # cap size
        }
    except Exception as e:
        return {"error": str(e)}


# --- Helper functions ---

def _parse_time(s: str, now: datetime) -> datetime:
    s = s.strip().lower()
    if not s:
        return now
    if s == "now":
        return now
    if s == "yesterday":
        return (now - timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0)
    if s == "today":
        return now.replace(hour=0, minute=0, second=0, microsecond=0)
    if s == "last week":
        return (now - timedelta(weeks=1)).replace(hour=0, minute=0, second=0, microsecond=0)
    if s == "this week":
        return (now - timedelta(days=now.weekday())).replace(hour=0, minute=0, second=0, microsecond=0)
    if s == "this month":
        return now.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    if s == "last month":
        first = now.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
        return (first - timedelta(days=1)).replace(day=1)

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

    for fmt in ("%Y-%m-%d", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%dT%H:%M:%SZ", "%Y-%m"):
        try:
            dt = datetime.strptime(s, fmt)
            return dt.replace(tzinfo=timezone.utc)
        except ValueError:
            continue
    raise ValueError(f"Cannot parse time: '{s}'")


def _parse_period(period: str) -> tuple[date, date]:
    today = date.today()
    if period == "today":
        return today, today
    if period == "yesterday":
        d = today - timedelta(days=1)
        return d, d
    if period == "this_week":
        return today - timedelta(days=today.weekday()), today
    if period == "last_week":
        end = today - timedelta(days=today.weekday() + 1)
        return end - timedelta(days=6), end
    if period == "this_month":
        return today.replace(day=1), today
    if period == "last_month":
        first_this = today.replace(day=1)
        last_prev = first_this - timedelta(days=1)
        return last_prev.replace(day=1), last_prev
    if period == "this_year":
        return today.replace(month=1, day=1), today
    if "/" in period:
        parts = period.split("/")
        return date.fromisoformat(parts[0]), date.fromisoformat(parts[1])
    return date.fromisoformat(period), date.fromisoformat(period)


def _metric_field(metric: str) -> str:
    mapping = {
        "temperature": "temperature_f",
        "humidity": "humidity_pct",
        "wind_speed": "wind_speed_mph",
        "wind_gust": "wind_speed_mph",
        "wind_direction": "wind_direction_deg",
        "pressure": "pressure_inhg",
        "rainfall": "rainfall_daily_in",
        "dew_point": "dew_point_f",
        "feels_like": "feels_like_f",
    }
    return mapping.get(metric, metric)


def _compare(value: float, op: str, threshold: float) -> bool:
    ops = {"gt": lambda a, b: a > b, "lt": lambda a, b: a < b,
           "gte": lambda a, b: a >= b, "lte": lambda a, b: a <= b,
           "eq": lambda a, b: a == b}
    return ops.get(op, lambda a, b: False)(value, threshold)


def _extremes(history: list[dict], field: str, unit: str, stat: str = "both") -> dict:
    vals = [(r["timestamp"], r[field]) for r in history if r.get(field) is not None]
    if not vals:
        return {"no_data": True}
    min_entry = min(vals, key=lambda x: x[1])
    max_entry = max(vals, key=lambda x: x[1])
    all_vals = [v[1] for v in vals]
    result = {"unit": unit, "mean": round(statistics.mean(all_vals), 2)}
    if stat in ("both", "min"):
        result["low"] = {"value": round(min_entry[1], 2), "at": min_entry[0]}
    if stat in ("both", "max"):
        result["high"] = {"value": round(max_entry[1], 2), "at": max_entry[0]}
    return result


def _aggregate_daily(data: list[dict], stat: str, metric: str) -> list[dict]:
    by_date: dict[str, list[float]] = {}
    for d in data:
        day = d["timestamp"][:10]
        by_date.setdefault(day, []).append(d["value"])

    if stat == "auto":
        stat = "sum" if metric == "rainfall" else "max" if "wind" in metric else "mean"

    result = []
    for day, vals in sorted(by_date.items()):
        if stat == "mean":
            v = round(statistics.mean(vals), 2)
        elif stat == "max":
            v = round(max(vals), 2)
        elif stat == "min":
            v = round(min(vals), 2)
        elif stat == "sum":
            v = round(sum(vals), 2)
        else:
            v = round(statistics.mean(vals), 2)
        result.append({"date": day, "value": v})
    return result


def _window_to_hours(window: str) -> int:
    if window.endswith("h"):
        return int(window[:-1])
    if window.endswith("d"):
        return int(window[:-1]) * 24
    return 6


def _period_to_hours(period: str) -> int:
    mapping = {"1h": 1, "3h": 3, "6h": 6, "12h": 12, "today": 24,
               "24h": 24, "48h": 48, "7d": 168}
    return mapping.get(period, 24)


def main():
    mcp.run()


if __name__ == "__main__":
    main()
