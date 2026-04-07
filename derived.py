"""Derived meteorological calculations.

All formulas use NWS standards where applicable.
Temperatures in Fahrenheit, wind in mph, pressure in inHg.
"""

import math


def heat_index(temp_f: float, humidity: float) -> float | None:
    """NWS heat index (Rothfusz regression). Valid when temp >= 80F and humidity >= 40%."""
    if temp_f < 80 or humidity < 40:
        return None
    hi = (
        -42.379
        + 2.04901523 * temp_f
        + 10.14333127 * humidity
        - 0.22475541 * temp_f * humidity
        - 0.00683783 * temp_f**2
        - 0.05481717 * humidity**2
        + 0.00122874 * temp_f**2 * humidity
        + 0.00085282 * temp_f * humidity**2
        - 0.00000199 * temp_f**2 * humidity**2
    )
    # Low humidity adjustment
    if humidity < 13 and 80 <= temp_f <= 112:
        hi -= ((13 - humidity) / 4) * math.sqrt((17 - abs(temp_f - 95)) / 17)
    # High humidity adjustment
    if humidity > 85 and 80 <= temp_f <= 87:
        hi += ((humidity - 85) / 10) * ((87 - temp_f) / 5)
    return round(hi, 1)


def wind_chill(temp_f: float, wind_mph: float) -> float | None:
    """NWS wind chill formula. Valid when temp <= 50F and wind >= 3mph."""
    if temp_f > 50 or wind_mph < 3:
        return None
    wc = (
        35.74
        + 0.6215 * temp_f
        - 35.75 * wind_mph**0.16
        + 0.4275 * temp_f * wind_mph**0.16
    )
    return round(wc, 1)


def feels_like(temp_f: float, humidity: float, wind_mph: float) -> float:
    """Feels-like temperature: heat index when hot, wind chill when cold, actual otherwise."""
    hi = heat_index(temp_f, humidity)
    if hi is not None:
        return hi
    wc = wind_chill(temp_f, wind_mph)
    if wc is not None:
        return wc
    return round(temp_f, 1)


def dew_point(temp_f: float, humidity: float) -> float:
    """Dew point via Magnus formula."""
    temp_c = (temp_f - 32) * 5 / 9
    a, b = 17.27, 237.7
    gamma = (a * temp_c) / (b + temp_c) + math.log(humidity / 100)
    dp_c = (b * gamma) / (a - gamma)
    return round(dp_c * 9 / 5 + 32, 1)


def f_to_c(temp_f: float) -> float:
    return round((temp_f - 32) * 5 / 9, 1)


def inches_to_mm(inches: float) -> float:
    return round(inches * 25.4, 1)


def mph_to_kph(mph: float) -> float:
    return round(mph * 1.60934, 1)


def inhg_to_hpa(inhg: float) -> float:
    return round(inhg * 33.8639, 1)


def beaufort_scale(wind_mph: float) -> tuple[int, str]:
    """Wind speed to Beaufort scale number and description."""
    thresholds = [
        (1, 0, "Calm"),
        (3, 1, "Light air"),
        (7, 2, "Light breeze"),
        (12, 3, "Gentle breeze"),
        (18, 4, "Moderate breeze"),
        (24, 5, "Fresh breeze"),
        (31, 6, "Strong breeze"),
        (38, 7, "Near gale"),
        (46, 8, "Gale"),
        (54, 9, "Strong gale"),
        (63, 10, "Storm"),
        (72, 11, "Violent storm"),
        (999, 12, "Hurricane force"),
    ]
    for max_speed, scale, desc in thresholds:
        if wind_mph < max_speed:
            return scale, desc
    return 12, "Hurricane force"


def comfort_level(temp_f: float, humidity: float) -> str:
    """Simple comfort classification."""
    if temp_f < 32:
        return "frigid"
    if temp_f < 50:
        return "cold"
    if temp_f < 60:
        if humidity > 80:
            return "cool and damp"
        return "cool"
    if temp_f < 80:
        if humidity < 30:
            return "comfortable but dry"
        if humidity > 70:
            return "warm and humid"
        return "comfortable"
    if temp_f < 90:
        if humidity > 60:
            return "hot and humid"
        return "hot"
    return "dangerously hot"


def frost_risk(temp_f: float, dew_point_f: float, wind_mph: float) -> str:
    """Estimate frost risk from current conditions."""
    if temp_f > 45:
        return "none"
    if temp_f > 38:
        if dew_point_f < 32 and wind_mph < 5:
            return "moderate"
        return "low"
    if temp_f > 32:
        if wind_mph < 5:
            return "high"
        return "moderate"
    return "freeze"


def growing_degree_days(high_f: float, low_f: float, base_f: float = 50) -> float:
    """Growing degree days for a single day."""
    avg = (high_f + low_f) / 2
    return round(max(0, avg - base_f), 1)


def pressure_trend_description(change_inhg_per_3h: float) -> str:
    """Describe pressure trend from 3-hour change."""
    if change_inhg_per_3h > 0.06:
        return "rising rapidly"
    if change_inhg_per_3h > 0.02:
        return "rising"
    if change_inhg_per_3h > -0.02:
        return "steady"
    if change_inhg_per_3h > -0.06:
        return "falling"
    return "falling rapidly"


def pressure_forecast_hint(change_inhg_per_3h: float) -> str:
    """Simple forecast hint based on pressure trend."""
    if change_inhg_per_3h < -0.06:
        return "Rapidly falling pressure suggests approaching storm or weather system within 6-12 hours"
    if change_inhg_per_3h < -0.02:
        return "Falling pressure suggests possible weather change in 12-24 hours"
    if change_inhg_per_3h > 0.06:
        return "Rapidly rising pressure suggests clearing conditions"
    if change_inhg_per_3h > 0.02:
        return "Rising pressure suggests improving or continued fair weather"
    return "Steady pressure suggests continuation of current conditions"


def enrich_conditions(readings: dict) -> dict:
    """Add all derived values to a readings dict."""
    temp = readings.get("temperature_f")
    humidity = readings.get("humidity_pct")
    wind = readings.get("wind_speed_mph", 0) or 0
    dp = readings.get("dew_point_f")
    pressure = readings.get("pressure_inhg")

    derived = {}

    if temp is not None and humidity is not None:
        derived["heat_index_f"] = heat_index(temp, humidity)
        derived["feels_like_f"] = feels_like(temp, humidity, wind)
        derived["comfort_level"] = comfort_level(temp, humidity)
        if dp is None:
            dp = dew_point(temp, humidity)
            derived["dew_point_f"] = dp
        derived["frost_risk"] = frost_risk(temp, dp, wind)

        # Metric conversions
        derived["temperature_c"] = f_to_c(temp)
        derived["feels_like_c"] = f_to_c(derived["feels_like_f"])
        derived["dew_point_c"] = f_to_c(dp)

    if temp is not None:
        derived["wind_chill_f"] = wind_chill(temp, wind)

    if wind is not None:
        bft_num, bft_desc = beaufort_scale(wind)
        derived["beaufort_scale"] = bft_num
        derived["beaufort_description"] = bft_desc
        derived["wind_speed_kph"] = mph_to_kph(wind)

    gust = readings.get("wind_gust_mph")
    if gust is not None:
        derived["wind_gust_kph"] = mph_to_kph(gust)

    if pressure is not None:
        derived["pressure_hpa"] = inhg_to_hpa(pressure)

    rain = readings.get("rainfall_in")
    if rain is not None:
        derived["rainfall_mm"] = inches_to_mm(rain)

    return derived
