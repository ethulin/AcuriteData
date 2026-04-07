"""MyAcurite API client for marapi.myacurite.com and dataapi.myacurite.com."""

import time
from datetime import datetime, date, timedelta, timezone

import httpx


BASE_URL = "https://marapi.myacurite.com"
DATA_URL = "https://dataapi.myacurite.com/mar-sensor-readings"
TOKEN_MAX_AGE = 5 * 3600  # refresh after 5 hours

HEADERS = {
    "Content-Type": "application/json;charset=UTF-8",
    "Origin": "https://www.myacurite.com/",
    "Referer": "https://www.myacurite.com/",
    "Accept": "application/json",
}

CARDINAL_DIRS = [
    "N", "NNE", "NE", "ENE", "E", "ESE", "SE", "SSE",
    "S", "SSW", "SW", "WSW", "W", "WNW", "NW", "NNW",
]

# Channel number -> (field_name, unit_key) for dataapi responses
CHANNEL_MAP = {
    "1": ("temperature_f", "F"),
    "2": ("humidity_pct", "RH"),
    "3": ("wind_speed_mph", "MPH"),
    "4": ("wind_direction_deg", ""),
    "5": ("feels_like_f", "F"),
    "6": ("dew_point_f", "F"),
    "7": ("heat_index_f", "F"),
    "8": ("wind_chill_f", "F"),
    "9": ("pressure_inhg", "INHG"),
    "10": ("rainfall_hourly_in", "IN"),
    "11": ("rainfall_daily_in", "IN"),
    "12": ("wind_speed_avg_mph", "MPH"),
}


def degrees_to_cardinal(deg: float) -> str:
    idx = round(deg / 22.5) % 16
    return CARDINAL_DIRS[idx]


class AcuriteClient:
    def __init__(self, email: str, password: str, device_mac: str | None = None):
        self.email = email
        self.password = password
        self.device_mac = device_mac
        self.token: str | None = None
        self.account_id: str | None = None
        self.token_time: float = 0
        self._device_path: str | None = None  # cached dataapi path
        self._client = httpx.Client(timeout=30, headers=HEADERS)

    def login(self):
        resp = self._client.post(
            f"{BASE_URL}/users/login",
            json={"email": self.email, "password": self.password, "remember": True},
        )
        resp.raise_for_status()
        data = resp.json()
        self.token = data["token_id"]
        self.account_id = str(data["user"]["account_users"][0]["account_id"])
        self.token_time = time.time()

    def _ensure_auth(self):
        if self.token is None or (time.time() - self.token_time) > TOKEN_MAX_AGE:
            self.login()

    def _request(self, method: str, path: str, **kwargs) -> dict:
        self._ensure_auth()
        headers = {"x-one-vue-token": self.token}
        resp = self._client.request(method, f"{BASE_URL}{path}", headers=headers, **kwargs)
        if resp.status_code == 401:
            self.token = None
            self._ensure_auth()
            headers = {"x-one-vue-token": self.token}
            resp = self._client.request(method, f"{BASE_URL}{path}", headers=headers, **kwargs)
        resp.raise_for_status()
        return resp.json()

    def get_hubs(self) -> list[dict]:
        self._ensure_auth()
        data = self._request("GET", f"/accounts/{self.account_id}/dashboard/hubs/")
        return data.get("account_hubs", [])

    def get_hub_data(self, hub_id: str) -> dict:
        self._ensure_auth()
        return self._request("GET", f"/accounts/{self.account_id}/dashboard/hubs/{hub_id}")

    def _find_hub_and_device(self) -> tuple[str, dict]:
        """Find the hub and device, cache the dataapi path."""
        hubs = self.get_hubs()
        if not hubs:
            raise ValueError("No hubs found on your MyAcurite account")

        for hub in hubs:
            hub_data = self.get_hub_data(str(hub["id"]))
            devices = hub_data.get("devices", [])
            for device in devices:
                if self.device_mac:
                    mac = device.get("mac_address", "")
                    if mac and mac.upper() == self.device_mac.upper():
                        self._cache_device_path(device)
                        return str(hub["id"]), device
                else:
                    self._cache_device_path(device)
                    return str(hub["id"]), device

        if self.device_mac:
            raise ValueError(
                f"Device {self.device_mac} not found. "
                f"Available hubs: {[h.get('name', h['id']) for h in hubs]}"
            )
        raise ValueError("No devices found")

    def _cache_device_path(self, device: dict):
        """Extract the dataapi device path from meta_file URL."""
        meta = device.get("meta_file", "")
        if meta and "/mar-sensor-readings/" in meta:
            # URL like https://dataapi.myacurite.com/mar-sensor-readings/XXXX/meta.json
            path = meta.split("/mar-sensor-readings/")[1]
            self._device_path = path.rsplit("/", 1)[0]  # strip /meta.json

    def _ensure_device_path(self):
        if not self._device_path:
            self._find_hub_and_device()
        if not self._device_path:
            raise ValueError("Could not determine device data path")

    def _parse_sensors(self, device: dict) -> dict:
        """Parse sensor array into a clean dict."""
        sensors = device.get("sensors", [])
        readings = {
            "device_name": device.get("name", "Unknown"),
            "mac_address": device.get("mac_address", ""),
            "model": device.get("model_code", ""),
            "last_check_in": device.get("last_check_in_at", ""),
            "battery": device.get("battery_level", ""),
        }

        sensor_map = {}
        for sensor in sensors:
            code = sensor.get("sensor_code", "")
            value = sensor.get("last_reading_value")
            unit = sensor.get("chart_unit", "")
            if value is not None:
                try:
                    value = float(value)
                except (ValueError, TypeError):
                    pass
            sensor_map[code] = {"value": value, "unit": unit}

        field_mappings = {
            "Temperature": "temperature_f",
            "Humidity": "humidity_pct",
            "Dew Point": "dew_point_f",
            "Wind Speed": "wind_speed_mph",
            "Wind Speed Avg": "wind_speed_avg_mph",
            "WindSpeedAvg": "wind_speed_avg_mph",
            "Wind Direction": "wind_direction_deg",
            "Barometric Pressure": "pressure_inhg",
            "Rainfall": "rainfall_in",
            "Heat Index": "heat_index_f",
            "Wind Chill": "wind_chill_f",
            "Feels Like": "feels_like_f",
        }

        for sensor_code, field_name in field_mappings.items():
            if sensor_code in sensor_map:
                readings[field_name] = sensor_map[sensor_code]["value"]

        if "wind_direction_deg" in readings and readings["wind_direction_deg"] is not None:
            readings["wind_direction_cardinal"] = degrees_to_cardinal(readings["wind_direction_deg"])

        readings["_raw_sensors"] = {k: v["value"] for k, v in sensor_map.items()}
        return readings

    # --- Live data (marapi) ---

    def get_current_conditions(self) -> dict:
        hub_id, device = self._find_hub_and_device()
        return self._parse_sensors(device)

    def get_device_info(self) -> dict:
        hub_id, device = self._find_hub_and_device()
        hubs = self.get_hubs()
        hub_name = next((h.get("name", "") for h in hubs if str(h["id"]) == hub_id), "")
        return {
            "device_name": device.get("name", ""),
            "mac_address": device.get("mac_address", ""),
            "model": device.get("model_code", ""),
            "hub_name": hub_name,
            "battery_level": device.get("battery_level", ""),
            "signal_strength": device.get("signal_strength", ""),
            "last_check_in": device.get("last_check_in_at", ""),
            "firmware": device.get("firmware_version", ""),
        }

    # --- Historical data (dataapi) ---

    def _fetch_day_data(self, day: date, resolution: str = "1h-summaries") -> dict:
        """Fetch one day of historical data from dataapi.myacurite.com.

        resolution: '5m-summaries', '15m-summaries', or '1h-summaries'
        Returns raw channel data dict.
        """
        self._ensure_device_path()
        url = f"{DATA_URL}/{self._device_path}/{resolution}/{day.isoformat()}.json"
        resp = self._client.get(url)
        if resp.status_code == 404:
            return {}
        resp.raise_for_status()
        return resp.json()

    def get_history(
        self,
        start: date,
        end: date,
        resolution: str = "auto",
    ) -> list[dict]:
        """Get historical readings for a date range.

        Returns a list of dicts with timestamp + all sensor fields.
        Resolution auto-selects based on range:
          <= 2 days: 5m, <= 7 days: 15m, else: 1h
        """
        days = (end - start).days + 1
        if resolution == "auto":
            if days <= 2:
                resolution = "5m-summaries"
            elif days <= 7:
                resolution = "15m-summaries"
            else:
                resolution = "1h-summaries"

        readings = []
        current = start
        while current <= end:
            day_data = self._fetch_day_data(current, resolution)
            if day_data:
                readings.extend(self._parse_day_data(day_data))
            current += timedelta(days=1)

        readings.sort(key=lambda r: r["timestamp"])
        return readings

    def _parse_day_data(self, day_data: dict) -> list[dict]:
        """Parse dataapi channel data into flat reading dicts.

        Rainfall (channel 11) is a cumulative daily gauge that resets around
        07:00 UTC (midnight Pacific). We compute incremental rainfall from
        consecutive readings to avoid double-counting across the UTC reset.
        """
        by_time: dict[str, dict] = {}

        for channel_str, (field_name, unit_key) in CHANNEL_MAP.items():
            entries = day_data.get(channel_str, [])
            for entry in entries:
                ts = entry.get("happened_at", "")
                raw = entry.get("raw_values", {})

                if unit_key and unit_key in raw:
                    value = raw[unit_key]
                elif raw:
                    value = next(iter(raw.values()))
                else:
                    continue

                if ts not in by_time:
                    by_time[ts] = {"timestamp": ts}
                by_time[ts][field_name] = value

        return list(by_time.values())

    def get_daily_rainfall(self, start: date, end: date) -> dict[str, float]:
        """Get actual daily rainfall totals, correctly handling cumulative resets.

        Channel 11 is a cumulative gauge that resets at ~07:00 UTC (midnight
        Pacific). The true daily total for a local day is the max value seen
        between 07:00 UTC on that day and 07:00 UTC the next day.

        Returns dict of {local_date_str: rainfall_inches}.
        """
        # Fetch one extra day on each side for the UTC/Pacific boundary
        fetch_start = start - timedelta(days=1)
        fetch_end = end + timedelta(days=1)

        # Collect all channel 11 readings with timestamps
        all_readings: list[tuple[str, float]] = []
        current = fetch_start
        while current <= fetch_end:
            raw = self._fetch_day_data(current, "1h-summaries")
            for entry in raw.get("11", []):
                ts = entry.get("happened_at", "")
                val = entry.get("raw_values", {}).get("IN")
                if ts and val is not None:
                    all_readings.append((ts, val))
            current += timedelta(days=1)

        all_readings.sort(key=lambda x: x[0])

        # Group by local day (Pacific = UTC-7 roughly; reset is at ~07:00 UTC)
        # A "local day" runs from 07:00 UTC to 06:59 UTC next day
        daily_rain: dict[str, float] = {}
        for ts, val in all_readings:
            # Parse hour from timestamp to determine local date
            try:
                dt = datetime.fromisoformat(ts)
                # Shift by -7 hours to approximate Pacific time
                local_dt = dt - timedelta(hours=7)
                local_date = local_dt.date().isoformat()
            except (ValueError, TypeError):
                continue

            if local_date not in daily_rain:
                daily_rain[local_date] = 0.0
            # The daily gauge is cumulative; the max value is the day's total
            daily_rain[local_date] = max(daily_rain[local_date], val)

        # Filter to requested range
        return {d: round(v, 2) for d, v in daily_rain.items()
                if start.isoformat() <= d <= end.isoformat()}

    def get_meta(self) -> dict:
        """Get all-time records from meta.json."""
        self._ensure_device_path()
        url = f"{DATA_URL}/{self._device_path}/meta.json"
        resp = self._client.get(url)
        resp.raise_for_status()
        return resp.json()
