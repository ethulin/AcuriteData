"""MyAcurite API client for marapi.myacurite.com."""

import time
from datetime import datetime, timezone

import httpx


BASE_URL = "https://marapi.myacurite.com"
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
        """Find the hub and device matching device_mac, or return the first one."""
        hubs = self.get_hubs()
        if not hubs:
            raise ValueError("No hubs found on your MyAcurite account")

        for hub in hubs:
            hub_data = self.get_hub_data(str(hub["id"]))
            devices = hub_data.get("devices", [])
            for device in devices:
                if self.device_mac:
                    mac = device.get("mac_address", "")
                    if mac.upper() == self.device_mac.upper():
                        return str(hub["id"]), device
                else:
                    return str(hub["id"]), device

        if self.device_mac:
            raise ValueError(
                f"Device {self.device_mac} not found. "
                f"Available hubs: {[h.get('name', h['id']) for h in hubs]}"
            )
        raise ValueError("No devices found")

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

        # Map known sensor codes to clean field names
        field_mappings = {
            "Temperature": ("temperature_f", None),
            "Humidity": ("humidity_pct", None),
            "Dew Point": ("dew_point_f", None),
            "Wind Speed": ("wind_speed_mph", None),
            "Wind Speed Avg": ("wind_speed_avg_mph", None),
            "WindSpeedAvg": ("wind_speed_avg_mph", None),
            "Wind Direction": ("wind_direction_deg", None),
            "Barometric Pressure": ("pressure_inhg", None),
            "Rainfall": ("rainfall_in", None),
            "Heat Index": ("heat_index_f", None),
            "Wind Chill": ("wind_chill_f", None),
            "Feels Like": ("feels_like_f", None),
        }

        for sensor_code, (field_name, _) in field_mappings.items():
            if sensor_code in sensor_map:
                readings[field_name] = sensor_map[sensor_code]["value"]

        # Add wind direction cardinal
        if "wind_direction_deg" in readings and readings["wind_direction_deg"] is not None:
            readings["wind_direction_cardinal"] = degrees_to_cardinal(readings["wind_direction_deg"])

        # Store raw sensor map for debugging
        readings["_raw_sensors"] = {k: v["value"] for k, v in sensor_map.items()}

        return readings

    def get_current_conditions(self) -> dict:
        """Get current weather conditions from the station."""
        hub_id, device = self._find_hub_and_device()
        return self._parse_sensors(device)

    def get_device_info(self) -> dict:
        """Get device metadata."""
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

    def get_all_hub_data(self) -> dict:
        """Get full raw hub data for discovery/debugging."""
        hubs = self.get_hubs()
        result = {}
        for hub in hubs:
            hub_id = str(hub["id"])
            result[hub_id] = {
                "name": hub.get("name", ""),
                "data": self.get_hub_data(hub_id),
            }
        return result
