"""Sanity check suite for Acurite weather station data.

Fetches real data from the APIs and flags anything physically impossible
or suspicious. Run from the project directory with the venv active:
    python3 sanity_check.py
"""

import os
import sys
import statistics
from dataclasses import dataclass, field
from datetime import datetime, date, timedelta, timezone
from zoneinfo import ZoneInfo

from dotenv import load_dotenv

from acurite_weather.acurite_api import AcuriteClient
from acurite_weather.derived import (
    dew_point as calc_dew_point,
    heat_index as calc_heat_index,
    wind_chill as calc_wind_chill,
    feels_like as calc_feels_like,
)

PACIFIC = ZoneInfo("America/Los_Angeles")


@dataclass
class CheckResult:
    name: str
    status: str  # PASS, WARN, FAIL
    detail: str
    examples: list[str] = field(default_factory=list)


def status_icon(s: str) -> str:
    return {"PASS": "  [PASS]", "WARN": "  [WARN]", "FAIL": "  [FAIL]"}[s]


class SanityChecker:
    def __init__(self, client: AcuriteClient):
        self.client = client
        self.results: list[CheckResult] = []
        self.today = date.today()
        self.yesterday = self.today - timedelta(days=1)
        self.week_ago = self.today - timedelta(days=7)

    def add(self, name: str, status: str, detail: str, examples: list[str] | None = None):
        self.results.append(CheckResult(name, status, detail, examples or []))

    def run_all(self):
        print("Fetching data...")
        self._fetch_data()
        print(f"  Live: {len(self.current)} fields")
        print(f"  5m:   {len(self.hist_5m)} readings (2 days)")
        print(f"  1h:   {len(self.hist_1h)} readings (7 days)")
        print()

        self.check_physical_ranges()
        self.check_derived_calculations()
        self.check_cross_resolution()
        self.check_rainfall()
        self.check_data_gaps()
        self.check_timestamp_ordering()
        self.check_live_vs_historical()
        self.check_dst()

    def _fetch_data(self):
        self.current = self.client.get_current_conditions()
        self.current.pop("_raw_sensors", None)
        self.hist_5m = self.client.get_history(self.yesterday, self.today, "5m-summaries")
        self.hist_1h = self.client.get_history(self.week_ago, self.today, "1h-summaries")
        self.daily_rain = self.client.get_daily_rainfall(self.week_ago, self.today)

    # --- Check 1: Physical ranges ---

    def check_physical_ranges(self):
        checks = [
            ("temperature_f", -40, 140, 10, 105),
            ("humidity_pct", 0, 100, 5, 100),
            ("wind_speed_mph", 0, 150, 0, 60),
            ("wind_direction_deg", 0, 360, 0, 360),
            ("pressure_inhg", 25, 33, 28.5, 31.0),
        ]

        for field, fail_lo, fail_hi, warn_lo, warn_hi in checks:
            vals = [(r["timestamp"], r[field]) for r in self.hist_1h
                    if r.get(field) is not None]
            if not vals:
                self.add(f"range_{field}", "WARN", f"No {field} data found")
                continue

            fails = [(ts, v) for ts, v in vals if v < fail_lo or v > fail_hi]
            warns = [(ts, v) for ts, v in vals
                     if (v < warn_lo or v > warn_hi) and (fail_lo <= v <= fail_hi)]
            all_v = [v for _, v in vals]

            if fails:
                self.add(f"range_{field}", "FAIL",
                         f"{len(fails)} impossible values in {len(vals)} readings "
                         f"(range {min(all_v):.1f}-{max(all_v):.1f})",
                         [f"  {ts}: {v}" for ts, v in fails[:5]])
            elif warns:
                self.add(f"range_{field}", "WARN",
                         f"{len(warns)} suspicious values in {len(vals)} readings "
                         f"(range {min(all_v):.1f}-{max(all_v):.1f})",
                         [f"  {ts}: {v}" for ts, v in warns[:5]])
            else:
                self.add(f"range_{field}", "PASS",
                         f"{len(vals)} readings, range {min(all_v):.1f}-{max(all_v):.1f}")

        # Dew point <= temperature check
        pairs = [(r["timestamp"], r["dew_point_f"], r["temperature_f"])
                 for r in self.hist_1h
                 if r.get("dew_point_f") is not None and r.get("temperature_f") is not None]
        violations = [(ts, dp, t) for ts, dp, t in pairs if dp > t + 1.0]
        if violations:
            self.add("range_dewpoint_vs_temp", "FAIL",
                     f"{len(violations)} readings where dew point > temp + 1F",
                     [f"  {ts}: dp={dp:.1f} temp={t:.1f}" for ts, dp, t in violations[:5]])
        else:
            self.add("range_dewpoint_vs_temp", "PASS",
                     f"All {len(pairs)} readings: dew point <= temperature")

        # Daily rainfall
        rain_vals = list(self.daily_rain.values())
        if rain_vals:
            neg = [d for d, v in self.daily_rain.items() if v < 0]
            high = [d for d, v in self.daily_rain.items() if v > 3.0]
            if neg:
                self.add("range_rainfall", "FAIL", f"Negative rainfall on {neg}")
            elif high:
                self.add("range_rainfall", "WARN",
                         f"High rainfall days (>3in): {', '.join(f'{d}={self.daily_rain[d]}in' for d in high)}")
            else:
                self.add("range_rainfall", "PASS",
                         f"{len(rain_vals)} days, max {max(rain_vals):.2f} in, "
                         f"total {sum(rain_vals):.2f} in")

    # --- Check 2: Derived calculations ---

    def check_derived_calculations(self):
        for calc_name, calc_fn, api_field, condition in [
            ("dew_point", lambda r: calc_dew_point(r["temperature_f"], r["humidity_pct"]),
             "dew_point_f", lambda r: True),
            ("feels_like", lambda r: calc_feels_like(r["temperature_f"], r["humidity_pct"],
                                                      r.get("wind_speed_mph", 0) or 0),
             "feels_like_f", lambda r: True),
            ("heat_index", lambda r: calc_heat_index(r["temperature_f"], r["humidity_pct"]),
             "heat_index_f", lambda r: r.get("temperature_f", 0) >= 80 and r.get("humidity_pct", 0) >= 40),
            ("wind_chill", lambda r: calc_wind_chill(r["temperature_f"], r.get("wind_speed_mph", 0) or 0),
             "wind_chill_f", lambda r: r.get("temperature_f", 999) <= 50 and (r.get("wind_speed_mph", 0) or 0) >= 3),
        ]:
            diffs = []
            for r in self.hist_5m:
                if (r.get("temperature_f") is None or r.get("humidity_pct") is None
                        or r.get(api_field) is None):
                    continue
                if not condition(r):
                    continue
                computed = calc_fn(r)
                if computed is None:
                    continue
                diff = abs(computed - r[api_field])
                diffs.append((diff, r["timestamp"], computed, r[api_field]))

            if not diffs:
                self.add(f"derived_{calc_name}", "PASS",
                         "No readings in valid range (skipped)")
                continue

            max_diff = max(diffs, key=lambda x: x[0])
            median_diff = statistics.median([d[0] for d in diffs])
            # Feels-like has wider tolerance — API may use different wind inputs
            fail_thresh = 8.0 if calc_name == "feels_like" else 5.0
            warn_thresh = 4.0 if calc_name == "feels_like" else 2.0
            fail_count = sum(1 for d in diffs if d[0] > fail_thresh)
            warn_count = sum(1 for d in diffs if warn_thresh < d[0] <= fail_thresh)

            if fail_count:
                self.add(f"derived_{calc_name}", "FAIL",
                         f"{fail_count} readings differ > 5F (median {median_diff:.1f}F, max {max_diff[0]:.1f}F)",
                         [f"  {max_diff[1]}: computed={max_diff[2]:.1f} api={max_diff[3]:.1f}"])
            elif warn_count:
                self.add(f"derived_{calc_name}", "WARN",
                         f"{warn_count} readings differ > 2F (median {median_diff:.1f}F, max {max_diff[0]:.1f}F)")
            else:
                self.add(f"derived_{calc_name}", "PASS",
                         f"{len(diffs)} readings, median diff {median_diff:.1f}F, max {max_diff[0]:.1f}F")

    # --- Check 3: Cross-resolution consistency ---

    def check_cross_resolution(self):
        data_5m = self.client.get_history(self.yesterday, self.yesterday, "5m-summaries")
        data_15m = self.client.get_history(self.yesterday, self.yesterday, "15m-summaries")
        data_1h = self.client.get_history(self.yesterday, self.yesterday, "1h-summaries")

        # Reading count check
        for label, data, expected in [("5m", data_5m, 288), ("15m", data_15m, 96), ("1h", data_1h, 24)]:
            pct = len(data) / expected * 100 if expected else 0
            if pct < 50:
                self.add(f"resolution_count_{label}", "FAIL",
                         f"{len(data)}/{expected} readings ({pct:.0f}%)")
            elif pct < 80:
                self.add(f"resolution_count_{label}", "WARN",
                         f"{len(data)}/{expected} readings ({pct:.0f}%)")
            else:
                self.add(f"resolution_count_{label}", "PASS",
                         f"{len(data)}/{expected} readings ({pct:.0f}%)")

        # Min/max consistency
        for field in ["temperature_f", "pressure_inhg"]:
            ranges = {}
            for label, data in [("5m", data_5m), ("15m", data_15m), ("1h", data_1h)]:
                vals = [r[field] for r in data if r.get(field) is not None]
                if vals:
                    ranges[label] = (min(vals), max(vals))

            if len(ranges) < 2:
                continue

            parts = " | ".join(f"{k}: {v[0]:.1f}-{v[1]:.1f}" for k, v in ranges.items())
            # Check if min/max diverge significantly
            all_mins = [v[0] for v in ranges.values()]
            all_maxs = [v[1] for v in ranges.values()]
            min_spread = max(all_mins) - min(all_mins)
            max_spread = max(all_maxs) - min(all_maxs)

            if min_spread > 5 or max_spread > 5:
                self.add(f"resolution_{field}", "WARN",
                         f"Large spread across resolutions: {parts}")
            else:
                self.add(f"resolution_{field}", "PASS", parts)

    # --- Check 4: Rainfall correctness ---

    def check_rainfall(self):
        # Independently compute daily rainfall using the same local-day logic
        # as get_daily_rainfall(): group by local date (UTC-7), take max per day
        report_lines = []
        any_fail = False
        any_warn = False

        # Fetch raw channel 11 data for the full range + buffer days
        all_ch11: list[tuple[str, float]] = []
        for day_offset in range(-1, 9):  # extra days for boundary
            day = self.today - timedelta(days=day_offset)
            raw = self.client._fetch_day_data(day, "1h-summaries")
            for e in raw.get("11", []):
                ts = e.get("happened_at", "")
                v = e.get("raw_values", {}).get("IN")
                if ts and v is not None:
                    all_ch11.append((ts, v))

        all_ch11.sort()

        # Group by local day (UTC-7) and take max — same logic as get_daily_rainfall
        independent_rain: dict[str, float] = {}
        naive_sums: dict[str, float] = {}
        for ts, v in all_ch11:
            try:
                dt = datetime.fromisoformat(ts)
                local_dt = dt - timedelta(hours=7)
                local_date = local_dt.date().isoformat()
            except (ValueError, TypeError):
                continue
            independent_rain[local_date] = max(independent_rain.get(local_date, 0), v)
            naive_sums[local_date] = naive_sums.get(local_date, 0) + v

        # Compare against get_daily_rainfall() for each day in range
        for day_offset in range(8):
            day = (self.today - timedelta(days=day_offset)).isoformat()
            api_value = self.daily_rain.get(day, 0)
            independent_max = independent_rain.get(day, 0)
            naive_sum = naive_sums.get(day, 0)

            diff = abs(api_value - independent_max)
            ratio = naive_sum / independent_max if independent_max > 0.001 else 0

            status = ""
            if diff > 0.05:
                status = "FAIL"
                any_fail = True
            elif diff > 0.01:
                status = "WARN"
                any_warn = True

            if independent_max > 0.001 or api_value > 0.001:
                report_lines.append(
                    f"  {day}: api={api_value:.2f} indep={independent_max:.2f} "
                    f"naive_sum={naive_sum:.2f} ratio={ratio:.1f}x"
                    + (f" {status}" if status else "")
                )

        # Check monotonicity within yesterday
        raw_yesterday = self.client._fetch_day_data(self.yesterday, "1h-summaries")
        ch11_y = raw_yesterday.get("11", [])
        mono_violations = []
        if ch11_y:
            prev_val = -1
            prev_ts = ""
            for e in ch11_y:
                ts = e["happened_at"]
                v = e["raw_values"].get("IN", 0)
                hour = int(ts[11:13])
                # Allow decrease near the 07:00 UTC reset (hours 6-8)
                if prev_val >= 0 and v < prev_val - 0.001 and not (6 <= hour <= 8):
                    mono_violations.append(f"  {prev_ts}={prev_val:.2f} -> {ts}={v:.2f}")
                prev_val = v
                prev_ts = ts

        if mono_violations:
            self.add("rainfall_monotonic", "WARN",
                     f"{len(mono_violations)} unexpected decreases in channel 11",
                     mono_violations[:5])
        else:
            self.add("rainfall_monotonic", "PASS",
                     "Channel 11 monotonically non-decreasing (outside reset window)")

        if any_fail:
            self.add("rainfall_correctness", "FAIL",
                     "get_daily_rainfall() disagrees with independent max calculation",
                     report_lines)
        elif any_warn:
            self.add("rainfall_correctness", "WARN",
                     "Minor discrepancy in daily rainfall", report_lines)
        else:
            self.add("rainfall_correctness", "PASS",
                     "get_daily_rainfall() matches independent max calculation",
                     report_lines)

    # --- Check 5: Data gaps ---

    def check_data_gaps(self):
        timestamps = sorted([r["timestamp"] for r in self.hist_5m])
        if len(timestamps) < 2:
            self.add("data_gaps", "WARN", "Not enough 5m data to check gaps")
            return

        gaps = []
        for i in range(1, len(timestamps)):
            t1 = datetime.fromisoformat(timestamps[i - 1])
            t2 = datetime.fromisoformat(timestamps[i])
            delta = (t2 - t1).total_seconds()
            if delta > 450:  # > 7.5 minutes
                gaps.append((timestamps[i - 1], timestamps[i], delta / 60))

        if not gaps:
            self.add("data_gaps", "PASS",
                     f"{len(timestamps)} readings, no gaps > 7.5 min")
            return

        longest = max(gaps, key=lambda g: g[2])
        total_missing = sum(g[2] - 5 for g in gaps)

        if longest[2] > 120:
            self.add("data_gaps", "FAIL",
                     f"{len(gaps)} gaps, longest {longest[2]:.0f} min "
                     f"({longest[0]} to {longest[1]}), total missing ~{total_missing:.0f} min",
                     [f"  {g[0]} -> {g[1]} ({g[2]:.0f} min)" for g in gaps[:5]])
        elif longest[2] > 30 or total_missing > 60:
            self.add("data_gaps", "WARN",
                     f"{len(gaps)} gaps, longest {longest[2]:.0f} min, "
                     f"total missing ~{total_missing:.0f} min")
        else:
            self.add("data_gaps", "PASS",
                     f"{len(gaps)} small gaps, longest {longest[2]:.0f} min")

    # --- Check 6: Timestamp ordering ---

    def check_timestamp_ordering(self):
        for label, data in [("5m", self.hist_5m), ("1h", self.hist_1h)]:
            timestamps = [r["timestamp"] for r in data]
            out_of_order = 0
            duplicates = 0
            for i in range(1, len(timestamps)):
                if timestamps[i] < timestamps[i - 1]:
                    out_of_order += 1
                elif timestamps[i] == timestamps[i - 1]:
                    duplicates += 1

            if out_of_order:
                self.add(f"ordering_{label}", "FAIL",
                         f"{out_of_order} out-of-order timestamps in {len(timestamps)} readings")
            elif duplicates:
                self.add(f"ordering_{label}", "WARN",
                         f"{duplicates} duplicate timestamps in {len(timestamps)} readings")
            else:
                self.add(f"ordering_{label}", "PASS",
                         f"{len(timestamps)} readings, strictly ascending")

    # --- Check 7: Live vs historical ---

    def check_live_vs_historical(self):
        if not self.hist_5m:
            self.add("live_vs_hist", "WARN", "No 5m history to compare")
            return

        latest_hist = self.hist_5m[-1]
        live_ts = self.current.get("last_check_in", "")
        hist_ts = latest_hist.get("timestamp", "")

        try:
            t_live = datetime.fromisoformat(live_ts)
            t_hist = datetime.fromisoformat(hist_ts)
            delta_min = abs((t_live - t_hist).total_seconds()) / 60
        except (ValueError, TypeError):
            self.add("live_vs_hist", "WARN", f"Cannot parse timestamps: live={live_ts} hist={hist_ts}")
            return

        if delta_min > 30:
            self.add("live_vs_hist", "WARN",
                     f"Live and historical readings are {delta_min:.0f} min apart")
            return

        comparisons = []
        any_fail = False
        any_warn = False
        for field, warn_thresh, fail_thresh in [
            ("temperature_f", 3, 10),
            ("humidity_pct", 10, 30),
            ("pressure_inhg", 0.1, 0.5),
        ]:
            live_v = self.current.get(field)
            hist_v = latest_hist.get(field)
            if live_v is None or hist_v is None:
                continue
            diff = abs(live_v - hist_v)
            tag = ""
            if diff > fail_thresh:
                tag = " FAIL"
                any_fail = True
            elif diff > warn_thresh:
                tag = " WARN"
                any_warn = True
            comparisons.append(f"  {field}: live={live_v} hist={hist_v} diff={diff:.1f}{tag}")

        status = "FAIL" if any_fail else "WARN" if any_warn else "PASS"
        self.add("live_vs_hist", status,
                 f"Time delta: {delta_min:.0f} min", comparisons)

    # --- Check 8: DST edge case ---

    def check_dst(self):
        now_pacific = datetime.now(PACIFIC)
        utc_offset = now_pacific.utcoffset().total_seconds() / 3600
        hardcoded_offset = -7

        if utc_offset == hardcoded_offset:
            self.add("dst_offset", "PASS",
                     f"Currently UTC{utc_offset:+.0f} (PDT), matches hardcoded offset")
        else:
            self.add("dst_offset", "WARN",
                     f"Currently UTC{utc_offset:+.0f} (PST) but code uses UTC-7 (PDT). "
                     f"Rainfall between midnight and 1am local may be assigned to wrong day.")

        # Check if near DST transition
        for delta in range(-7, 8):
            check_date = self.today + timedelta(days=delta)
            prev_date = check_date - timedelta(days=1)
            check_offset = datetime(check_date.year, check_date.month, check_date.day,
                                    12, tzinfo=PACIFIC).utcoffset().total_seconds() / 3600
            prev_offset = datetime(prev_date.year, prev_date.month, prev_date.day,
                                   12, tzinfo=PACIFIC).utcoffset().total_seconds() / 3600
            if check_offset != prev_offset:
                self.add("dst_transition", "WARN",
                         f"DST transition on {check_date} (UTC{prev_offset:+.0f} -> UTC{check_offset:+.0f}). "
                         f"Rainfall data near this date may have ~1 hour boundary error.")
                return

        self.add("dst_transition", "PASS", "No DST transition within +/- 7 days")


def print_report(results: list[CheckResult]):
    print("=" * 64)
    print("  ACURITE IRIS DATA SANITY CHECK")
    print(f"  Run at: {datetime.now().isoformat(timespec='seconds')}")
    print("=" * 64)
    print()

    current_section = ""
    for r in results:
        section = r.name.split("_")[0]
        if section != current_section:
            current_section = section
            print(f"--- {section} ---")

        print(f"{status_icon(r.status)} {r.name}: {r.detail}")
        for ex in r.examples:
            print(f"       {ex}")

    print()
    print("=" * 64)
    passes = sum(1 for r in results if r.status == "PASS")
    warns = sum(1 for r in results if r.status == "WARN")
    fails = sum(1 for r in results if r.status == "FAIL")
    print(f"  SUMMARY: {passes} PASS, {warns} WARN, {fails} FAIL")
    if fails:
        print("  Result: FAIL")
    elif warns:
        print("  Result: PASS (with warnings)")
    else:
        print("  Result: PASS")
    print("=" * 64)
    return fails


def main():
    load_dotenv()
    required = ["ACURITE_EMAIL", "ACURITE_PASSWORD"]
    missing = [k for k in required if not os.environ.get(k)]
    if missing:
        sys.exit(f"Missing env vars: {', '.join(missing)}")

    client = AcuriteClient(
        email=os.environ["ACURITE_EMAIL"],
        password=os.environ["ACURITE_PASSWORD"],
        device_mac=os.environ.get("ACURITE_DEVICE_MAC"),
    )

    checker = SanityChecker(client)
    checker.run_all()
    fails = print_report(checker.results)
    sys.exit(1 if fails else 0)


if __name__ == "__main__":
    main()
