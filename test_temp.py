#!/usr/bin/env python3
"""
Test RTOFS 7-day (actually ~7-8 day) sea-surface temperature for ONE location.

This tries the two most common RTOFS surface files on NOMADS:
  - rtofs_glo_2ds_f000_3hrly_prog
  - rtofs_glo_2ds_f000_3hrly_diag

and falls back from today's run to yesterday's run.

Requires:
    pip install xarray netCDF4 pandas numpy
"""

import xarray as xr
import pandas as pd
import numpy as np
from datetime import datetime, timedelta, timezone
import sys

# ---------------------------------------------------------------------
# CONFIG: set your beach / spot here
BEACH_LAT = 33.65       # SoCal-ish
BEACH_LON = -118.0      # West is negative; RTOFS uses 0..360
MAX_DAYS = 7            # how far out to print
# ---------------------------------------------------------------------

BASE = "https://nomads.ncep.noaa.gov:9090/dods/rtofs"

CANDIDATE_FILES = [
    "rtofs_glo_2ds_f000_3hrly_prog",  # common surface forecast
    "rtofs_glo_2ds_f000_3hrly_diag",  # sometimes SST is here
]

def build_url(run_date: datetime, fname: str) -> str:
    date_str = run_date.strftime("%Y%m%d")
    return f"{BASE}/rtofs_global{date_str}/{fname}"

def try_open_rtofs():
    """Try today's run, then yesterday's; try both candidate files."""
    today = datetime.utcnow()
    for day_offset in [0, 1]:
        run_date = today - timedelta(days=day_offset)
        for fname in CANDIDATE_FILES:
            url = build_url(run_date, fname)
            print(f"Trying RTOFS URL: {url}")
            try:
                ds = xr.open_dataset(url)
                print(f"Opened: {url}")
                return ds, run_date, fname
            except Exception as e:
                print(f"  failed: {e}")
                continue
    print("ERROR: Could not open any RTOFS surface file for today or yesterday.")
    sys.exit(1)

def main():
    ds, run_date, fname = try_open_rtofs()

    # figure out coord names
    if "lat" in ds.coords:
        lat_name = "lat"
    elif "latitude" in ds.coords:
        lat_name = "latitude"
    else:
        print("Could not find latitude coord in dataset.")
        sys.exit(1)

    if "lon" in ds.coords:
        lon_name = "lon"
    elif "longitude" in ds.coords:
        lon_name = "longitude"
    else:
        print("Could not find longitude coord in dataset.")
        sys.exit(1)

    # RTOFS uses 0..360 longitudes
    lon_360 = BEACH_LON if BEACH_LON >= 0 else BEACH_LON + 360

    # pick a variable name
    # these surface files usually call SST just "temperature"
    var_name = None
    for cand in ["temperature", "temp", "sst"]:
        if cand in ds.data_vars:
            var_name = cand
            break
    if var_name is None:
        print("No temperature-like variable found. Try printing ds.data_vars.")
        print(ds.data_vars)
        sys.exit(1)

    # select nearest point (this should return time series)
    try:
        da = ds[var_name].sel(
            **{
                lat_name: BEACH_LAT,
                lon_name: lon_360,
            },
            method="nearest",
        )
    except Exception as e:
        print(f"Could not select nearest grid point: {e}")
        sys.exit(1)

    # times
    times = pd.to_datetime(ds["time"].values)
    if times.tz is None:
        times = times.tz_localize("UTC")
    else:
        times = times.tz_convert("UTC")

    print()
    print(f"RTOFS run date: {run_date.date()}  file: {fname}")
    print(f"Nearest model point: lat={float(da[lat_name]):.3f}, lon={float(da[lon_name]):.3f}")
    print(f"Total forecast steps: {len(times)}")

    now_utc = datetime.now(timezone.utc)
    cutoff = now_utc + timedelta(days=MAX_DAYS)

    print("\n--- RTOFS forecast SST (°F) ---")
    for t, v in zip(times, da.values):
        if t.to_pydatetime() > cutoff:
            break

        if np.isnan(v):
            out = "missing"
        else:
            # Kelvin -> °F
            sst_f = (float(v) - 273.15) * 9.0 / 5.0 + 32.0
            out = f"{sst_f:5.1f} °F"

        print(f"{t.isoformat()} -> {out}")

    ds.close()
    print("\nDone.")

if __name__ == "__main__":
    main()
