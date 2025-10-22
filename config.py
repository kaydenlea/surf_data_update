#!/usr/bin/env python3
"""
Minimal configuration for running the updater from this repository.
Reads all secrets from environment variables only. Do not hardcode secrets.
"""

import os
import sys
import logging
import threading

if sys.platform == "win32":
    os.environ["PYTHONIOENCODING"] = "utf-8"

# Secrets (must be set in environment / GitHub Actions secrets)
SUPABASE_URL = os.environ.get("SUPABASE_URL", "https://wborkytqlmkcgwzhsoiz.supabase.co")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY", "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6Indib3JreXRxbG1rY2d3emhzb2l6Iiwicm9sZSI6InNlcnZpY2Vfcm9sZSIsImlhdCI6MTc1NDExMzE0NywiZXhwIjoyMDY5Njg5MTQ3fQ.MGuDZwoAoxZlLMam_PG76NqA-Cug4aXbmjkhppetq0w")
VC_API_KEY = os.environ.get("VC_API_KEY")

# Script settings (match your existing project as needed)
DAYS_FORECAST = 7
BATCH_SIZE = 10
UPSERT_CHUNK = 3000
MAX_WORKERS = 3
LOG_LEVEL = logging.INFO
API_DELAY = 2.0
RETRY_DELAY = 65
MAX_RETRIES = 3

# NOAA / Open-Meteo rate limits
# Increased delays to avoid NOAA rate limiting (they have strict abuse detection)
NOAA_REQUEST_DELAY = 2.0  # Increased from 0.2 to 2.0 seconds between requests
NOAA_BATCH_DELAY = 5.0    # Increased from 0.5 to 5.0 seconds between location batches
NOAA_MAX_CONCURRENT = 1
NOAA_RETRY_DELAY = 600
NOAA_DATASET_TEST_DELAY = 3.0  # Increased from 1.0 to 3.0 seconds

OPENMETEO_REQUEST_DELAY = 1.0
OPENMETEO_BATCH_DELAY = 2.0
OPENMETEO_RETRY_DELAY = 60
OPENMETEO_MAX_RETRIES = 3

# Physical constants
TIDE_ADJUSTMENT_FT = 2.4

# Rate limiting globals
_noaa_last_request_time = 0
_noaa_request_lock = threading.Lock()

def get_noaa_rate_limit_globals():
    global _noaa_last_request_time, _noaa_request_lock
    return _noaa_last_request_time, _noaa_request_lock

def set_noaa_last_request_time(timestamp):
    global _noaa_last_request_time
    _noaa_last_request_time = timestamp

def setup_logging():
    logging.basicConfig(
        level=LOG_LEVEL,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler('surf_update_hybrid.log', encoding='utf-8'),
            logging.StreamHandler(sys.stdout)
        ]
    )
    return logging.getLogger("surf_update")

logger = setup_logging()

def validate_configuration():
    required = ["SUPABASE_URL", "SUPABASE_KEY", "VC_API_KEY"]
    missing = [k for k in required if not globals().get(k)]
    if missing:
        raise ValueError(f"Missing required configuration: {missing}")
    logger.info("Configuration validation passed")
    return True

# NOAA base URLs and API endpoints (kept here for clarity)
NOAA_BASE_URLS = [
    "http://nomads.ncep.noaa.gov:80/dods/wave/gfswave",
    "http://nomads.ncep.noaa.gov/dods/wave/gfswave",
    "https://nomads.ncep.noaa.gov/dods/wave/gfswave",
]

# NOAA Variable Mapping (must match dataset variable names)
NOAA_VARS = {
    "primary_swell_height": "swell_2",      # 2nd sequence swell (usually dominant)
    "primary_swell_period": "swper_2",
    "primary_swell_direction": "swdir_2",
    "secondary_swell_height": "swell_3",    # 3rd sequence swell
    "secondary_swell_period": "swper_3",
    "secondary_swell_direction": "swdir_3",
    "tertiary_swell_height": "swell_1",     # 1st sequence swell (tertiary for us)
    "tertiary_swell_period": "swper_1",
    "tertiary_swell_direction": "swdir_1",
    "surf_sig_height": "htsgwsfc",          # Significant wave height
    "wind_speed": "windsfc",                # Wind speed m/s
    "wind_direction": "wdirsfc",            # Wind direction degrees
    "primary_wave_period": "perpwsfc",      # Primary wave period
    "primary_wave_direction": "dirpwsfc",   # Primary wave direction
}

# Optional grid search parameters (maintain parity with original project)
LAT_OFFSETS = [-0.05, 0, 0.05]
LON_OFFSETS = [-0.1, 0, 0.1]

OPENMETEO_WEATHER_URL = "https://api.open-meteo.com/v1/forecast"
OPENMETEO_MARINE_URL = "https://marine-api.open-meteo.com/v1/marine"
VISUAL_CROSSING_BASE = "https://weather.visualcrossing.com/VisualCrossingWebServices/rest/services/timeline"
