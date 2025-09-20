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
SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY")
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
NOAA_REQUEST_DELAY = 0.2
NOAA_BATCH_DELAY = 1.5
NOAA_MAX_CONCURRENT = 1
NOAA_RETRY_DELAY = 600
NOAA_DATASET_TEST_DELAY = 1.0

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

OPENMETEO_WEATHER_URL = "https://api.open-meteo.com/v1/forecast"
OPENMETEO_MARINE_URL = "https://marine-api.open-meteo.com/v1/marine"
VISUAL_CROSSING_BASE = "https://weather.visualcrossing.com/VisualCrossingWebServices/rest/services/timeline"

