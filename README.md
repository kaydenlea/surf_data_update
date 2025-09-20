# surf_data_update

This repo runs the Waves & Waders data update on a daily schedule via GitHub Actions.

## How it works
- The scheduled workflow installs Python deps and runs `python main.py`.
- The script connects to Supabase and external APIs (NOAA, Open‑Meteo, Visual Crossing) and upserts data.
- Secrets are provided via GitHub Actions secrets.

## Setup
1) Copy the updater code into this repo root (same folder as this README):
   - From your existing project, copy these files into this folder:
     - `main.py`
     - `config.py`
     - `utils.py`
     - `database.py`
     - `noaa_handler.py`
     - `openmeteo_handler.py`
     - `swell_ranking.py`
   - Ensure imports are local (e.g., `from config import ...`).

2) Install dependencies locally (optional):
   - `python3 -m venv .venv && source .venv/bin/activate`
   - `pip install -r requirements.txt`
   - `python main.py`

3) Add GitHub Actions secrets in your repo settings (Settings → Secrets and variables → Actions):
   - `SUPABASE_URL` – Supabase project URL
   - `SUPABASE_KEY` – Supabase anon/service key (use the least privileged key that can upsert)
   - `VC_API_KEY` – Visual Crossing API key

4) Schedule
   - The workflow runs daily at 06:05 America/Los_Angeles (≈ 14:05 UTC). Edit cron if needed in `.github/workflows/daily-update.yml`.

## Notes
- Do NOT commit real keys to the repo. The updater reads them from environment variables.
- If the repo is public, keep only environment-based secrets in `config.py`.
- The scheduler runs on the default branch only.
