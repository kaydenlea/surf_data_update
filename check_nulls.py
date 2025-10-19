#!/usr/bin/env python3
"""
Quick script to check null counts per column in forecast_data table
"""

from database import supabase

# Get a sample record to see all columns
resp = supabase.table('forecast_data').select('*').limit(1).execute()
if resp.data:
    columns = list(resp.data[0].keys())
    print('Columns in forecast_data table:')
    print(columns)
    print()

# Count nulls for each important column
fields_to_check = [
    'weather',
    'wind_direction_deg',
    'wind_speed_mph',
    'primary_swell_height_ft',
    'primary_swell_period_s',
    'primary_swell_direction',
    'secondary_swell_height_ft',
    'secondary_swell_period_s',
    'secondary_swell_direction',
    'tertiary_swell_height_ft',
    'tertiary_swell_period_s',
    'tertiary_swell_direction',
    'surf_height_min_ft',
    'surf_height_max_ft',
    'wave_energy_kj',
    'water_temp_f',
    'temperature'
]

print('Counting nulls per column...')
print('=' * 60)

# Get total count first
total_resp = supabase.table('forecast_data').select('*', count='exact').execute()
total_count = total_resp.count

print(f'Total records: {total_count:,}')
print()
print(f'{"Column":<30} {"Nulls":>10} {"Percent":>10}')
print('-' * 60)

for field in fields_to_check:
    null_resp = supabase.table('forecast_data').select('id', count='exact').is_(field, 'null').execute()
    null_count = null_resp.count
    percent = (null_count / total_count * 100) if total_count > 0 else 0
    print(f'{field:<30} {null_count:>10,} {percent:>9.1f}%')

print()
print('Done!')
