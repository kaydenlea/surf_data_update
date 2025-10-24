from database import supabase
from datetime import datetime
import pytz

# Get current time in different formats
now_local = datetime.now()
now_utc = datetime.utcnow()
now_pacific = datetime.now(pytz.timezone('America/Los_Angeles'))

print(f'Local time: {now_local}')
print(f'Local ISO: {now_local.isoformat()}')
print(f'\nUTC time: {now_utc}')
print(f'UTC ISO: {now_utc.isoformat()}')
print(f'\nPacific time: {now_pacific}')
print(f'Pacific ISO: {now_pacific.isoformat()}')

# Query with different time formats
print(f'\n--- Query with local time ---')
result1 = supabase.table('county_tides_15min').select('timestamp,tide_level_ft').eq('county', 'Orange').lte('timestamp', now_local.isoformat()).order('timestamp', desc=True).limit(1).execute()
if result1.data:
    print(f'Found: {result1.data[0]["timestamp"]} - {result1.data[0]["tide_level_ft"]} ft')
else:
    print('No data')

print(f'\n--- Query with UTC time ---')
result2 = supabase.table('county_tides_15min').select('timestamp,tide_level_ft').eq('county', 'Orange').lte('timestamp', now_utc.isoformat()).order('timestamp', desc=True).limit(1).execute()
if result2.data:
    print(f'Found: {result2.data[0]["timestamp"]} - {result2.data[0]["tide_level_ft"]} ft')
else:
    print('No data')

print(f'\n--- Latest 3 timestamps in DB ---')
result3 = supabase.table('county_tides_15min').select('timestamp,tide_level_ft').eq('county', 'Orange').order('timestamp', desc=True).limit(3).execute()
for r in result3.data:
    print(f'  {r["timestamp"]} - {r["tide_level_ft"]} ft')
