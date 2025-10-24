from database import supabase
from datetime import datetime

now = datetime.now()

result = supabase.table('county_tides_15min').select('timestamp,tide_level_ft').eq('county', 'Orange').lte('timestamp', now.isoformat()).order('timestamp', desc=False).limit(1).execute()

print(f'Current time: {now}')
print(f'\nMost recent Orange county tide (before correction):')
if result.data:
    print(f'  {result.data[-1]["timestamp"]} - {result.data[-1]["tide_level_ft"]} ft')
    corrected = result.data[-1]['tide_level_ft'] - 2.4
    print(f'  Corrected: {corrected:.1f} ft')
else:
    print('  No data found!')
