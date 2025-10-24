from database import supabase
from datetime import datetime

result = supabase.table('county_tides_15min').select('timestamp').eq('county', 'Los Angeles').order('timestamp').limit(20).execute()

print('First 20 timestamps with intervals:')
prev = None
for r in result.data[:20]:
    ts = datetime.fromisoformat(r['timestamp'])
    minute_str = ts.strftime('%H:%M')

    if prev:
        delta = (ts - prev).total_seconds() / 60
        print(f'{minute_str} ({delta:.0f} min gap)')
    else:
        print(f'{minute_str}')

    prev = ts
