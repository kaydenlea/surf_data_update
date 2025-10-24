from database import supabase

start = '2025-10-23T07:00:00.000Z'
end = '2025-10-24T07:00:00.000Z'

result = supabase.table('county_tides_15min').select('timestamp,tide_level_ft,tide_level_m').eq('county', 'Orange').gte('timestamp', start).lte('timestamp', end).order('timestamp', desc=False).execute()

print(f'Query for Orange county between {start} and {end}:')
print(f'Result: {len(result.data)} records')

if result.data:
    print(f'\nFirst: {result.data[0]}')
    print(f'Last: {result.data[-1]}')
else:
    print('\nNo data returned!')

    # Check what timestamps exist for Orange
    all_orange = supabase.table('county_tides_15min').select('timestamp').eq('county', 'Orange').order('timestamp').limit(5).execute()
    print(f'\nFirst 5 Orange county timestamps in DB:')
    for r in all_orange.data:
        print(f'  {r["timestamp"]}')
