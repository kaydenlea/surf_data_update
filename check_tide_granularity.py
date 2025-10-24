from database import supabase

result = supabase.table('county_tides_15min').select('timestamp,tide_level_ft').eq('county', 'Los Angeles').order('timestamp').limit(10).execute()

print('Los Angeles 15-minute tide data:')
for i, r in enumerate(result.data):
    print(f'{i+1}. {r["timestamp"]} - {r["tide_level_ft"]} ft')
