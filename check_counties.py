from database import supabase
import json

result = supabase.table('county_tides_15min').select('county', count='exact').execute()
print(f'Total records: {result.count}')

counties = {}
all_data = supabase.table('county_tides_15min').select('county').execute()
for r in all_data.data:
    counties[r['county']] = counties.get(r['county'], 0) + 1

print('\nRecords per county:')
print(json.dumps(counties, indent=2, sort_keys=True))
