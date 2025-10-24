from database import supabase
import json

# Get total count
result = supabase.table('county_tides_15min').select('county', count='exact').limit(1).execute()
print(f'Total records: {result.count}')

# Fetch all data with pagination
counties = {}
page_size = 1000
page = 0

while True:
    start = page * page_size
    end = start + page_size - 1

    page_result = supabase.table('county_tides_15min').select('county').range(start, end).execute()

    if not page_result.data:
        break

    for r in page_result.data:
        counties[r['county']] = counties.get(r['county'], 0) + 1

    print(f'Processed page {page + 1}: {len(page_result.data)} records')

    if len(page_result.data) < page_size:
        break

    page += 1

print('\nRecords per county:')
print(json.dumps(counties, indent=2, sort_keys=True))
