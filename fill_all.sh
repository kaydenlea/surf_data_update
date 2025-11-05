#!/bin/bash
# Complete fill pipeline - fast then slow

echo "=== PHASE 1: Fast batch fill ==="
python fill_neighbors.py

echo ""
echo "=== PHASE 2: Slow cleanup fill ==="
python fill_neighbors_slow.py

echo ""
echo "=== VERIFICATION ==="
python -c "
from database import supabase
resp = supabase.table('grid_forecast_data').select('id', count='exact').is_('weather', 'null').execute()
print(f'Remaining weather nulls: {resp.count}')
if resp.count == 0:
    print('✓ All nulls filled successfully!')
else:
    print('⚠️ Some nulls remain')
" 2>&1 | grep -v "HTTP Request"
