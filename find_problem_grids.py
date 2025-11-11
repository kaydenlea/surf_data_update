#!/usr/bin/env python3
"""
Find which grid points have the wind gust < wind speed issue
"""
from database import supabase
from collections import defaultdict

print("Analyzing wind data by grid point...")
print("=" * 100)

# Get all records with wind data
response = supabase.table("grid_forecast_data").select(
    "grid_id, timestamp, wind_speed_mph, wind_gust_mph"
).not_.is_("wind_speed_mph", "null").not_.is_("wind_gust_mph", "null").limit(5000).execute()

records = response.data
print(f"Fetched {len(records)} records with wind data")

# Group by grid_id
grid_stats = defaultdict(lambda: {"total": 0, "bad": 0, "good": 0})

for rec in records:
    grid_id = rec.get('grid_id')
    wind_speed = rec.get('wind_speed_mph')
    wind_gust = rec.get('wind_gust_mph')

    if wind_speed is not None and wind_gust is not None:
        grid_stats[grid_id]["total"] += 1

        if wind_gust < wind_speed:
            grid_stats[grid_id]["bad"] += 1
        else:
            grid_stats[grid_id]["good"] += 1

# Calculate percentages and sort by bad percentage
grid_results = []
for grid_id, stats in grid_stats.items():
    if stats["total"] > 0:
        bad_pct = (stats["bad"] / stats["total"]) * 100
        grid_results.append({
            "grid_id": grid_id,
            "total": stats["total"],
            "bad": stats["bad"],
            "good": stats["good"],
            "bad_pct": bad_pct
        })

# Sort by bad percentage (descending)
grid_results.sort(key=lambda x: x["bad_pct"], reverse=True)

# Show top 20 worst grids
print("\nTop 20 Grid Points with Highest % of Bad Wind Data (gust < speed):")
print("=" * 100)
print(f"{'Grid ID':<10} {'Total Records':<15} {'Bad Records':<15} {'Good Records':<15} {'% Bad':<10}")
print("-" * 100)

for i, grid in enumerate(grid_results[:20]):
    print(f"{grid['grid_id']:<10} {grid['total']:<15} {grid['bad']:<15} {grid['good']:<15} {grid['bad_pct']:<10.1f}%")

# Show grids with 100% good data (for comparison)
print("\n" + "=" * 100)
print("Grid Points with 100% Good Wind Data (gust >= speed):")
print("=" * 100)
print(f"{'Grid ID':<10} {'Total Records':<15}")
print("-" * 100)

good_grids = [g for g in grid_results if g["bad_pct"] == 0][:20]
for grid in good_grids:
    print(f"{grid['grid_id']:<10} {grid['total']:<15}")

# Overall statistics
total_records = sum(g["total"] for g in grid_results)
total_bad = sum(g["bad"] for g in grid_results)
total_good = sum(g["good"] for g in grid_results)
overall_bad_pct = (total_bad / total_records * 100) if total_records > 0 else 0

print("\n" + "=" * 100)
print("OVERALL STATISTICS:")
print("=" * 100)
print(f"Total grid points analyzed: {len(grid_results)}")
print(f"Total records: {total_records}")
print(f"Bad records (gust < speed): {total_bad}")
print(f"Good records (gust >= speed): {total_good}")
print(f"Overall % bad: {overall_bad_pct:.1f}%")

# Count grids by problem severity
grids_0_pct = len([g for g in grid_results if g["bad_pct"] == 0])
grids_1_25_pct = len([g for g in grid_results if 0 < g["bad_pct"] <= 25])
grids_26_50_pct = len([g for g in grid_results if 25 < g["bad_pct"] <= 50])
grids_51_75_pct = len([g for g in grid_results if 50 < g["bad_pct"] <= 75])
grids_76_100_pct = len([g for g in grid_results if 75 < g["bad_pct"] <= 100])

print(f"\nGrid Points by Problem Severity:")
print(f"  0% bad (perfect): {grids_0_pct}")
print(f"  1-25% bad: {grids_1_25_pct}")
print(f"  26-50% bad: {grids_26_50_pct}")
print(f"  51-75% bad: {grids_51_75_pct}")
print(f"  76-100% bad: {grids_76_100_pct}")

print("\n" + "=" * 100)
print("ANALYSIS:")
if grids_76_100_pct > 0:
    print(f"*** {grids_76_100_pct} grid points have >75% bad data - suggests a systematic issue ***")
if grids_0_pct > len(grid_results) / 2:
    print(f"*** {grids_0_pct}/{len(grid_results)} grid points have perfect data - issue is location-specific ***")
print("=" * 100)
