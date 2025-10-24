timestamps = [
    '2025-10-23 00:00',
    '2025-10-23 00:06',
    '2025-10-23 00:15',
    '2025-10-23 00:30',
    '2025-10-23 00:45',
    '2025-10-23 01:00'
]

print("Testing current logic:")
for ts in timestamps:
    minute = int(ts.split(':')[1])
    divisible = minute % 15 == 0
    print(f'{ts} -> minute={minute:02d} -> {minute}%15={minute%15} -> include={divisible}')
