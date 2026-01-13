import re
from collections import Counter

path = "urls_discovered_all_years.txt"
years = []

with open(path, "r", encoding="utf-8") as f:
    for line in f:
        u = line.strip()
        if not u:
            continue
        m = re.search(r"_(\d{4})\.htm$", u)
        if m:
            years.append(int(m.group(1)))

c = Counter(years)
print("TOTAL URLs:", sum(c.values()))
print("AÑOS (desc):")
for y, n in sorted(c.items(), reverse=True):
    print(f"{y}: {n}")

print("\nMIN:", min(years) if years else None)
print("MAX:", max(years) if years else None)
