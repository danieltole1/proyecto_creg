import json

file_path = "embeddings_batch_c1.jsonl"

seen = set()
dups = {}
total = 0

print(f"🔍 Buscando custom_id duplicados en {file_path} ...")

with open(file_path, "r", encoding="utf-8") as f:
    for idx, line in enumerate(f, start=1):
        line = line.strip()
        if not line:
            continue
        total += 1
        try:
            obj = json.loads(line)
        except Exception as e:
            print(f"❌ Línea {idx}: JSON inválido: {e}")
            continue

        cid = obj.get("custom_id")
        if cid is None:
            print(f"⚠️ Línea {idx}: sin custom_id")
            continue

        if cid in seen:
            dups.setdefault(cid, []).append(idx)
        else:
            seen.add(cid)

print(f"📊 Total líneas leídas: {total}")
print(f"📊 custom_id únicos: {len(seen)}")
print(f"📊 custom_id duplicados: {len(dups)}")

if dups:
    print("\nAlgunos duplicados de ejemplo:")
    for i, (cid, lines) in enumerate(dups.items()):
        print(f" - custom_id={cid} en líneas {lines[:5]}")
        if i >= 10:
            break
else:
    print("\n✅ No hay custom_id duplicados.")
