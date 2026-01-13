import os

input_file = "embeddings_batch.jsonl"
out1 = "embeddings_batch_c1.jsonl"
out2 = "embeddings_batch_c2.jsonl"
max_c1 = 50000  # máximo permitido en un batch

print("📁 Leyendo embeddings_batch.jsonl...")

if not os.path.exists(input_file):
    print("❌ No se encontró embeddings_batch.jsonl")
    raise SystemExit()

total = 0
c1 = 0
c2 = 0

with open(input_file, "r", encoding="utf-8") as fin, \
     open(out1, "w", encoding="utf-8") as f1, \
     open(out2, "w", encoding="utf-8") as f2:
    for line in fin:
        if not line.strip():
            continue
        total += 1
        if total <= max_c1:
            f1.write(line)
            c1 += 1
        else:
            f2.write(line)
            c2 += 1

print(f"✅ Total líneas: {total}")
print(f"   - {out1}: {c1} líneas")
print(f"   - {out2}: {c2} líneas")
