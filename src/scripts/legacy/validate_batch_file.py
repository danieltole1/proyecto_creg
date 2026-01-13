import json
import sys
from collections import defaultdict

def validate_jsonl(filepath):
    """Valida completamente un archivo JSONL para batch API de OpenAI."""
    
    print(f"\n🔍 VALIDACIÓN EXHAUSTIVA: {filepath}")
    print("=" * 80)
    
    # Contadores
    total_lines = 0
    valid_lines = 0
    errors = defaultdict(list)
    custom_ids = defaultdict(list)
    duplicates = set()
    
    # Leer y validar línea por línea
    with open(filepath, "r", encoding="utf-8") as f:
        for line_num, line in enumerate(f, start=1):
            line = line.strip()
            if not line:
                continue
            
            total_lines += 1
            
            # Validar JSON
            try:
                obj = json.loads(line)
            except json.JSONDecodeError as e:
                errors["json_invalid"].append((line_num, str(e)))
                continue
            
            valid_lines += 1
            
            # Validar estructura requerida
            required = ["custom_id", "method", "url", "body"]
            for field in required:
                if field not in obj:
                    errors[f"missing_{field}"].append(line_num)
                    continue
            
            # Validar custom_id
            custom_id = obj.get("custom_id")
            if not custom_id:
                errors["empty_custom_id"].append(line_num)
                continue
            
            if not isinstance(custom_id, str):
                errors["custom_id_not_string"].append((line_num, type(custom_id).__name__))
                continue
            
            # Detectar duplicados
            custom_ids[custom_id].append(line_num)
            if custom_id in duplicates or len(custom_ids[custom_id]) > 1:
                duplicates.add(custom_id)
                errors["duplicate_custom_id"].append((custom_id, custom_ids[custom_id]))
            
            # Validar method
            if obj.get("method") not in ["POST"]:
                errors["invalid_method"].append((line_num, obj.get("method")))
            
            # Validar URL
            if obj.get("url") != "/v1/embeddings":
                errors["invalid_url"].append((line_num, obj.get("url")))
            
            # Validar body
            body = obj.get("body", {})
            if not isinstance(body, dict):
                errors["body_not_dict"].append(line_num)
                continue
            
            if "model" not in body or body["model"] != "text-embedding-3-small":
                errors["invalid_model"].append((line_num, body.get("model")))
            
            if "input" not in body:
                errors["missing_input"].append(line_num)
                continue
            
            if "encoding_format" not in body or body["encoding_format"] != "float":
                errors["invalid_encoding_format"].append((line_num, body.get("encoding_format")))
    
    # Reporte
    print(f"\n📊 RESUMEN")
    print(f"  Total líneas: {total_lines}")
    print(f"  Líneas válidas: {valid_lines}")
    print(f"  custom_id únicos: {len([k for k, v in custom_ids.items() if len(v) == 1])}")
    print(f"  custom_id duplicados: {len(duplicates)}")
    
    if errors:
        print(f"\n❌ ERRORES ENCONTRADOS:")
        for error_type, details in errors.items():
            print(f"\n  {error_type}: {len(details)} ocurrencias")
            if error_type == "duplicate_custom_id":
                for cid, lines in details[:10]:
                    print(f"    - {cid}: líneas {lines}")
            else:
                for item in details[:5]:
                    print(f"    - {item}")
            if len(details) > 5:
                print(f"    ... y {len(details) - 5} más")
    else:
        print(f"\n✅ ARCHIVO VÁLIDO - SIN ERRORES")
    
    print("\n" + "=" * 80)
    return len(errors) == 0

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Uso: python validate_batch_file.py <archivo.jsonl>")
        sys.exit(1)
    
    filepath = sys.argv[1]
    is_valid = validate_jsonl(filepath)
    sys.exit(0 if is_valid else 1)
