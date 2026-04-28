import struct
import os
from heap_file import DEPARTMENT_EMPLOYEE_FORMAT, PAGE_SIZE

def verify_from_binary():
    RECORD_FORMAT = DEPARTMENT_EMPLOYEE_FORMAT  
    RECORD_SIZE = struct.calcsize(RECORD_FORMAT)
    HEAP_FILE = 'department_employees.bin'
    GROUP_KEY_IDX = 2

    file_size = os.path.getsize(HEAP_FILE)
    print(f"Tamaño real del archivo en disco: {file_size} bytes")
    print(f"RECORD_SIZE: {RECORD_SIZE}, Records per page: {PAGE_SIZE // RECORD_SIZE}")

    counts = {}
    total_pages = (file_size + PAGE_SIZE - 1) // PAGE_SIZE
    print(f"Analizando {total_pages} páginas de {HEAP_FILE}...")

    with open(HEAP_FILE, 'rb') as f:
        for page_id in range(total_pages):
            f.seek(page_id * PAGE_SIZE)
            page_data = f.read(PAGE_SIZE)
            for i in range(0, PAGE_SIZE, RECORD_SIZE):
                chunk = page_data[i: i + RECORD_SIZE]
                if len(chunk) < RECORD_SIZE:
                    break
                if chunk == b'\x00' * RECORD_SIZE:
                    continue
                rec = struct.unpack(RECORD_FORMAT, chunk)
                if rec[0] <= 0:
                    continue
                from_date = rec[GROUP_KEY_IDX].decode('utf-8', errors='ignore').strip('\x00').strip()
                if from_date:
                    counts[from_date] = counts.get(from_date, 0) + 1

    print("\n" + "="*40)
    print("   RESULTADOS DEL BINARIO (TOP 10)")
    print("="*40)
    print(f"{'from_date':<20} | {'COUNT(*)':<10}")
    print("-" * 35)
    sorted_dates = sorted(counts.keys())
    for date in sorted_dates[:10]:
        print(f"{date:<20} | {counts[date]:<10}")
    print("-" * 35)
    print(f"Total de fechas únicas: {len(counts)}")
    print(f"Suma total de registros: {sum(counts.values())}")
    print("="*40)

if __name__ == "__main__":
    verify_from_binary()