import struct
import os
import math

# El prefijo '=' evita problemas de alineación
EMPLOYEE_FORMAT = '=i10s20s20s1s10s' 
DEPARTMENT_FORMAT = '=i4s10s10s'
PAGE_SIZE = 4096

def parse_employee(line: str):
    parts = line.strip().split(';')
    if len(parts) < 6: return None
    return (
        int(parts[0]), 
        parts[1].encode('utf-8')[:10].ljust(10, b'\0'), 
        parts[2].encode('utf-8')[:15].ljust(15, b'\0'), 
        parts[3].encode('utf-8')[:15].ljust(15, b'\0'), 
        parts[4].encode('utf-8')[:1].ljust(1, b'\0'), 
        parts[5].encode('utf-8')[:10].ljust(10, b'\0')
    )

def parse_department(line: str):
    parts = line.strip().split(';')
    if len(parts) < 4: return None
    return (
        int(parts[0]), 
        parts[1].encode('utf-8')[:4].ljust(4, b'\0'), 
        parts[2].encode('utf-8')[:10].ljust(10, b'\0'), 
        parts[3].encode('utf-8')[:10].ljust(10, b'\0')
    )

def write_page_data(f, records, record_format, page_size):
    page_data = b""
    for rec in records:
        page_data += struct.pack(record_format, *rec)

    page_data = page_data.ljust(page_size, b'\x00')
    f.write(page_data)

def export_to_heap(csv_path, heap_path, record_format, page_size):
    record_size = struct.calcsize(record_format)
    records_per_page = page_size // record_size
    
    if not os.path.exists(csv_path):
        print(f"Error: No se encuentra {csv_path}")
        return

    with open(csv_path, 'r', encoding='utf-8') as f_csv, open(heap_path, 'wb') as f_heap:
        next(f_csv, None)
        buffer = []
        for line in f_csv:
            if not line.strip(): continue
            
            record = None
            try:
                if record_format == EMPLOYEE_FORMAT:
                    record = parse_employee(line)
                elif record_format == DEPARTMENT_FORMAT:
                    record = parse_department(line)
                
                if record:
                    buffer.append(record)
                
                if len(buffer) == records_per_page:
                    write_page_data(f_heap, buffer, record_format, page_size)
                    buffer = []
            except (ValueError, IndexError):
                continue
        
        if buffer:
            write_page_data(f_heap, buffer, record_format, page_size)

def read_page(heap_path, page_id, page_size, record_format):
    record_size = struct.calcsize(record_format)
    if not os.path.exists(heap_path): return []
    
    with open(heap_path, 'rb') as f:
        f.seek(page_id * page_size)
        page_data = f.read(page_size)
        if not page_data: return []
        
        records = []
        for i in range(0, (len(page_data) // record_size) * record_size, record_size):
            chunk = page_data[i : i + record_size]
        
            if not chunk or chunk[0] == 0: 
                break
            records.append(struct.unpack(record_format, chunk))
    return records

def write_page(heap_path, page_id, records, record_format, page_size):
    # Abrir en modo r+b para no sobrescribir todo el archivo
    mode = 'r+b' if os.path.exists(heap_path) else 'wb'
    with open(heap_path, mode) as f:
        f.seek(page_id * page_size)
        write_page_data(f, records, record_format, page_size)

def count_pages(heap_path, page_size):
    if not os.path.exists(heap_path): return 0
    file_size = os.path.getsize(heap_path)
    return math.ceil(file_size / page_size)