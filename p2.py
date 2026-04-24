import struct
import os

EMPLOYEE_FORMAT = 'i10s10s12s1s10s'
DEPARTMENT_FORMAT = 'i4s10s10s'
PAGE_SIZE = 4096

def parse_employee(line: str):
    parts = line.strip().split(';')
    return (int(parts[0]), parts[1].encode(), parts[2].encode(), 
            parts[3].encode(), parts[4].encode(), parts[5].encode())

def parse_department(line: str):
    parts = line.strip().split(';')
    return (int(parts[0]), parts[1].encode(), parts[2].encode(), parts[3].encode())

# Exporta un CSV a un heap file binario paginado.
def export_to_heap(csv_path: str, heap_path: str, record_format: str, page_size: int):
    record_size = struct.calcsize(record_format)
    records_per_page = page_size // record_size
    
    with open(csv_path, 'r') as f_csv, open(heap_path, 'wb') as f_heap:
        next(f_csv) # Saltar header 
        buffer = []
        for line in f_csv:
            # Se parsea el CSV según el formato indicado
            if record_format == EMPLOYEE_FORMAT:
                record = parse_employee(line)
            elif record_format == DEPARTMENT_FORMAT:
                record = parse_department(line)
            buffer.append(record)
            
            if len(buffer) == records_per_page:
                write_page_data(f_heap, buffer, record_format, page_size)
                buffer = []
        
        # Se escribe los registros restantes en la ultima pagina
        if buffer:
            write_page_data(f_heap, buffer, record_format, page_size)

# Lee una página del heap file y retorna sus registros.
def read_page(heap_path: str, page_id: int, page_size: int, record_format: str) -> list[tuple]:
    record_size = struct.calcsize(record_format)
    with open(heap_path, 'rb') as f:
        f.seek(page_id * page_size)
        page_data = f.read(page_size)
        
        records = []
        for i in range(0, page_size, record_size):
            chunk = page_data[i : i + record_size]
            # Si el registro empieza con un byte nulo (o es todo nulo), es padding
            if chunk[0] == 0: 
                break
            records.append(struct.unpack(record_format, chunk))
    return records

# Escribe una lista de registros en la página indicada
def write_page(heap_path: str, page_id: int, records: list[tuple], record_format: str, page_size: int):
    with open(heap_path, 'r+b') as f:
        f.seek(page_id * page_size)
        write_page_data(f, records, record_format, page_size)

# Retorna el número total de páginas del heap file.
def count_pages(heap_path: str, page_size: int) -> int:
    file_size = os.path.getsize(heap_path)
    return (file_size + page_size - 1) // page_size

# Ayuda a estandarizar el llenado de paginass
def write_page_data(f, records, record_format, page_size):
    page_data = b""
    for rec in records:
        page_data += struct.pack(record_format, *rec)
    # Llena el resto de la página con bytes vacíos si es necesario
    page_data = page_data.ljust(page_size, b'\x00')
    f.write(page_data)



if __name__ == "__main__":
    export_to_heap('Employee', 'employees.bin', EMPLOYEE_FORMAT, PAGE_SIZE)
    export_to_heap('department_employee', 'department_employee.bin', DEPARTMENT_FORMAT, PAGE_SIZE)  
    print(f"Total páginas empleados: {count_pages('employees.bin', PAGE_SIZE)}")
    print(f"Total páginas departamentos: {count_pages('department_employee.bin', PAGE_SIZE)}")
    records = read_page('employees.bin', 0, PAGE_SIZE, EMPLOYEE_FORMAT)
    print("Registros en la primera página de empleados:")
    for rec in records:
        print(rec)
    records = read_page('department_employee.bin', 0, PAGE_SIZE, DEPARTMENT_FORMAT)
    print("Registros en la primera página de departamentos:")
    for rec in records:
        print(rec)