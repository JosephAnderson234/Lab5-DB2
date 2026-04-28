import struct
import os
import math

def direct_read_page(path: str, page_id: int, page_size: int) -> bytes:
    """Lee una página del archivo. Retorna exactamente page_size bytes."""
    offset = page_id * page_size
    try:
        fd = os.open(path, os.O_RDONLY)
        try:
            os.lseek(fd, offset, os.SEEK_SET)
            data = os.read(fd, page_size)
            return data.ljust(page_size, b'\x00')
        finally:
            os.close(fd)
    except OSError:
        return b'\x00' * page_size


def direct_write_page(path: str, page_id: int, data: bytes, page_size: int):
    """Escribe una página en el archivo."""
    data = data.ljust(page_size, b'\x00')[:page_size]
    offset = page_id * page_size
    fd = os.open(path, os.O_WRONLY | os.O_CREAT, 0o644)
    try:
        os.lseek(fd, offset, os.SEEK_SET)
        os.write(fd, data)
    finally:
        os.close(fd)

EMPLOYEE_FORMAT = '=i10s20s20s1s10s'
DEPARTMENT_EMPLOYEE_FORMAT = '=i4s10s10s'
PAGE_SIZE = 4096

def parse_employee(line: str):
    parts = line.strip().split(';')
    return (int(parts[0]), parts[1].encode(), parts[2].encode(),
            parts[3].encode(), parts[4].encode(), parts[5].encode())

def parse_department_employee(line: str):
    parts = line.strip().split(';')
    return (int(parts[0]), parts[1].encode(), parts[2].encode(), parts[3].encode())

def pack_page(records: list, record_format: str, page_size: int) -> bytes:
    """Serializa una lista de registros a bytes de tamaño page_size."""
    data = b""
    for rec in records:
        data += struct.pack(record_format, *rec)
    return data.ljust(page_size, b'\x00')[:page_size]

# Helper mantenido para compatibilidad con external_hashing (escribe a file object abierto)
def write_page_data(f, records, record_format, page_size):
    f.write(pack_page(records, record_format, page_size))

# Exporta un CSV a un heap file binario paginado.
def export_to_heap(csv_path: str, heap_path: str, record_format: str, page_size: int):
    record_size = struct.calcsize(record_format)
    records_per_page = page_size // record_size

    # Borrar archivo previo para empezar limpio
    if os.path.exists(heap_path):
        os.remove(heap_path)

    with open(csv_path, 'r') as f_csv:
        next(f_csv)  # Saltar header
        buffer = []
        page_id = 0
        for line in f_csv:
            if record_format == EMPLOYEE_FORMAT:
                record = parse_employee(line)
            else:
                record = parse_department_employee(line)
            buffer.append(record)

            if len(buffer) == records_per_page:
                direct_write_page(heap_path, page_id, pack_page(buffer, record_format, page_size), page_size)
                page_id += 1
                buffer = []

        if buffer:
            direct_write_page(heap_path, page_id, pack_page(buffer, record_format, page_size), page_size)

# Lee una página del heap file y retorna sus registros.
def read_page(heap_path: str, page_id: int, page_size: int, record_format: str) -> list[tuple]:
    record_size = struct.calcsize(record_format)
    page_data = direct_read_page(heap_path, page_id, page_size)

    records = []
    for i in range(0, page_size, record_size):
        chunk = page_data[i: i + record_size]
        if len(chunk) < record_size:
            break
        if chunk == b'\x00' * record_size:
            break
        records.append(struct.unpack(record_format, chunk))
    return records

# Escribe una lista de registros en la página indicada usando O_DIRECT.
def write_page(heap_path: str, page_id: int, records: list[tuple], record_format: str, page_size: int):
    data = pack_page(records, record_format, page_size)
    direct_write_page(heap_path, page_id, data, page_size)

# Retorna el número total de páginas del heap file.
def count_pages(heap_path: str, page_size: int) -> int:
    file_size = os.path.getsize(heap_path)
    return (file_size + page_size - 1) // page_size


if __name__ == "__main__":
    export_to_heap('employees.csv', 'employees.bin', EMPLOYEE_FORMAT, PAGE_SIZE)
    export_to_heap('department_employees.csv', 'department_employees.bin', DEPARTMENT_EMPLOYEE_FORMAT, PAGE_SIZE)
    print(f"Total páginas empleados: {count_pages('employees.bin', PAGE_SIZE)}")
    print(f"Total páginas departamentos: {count_pages('department_employees.bin', PAGE_SIZE)}")
    records = read_page('employees.bin', 0, PAGE_SIZE, EMPLOYEE_FORMAT)
    print("Registros en la primera página de empleados:")
    for rec in records:
        print(rec)
    records = read_page('department_employees.bin', 0, PAGE_SIZE, DEPARTMENT_EMPLOYEE_FORMAT)
    print("Registros en la primera página de departamentos:")
    for rec in records:
        print(rec)
