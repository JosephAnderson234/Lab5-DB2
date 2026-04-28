import os
import struct
import time
import math
import hashlib
from heap_file import read_page, write_page, count_pages, PAGE_SIZE, write_page_data, DEPARTMENT_EMPLOYEE_FORMAT

DEPT_EMP_FORMAT = DEPARTMENT_EMPLOYEE_FORMAT
RECORD_SIZE = struct.calcsize(DEPT_EMP_FORMAT)


def hash_partition(value: str, k: int) -> int:
    """h_p(group_key) % k  →  índice de partición [0, k-1]"""
    hash_val = int(hashlib.md5(value.encode()).hexdigest(), 16)
    return hash_val % k


def hash_aggregate(value: str, table_size: int = 1000) -> int:
    """h_r(group_key)  →  índice en la tabla hash de la partición"""
    hash_val = int(hashlib.md5(value.encode()).hexdigest(), 16)
    return hash_val % table_size


def partition_data(heap_path: str, page_size: int, buffer_size: int, group_key: int) -> tuple[list[str], int, int]:
    """
    Fase 1: Particionamiento
    Lee el heap file página a página, aplica h_p % k y escribe particiones.

    Returns:
        (partition_paths, pages_read, pages_written)
    """
    B = buffer_size // page_size
    k = max(1, B - 1)
    num_records_per_page = page_size // RECORD_SIZE

    partition_paths = [f"partition_{i}.bin" for i in range(k)]
    # Borrar particiones previas para escritura limpia
    for p in partition_paths:
        if os.path.exists(p):
            os.remove(p)

    partition_files = [open(p, 'wb') for p in partition_paths]
    partition_buffers = [[] for _ in range(k)]
    partition_page_ids = [0] * k   # página actual de escritura por partición

    total_pages = count_pages(heap_path, page_size)
    pages_read = 0
    pages_written = 0

    for page_id in range(total_pages):
        records = read_page(heap_path, page_id, page_size, DEPT_EMP_FORMAT)
        pages_read += 1
        for record in records:
            group_value = record[group_key].decode('utf-8').strip('\x00')
            part_idx = hash_partition(group_value, k)
            partition_buffers[part_idx].append(record)

            # Flush cuando el buffer de esa partición se llena
            if len(partition_buffers[part_idx]) == num_records_per_page:
                write_page_data(partition_files[part_idx],
                                partition_buffers[part_idx],
                                DEPT_EMP_FORMAT, page_size)
                partition_page_ids[part_idx] += 1
                pages_written += 1
                partition_buffers[part_idx] = []

    # Flush buffers restantes
    for i in range(k):
        if partition_buffers[i]:
            write_page_data(partition_files[i],
                            partition_buffers[i],
                            DEPT_EMP_FORMAT, page_size)
            pages_written += 1
        partition_files[i].close()

    return partition_paths, pages_read, pages_written


def aggregate_partitions(partition_paths: list[str], page_size: int, buffer_size: int,
                         group_key: int) -> tuple[dict, int]:
    """
    Fase 2: Construcción y agregación
    Para cada partición: carga en memoria, construye tabla hash y acumula COUNT(*).

    Returns:
        (resultado {valor_grupo: count}, pages_read_total)
    """
    aggregated_result = {}
    # --- Bug 1 Fix: contar páginas leídas en la Fase 2 ---
    pages_read = 0

    for partition_path in partition_paths:
        if not os.path.exists(partition_path) or os.path.getsize(partition_path) == 0:
            continue

        hash_table = {}
        total_pages = count_pages(partition_path, page_size)

        for page_id in range(total_pages):
            records = read_page(partition_path, page_id, page_size, DEPT_EMP_FORMAT)
            pages_read += 1          # ← Bug 1 Fix

            for record in records:
                group_value = record[group_key].decode('utf-8').strip('\x00')
                if group_value:
                    hash_table[group_value] = hash_table.get(group_value, 0) + 1

        # Merge con resultado global
        for key, count in hash_table.items():
            aggregated_result[key] = aggregated_result.get(key, 0) + count

    return aggregated_result, pages_read


def external_hash_group_by(heap_path: str, page_size: int, buffer_size: int, group_key_idx: int = 2) -> dict:
    """
    Ejecuta External Hashing completo y retorna estadísticas + resultado.
    """
    start_total = time.time()

    # Fase 1: Particionamiento
    print("--- Fase 1: Particionamiento ---")
    start_phase1 = time.time()
    partition_paths, pages_read_p1, pages_written = partition_data(
        heap_path, page_size, buffer_size, group_key_idx)
    time_phase1 = time.time() - start_phase1

    print(f"Particiones creadas: {len(partition_paths)}")
    print(f"Páginas leídas (Fase 1): {pages_read_p1}")
    print(f"Páginas escritas (Fase 1): {pages_written}")
    print(f"Tiempo Fase 1: {time_phase1:.4f}s")

    # Fase 2: Agregación
    print("\n--- Fase 2: Agregación ---")
    start_phase2 = time.time()
    result, pages_read_p2 = aggregate_partitions(partition_paths, page_size, buffer_size, group_key_idx)
    time_phase2 = time.time() - start_phase2

    print(f"Grupos únicos encontrados: {len(result)}")
    print(f"Páginas leídas (Fase 2): {pages_read_p2}")
    print(f"Tiempo Fase 2: {time_phase2:.4f}s")

    # Limpiar archivos temporales
    for path in partition_paths:
        if os.path.exists(path):
            os.remove(path)

    return {
        'result': result,
        'partitions_created': len(partition_paths),
        'pages_read': pages_read_p1 + pages_read_p2,   # Bug 1 Fix: ambas fases
        'pages_written': pages_written,
        'time_phase1_sec': time_phase1,
        'time_phase2_sec': time_phase2,
        'time_total_sec': time.time() - start_total
    }


if __name__ == "__main__":
    from heap_file import export_to_heap, DEPARTMENT_EMPLOYEE_FORMAT

    print("=== Generando Heap File ===")
    export_to_heap('department_employees.csv', 'department_employees.bin',
                   DEPARTMENT_EMPLOYEE_FORMAT, PAGE_SIZE)

    print("\n=== Ejecutando External Hashing: GROUP BY from_date ===")
    BUFFER_SIZE = 10 * PAGE_SIZE
    stats = external_hash_group_by('department_employees.bin', PAGE_SIZE, BUFFER_SIZE, group_key_idx=2)

    print("\n=== RESULTADO ===")
    print(f"Particiones creadas: {stats['partitions_created']}")
    print(f"Páginas leídas (total): {stats['pages_read']}")
    print(f"Páginas escritas: {stats['pages_written']}")
    print(f"I/O Total: {stats['pages_read'] + stats['pages_written']}")
    print(f"Tiempo Fase 1: {stats['time_phase1_sec']:.4f}s")
    print(f"Tiempo Fase 2: {stats['time_phase2_sec']:.4f}s")
    print(f"Tiempo Total: {stats['time_total_sec']:.4f}s")

    print(f"\n=== GROUP BY Resultados (top 10) ===")
    sorted_result = sorted(stats['result'].items())
    for i, (from_date, count) in enumerate(sorted_result[:10]):
        print(f"from_date: {from_date}, COUNT: {count}")
    if len(sorted_result) > 10:
        print(f"... y {len(sorted_result) - 10} más")
