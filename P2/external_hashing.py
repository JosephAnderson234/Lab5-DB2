import os
import struct
import time
import math
import hashlib
from heap_file import read_page, write_page, count_pages, PAGE_SIZE, write_page_data, DEPARTMENT_EMPLOYEE_FORMAT

# Usar el formato definido en heap_file
DEPT_EMP_FORMAT = DEPARTMENT_EMPLOYEE_FORMAT
RECORD_SIZE = struct.calcsize(DEPT_EMP_FORMAT)

def hash_partition(value: str, k: int) -> int:
    """
    Función hash para particionar: h_p(group_key) % k
    value: valor del group_key (from_date)
    k: número de particiones
    Retorna: índice de partición [0, k-1]
    """
    hash_val = int(hashlib.md5(value.encode()).hexdigest(), 16)
    return hash_val % k

def hash_aggregate(value: str, table_size: int = 1000) -> int:
    """
    Función hash para agregación dentro de cada partición: h_r(group_key)
    value: valor del group_key
    table_size: tamaño de la tabla hash
    Retorna: índice en la tabla hash
    """
    hash_val = int(hashlib.md5(value.encode()).hexdigest(), 16)
    return hash_val % table_size

def partition_data(heap_path: str, page_size: int, buffer_size: int, group_key: str) -> list[str]:
    """
    Fase 1: Particionamiento
    Lee el heap file página a página.
    Aplica h_p(group_key) % k para asignar cada tupla a una partición.
    Escribe las particiones como archivos temporales.
    
    Args:
        heap_path: ruta al heap file
        page_size: tamaño de página
        buffer_size: tamaño del buffer en bytes
        group_key: índice del campo a agrupar (para from_date es 2)
    
    Returns:
        lista de rutas de archivos de particiones
    """
    B = buffer_size // page_size
    k = max(1, B - 1)
    num_records_per_page = page_size // RECORD_SIZE
    
    partition_paths = [f"partition_{i}.bin" for i in range(k)]
    partition_files = [open(p, 'wb') for p in partition_paths]
    partition_buffers = [[] for _ in range(k)]
    
    total_pages = count_pages(heap_path, page_size)
    
    for page_id in range(total_pages):
        records = read_page(heap_path, page_id, page_size, DEPT_EMP_FORMAT)
        for record in records:
            group_value = record[group_key].decode('utf-8').strip('\x00')
            part_idx = hash_partition(group_value, k)
            partition_buffers[part_idx].append(record)
            
            # Flush cuando el buffer de esa partición se llena
            if len(partition_buffers[part_idx]) == num_records_per_page:
                write_page_data(partition_files[part_idx], 
                            partition_buffers[part_idx], 
                            DEPT_EMP_FORMAT, page_size)
                partition_buffers[part_idx] = []
    
    # Flush buffers restantes
    for i in range(k):
        if partition_buffers[i]:
            write_page_data(partition_files[i], 
                        partition_buffers[i], 
                        DEPT_EMP_FORMAT, page_size)
        partition_files[i].close()
    
    return partition_paths

def aggregate_partitions(partition_paths: list[str], page_size: int, buffer_size: int,
                        group_key: int) -> dict:
    """
    Fase 2: Construcción y agregación
    Para cada partición:
    - Carga en memoria.
    - Construye tabla hash con h_r(group_key).
    - Acumula COUNT(*) por valor de group_key.
    
    Args:
        partition_paths: lista de rutas a particiones
        page_size: tamaño de página
        buffer_size: tamaño del buffer
        group_key: índice del campo a agrupar
    
    Returns:
        diccionario {valor_grupo: count, ...}
    """
    aggregated_result = {}
    
    for partition_path in partition_paths:
        if not os.path.exists(partition_path) or os.path.getsize(partition_path) == 0:
            continue
        
        # Leer partición completa y parsear registros
        hash_table = {}
        record_size = struct.calcsize(DEPT_EMP_FORMAT)
        
        total_pages = count_pages(partition_path, page_size)
        
        for page_id in range(total_pages):
            records = read_page(partition_path, page_id, page_size, DEPT_EMP_FORMAT)
            
            for record in records:
                group_value = record[group_key].decode('utf-8').strip('\x00')
                
                if group_value:  # Evitar valores nulos
                    if group_value not in hash_table:
                        hash_table[group_value] = 0
                    hash_table[group_value] += 1
        
        # Merger con resultado global
        for key, count in hash_table.items():
            if key not in aggregated_result:
                aggregated_result[key] = 0
            aggregated_result[key] += count
    
    return aggregated_result

def external_hash_group_by(heap_path: str, page_size: int, buffer_size: int, group_key_idx: int = 2) -> dict:
    """
    Ejecuta External Hashing completo y retorna estadísticas.
    
    Args:
        heap_path: ruta al heap file
        page_size: tamaño de página
        buffer_size: tamaño del buffer
        group_key_idx: índice del campo a agrupar (default 2 para from_date)
    
    Returns:
        {
            'result': {valor: count, ...},
            'partitions_created': int,
            'pages_read': int,
            'pages_written': int,
            'time_phase1_sec': float,
            'time_phase2_sec': float,
            'time_total_sec': float
        }
    """
    start_total = time.time()
    
    # Fase 1: Particionamiento
    print("--- Fase 1: Particionamiento ---")
    start_phase1 = time.time()
    
    partition_paths = partition_data(heap_path, page_size, buffer_size, group_key_idx)
    
    time_phase1 = time.time() - start_phase1
    total_pages_input = count_pages(heap_path, page_size)
    
    print(f"Particiones creadas: {len(partition_paths)}")
    print(f"Páginas leídas: {total_pages_input}")
    print(f"Tiempo Fase 1: {time_phase1:.4f}s")
    
    # Fase 2: Agregación
    print("\n--- Fase 2: Agregación ---")
    start_phase2 = time.time()
    
    result = aggregate_partitions(partition_paths, page_size, buffer_size, group_key_idx)
    
    time_phase2 = time.time() - start_phase2
    
    print(f"Grupos únicos encontrados: {len(result)}")
    print(f"Tiempo Fase 2: {time_phase2:.4f}s")
    
    # Contar páginas escritas antes de eliminar
    pages_written = 0
    for path in partition_paths:
        if os.path.exists(path):
            pages_written += math.ceil(os.path.getsize(path) / page_size)
    
    # Limpiar archivos temporales
    for path in partition_paths:
        if os.path.exists(path):
            os.remove(path)
    
    time_total = time.time() - start_total
    
    return {
        'result': result,
        'partitions_created': len(partition_paths),
        'pages_read': total_pages_input,
        'pages_written': pages_written,
        'time_phase1_sec': time_phase1,
        'time_phase2_sec': time_phase2,
        'time_total_sec': time_total
    }

if __name__ == "__main__":
    # Primero crear el heap file
    from heap_file import export_to_heap, DEPARTMENT_EMPLOYEE_FORMAT
    
    print("=== Generando Heap File ===")
    export_to_heap('department_employees.csv', 'department_employees.bin', DEPARTMENT_EMPLOYEE_FORMAT, PAGE_SIZE)
    
    print("\n=== Ejecutando External Hashing: GROUP BY from_date ===")
    
    BUFFER_SIZE = 10 * PAGE_SIZE  # 10 páginas en buffer
    stats = external_hash_group_by('department_employees.bin', PAGE_SIZE, BUFFER_SIZE, group_key_idx=2)
    
    print("\n=== RESULTADO ===")
    print(f"Particiones creadas: {stats['partitions_created']}")
    print(f"Páginas leídas: {stats['pages_read']}")
    print(f"Páginas escritas: {stats['pages_written']}")
    print(f"Tiempo Fase 1: {stats['time_phase1_sec']:.4f}s")
    print(f"Tiempo Fase 2: {stats['time_phase2_sec']:.4f}s")
    print(f"Tiempo Total: {stats['time_total_sec']:.4f}s")
    
    print(f"\n=== GROUP BY Resultados (top 10) ===")
    sorted_result = sorted(stats['result'].items())
    for i, (from_date, count) in enumerate(sorted_result):
        if i < 10:
            print(f"from_date: {from_date}, COUNT: {count}")
    
    if len(sorted_result) > 10:
        print(f"... y {len(sorted_result) - 10} más")
