import os
import math
import heapq
import time
import struct
from heap_file import read_page, write_page, count_pages, export_to_heap, pack_page

RECORD_FORMAT = '=i10s20s20s1s10s'
RECORD_SIZE = struct.calcsize(RECORD_FORMAT)


def generate_runs(heap_path, page_size, buffer_size, sort_key_idx):
    """Fase 1: Generación de runs ordenados usando B páginas de buffer."""
    B = buffer_size // page_size
    total_pages = count_pages(heap_path, page_size)
    run_paths = []
    pages_read = 0
    pages_written = 0

    for i in range(0, total_pages, B):
        temp_buffer = []

        for j in range(i, min(i + B, total_pages)):
            records = read_page(heap_path, j, page_size, RECORD_FORMAT)
            temp_buffer.extend(records)
            pages_read += 1

        temp_buffer.sort(key=lambda x: x[sort_key_idx])

        run_path = f"run_{len(run_paths)}.bin"
        if os.path.exists(run_path):
            os.remove(run_path)
        run_paths.append(run_path)

        num_records_per_page = page_size // RECORD_SIZE
        for p_id in range(math.ceil(len(temp_buffer) / num_records_per_page)):
            start = p_id * num_records_per_page
            end = start + num_records_per_page
            write_page(run_path, p_id, temp_buffer[start:end], RECORD_FORMAT, page_size)
            pages_written += 1

    return run_paths, pages_read, pages_written


def _merge_runs(run_paths, output_path, page_size, sort_key_idx):
    """
    Merge de un lote de runs hacia output_path.
    Asume que len(run_paths) <= B-1 (caben en buffer).
    Retorna (pages_read, pages_written).
    """
    num_records_per_page = page_size // RECORD_SIZE
    pages_read = 0
    pages_written = 0

    # Un buffer de entrada (1 página) por cada run del lote
    run_buffers = {}
    run_page_ids = {}

    for i, path in enumerate(run_paths):
        run_page_ids[i] = 0
        page = read_page(path, 0, page_size, RECORD_FORMAT)
        run_buffers[i] = page
        if page:
            pages_read += 1

    # Inicializar min-heap con el primer registro de cada run
    min_heap = []
    for i in range(len(run_paths)):
        if run_buffers[i]:
            record = run_buffers[i].pop(0)
            heapq.heappush(min_heap, (record[sort_key_idx], record, i))

    if os.path.exists(output_path):
        os.remove(output_path)

    output_buffer = []
    output_page_id = 0

    while min_heap:
        val, record, run_idx = heapq.heappop(min_heap)
        output_buffer.append(record)

        # Flush del buffer de salida cuando se llena (1 página)
        if len(output_buffer) == num_records_per_page:
            write_page(output_path, output_page_id, output_buffer, RECORD_FORMAT, page_size)
            output_page_id += 1
            pages_written += 1
            output_buffer = []

        # Si el buffer del run se agotó, cargar siguiente página
        if not run_buffers[run_idx]:
            run_page_ids[run_idx] += 1
            next_page = read_page(run_paths[run_idx], run_page_ids[run_idx], page_size, RECORD_FORMAT)
            run_buffers[run_idx] = next_page
            if next_page:
                pages_read += 1

        if run_buffers[run_idx]:
            next_record = run_buffers[run_idx].pop(0)
            heapq.heappush(min_heap, (next_record[sort_key_idx], next_record, run_idx))

    # Escribir registros restantes en el buffer de salida
    if output_buffer:
        write_page(output_path, output_page_id, output_buffer, RECORD_FORMAT, page_size)
        pages_written += 1

    return pages_read, pages_written


def multiway_merge(run_paths, output_path, page_size, buffer_size, sort_key_idx):
    """
    Fase 2: Multiway merge respetando el límite de B-1 buffers de entrada.
    Si hay más runs que B-1, hace pasadas intermedias en lotes hasta
    reducir el número de runs a un nivel que quepa en una sola pasada final.
    """
    B = buffer_size // page_size
    max_streams = B - 1   # B-1 buffers de entrada, 1 buffer de salida
    pages_read = 0
    pages_written = 0

    current_runs = list(run_paths)
    round_num = 0

    # Reducir runs en pasadas intermedias hasta que quepan en una sola pasada
    while len(current_runs) > max_streams:
        next_round_runs = []

        for i in range(0, len(current_runs), max_streams):
            batch = current_runs[i: i + max_streams]
            temp_out = f"temp_round{round_num}_batch{i}.bin"
            r, w = _merge_runs(batch, temp_out, page_size, sort_key_idx)
            pages_read += r
            pages_written += w
            next_round_runs.append(temp_out)

            # Eliminar runs del lote procesado (no eliminar los runs originales
            # en la primera ronda, ya que generate_runs los creó; sí eliminar
            # temporales de rondas anteriores)
            if round_num > 0:
                for p in batch:
                    if os.path.exists(p):
                        os.remove(p)

        # En rondas posteriores a la primera también limpiar los runs originales
        if round_num == 0:
            for p in current_runs:
                if os.path.exists(p):
                    os.remove(p)

        current_runs = next_round_runs
        round_num += 1

    # Pasada final: merge de los runs restantes hacia el archivo de salida
    r, w = _merge_runs(current_runs, output_path, page_size, sort_key_idx)
    pages_read += r
    pages_written += w

    # Limpiar temporales de la última ronda intermedia (si hubo más de una ronda)
    if round_num > 0:
        for p in current_runs:
            if os.path.exists(p):
                os.remove(p)

    return pages_read, pages_written


def external_sort(heap_path, output_path, page_size, buffer_size, sort_key_idx):
    """Ejecuta TPMMS y retorna métricas de rendimiento."""
    start_total = time.time()

    # Fase 1: Generación de runs
    start_p1 = time.time()
    run_paths, r1, w1 = generate_runs(heap_path, page_size, buffer_size, sort_key_idx)
    end_p1 = time.time()

    # Fase 2: Merge respetando límite de buffer
    start_p2 = time.time()
    r2, w2 = multiway_merge(run_paths, output_path, page_size, buffer_size, sort_key_idx)
    end_p2 = time.time()

    return {
        'runs_generated': len(run_paths),
        'pages_read': r1 + r2,
        'pages_written': w1 + w2,
        'time_phase1_sec': end_p1 - start_p1,
        'time_phase2_sec': end_p2 - start_p2,
        'time_total_sec': time.time() - start_total
    }


if __name__ == '__main__':
    PAGE_SIZE = 4096
    BUFFER_SIZE = 64 * 1024

    CSV_FILE = 'employees.csv'
    HEAP_FILE = 'employees.bin'
    OUTPUT_FILE = 'sorted_employees.bin'

    if not os.path.exists(CSV_FILE):
        print(f"Error: No se encuentra {CSV_FILE}.")
    else:
        print("--- Iniciando Proceso ---")
        export_to_heap(CSV_FILE, HEAP_FILE, RECORD_FORMAT, PAGE_SIZE)

        n_pages = count_pages(HEAP_FILE, PAGE_SIZE)
        print(f"Archivo cargado: {n_pages} páginas.")

        if n_pages > 0:
            print("Ejecutando External Sort (TPMMS) por hire_date...")
            stats = external_sort(HEAP_FILE, OUTPUT_FILE, PAGE_SIZE, BUFFER_SIZE, 5)

            print("\n--- Métricas Finales ---")
            for key, value in stats.items():
                print(f"{key}: {value}")