import os
import math
import heapq
import time
import struct
import csv
from heap_file import read_page, write_page, count_pages, export_to_heap

RECORD_FORMAT = '=i10s20s20s1s10s' 
RECORD_SIZE = struct.calcsize(RECORD_FORMAT)

def generate_runs(heap_path, page_size, buffer_size, sort_key_idx):
    """Fase 1: Generación de runs ordenados"""
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
        run_paths.append(run_path)
        
        num_records_per_page = page_size // RECORD_SIZE
        # Escribir runs como archivos temporales 
        for p_id in range(math.ceil(len(temp_buffer) / num_records_per_page)):
            start = p_id * num_records_per_page
            end = start + num_records_per_page
            write_page(run_path, p_id, temp_buffer[start:end], RECORD_FORMAT, page_size)
            pages_written += 1
            
    return run_paths, pages_read, pages_written
def multiway_merge(run_paths, output_path, page_size, buffer_size, sort_key_idx):
    """Fase 2: Multiway Merge usando Min-Heap robusto contra errores de unpack [cite: 86]"""
    # Abrir todos los runs generados en la Fase 1
    run_files = [open(path, 'rb') for path in run_paths]
    output_f = open(output_path, 'wb')
    
    min_heap = []
    # Cargar el primer registro válido de cada run al heap 
    for i, f in enumerate(run_files):
        data = f.read(RECORD_SIZE)
        # Verificamos que se lean exactamente los bytes del registro 
        if data and len(data) == RECORD_SIZE:
            try:
                record = struct.unpack(RECORD_FORMAT, data)
                if record[0] != 0:
                    heapq.heappush(min_heap, (record[sort_key_idx], record, i))
            except struct.error:
                continue 

    output_buffer = []
    num_records_per_page = page_size // RECORD_SIZE
    pages_read = len(run_paths) 
    pages_written = 0

    while min_heap:
        val, record, run_idx = heapq.heappop(min_heap)
        output_buffer.append(record)
        # Cuando el buffer de salida se llena, escribirlo a disco
        if len(output_buffer) == num_records_per_page:
            for r in output_buffer:
                output_f.write(struct.pack(RECORD_FORMAT, *r))
            pages_written += 1
            output_buffer = []

        # Leer el siguiente registro del mismo run 
        data = run_files[run_idx].read(RECORD_SIZE)
        if data and len(data) == RECORD_SIZE:
            try:
                next_record = struct.unpack(RECORD_FORMAT, data)
                # Validar que no sea un registro vacío
                if next_record[0] != 0:
                    heapq.heappush(min_heap, (next_record[sort_key_idx], next_record, run_idx))
            except struct.error:
                pass # Fin del contenido útil en este run

    # Escribir registros restantes y completar la página con padding 
    if output_buffer:
        for r in output_buffer:
            output_f.write(struct.pack(RECORD_FORMAT, *r))
        padding = page_size - (len(output_buffer) * RECORD_SIZE)
        output_f.write(b'\0' * padding)
        pages_written += 1

    for f in run_files: f.close()
    output_f.close()
    for path in run_paths:
        if os.path.exists(path): os.remove(path)
    
    return pages_read, pages_written

def external_sort(heap_path, output_path, page_size, buffer_size, sort_key_idx):
    """Ejecuta TPMMS y retorna métricas de rendimiento"""
    start_total = time.time()
    
    # Ejecución Fase 1
    start_p1 = time.time()
    run_paths, r1, w1 = generate_runs(heap_path, page_size, buffer_size, sort_key_idx)
    end_p1 = time.time()
    
    # Ejecución Fase 2
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
    
    CSV_FILE = 'employee.csv' 
    HEAP_FILE = 'employee.bin'
    OUTPUT_FILE = 'sorted_employee.bin'

    if not os.path.exists(CSV_FILE):
        print(f"Error: No se encuentra {CSV_FILE}. Verifica el nombre del archivo.")
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