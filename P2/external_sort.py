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
    B = buffer_size // page_size
    num_records_per_page = page_size // RECORD_SIZE
    
    # Buffer de página por run: {run_idx: [records], cursor}
    run_buffers = {}
    run_page_ids = {}
    pages_read = 0
    pages_written = 0
    
    for i, path in enumerate(run_paths):
        run_page_ids[i] = 0
        run_buffers[i] = read_page(path, 0, page_size, RECORD_FORMAT)
        if run_buffers[i]:
            pages_read += 1 

    min_heap = []
    for i in range(len(run_paths)):
        if run_buffers[i]:
            record = run_buffers[i].pop(0)
            heapq.heappush(min_heap, (record[sort_key_idx], record, i))
    
    output_buffer = []
    output_f = open(output_path, 'wb')
    
    while min_heap:
        val, record, run_idx = heapq.heappop(min_heap)
        output_buffer.append(record)
        
        if len(output_buffer) == num_records_per_page:
            for r in output_buffer:
                output_f.write(struct.pack(RECORD_FORMAT, *r))
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
    
    if output_buffer:
        for r in output_buffer:
            output_f.write(struct.pack(RECORD_FORMAT, *r))
        padding = page_size - (len(output_buffer) * RECORD_SIZE)
        output_f.write(b'\x00' * padding)
        pages_written += 1
    
    output_f.close()
    for path in run_paths:
        if os.path.exists(path):
            os.remove(path)
    
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
    
    CSV_FILE = 'employees.csv' 
    HEAP_FILE = 'employees.bin'
    OUTPUT_FILE = 'sorted_employees.bin'

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