import time
import os
import sys
from external_hashing import external_hash_group_by

PAGE_SIZE = 4096
# Asegúrate de que el archivo esté en la misma carpeta o usa la ruta correcta
HEAP_FILE = 'department_employees.bin' 
buffers = [64, 128, 256]

print(f"{'BUFFER_SIZE':<12} | {'B (págs)':<8} | {'Partic.':<7} | {'T. Fase 1':<10} | {'T. Fase 2':<10} | {'T. Total':<10} | {'I/O Total'}")
print("-" * 87)

for kb in buffers:
    buf_size = kb * 1024
    
    # Bloqueamos los print internos de la función para no ensuciar la tabla
    sys.stdout = open(os.devnull, 'w')
    try:
        stats = external_hash_group_by(HEAP_FILE, PAGE_SIZE, buf_size, 2)
    finally:
        sys.stdout = sys.__stdout__ # Restauramos la consola
    
    b_pages = buf_size // PAGE_SIZE
    io_total = stats['pages_read'] + stats['pages_written']
    
    print(f"{kb:>3} KB      | {b_pages:>8} | {stats['partitions_created']:>7} | "
        f"{stats['time_phase1_sec']:>10.4f} | {stats['time_phase2_sec']:>10.4f} | "
        f"{stats['time_total_sec']:>10.4f} | {io_total:>9}")