import time
from external_sort import external_sort

PAGE_SIZE = 4096
HEAP_FILE = 'employees.bin'
OUTPUT_FILE = 'sorted_employees.bin'
buffers = [64, 128, 256]

print(f"{'BUFFER_SIZE':<12} | {'B (págs)':<8} | {'Runs':<6} | {'T. Fase 1':<10} | {'T. Fase 2':<10} | {'T. Total':<10} | {'I/O Total'}")
print("-" * 85)

for kb in buffers:
    buf_size = kb * 1024
    stats = external_sort(HEAP_FILE, OUTPUT_FILE, PAGE_SIZE, buf_size, 5) # 5 es hire_date
    
    b_pages = buf_size // PAGE_SIZE
    io_total = stats['pages_read'] + stats['pages_written']
    
    print(f"{kb:>3} KB      | {b_pages:>8} | {stats['runs_generated']:>6} | "
        f"{stats['time_phase1_sec']:>10.4f} | {stats['time_phase2_sec']:>10.4f} | "
        f"{stats['time_total_sec']:>10.4f} | {io_total:>9}")