import os
import matplotlib.pyplot as plt
from external_sort import external_sort

def main():
    # --- Configuración ---
    PAGE_SIZE = 4096
    HEAP_FILE = 'employees.bin'
    OUTPUT_FILE = 'sorted_employees.bin'
    SORT_KEY_IDX = 5  # hire_date
    buffers_kb = [64, 128, 256]
    
    labels = []
    total_times = []
    results = []

    # --- Procesamiento ---
    if not os.path.exists(HEAP_FILE):
        print(f"Error: No se encuentra {HEAP_FILE}")
        return

    for kb in buffers_kb:
        size_bytes = kb * 1024
        stats = external_sort(HEAP_FILE, OUTPUT_FILE, PAGE_SIZE, size_bytes, SORT_KEY_IDX)
        
        b_pages = size_bytes // PAGE_SIZE
        io_total = stats['pages_read'] + stats['pages_written']
        
        results.append({
            'kb': kb, 'b': b_pages, 'runs': stats['runs_generated'],
            't1': stats['time_phase1_sec'], 't2': stats['time_phase2_sec'],
            'total': stats['time_total_sec'], 'io': io_total
        })
        
        labels.append(f"{kb} KB")
        total_times.append(stats['time_total_sec'])

    # --- Imprimir Tabla ---
    print("\nMETRICAS DE EXTERNAL SORTING")
    print(f"{'BUFFER':<10} | {'B (págs)':<8} | {'Runs':<6} | {'T. Fase 1':<10} | {'T. Fase 2':<10} | {'T. Total':<10} | {'I/O Total'}")
    print("-" * 85)
    for r in results:
        print(f"{r['kb']:>3} KB      | {r['b']:>8} | {r['runs']:>6} | {r['t1']:>9.4f}s | {r['t2']:>9.4f}s | {r['total']:>9.4f}s | {r['io']:>9}")

    # --- Generar Gráfica ---
    plt.figure(figsize=(8, 5))
    plt.plot(labels, total_times, marker='o', color='blue', linewidth=2)
    for i, v in enumerate(total_times):
        plt.text(i, v + (max(total_times)*0.02), f"{v:.4f}s", ha='center', color='blue')
    
    plt.title('External Sort: Tiempo Total vs. BUFFER_SIZE')
    plt.xlabel('Tamaño de Buffer')
    plt.ylabel('Tiempo Total (s)')
    plt.grid(True, linestyle='--', alpha=0.7)
    plt.tight_layout()
    plt.savefig('grafica_external_sort.png')
    print("\nGráfica guardada como 'grafica_external_sort.png'")

if __name__ == '__main__':
    main()