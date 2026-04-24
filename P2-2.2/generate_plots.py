import matplotlib.pyplot as plt
import os
import time
# Importamos la función de tu archivo corregido
from external_sort import external_sort

def run_performance_analysis():
    # --- Configuración de Parámetros ---
    PAGE_SIZE = 4096
    HEAP_FILE = 'employee.bin'
    OUTPUT_FILE = 'sorted_employee.bin'
    SORT_KEY_IDX = 5  # hire_date
    
    # Tamaños de buffer solicitados
    buffer_configs = {
        "64 KB": 64 * 1024,
        "128 KB": 128 * 1024,
        "256 KB": 256 * 1024
    }
    
    labels = []
    total_times = []
    io_totals = []

    print(f"{'Buffer':<10} | {'Tiempo Total (s)':<18} | {'I/O Total (Pág)':<15}")
    print("-" * 50)

    for label, size in buffer_configs.items():
      
        stats = external_sort(HEAP_FILE, OUTPUT_FILE, PAGE_SIZE, size, SORT_KEY_IDX)
        
        labels.append(label)
        total_times.append(stats['time_total_sec'])
        io_totals.append(stats['pages_read'] + stats['pages_written'])
        
        print(f"{label:<10} | {stats['time_total_sec']:<18.4f} | {io_totals[-1]:<15}")

    # --- Generación de la Gráfica ---
    plt.figure(figsize=(10, 6))
    
    plt.plot(labels, total_times, marker='o', linestyle='-', color='b', linewidth=2, label='Tiempo Total (s)')

    for i, txt in enumerate(total_times):
        plt.annotate(f"{txt:.2f}s", (labels[i], total_times[i]), textcoords="offset points", xytext=(0,10), ha='center')

    plt.title('Rendimiento de External Sort vs. BUFFER_SIZE', fontsize=14)
    plt.xlabel('Tamaño de Buffer (RAM)', fontsize=12)
    plt.ylabel('Segundos', fontsize=12)
    plt.grid(True, linestyle='--', alpha=0.7)
    plt.legend()

    
    plt.savefig('grafica_external_sort.png')
    print("\nGráfica guardada como 'grafica_external_sort.png'")

if __name__ == '__main__':

    if os.path.exists('employee.bin'):
        run_performance_analysis()
    else:
        print("Error: No se encuentra 'employee.bin'. Ejecuta primero external_sort.py para generarlo.")