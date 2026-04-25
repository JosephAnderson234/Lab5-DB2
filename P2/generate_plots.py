import matplotlib.pyplot as plt
import os
import time
# Importamos la función de tu archivo corregido
from external_sort import external_sort

def run_performance_analysis():
    # --- Configuración de Parámetros ---
    PAGE_SIZE = 4096
    HEAP_FILE = 'employees.bin'
    OUTPUT_FILE = 'sorted_employees.bin'
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
    fig, ax1 = plt.subplots(figsize=(10, 6))
    ax2 = ax1.twinx()

    ax1.plot(labels, total_times, marker='o', color='b', linewidth=2, label='Tiempo Total (s)')
    ax2.plot(labels, io_totals, marker='s', color='r', linestyle='--', linewidth=2, label='I/O (páginas)')

    for i, txt in enumerate(total_times):
        ax1.annotate(f"{txt:.2f}s", (labels[i], total_times[i]), textcoords="offset points", xytext=(0,10), ha='center', color='b')
    for i, txt in enumerate(io_totals):
        ax2.annotate(f"{txt}", (labels[i], io_totals[i]), textcoords="offset points", xytext=(0,-15), ha='center', color='r')

    ax1.set_title('Rendimiento de External Sort vs. BUFFER_SIZE', fontsize=14)
    ax1.set_xlabel('Tamaño de Buffer (RAM)', fontsize=12)
    ax1.set_ylabel('Tiempo (s)', color='b', fontsize=12)
    ax2.set_ylabel('I/O Total (páginas)', color='r', fontsize=12)

    lines1, labels1 = ax1.get_legend_handles_labels()
    lines2, labels2 = ax2.get_legend_handles_labels()
    ax1.legend(lines1 + lines2, labels1 + labels2, loc='upper right')

    ax1.grid(True, linestyle='--', alpha=0.7)
    plt.tight_layout()
    plt.savefig('grafica_external_sort.png')
    print("\nGráfica guardada como 'grafica_external_sort.png'")

if __name__ == '__main__':

    if os.path.exists('employees.bin'):
        run_performance_analysis()
    else:
        print("Error: No se encuentra 'employees.bin'. Ejecuta primero external_sort.py para generarlo.")