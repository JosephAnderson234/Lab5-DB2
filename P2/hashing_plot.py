import matplotlib.pyplot as plt
import os
import math
from external_hashing import external_hash_group_by
from heap_file import export_to_heap, DEPARTMENT_EMPLOYEE_FORMAT, PAGE_SIZE, EMPLOYEE_FORMAT

def run_hashing_performance_analysis():
    HEAP_FILE = 'department_employees.bin'
    GROUP_KEY_IDX = 2  # from_date

    buffer_configs = {
        "64 KB":  64  * 1024,
        "128 KB": 128 * 1024,
        "256 KB": 256 * 1024
    }

    labels         = []
    total_times    = []
    io_totals      = []
    phase1_times   = []
    phase2_times   = []
    partitions_list = []

    print(f"{'Buffer':<10} | {'B (págs)':<10} | {'Particiones':<13} | "
        f"{'T.Fase1 (s)':<13} | {'T.Fase2 (s)':<13} | "
        f"{'T.Total (s)':<13} | {'I/O (págs)':<10}")
    print("-" * 90)

    for label, size in buffer_configs.items():
        B = size // PAGE_SIZE
        stats = external_hash_group_by(HEAP_FILE, PAGE_SIZE, size, GROUP_KEY_IDX)

        io = stats['pages_read'] + stats['pages_written']

        labels.append(label)
        total_times.append(stats['time_total_sec'])
        io_totals.append(io)
        phase1_times.append(stats['time_phase1_sec'])
        phase2_times.append(stats['time_phase2_sec'])
        partitions_list.append(stats['partitions_created'])

        print(f"{label:<10} | {B:<10} | {stats['partitions_created']:<13} | "
            f"{stats['time_phase1_sec']:<13.4f} | {stats['time_phase2_sec']:<13.4f} | "
            f"{stats['time_total_sec']:<13.4f} | {io:<10}")

    # --- Gráfica: Tiempo Total + I/O (doble eje Y) ---
    fig, ax1 = plt.subplots(figsize=(10, 6))
    ax2 = ax1.twinx()

    ax1.plot(labels, total_times, marker='o', color='b', linewidth=2, label='Tiempo Total (s)')
    ax1.plot(labels, phase1_times, marker='^', color='cyan', linewidth=2, linestyle='--', label='Tiempo Fase 1 (s)')
    ax1.plot(labels, phase2_times, marker='v', color='navy', linewidth=2, linestyle='--', label='Tiempo Fase 2 (s)')
    ax2.plot(labels, io_totals, marker='s', color='r', linewidth=2, linestyle=':', label='I/O Total (páginas)')

    # Anotaciones tiempo total
    for i, txt in enumerate(total_times):
        ax1.annotate(f"{txt:.3f}s", (labels[i], total_times[i]),
                    textcoords="offset points", xytext=(0, 10), ha='center', color='b', fontsize=9)

    # Anotaciones I/O
    for i, txt in enumerate(io_totals):
        ax2.annotate(f"{txt}", (labels[i], io_totals[i]),
                    textcoords="offset points", xytext=(0, -15), ha='center', color='r', fontsize=9)

    # Anotaciones particiones sobre eje x
    for i, p in enumerate(partitions_list):
        ax1.annotate(f"{p} part.", (labels[i], phase1_times[i]),
                    textcoords="offset points", xytext=(0, 10), ha='center', color='gray', fontsize=8)

    ax1.set_title('Rendimiento de External Hashing vs. BUFFER_SIZE', fontsize=14)
    ax1.set_xlabel('Tamaño de Buffer (RAM)', fontsize=12)
    ax1.set_ylabel('Tiempo (s)', color='b', fontsize=12)
    ax2.set_ylabel('I/O Total (páginas)', color='r', fontsize=12)

    lines1, labels1 = ax1.get_legend_handles_labels()
    lines2, labels2 = ax2.get_legend_handles_labels()
    ax1.legend(lines1 + lines2, labels1 + labels2, loc='upper right')

    ax1.grid(True, linestyle='--', alpha=0.7)
    plt.tight_layout()
    plt.savefig('grafica_external_hashing.png')
    print("\nGráfica guardada como 'grafica_external_hashing.png'")


if __name__ == '__main__':
    # Siempre regenerar para asegurar que existe
    print("Generando heap file...")
    export_to_heap('department_employees.csv', 'department_employees.bin',
                DEPARTMENT_EMPLOYEE_FORMAT, PAGE_SIZE)
    print("Heap file generado.\n")

    run_hashing_performance_analysis()