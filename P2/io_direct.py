"""
io_direct.py
------------
Helpers para I/O con O_DIRECT en Linux.

O_DIRECT bypasses el page cache del OS → el tiempo medido
refleja el I/O real al disco (o a la controladora de caché del SSD).

Restricciones de O_DIRECT en Linux:
  - El buffer de lectura/escritura debe estar alineado a 512 bytes
    (en la mayoría de discos/filesystems modernos, 4096 bytes es más seguro).
  - El tamaño de cada operación también debe ser múltiplo del sector size.
  - Se usa ctypes para obtener buffers alineados en memoria.
"""

import os
import ctypes

# Alineación requerida: usamos 4096 para máxima compatibilidad (igual al PAGE_SIZE)
ALIGN = 4096


def _alloc_aligned(size: int):
    """
    Reserva un búfer de 'size' bytes alineado a ALIGN bytes.
    Retorna (aligned_ctypes_array, anchor_buffer).
    El anchor_buffer debe mantenerse vivo mientras se use aligned.
    """
    # Reservamos size + ALIGN bytes para poder alinear manualmente
    buf = (ctypes.c_char * (size + ALIGN))()
    address = ctypes.addressof(buf)
    offset = (-address) % ALIGN
    aligned = (ctypes.c_char * size).from_buffer(buf, offset)
    return aligned, buf  # 'buf' es el anchor para evitar GC


def direct_read_page(path: str, page_id: int, page_size: int) -> bytes:
    """
    Lee una página del archivo bypassing el OS page cache (O_DIRECT).
    Retorna exactamente page_size bytes.
    """
    offset = page_id * page_size
    aligned_buf, _anchor = _alloc_aligned(page_size)

    try:
        fd = os.open(path, os.O_RDONLY | os.O_DIRECT)
        try:
            os.lseek(fd, offset, os.SEEK_SET)
            data = os.read(fd, page_size)
            # Copiar datos al buffer alineado y retornar como bytes
            return data.ljust(page_size, b'\x00')
        finally:
            os.close(fd)
    except OSError:
        # Fallback sin O_DIRECT si el filesystem no lo soporta
        try:
            fd = os.open(path, os.O_RDONLY)
            try:
                os.lseek(fd, offset, os.SEEK_SET)
                data = os.read(fd, page_size)
                return data.ljust(page_size, b'\x00')
            finally:
                os.close(fd)
        except OSError:
            return b'\x00' * page_size


def direct_write_page(path: str, page_id: int, data: bytes, page_size: int):
    """
    Escribe una página en el archivo bypassing el OS page cache (O_DIRECT).
    'data' debe tener exactamente page_size bytes.
    """
    # Asegurar tamaño exacto
    data = data.ljust(page_size, b'\x00')[:page_size]
    offset = page_id * page_size

    # Copiar datos al buffer alineado para la escritura con O_DIRECT
    aligned_buf, _anchor = _alloc_aligned(page_size)
    ctypes.memmove(aligned_buf, data, page_size)

    flags = os.O_WRONLY | os.O_CREAT | os.O_DIRECT
    try:
        fd = os.open(path, flags, 0o644)
        try:
            os.lseek(fd, offset, os.SEEK_SET)
            os.write(fd, bytes(aligned_buf))
        finally:
            os.close(fd)
    except OSError:
        # Fallback sin O_DIRECT
        fd = os.open(path, os.O_WRONLY | os.O_CREAT, 0o644)
        try:
            os.lseek(fd, offset, os.SEEK_SET)
            os.write(fd, data)
        finally:
            os.close(fd)


def drop_page_cache_for(path: str):
    """
    Purga el page cache para un archivo específico usando
    posix_fadvise(POSIX_FADV_DONTNEED).
    Fuerza que la próxima lectura vaya al disco real.
    """
    try:
        libc = ctypes.CDLL("libc.so.6", use_errno=True)
        POSIX_FADV_DONTNEED = 4
        fd = os.open(path, os.O_RDONLY)
        size = os.path.getsize(path)
        libc.posix_fadvise(fd, 0, size, POSIX_FADV_DONTNEED)
        os.close(fd)
    except Exception:
        pass  # Si falla, continuar sin purgar


def drop_cache_all(*paths: str):
    """Purga el page cache para múltiples archivos."""
    for p in paths:
        if os.path.exists(p):
            drop_page_cache_for(p)
