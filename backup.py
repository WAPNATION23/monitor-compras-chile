"""
backup.py — Backup y Recuperación de la Base de Datos
═════════════════════════════════════════════════════

Uso:
    python backup.py                          # Crea backup con timestamp
    python backup.py --restore backup_xyz.db  # Restaura desde un backup
    python backup.py --list                   # Lista backups existentes

Los backups se guardan en ./backups/ con formato:
    auditoria_estado_YYYYMMDD_HHMMSS.db
"""

from __future__ import annotations

import argparse
import shutil
import sqlite3
import sys
from datetime import datetime
from pathlib import Path

from config import DB_NAME

BACKUP_DIR = Path("backups")


def create_backup(db_path: str = DB_NAME) -> Path:
    """Crea un backup consistente de la BD usando la API de backup de SQLite."""
    src = Path(db_path)
    if not src.exists():
        print(f"❌ Base de datos no encontrada: {src}")
        sys.exit(1)

    BACKUP_DIR.mkdir(exist_ok=True)
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    dest = BACKUP_DIR / f"{src.stem}_{ts}{src.suffix}"

    # Usar sqlite3 backup API para consistencia (no copia archivos en uso)
    src_conn = sqlite3.connect(src)
    dst_conn = sqlite3.connect(dest)
    try:
        src_conn.backup(dst_conn)
    finally:
        dst_conn.close()
        src_conn.close()

    size_mb = dest.stat().st_size / (1024 * 1024)
    print(f"✅ Backup creado: {dest} ({size_mb:.1f} MB)")
    return dest


def restore_backup(backup_path: str, db_path: str = DB_NAME) -> None:
    """Restaura la BD desde un archivo de backup."""
    src = Path(backup_path)
    if not src.exists():
        print(f"❌ Archivo de backup no encontrado: {src}")
        sys.exit(1)

    dest = Path(db_path)

    # Validar que el backup es una BD SQLite válida
    try:
        conn = sqlite3.connect(src)
        conn.execute("SELECT count(*) FROM sqlite_master")
        conn.close()
    except sqlite3.DatabaseError:
        print(f"❌ El archivo no es una base de datos SQLite válida: {src}")
        sys.exit(1)

    # Crear backup del estado actual antes de restaurar
    if dest.exists():
        safety = BACKUP_DIR / f"{dest.stem}_pre_restore_{datetime.now().strftime('%Y%m%d_%H%M%S')}{dest.suffix}"
        BACKUP_DIR.mkdir(exist_ok=True)
        shutil.copy2(dest, safety)
        print(f"🔒 Backup de seguridad del estado actual: {safety}")

    shutil.copy2(src, dest)
    print(f"✅ Base de datos restaurada desde: {src}")


def list_backups() -> list[Path]:
    """Lista los backups existentes ordenados por fecha."""
    if not BACKUP_DIR.exists():
        print("No hay backups aún. Ejecuta: python backup.py")
        return []

    backups = sorted(BACKUP_DIR.glob("*.db"), key=lambda p: p.stat().st_mtime, reverse=True)
    if not backups:
        print("No hay backups aún.")
        return []

    print(f"{'Archivo':<50} {'Tamaño':>10} {'Fecha':>20}")
    print("─" * 80)
    for b in backups:
        size = b.stat().st_size / (1024 * 1024)
        mtime = datetime.fromtimestamp(b.stat().st_mtime).strftime("%Y-%m-%d %H:%M:%S")
        print(f"{b.name:<50} {size:>8.1f} MB {mtime:>20}")

    return backups


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Backup y recuperación de la BD.")
    parser.add_argument("--restore", type=str, help="Ruta al backup para restaurar.")
    parser.add_argument("--list", action="store_true", help="Listar backups existentes.")
    args = parser.parse_args()

    if args.list:
        list_backups()
    elif args.restore:
        restore_backup(args.restore)
    else:
        create_backup()
