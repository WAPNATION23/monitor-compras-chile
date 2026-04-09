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
import logging
import shutil
import sqlite3
import sys
from datetime import datetime
from pathlib import Path

from config import DB_NAME

logger = logging.getLogger(__name__)

BACKUP_DIR = Path("backups")


def create_backup(db_path: str = DB_NAME) -> Path:
    """Crea un backup consistente de la BD usando la API de backup de SQLite."""
    src = Path(db_path)
    if not src.exists():
        logger.error("Base de datos no encontrada: %s", src)
        sys.exit(1)

    BACKUP_DIR.mkdir(exist_ok=True)
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    dest = BACKUP_DIR / f"{src.stem}_{ts}{src.suffix}"

    # Usar sqlite3 backup API para consistencia (no copia archivos en uso)
    with sqlite3.connect(src) as src_conn, sqlite3.connect(dest) as dst_conn:
        src_conn.backup(dst_conn)

    size_mb = dest.stat().st_size / (1024 * 1024)
    logger.info("Backup creado: %s (%.1f MB)", dest, size_mb)
    return dest


def restore_backup(backup_path: str, db_path: str = DB_NAME) -> None:
    """Restaura la BD desde un archivo de backup."""
    src = Path(backup_path)
    if not src.exists():
        logger.error("Archivo de backup no encontrado: %s", src)
        sys.exit(1)

    dest = Path(db_path)

    # Validar que el backup es una BD SQLite válida
    try:
        with sqlite3.connect(src) as conn:
            conn.execute("SELECT count(*) FROM sqlite_master")
    except sqlite3.DatabaseError:
        logger.error("El archivo no es una base de datos SQLite válida: %s", src)
        sys.exit(1)

    # Crear backup del estado actual antes de restaurar
    if dest.exists():
        safety = BACKUP_DIR / f"{dest.stem}_pre_restore_{datetime.now().strftime('%Y%m%d_%H%M%S')}{dest.suffix}"
        BACKUP_DIR.mkdir(exist_ok=True)
        shutil.copy2(dest, safety)
        logger.info("Backup de seguridad del estado actual: %s", safety)

    shutil.copy2(src, dest)
    logger.info("Base de datos restaurada desde: %s", src)


def list_backups() -> list[Path]:
    """Lista los backups existentes ordenados por fecha."""
    if not BACKUP_DIR.exists():
        logger.info("No hay backups aún. Ejecuta: python backup.py")
        return []

    backups = sorted(BACKUP_DIR.glob("*.db"), key=lambda p: p.stat().st_mtime, reverse=True)
    if not backups:
        logger.info("No hay backups aún.")
        return []

    logger.info("%-50s %10s %20s", "Archivo", "Tamaño", "Fecha")
    for b in backups:
        size = b.stat().st_size / (1024 * 1024)
        mtime = datetime.fromtimestamp(b.stat().st_mtime).strftime("%Y-%m-%d %H:%M:%S")
        logger.info("%-50s %8.1f MB %20s", b.name, size, mtime)

    return backups


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(message)s")
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
