"""
GeneradorExpediente — Generación automática de expedientes de auditoría.
═══════════════════════════════════════════════════════════════════════
Orquesta TODOS los extractores y el CrossReferencer para generar un
expediente público completo dado un nombre o RUT.

Uso CLI:
    py generar_expediente.py "Camila Flores"
    py generar_expediente.py --rut 76123456-7
    py generar_expediente.py "Flores" --output expediente_flores.md

Uso como módulo:
    from generar_expediente import GeneradorExpediente
    gen = GeneradorExpediente()
    expediente = gen.investigar("Camila Flores")
    gen.exportar_markdown(expediente, "expediente_flores.md")
"""

from __future__ import annotations

import argparse
import logging
import sqlite3
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path

import pandas as pd

from config import DB_NAME
from cross_referencer import CrossReferencer
from datos_gob_connector import DatosGobConnector
from infolobby_connector import InfoLobbyConnector

logger = logging.getLogger(__name__)

# ─────────────────── Modelo de datos ─────────────────── #


@dataclass
class Expediente:
    """Resultado consolidado de una investigación."""

    nombre_investigado: str
    rut: str = ""
    cargo: str = ""
    fecha_generacion: str = field(
        default_factory=lambda: datetime.now().strftime("%Y-%m-%d %H:%M")
    )

    # Datos crudos por fuente
    aportes_servel: pd.DataFrame = field(default_factory=pd.DataFrame)
    audiencias_lobby: pd.DataFrame = field(default_factory=pd.DataFrame)
    donativos_lobby: pd.DataFrame = field(default_factory=pd.DataFrame)
    viajes_lobby: pd.DataFrame = field(default_factory=pd.DataFrame)
    lobby_catalogos: dict = field(default_factory=dict)
    cruce_servel_compras: pd.DataFrame = field(default_factory=pd.DataFrame)
    datasets_gob: list = field(default_factory=list)

    # Resumen
    banderas_rojas: list[str] = field(default_factory=list)
    nivel_alerta: str = "🟢 Sin hallazgos"
    fuentes_consultadas: list[dict] = field(default_factory=list)


# ─────────────────── Motor de investigación ─────────────────── #


class GeneradorExpediente:
    """Orquesta todas las fuentes para generar un expediente automático."""

    def __init__(self, db_path: str | Path = DB_NAME) -> None:
        self.db_path = Path(db_path)
        self.lobby = InfoLobbyConnector()
        self.xref = CrossReferencer(db_path)
        self.datos_gob = DatosGobConnector()

    def investigar(self, nombre: str, rut: str = "") -> Expediente:
        """
        Ejecuta la investigación completa para un sujeto.

        Args:
            nombre: Nombre completo o parcial del sujeto.
            rut: RUT opcional para búsqueda exacta.

        Returns:
            Expediente con todos los hallazgos.
        """
        exp = Expediente(nombre_investigado=nombre, rut=rut)
        print(f"\n{'═' * 60}")
        print("  OJO DEL PUEBLO — INVESTIGACIÓN AUTOMÁTICA")
        print(f"  Sujeto: {nombre}" + (f" (RUT: {rut})" if rut else ""))
        print(f"  Fecha: {exp.fecha_generacion}")
        print(f"{'═' * 60}\n")

        self._buscar_servel(exp)
        self._buscar_lobby_sparql(exp)
        self._buscar_lobby_catalogos(exp)
        self._buscar_datos_gob(exp)
        self._ejecutar_cruces(exp)
        self._evaluar_banderas(exp)

        self._imprimir_resumen(exp)
        return exp

    # ─────────────────── SERVEL ─────────────────── #

    def _buscar_servel(self, exp: Expediente) -> None:
        """Busca aportes de campaña en la DB local."""
        print("[1/6] 🗳️  SERVEL — Aportes de campaña...")
        exp.fuentes_consultadas.append({
            "fuente": "SERVEL (DB local)",
            "url": "servel.cl",
            "estado": "pendiente",
        })

        try:
            with sqlite3.connect(self.db_path) as conn:
                check = pd.read_sql_query(
                    "SELECT name FROM sqlite_master "
                    "WHERE type='table' AND name='aportes_servel'",
                    conn,
                )
                if check.empty:
                    print("  ⚠️  Tabla aportes_servel no existe.")
                    exp.fuentes_consultadas[-1]["estado"] = "tabla no existe"
                    return

                df = pd.read_sql_query("SELECT * FROM aportes_servel", conn)

            # Buscar por nombre o RUT
            mask = df.apply(
                lambda row: row.astype(str).str.contains(
                    exp.nombre_investigado, case=False, na=False
                ).any(),
                axis=1,
            )
            if exp.rut:
                rut_clean = exp.rut.replace("-", "").replace(".", "")
                mask_rut = df.apply(
                    lambda row: row.astype(str).str.replace(
                        "-", "", regex=False
                    ).str.contains(rut_clean, case=False, na=False).any(),
                    axis=1,
                )
                mask = mask | mask_rut

            exp.aportes_servel = df[mask]
            n = len(exp.aportes_servel)
            estado = f"✅ {n} aportes encontrados" if n else "Sin coincidencias"
            exp.fuentes_consultadas[-1]["estado"] = estado
            print(f"  {estado}")

            if n:
                total = exp.aportes_servel["monto_aporte"].sum()
                print(f"  💰 Monto total: ${total:,.0f} CLP")

        except (sqlite3.Error, pd.errors.DatabaseError) as exc:
            logger.error("Error consultando SERVEL: %s", exc)
            exp.fuentes_consultadas[-1]["estado"] = f"Error: {exc}"
            print(f"  ❌ Error: {exc}")

    # ─────────────────── InfoLobby SPARQL ─────────────────── #

    def _buscar_lobby_sparql(self, exp: Expediente) -> None:
        """Busca audiencias y donativos vía SPARQL."""
        print("[2/6] 🤝 InfoLobby — Audiencias (SPARQL)...")
        exp.fuentes_consultadas.append({
            "fuente": "InfoLobby (SPARQL)",
            "url": "datos.infolobby.cl",
            "estado": "pendiente",
        })

        exp.audiencias_lobby = self.lobby.buscar_por_autoridad(
            exp.nombre_investigado
        )
        if not exp.audiencias_lobby.empty:
            n = len(exp.audiencias_lobby)
            exp.fuentes_consultadas[-1]["estado"] = f"✅ {n} audiencias"
            print(f"  ✅ {n} audiencias encontradas")
        else:
            exp.fuentes_consultadas[-1]["estado"] = "Sin resultados / API no disponible"
            print("  ⚠️  Sin resultados (SPARQL puede estar bloqueado)")

    # ─────────────────── InfoLobby CSV Fallback ─────────────────── #

    def _buscar_lobby_catalogos(self, exp: Expediente) -> None:
        """Busca en catálogos CSV de InfoLobby (fallback)."""
        print("[3/6] 📋 InfoLobby — Catálogos CSV (fallback)...")
        exp.fuentes_consultadas.append({
            "fuente": "InfoLobby (CSV)",
            "url": "infolobby.cl/Datos",
            "estado": "pendiente",
        })

        exp.lobby_catalogos = self.lobby.buscar_en_catalogos(
            exp.nombre_investigado
        )
        total = sum(len(df) for df in exp.lobby_catalogos.values())
        if total:
            cats = ", ".join(
                f"{k}: {len(v)}" for k, v in exp.lobby_catalogos.items()
            )
            exp.fuentes_consultadas[-1]["estado"] = f"✅ {total} registros ({cats})"
            print(f"  ✅ {total} registros encontrados")
            for cat, df in exp.lobby_catalogos.items():
                print(f"     📂 {cat}: {len(df)} coincidencias")
        else:
            exp.fuentes_consultadas[-1]["estado"] = "Sin coincidencias en catálogos"
            print("  ⚠️  Sin coincidencias en catálogos CSV")

    # ─────────────────── datos.gob.cl ─────────────────── #

    def _buscar_datos_gob(self, exp: Expediente) -> None:
        """Busca datasets relacionados en datos.gob.cl."""
        print("[4/6] 🏛️  datos.gob.cl — Datasets relacionados...")
        exp.fuentes_consultadas.append({
            "fuente": "datos.gob.cl (CKAN)",
            "url": "datos.gob.cl",
            "estado": "pendiente",
        })

        try:
            results = self.datos_gob.search_datasets(
                exp.nombre_investigado, rows=5
            )
            exp.datasets_gob = results
            if results:
                exp.fuentes_consultadas[-1]["estado"] = (
                    f"✅ {len(results)} datasets"
                )
                print(f"  ✅ {len(results)} datasets encontrados")
                for ds in results[:3]:
                    title = ds.get("title", "Sin título")[:60]
                    print(f"     📊 {title}")
            else:
                exp.fuentes_consultadas[-1]["estado"] = "Sin datasets"
                print("  ⚠️  Sin datasets relacionados")

        except Exception as exc:
            logger.error("Error consultando datos.gob.cl: %s", exc)
            exp.fuentes_consultadas[-1]["estado"] = f"Error: {exc}"
            print(f"  ❌ Error: {exc}")

    # ─────────────────── Cruces forenses ─────────────────── #

    def _ejecutar_cruces(self, exp: Expediente) -> None:
        """Ejecuta cruces del CrossReferencer."""
        print("[5/6] 🔍 Cruces forenses — SERVEL ↔ Compras públicas...")
        exp.fuentes_consultadas.append({
            "fuente": "CrossReferencer (cruces)",
            "url": "local",
            "estado": "pendiente",
        })

        try:
            df_cruce = self.xref.cruce_servel_compras()
            if not df_cruce.empty:
                # Filtrar por el nombre investigado
                mask = df_cruce.apply(
                    lambda row: row.astype(str).str.contains(
                        exp.nombre_investigado, case=False, na=False
                    ).any(),
                    axis=1,
                )
                exp.cruce_servel_compras = df_cruce[mask]
                n = len(exp.cruce_servel_compras)
                if n:
                    exp.fuentes_consultadas[-1]["estado"] = (
                        f"🔴 {n} cruces detectados"
                    )
                    print(f"  🔴 {n} CRUCES APORTE→COMPRA DETECTADOS")
                else:
                    exp.fuentes_consultadas[-1]["estado"] = "Sin cruces directos"
                    print("  ✅ Sin cruces directos detectados")
            else:
                exp.fuentes_consultadas[-1]["estado"] = (
                    "Sin datos para cruce (tablas vacías)"
                )
                print("  ⚠️  Sin datos suficientes para cruce")

        except (sqlite3.Error, pd.errors.DatabaseError) as exc:
            logger.error("Error en cruces: %s", exc)
            exp.fuentes_consultadas[-1]["estado"] = f"Error: {exc}"
            print(f"  ❌ Error: {exc}")

    # ─────────────────── Evaluación de banderas rojas ─────────────────── #

    def _evaluar_banderas(self, exp: Expediente) -> None:
        """Evalúa banderas rojas basado en los hallazgos."""
        print("[6/6] 🚩 Evaluando banderas rojas...")

        # Bandera: Cruces aporte → compra
        if not exp.cruce_servel_compras.empty:
            n = len(exp.cruce_servel_compras)
            total = exp.cruce_servel_compras.get(
                "retorno_licitaciones", pd.Series([0])
            ).sum()
            exp.banderas_rojas.append(
                f"🔴 {n} aportantes de campaña ganaron licitaciones "
                f"(retorno total: ${total:,.0f} CLP)"
            )

        # Bandera: Aportes de alto monto
        if not exp.aportes_servel.empty:
            max_aporte = exp.aportes_servel["monto_aporte"].max()
            if max_aporte > 10_000_000:
                exp.banderas_rojas.append(
                    f"🟡 Aporte de alto monto detectado: ${max_aporte:,.0f} CLP"
                )

        # Bandera: Audiencias de lobby
        if not exp.audiencias_lobby.empty:
            exp.banderas_rojas.append(
                f"🟡 {len(exp.audiencias_lobby)} audiencias de lobby registradas"
            )

        # Bandera: Hallazgos en catálogos CSV
        total_csv = sum(len(df) for df in exp.lobby_catalogos.values())
        if total_csv:
            exp.banderas_rojas.append(
                f"🟡 {total_csv} registros encontrados en catálogos InfoLobby"
            )

        # Determinar nivel de alerta
        rojas = sum(1 for b in exp.banderas_rojas if "🔴" in b)
        amarillas = sum(1 for b in exp.banderas_rojas if "🟡" in b)

        if rojas > 0:
            exp.nivel_alerta = "🔴 Cruces confirmados — Requiere revisión profunda"
        elif amarillas >= 2:
            exp.nivel_alerta = "🟡 Patrones sospechosos detectados"
        elif amarillas == 1:
            exp.nivel_alerta = "🟡 Hallazgo menor — Monitorear"
        else:
            exp.nivel_alerta = "🟢 Sin hallazgos significativos"

        if exp.banderas_rojas:
            for b in exp.banderas_rojas:
                print(f"  {b}")
        else:
            print("  🟢 Sin banderas rojas detectadas")

    # ─────────────────── Resumen en consola ─────────────────── #

    def _imprimir_resumen(self, exp: Expediente) -> None:
        """Imprime resumen final en consola."""
        print(f"\n{'─' * 60}")
        print(f"  RESULTADO: {exp.nivel_alerta}")
        print(f"{'─' * 60}")
        print(f"  SERVEL:     {len(exp.aportes_servel)} aportes")
        print(f"  Lobby:      {len(exp.audiencias_lobby)} audiencias")
        csv_total = sum(len(df) for df in exp.lobby_catalogos.values())
        print(f"  Catálogos:  {csv_total} registros CSV")
        print(f"  Cruces:     {len(exp.cruce_servel_compras)} coincidencias")
        print(f"  datos.gob:  {len(exp.datasets_gob)} datasets")
        print(f"{'─' * 60}\n")

    # ─────────────────── Exportar a Markdown ─────────────────── #

    def exportar_markdown(
        self, exp: Expediente, output_path: str | Path | None = None,
    ) -> str:
        """
        Genera el expediente en formato Markdown.

        Args:
            exp: Expediente con los hallazgos.
            output_path: Ruta de salida (opcional). Si None, retorna string.

        Returns:
            Contenido Markdown del expediente.
        """
        md = []
        md.append(f"# EXPEDIENTE PÚBLICO — {exp.nombre_investigado.upper()}")
        md.append("## Ojo del Pueblo — Auditoría Cívica Automatizada")
        md.append(f"### Generado: {exp.fecha_generacion}\n")
        md.append("---\n")

        # Sujeto
        md.append("## SUJETO INVESTIGADO\n")
        md.append("| Campo | Detalle |")
        md.append("|-------|---------|")
        md.append(f"| **Nombre** | {exp.nombre_investigado} |")
        if exp.rut:
            md.append(f"| **RUT** | {exp.rut} |")
        if exp.cargo:
            md.append(f"| **Cargo** | {exp.cargo} |")
        md.append(f"| **Fecha investigación** | {exp.fecha_generacion} |")
        md.append(f"| **Nivel de alerta** | {exp.nivel_alerta} |")
        md.append("")

        # SERVEL
        md.append("---\n")
        md.append("## 1. APORTES DE CAMPAÑA (SERVEL)\n")
        if not exp.aportes_servel.empty:
            total = exp.aportes_servel["monto_aporte"].sum()
            md.append(f"**Total encontrado:** {len(exp.aportes_servel)} aportes "
                      f"por **${total:,.0f} CLP**\n")
            md.append(exp.aportes_servel.to_markdown(index=False))
        else:
            md.append("Sin aportes vinculados en la base de datos local.\n")
            md.append("**Acción requerida:**")
            md.append("- [ ] Descargar aportes SERVEL de campañas relevantes")
            md.append("- [ ] Cargar con: `ServelExtractor().procesar_csv_aportes('archivo.csv')`")
        md.append("")

        # Lobby SPARQL
        md.append("---\n")
        md.append("## 2. AUDIENCIAS LEY DEL LOBBY\n")
        if not exp.audiencias_lobby.empty:
            md.append(f"**{len(exp.audiencias_lobby)} audiencias encontradas.**\n")
            md.append(exp.audiencias_lobby.to_markdown(index=False))
        else:
            md.append("Sin audiencias encontradas vía SPARQL.\n")
        md.append("")

        # Lobby CSV
        if exp.lobby_catalogos:
            md.append("### Catálogos CSV de InfoLobby\n")
            for cat, df in exp.lobby_catalogos.items():
                md.append(f"#### {cat.upper()} ({len(df)} registros)\n")
                md.append(df.head(20).to_markdown(index=False))
                md.append("")
        md.append("")

        # datos.gob.cl
        md.append("---\n")
        md.append("## 3. DATASETS RELACIONADOS (datos.gob.cl)\n")
        if exp.datasets_gob:
            for ds in exp.datasets_gob[:5]:
                title = ds.get("title", "Sin título")
                org = ds.get("organization", {}).get("title", "N/D")
                notes = (ds.get("notes") or "")[:200]
                md.append(f"### {title}")
                md.append(f"- **Organismo:** {org}")
                md.append(f"- **Descripción:** {notes}")
                n_res = len(ds.get("resources", []))
                md.append(f"- **Recursos descargables:** {n_res}")
                md.append("")
        else:
            md.append("Sin datasets relacionados encontrados.\n")
        md.append("")

        # Cruces
        md.append("---\n")
        md.append("## 4. CRUCES FORENSES — SERVEL ↔ COMPRAS PÚBLICAS\n")
        if not exp.cruce_servel_compras.empty:
            md.append(
                f"**🔴 {len(exp.cruce_servel_compras)} CRUCES DETECTADOS:**\n"
            )
            md.append(exp.cruce_servel_compras.to_markdown(index=False))
        else:
            md.append("Sin cruces directos detectados.\n")
            md.append(
                "> Esto puede deberse a falta de datos en la tabla "
                "`ordenes_items` o `aportes_servel`."
            )
        md.append("")

        # Banderas rojas
        md.append("---\n")
        md.append("## 5. BANDERAS ROJAS\n")
        md.append(f"**Nivel de alerta: {exp.nivel_alerta}**\n")
        if exp.banderas_rojas:
            for b in exp.banderas_rojas:
                md.append(f"- {b}")
        else:
            md.append("- Sin banderas rojas detectadas.")
        md.append("")

        # Fuentes
        md.append("---\n")
        md.append("## 6. FUENTES CONSULTADAS\n")
        md.append("| Fuente | URL | Estado |")
        md.append("|--------|-----|--------|")
        for f in exp.fuentes_consultadas:
            md.append(f"| {f['fuente']} | {f['url']} | {f['estado']} |")
        md.append("")

        # Disclaimer
        md.append("---\n")
        md.append(
            "> **Disclaimer:** Este expediente se basa exclusivamente en "
            "información pública disponible en portales del Estado de Chile. "
            "No constituye acusación legal alguna. Los datos se presentan "
            "para promover la transparencia y la participación ciudadana.\n"
        )
        md.append("---\n")
        md.append(
            "*Generado automáticamente por Ojo del Pueblo v1.0.0 — "
            "Auditoría Cívica Open Source*  "
        )
        md.append("*github.com/WAPNATION23/monitor-compras-chile*")

        content = "\n".join(md)

        if output_path:
            Path(output_path).write_text(content, encoding="utf-8")
            print(f"📄 Expediente guardado en: {output_path}")

        return content


# ─────────────────── CLI ─────────────────── #


def main() -> None:
    """Entry point para uso por línea de comandos."""
    parser = argparse.ArgumentParser(
        description="🔍 Ojo del Pueblo — Generador de Expedientes",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=(
            "Ejemplos:\n"
            '  py generar_expediente.py "Camila Flores"\n'
            '  py generar_expediente.py --rut 76123456-7\n'
            '  py generar_expediente.py "Juan Perez" -o expediente_perez.md\n'
        ),
    )
    parser.add_argument(
        "nombre",
        nargs="?",
        default="",
        help="Nombre del sujeto a investigar",
    )
    parser.add_argument(
        "--rut", "-r",
        default="",
        help="RUT del sujeto (formato 12345678-9)",
    )
    parser.add_argument(
        "--output", "-o",
        default="",
        help="Ruta del archivo .md de salida",
    )
    parser.add_argument(
        "--db",
        default=DB_NAME,
        help=f"Base de datos SQLite (default: {DB_NAME})",
    )

    args = parser.parse_args()

    if not args.nombre and not args.rut:
        parser.error("Especifica un nombre o --rut")

    # Configurar logging
    logging.basicConfig(
        level=logging.INFO,
        format="%(levelname)s | %(name)s | %(message)s",
    )

    nombre = args.nombre or args.rut
    gen = GeneradorExpediente(db_path=args.db)
    exp = gen.investigar(nombre, rut=args.rut)

    # Auto-generar nombre de archivo si no se especifica
    output = args.output
    if not output:
        slug = nombre.lower().replace(" ", "_")[:30]
        output = f"expediente_{slug}.md"

    gen.exportar_markdown(exp, output)


if __name__ == "__main__":
    main()
