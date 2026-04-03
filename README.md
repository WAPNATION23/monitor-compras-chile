# 🕵️‍♂️ Monitor Ciudadano de Compras Públicas — Chile (El "Jarbas" Chileno)

> Inspirado en la arquitectura forense de [Operação Serenata de Amor](https://github.com/okfn-brasil/serenata-de-amor) 🇧🇷

Herramienta open-source de **Inteligencia Cívica y Auditoría Forense** diseñada para detectar sistemáticamente anomalías, sobreprecios, fraccionamiento, horarios sospechosos ("Vampiros") y tratos directos anómalos en las compras del Estado chileno. Utiliza extracción de datos directa desde la API pública de [Mercado Público](https://www.mercadopublico.cl/).

## 📦 Arquitectura del Sistema

El ecosistema está compuesto por 5 pilares autónomos:

1. **Extractor de OC (`main.py`, `extractor.py`, `processor.py`)**: Descarga diaria de órdenes de compra, limpieza de datos, deduplicación automática y clasificación de perfiles de riesgo político (GOREs, Municipalidades, FFAA, Fundaciones, MOP).
2. **Extractor de Licitaciones (`licitaciones_extractor.py`)**: Descarga licitaciones por fecha, estado, organismo o proveedor. Permite cruzar datos entre licitaciones y OC.
3. **Motor Forense "Rosie" (`detector.py`)**: Analiza la base de datos (SQLite) aplicando 7 modelos estadísticos y lógicos para detectar fraude.
4. **Módulo de Mando (`dashboard.py`)**: Aplicación web en Streamlit (Dark Theme) para analistas de datos, periodistas y ciudadanos. Incluye radares interactivos, cruce de datos y un módulo de "Crowdsourcing".
5. **Sistema de Alerta Automática (`notifier.py`, GitHub Actions)**: Ejecución Serverless todos los días a las 04:00 AM (CLT). Alertas a Telegram con **protección anti-spam** (máx. 10 alertas/ejecución, rate limiting, deduplicación).

## 🧠 Los 7 Algoritmos Forenses (El Arsenal Serenata)

| Algoritmo | Descripción y Modus Operandi Detectado |
|-----------|----------------------------------------|
| **Z-Score Modificado** | Detecta **Sobreprecios Descarados**. Compara el valor de un ítem contra la mediana histórica nacional de ese producto específico usando MAD (Median Absolute Deviation). |
| **IQR (Rango Intercuartílico)** | Método robusto ante outliers. Marca como anómalo todo precio que supere Q3 + 1.5×IQR dentro de su grupo de producto. |
| **Horarios Vampiro 🧛‍♂️** | Detecta adjudicaciones millonarias (>10MM) aprobadas de madrugada, fines de semana o feriados. La corrupción adora la oscuridad. |
| **Fraccionamiento 💧** | El "Goteo". Detecta cuando un municipio le compra 3+ veces al mismo proveedor en menos de 7 días, acumulando >$1.900.000, evadiendo así la obligación de Licitación Pública (>30 UTM). |
| **Ley del Fantasma 👻** | Detecta "Empresas Multigiro/De Papel". Proveedores que un lunes ganan "Construcción de Puentes" y un martes "Servicios de Banquetería". |
| **Ley de Benford 🧮** | Aplica Matemática Forense para detectar montos falsificados. En transacciones naturales el dígito "1" aparece ~30% de las veces. |
| **Red de Araña 🕷️** | Rastrea "Tratos Directos" bajo justificaciones de "Asesorías/Estudios" con sumas ridículamente redondas y exactas (ej. $50.000.000). |

## 🚀 Instalación y Despliegue Local

1. Clona el repositorio y entra a la carpeta.
2. Instala las dependencias:
```bash
pip install -r requirements.txt
```
3. Inicializa el **Panel de Mando Forense (Dashboard)**:
```bash
streamlit run dashboard.py
```
*(El dashboard levantará en `http://localhost:8501`)*

## ⚡ Uso del Motor de Extracción (CLI)

```bash
# Extraer OC del día anterior y ejecutar TODOS los algoritmos forenses
python main.py --metodo serenata

# Ejecutar y enviar alertas a Telegram
python main.py --metodo serenata --telegram

# Solo estadísticos (IQR + Z-Score, sin forenses)
python main.py --metodo estadistico

# Escanear una fecha específica del pasado (ej. 15 de Marzo 2026)
python main.py --fecha 15032026

# Solo correr los algoritmos sobre la base de datos existente (sin gastar cuota API)
python main.py --solo-analisis --metodo serenata

# Explorador SQL forense (consultas pre-armadas)
python explorador_sql.py
```

## 📡 Endpoints de la API de Mercado Público Utilizados

| Endpoint | Módulo | Descripción |
|----------|--------|-------------|
| `ordenesdecompra.json` | `extractor.py` | Órdenes de compra por fecha, código, estado |
| `licitaciones.json` | `licitaciones_extractor.py` | Licitaciones por fecha, estado, organismo |
| `BuscarProveedor` | `proveedor_lookup.py` | Búsqueda de proveedor por RUT |
| `BuscarComprador` | `proveedor_lookup.py` | Listado de organismos públicos |

## 🛡️ Protecciones Anti-Spam del Bot Telegram

Para evitar spam de alertas falsas, el notifier incluye:
- **Máximo 10 alertas** individuales por ejecución
- **Rate limiting** de 1.5 segundos entre mensajes
- **Deduplicación**: la misma OC no se alerta dos veces
- **Resumen diario**: muestra cuántas alertas se enviaron vs omitidas

## 🔑 Credenciales (Entorno Local)

Configura las siguientes Variables de Entorno:
- `MERCADO_PUBLICO_TICKET` — Tu llave personal de la API ([solicitar aquí](https://api.mercadopublico.cl/modules/IniciarSesion.aspx))
- `TELEGRAM_TOKEN` — Token del BotFather
- `TELEGRAM_CHAT_ID` — Tu chat personal o grupo de alertas

> ⚠️ **NUNCA** hardcodear tokens en el código fuente. Usar variables de entorno o un archivo `.env` (no versionado).

## 🤝 Cómo la Comunidad puede potenciar este proyecto

1. **Inteligencia Colectiva (Crowdsourcing)**: Los periodistas o ciudadanos pueden usar el **formulario de transmisión segura en el Dashboard** para reportar hallazgos.
2. **Cruces de Bases de Datos Externas**: Se hace un llamado a la comunidad Python para construir módulos conectores (`connectors/`) hacia:
   - **API de InfoLobby**: Para cruzar Audiencias con autoridades y fechas de Licitaciones "A La Medida".
   - **Registro Civil / SII**: Para detectar fechas de conformación legal y cruzar parentescos (Nepotismo).
   - **Data.gov.cl (Chileindica/Subdere)**: Para cruzar presupuestos macro aprobados con gastos reales.

## 📄 Licencia

MIT — Herramienta diseñada para el Empoderamiento Tecnológico del Ciudadano, Fiscalización Robusta y Transparencia del Estado.
