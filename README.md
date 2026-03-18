# 🇨🇱 Monitor Ciudadano de Compras Públicas — Chile

> Clon chileno de [Operação Serenata de Amor](https://github.com/okfn-brasil/serenata-de-amor) 🇧🇷

Herramienta open-source para detectar **anomalías y sobreprecios** en las compras del Estado chileno, extrayendo datos desde la API pública de [Mercado Público](https://www.mercadopublico.cl/).

## 📦 Arquitectura

```
monitor-compras-chile/
├── config.py           # Configuración central (API, DB, umbrales)
├── extractor.py        # MercadoPublicoExtractor — conexión API
├── processor.py        # DataProcessor — flatten JSON + SQLite
├── detector.py         # AnomalyDetector — IQR + Z-Score
├── main.py             # Pipeline principal (CLI)
├── requirements.txt    # Dependencias Python
└── README.md
```

## 🚀 Instalación

```bash
pip install -r requirements.txt
```

## ⚡ Uso

```bash
# Extraer OC de ayer y analizar
python main.py

# Fecha específica (formato ddmmaaaa)
python main.py --fecha 15032026

# Solo analizar datos existentes en la BD
python main.py --solo-analisis

# Elegir método de detección
python main.py --metodo iqr       # Solo IQR
python main.py --metodo zscore    # Solo Z-Score
python main.py --metodo both      # Ambos (default)

# Logging detallado
python main.py -v
```

## 🔍 Métodos de Detección

| Método | Descripción |
|--------|-------------|
| **IQR** | Marca como anomalía si `precio > Q3 + 1.5 × IQR`. Robusto ante distribuciones sesgadas. |
| **Z-Score Modificado** | Usa la mediana y MAD en lugar de media y desviación estándar. Ideal para datos no-normales. |

## 🔑 API Key

El proyecto usa un ticket de prueba de ChileCompra. Para producción, solicita tu propio ticket en:
https://api.mercadopublico.cl → **Participa** → **Solicitud de Ticket**

## 📄 Licencia

MIT — Uso libre para auditoría ciudadana y transparencia.
