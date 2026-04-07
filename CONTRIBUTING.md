# Contribuir a Ojo del Pueblo

Gracias por tu interés en mejorar la fiscalización ciudadana del gasto público en Chile.

> **Estado actual: Beta pública.** El sistema está auditado y funcional, pero en desarrollo activo.
> Tu feedback es especialmente valioso en esta etapa.

## Cómo contribuir

1. Haz fork del repositorio
2. Crea una rama para tu cambio: `git checkout -b mi-mejora`
3. Asegúrate de que los tests pasen: `py -m pytest tests/test_core.py -q`
4. Envía un Pull Request describiendo el cambio

## Prioridades actuales

Ver la tabla de madurez en el README. Las áreas donde más se necesita ayuda:

- **Calidad de datos**: Mejorar cobertura de `rut_proveedor`, `fecha_creacion`, `nombre_producto`
- **Conectores externos**: Implementar scraper real del Diario Oficial, datos SERVEL
- **Seguridad**: Revisión continua de superficie de ataque
- **Tests**: Agregar tests con payloads reales anonimizados de la API

## Reglas

- No commitear secrets (tokens, API keys) — usar `.env`
- No insertar datos sintéticos como si fueran reales
- Cada detector/cruce debe documentar: qué detecta, supuestos, limitaciones
- Mantener los tests pasando antes de hacer PR

## Entorno de desarrollo

```bash
pip install -r requirements.txt
cp .env.example .env  # Configurar tus claves
py -m pytest tests/test_core.py -q
streamlit run dashboard.py
```

## Reportar problemas

Abre un issue con:
- Qué esperabas que pasara
- Qué pasó en realidad
- Pasos para reproducir
- Versión de Python y sistema operativo

También son bienvenidos:
- **Datos sospechosos** que descubriste usando la herramienta
- **Falsos positivos** del detector (ayudan a calibrar umbrales)
- **Sugerencias de UX** para el dashboard

## Changelog

Todo cambio relevante se documenta en los commits y releases del repositorio.
Si tu PR es aceptado, asegúrate de describir el cambio claramente para que quede en el historial.

## Licencia

Al contribuir, aceptas que tu código se publique bajo AGPL-3.0.
