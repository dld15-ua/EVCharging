# EVCharging — Sistema Distribuido de Recarga de Vehículos Eléctricos

Sistema distribuido para la gestión de puntos de recarga de vehículos eléctricos, desarrollado para la asignatura de Sistemas Distribuidos (Universidad de Alicante, curso 2025/26).

## Arquitectura

El sistema se compone de 6 microservicios + Apache Kafka como bus de mensajería:

| Servicio | Descripción | Comunicación |
|----------|-------------|--------------|
| **Central** | Servidor central. API REST + Socket TLS + Kafka | HTTPS :5001, Socket :65000 |
| **Registry** | Registro de CPs y generación de credenciales | HTTPS :5000 |
| **Engine** | Simulación del suministro eléctrico | Kafka + Socket local |
| **Monitor** | Monitoriza Engine y reporta a Central | Socket TLS + Socket local |
| **Driver** | Aplicación del conductor | Kafka |
| **Weather** | Alertas de clima desde OpenWeatherMap | HTTPS → Central |

Consultar [SPEC.md](SPEC.md) para la especificación técnica completa.

## Requisitos

- Docker y Docker Compose v2+
- (Opcional) Python 3.11+ para ejecución local sin Docker

## Despliegue con Docker

### 1. Generar certificados SSL

```bash
./scripts/generate_certs.sh
```

### 2. Configurar variables de entorno

```bash
cp .env.example .env
# Editar .env si es necesario (API key de OpenWeather, puertos, etc.)
```

### 3. Levantar el sistema

```bash
docker compose up --build
```

Esto arrancará:
- **Kafka** (KRaft mode, sin Zookeeper)
- **Central** (API + Socket + BD)
- **Registry** (registro de CPs)
- **2 Engines** (ALC1, ALC2)
- **2 Monitores** (ALC1, ALC2) — se registran automáticamente
- **Driver** (modo interactivo)
- **Weather** (consulta OpenWeatherMap)

### 4. Acceder al frontend

Abrir en el navegador: `https://localhost:5001`

(Aceptar el certificado autofirmado)

### 5. Usar el Driver

```bash
docker attach evcharging-driver
```

Introducir el ID del CP (ej: `ALC1`) y pulsar ENTER para solicitar carga.

### 6. Parar el sistema

```bash
docker compose down
```

Para borrar también la BD:
```bash
docker compose down -v
```

## Estructura del Proyecto

```
EVCharging/
├── docker-compose.yml
├── .env.example
├── SPEC.md
├── README.md
├── services/
│   ├── central/          # EV_Central + database_manager + frontend
│   ├── registry/         # EV_Registry
│   ├── engine/           # EV_CP_E (Engine)
│   ├── monitor/          # EV_CP_M (Monitor)
│   ├── driver/           # EV_Driver
│   └── weather/          # EV_Weather
├── certs/                # Certificados SSL (generados)
└── scripts/
    └── generate_certs.sh
```

## Añadir más Puntos de Carga

Para añadir un nuevo CP (ej: `CP003`), añadir al `docker-compose.yml`:

```yaml
  engine-cp003:
    build: ./services/engine
    environment:
      - CP_ID=CP003
      - ENGINE_PORT=65001
      # ... (ver ejemplos existentes)

  monitor-cp003:
    build: ./services/monitor
    environment:
      - CP_ID=CP003
      - ENGINE_HOST=engine-cp003
      # ... (ver ejemplos existentes)
```

## Seguridad

- **SSL/TLS** en todas las conexiones HTTP y Socket
- **Cifrado Fernet** (simétrico) en payload de Kafka y Socket
- **Tokens** únicos por CP, generados dinámicamente
- **Revocación** instantánea desde el panel de administración
