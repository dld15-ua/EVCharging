# EVCharging — Especificación Técnica y Plan de Dockerización

> **Autor**: Danil Launov Draganov  
> **Asignatura**: Sistemas Distribuidos — Universidad de Alicante — Curso 2025/26  
> **Fecha del documento**: 2026-09-26

---

## 1. Visión General del Sistema

EVCharging es un sistema distribuido de gestión de puntos de recarga de vehículos eléctricos. Originalmente se desplegó en 3 máquinas (1 host Windows + 2 VMs Linux), usando una combinación de:

- **Apache Kafka** como bus de mensajería asíncrona.
- **Sockets TCP con SSL/TLS** para la comunicación Monitor ↔ Central.
- **APIs REST con HTTPS** (Flask + certificados autofirmados) para Registry ↔ Central, Weather ↔ Central y el Frontend.
- **SQLite** como base de datos embebida en la Central.
- **Cifrado Fernet** (simétrico) para proteger el payload de los mensajes Kafka y Socket.
- **Sistema de Tokens** para autenticación de los Charge Points.

---

## 2. Arquitectura Distribuida Original (VMs)

```
┌─────────────────────────────────────────────────────────┐
│         Host Windows (192.168.56.1)                     │
│                                                         │
│  ┌──────────────┐  ┌──────────────┐  ┌───────────────┐  │
│  │ Apache Kafka  │  │  EV_Central  │  │  Frontend     │  │
│  │  (broker)     │  │  (API+Socket │  │  (index.html) │  │
│  │  puerto 9092  │  │   +Kafka)    │  │  HTTPS :5001  │  │
│  └──────────────┘  │  SSL :65000  │  └───────────────┘  │
│                     │  HTTPS :5001 │                     │
│                     │  SQLite DB   │                     │
│                     └──────────────┘                     │
└─────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────┐
│        VM Debian (192.168.56.120)                        │
│                                                         │
│  ┌──────────────┐  ┌──────────────┐  ┌───────────────┐  │
│  │  EV_CP_E     │  │  EV_CP_M     │  │  EV_Weather   │  │
│  │  (Engine)    │  │  (Monitor)   │  │               │  │
│  │  Kafka +     │  │  SSL Socket  │  │  REST client  │  │
│  │  Socket      │  │  → Central   │  │  → Central    │  │
│  │  :65001      │  │              │  │  OpenWeather   │  │
│  └──────────────┘  └──────────────┘  └───────────────┘  │
└─────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────┐
│        VM Linux Mint (192.168.56.110)                    │
│                                                         │
│  ┌──────────────┐  ┌──────────────┐                     │
│  │  EV_Registry  │  │  EV_Driver   │                     │
│  │  HTTPS :5000  │  │  Kafka       │                     │
│  │  REST API     │  │  productor + │                     │
│  │  → Central    │  │  consumidor  │                     │
│  └──────────────┘  └──────────────┘                     │
└─────────────────────────────────────────────────────────┘
```

---

## 3. Módulos — Especificación Detallada

### 3.1 EV_Central (`EV_Central.py`)

**Rol**: Servidor central. Orquesta todo el sistema.

**Componentes que ejecuta simultáneamente** (hilos):
1. **Servidor SSL/TLS Socket** (puerto configurable, ej. 65000) — acepta conexiones de Monitores.
2. **Consumidor Kafka `driver_central`** — recibe peticiones REQUEST de drivers.
3. **Consumidor Kafka `cp_central`** — recibe mensajes CARGANDO/TICKET/EVENT de engines.
4. **API REST Flask con HTTPS** (puerto 5001) — endpoints internos y comunicación AJAX con el Frontend.

**Dependencias**:
- `database_manager.py` (SQLite)
- `server_cert.pem`, `server_key.pem` (certificados SSL)
- Kafka broker

**Endpoints REST**:
| Método | Ruta | Descripción |
|--------|------|-------------|
| GET | `/cps` | Lista todos los CPs con estado, precio, token, info de carga en tiempo real |
| POST | `/api/internal/registro-cp` | Recibe datos de registro desde Registry (interno) |
| POST | `/api/weather` | Recibe alertas de clima desde EV_Weather |
| GET/POST | `/api/weather/config` | Obtiene/Modifica el límite térmico y ciudades dinámicamente |
| GET | `/api/logs` | Devuelve historial de auditoría |
| POST | `/cps/<cp_id>/parar` | Orden de parada manual desde frontend |
| POST | `/cps/<cp_id>/reanudar` | Orden de reanudación desde frontend |
| POST | `/cps/<cp_id>/revocar` | Revocación de credenciales desde frontend |

### 3.1.b EV_Frontend (`EV_Frontend.py`)

**Rol**: Servidor Web ligero e independiente.
**Componentes**: Servidor Flask (`HTTPS :5002`) que sirve el HTML/JS/CSS rediseñado. Consume la API REST de Central (`:5001`) aprovechando CORS.

**Topics Kafka**:
| Topic | Rol en Central |
|-------|---------------|
| `driver_central` | Consumidor (recibe REQUEST de drivers) |
| `central_driver` | Productor (envía AUTORIZADO/DENEGADO/CARGANDO_DRIVER/TICKET/ERROR a drivers) |
| `cp_central` | Consumidor (recibe CARGANDO/TICKET/EVENT de engines) |
| `central_cp` | Productor (envía START/PARAR_CP/REANUDAR_CP a engines) |

**Protocolo Socket (Monitor ↔ Central)**:
- Empaquetado binario: `STX(0x02) + payload + ETX(0x03) + LRC`
- Respuestas: `ACK(0x06)` o `NACK(0x15)`
- Mensajes: `AUT,<cp_id>,<token>` | `STAT,<cp_id>,OK/KO` | `EVENT,<cp_id>,AVERIADO`
- SSL/TLS obligatorio (server-side certificates)
- Payload cifrado con Fernet tras autenticación

**Estados de un CP**:
`DESCONECTADO` → `ACTIVADO` → `SUMINISTRANDO` → `ACTIVADO`  
`ACTIVADO` → `PARADO` (manual/clima) → `ACTIVADO`  
`ACTIVADO` → `AVERIADO` (fallo engine)  
`ACTIVADO` → `NO_AUTORIZADO` (credenciales revocadas)

---

### 3.2 EV_Registry (`EV_Registry.py` — 72 líneas)

**Rol**: Servicio de registro y generación de credenciales para CPs.

**Funcionalidad**:
1. Recibe POST `/registro` con `{cp_id, ubicacion}` desde el Monitor.
2. Genera un `token` (`secrets.token_hex(16)`) y una `clave_cifrado` (`Fernet.generate_key()`).
3. Envía las credenciales a la Central via POST a `https://<CENTRAL_IP>:5001/api/internal/registro-cp`.
4. Responde al Monitor con `{token, clave_cifrado}`.

**Argumentos CLI**: Ninguno (IP de Central hardcodeada como `https://192.168.56.1:5001`).

**Puerto**: 5000 (HTTPS con `registry_cert.pem` / `registry_key.pem`).

**Dependencias**: `flask`, `requests`, `cryptography`, certificados propios.

---

### 3.3 EV_CP_E — Engine (`EV_CP_E.py` — 315 líneas)

**Rol**: Simula la lógica física del punto de recarga (suministro eléctrico).

**Componentes** (hilos):
1. **Consumidor Kafka `central_cp`** — recibe órdenes START/PARAR/REANUDAR de Central.
2. **Servidor Socket TCP** (127.0.0.1, puerto configurable) — recibe PING del Monitor local y clave de cifrado.
3. **Hilo principal** — interfaz de usuario para simular averías (ko/ok).

**Argumentos CLI**: `python3 EV_CP_E.py <broker_ip> <broker_puerto> <cp_id> <puerto>`

**Proceso de carga** (simulación):
- Energía total: 20 kWh
- Potencia de carga: 15 kW
- Aceleración: ×10
- Envía mensajes `CARGANDO` cada segundo y `TICKET` al finalizar.
- Mensajes cifrados con Fernet antes de enviar por Kafka.

**Protocolo Engine ↔ Monitor** (socket local plano):
- `SET_KEY,<clave>` → responde `OK_KEY`
- `PING` → responde `OK,<cp_id>` o `KO,<cp_id>`

---

### 3.4 EV_CP_M — Monitor (`EV_CP_M.py` — 256 líneas)

**Rol**: Intermediario entre Engine y Central. Monitoriza el Engine y reporta estado a Central.

**Flujo**:
1. Auto-registro: si arranca sin token, solicita credenciales automáticamente al Registry (`POST /registro`).
2. Con el token, se conecta a Central vía SSL Socket y envía `AUT,<cp_id>,<token>`.
3. Conecta con el Engine local vía socket y envía `SET_KEY,<clave>`.
4. Bucle: cada segundo hace PING al Engine y envía `STAT,<cp_id>,OK/KO` o `EVENT,<cp_id>,AVERIADO` a Central (cifrado con Fernet).

**Argumentos CLI**: `python3 EV_CP_M.py <central_ip> <central_puerto> <engine_ip> <engine_puerto> <cp_id> <ubicacion_cp>`

**URLs hardcodeadas**: URL de Registry `https://192.168.56.110:5000`.

---

### 3.5 EV_Driver (`EV_Driver.py` — 157 líneas)

**Rol**: Aplicación del conductor que solicita carga en un CP.

**Modos de operación**:
1. **Manual**: el usuario introduce el CP ID por consola.
2. **Fichero**: lee CPs de un fichero de texto, uno por línea.

**Argumentos CLI**: `python3 EV_Driver.py <ip_broker> <puerto_broker> <driver_id> [fichero_servicios]`

**Topics Kafka**:
- Productor: `driver_central` (envía `REQUEST,<driver_id>,<cp_id>`)
- Consumidor: `central_driver` (recibe AUTORIZADO/DENEGADO/CARGANDO_DRIVER/TICKET/ERROR)

---

### 3.6 EV_Weather (`EV_Weather.py`)

**Rol**: Consulta la API de OpenWeatherMap y notifica alertas de clima a Central.

**Funcionamiento**:
- Consulta temperatura cada 4 segundos.
- Antes de cada consulta, pide a Central (`/api/weather/config`) el límite de temperatura dinámico y la lista de ciudades a rastrear.
- Si temperatura < Límite Dinámico → envía `{cp_id, estado_clima: "alerta"}` a Central.
- Si temperatura ≥ Límite Dinámico y antes era alerta → envía `{cp_id, estado_clima: "normal"}`.
- Sincroniza rutinariamente cada 60s para asegurar consistencia con Central.
**API Key hardcodeada**: (configurar en `.env` como `OPENWEATHER_API_KEY`)

---

### 3.7 database_manager.py (274 líneas)

**Rol**: Capa de acceso a datos con SQLite.

**Tablas**:

```sql
puntos_recarga (
    cp_id TEXT PRIMARY KEY,
    ubicacion TEXT,
    precio_kwh REAL,
    estado TEXT,         -- DESCONECTADO|ACTIVADO|SUMINISTRANDO|PARADO|AVERIADO|NO_AUTORIZADO
    token TEXT,
    clave_cifrado TEXT
)

conductor (
    driver_id TEXT PRIMARY KEY,
    nombre TEXT
)

cargas (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    cp_id TEXT REFERENCES puntos_recarga(cp_id),
    driver_id TEXT REFERENCES conductor(driver_id),
    energia REAL,
    coste REAL,
    fecha DATETIME DEFAULT CURRENT_TIMESTAMP
)
```

**Datos de prueba**: 5 CPs (ALC1, ALC2, CP001-CP003) y 7 conductores (D001-D007).

**Thread-safety**: usa `threading.Lock()` en cada operación.

---

### 3.8 Frontend (`templates/index.html` — 190 líneas)

**Rol**: Panel de monitorización web servido por la API Central.

**Funcionalidades**:
- Dashboard con tarjetas de colores por estado de cada CP.
- Información en tiempo real de carga (kWh, coste, conductor).
- Botones: PARAR, REANUDAR, REVOCAR CLAVES por CP.
- Panel de logs de auditoría con scroll.
- Polling cada 2 segundos via `fetch()` a `/cps` y `/api/logs`.

---

## 4. Seguridad

| Mecanismo | Detalle |
|-----------|---------|
| **SSL/TLS (Socket)** | Central actúa como TLS server; Monitor se conecta como TLS client (`CERT_NONE`). Certificados autofirmados `server_cert.pem`/`server_key.pem`. |
| **HTTPS (API REST)** | Central (puerto 5001) y Registry (puerto 5000) usan certificados SSL autofirmados. `verify=False` en las llamadas entre servicios. |
| **Cifrado Fernet** | Cifrado simétrico del payload de mensajes Kafka (Engine ↔ Central) y Socket (Monitor ↔ Central). Clave única por CP generada dinámicamente. |
| **Tokens** | Token hex de 32 caracteres por CP, generado por Registry y almacenado en BD. Central valida token en mensaje `AUT` del monitor. |
| **Revocación** | Desde el frontend se pueden revocar credenciales → token y clave se ponen a NULL, estado → `NO_AUTORIZADO`. Central corta la conexión socket inmediatamente. |

---

## 5. Protocolo de Mensajes

### 5.1 Empaquetado Binario (Socket Monitor ↔ Central)

```
[STX 0x02] [payload bytes] [ETX 0x03] [LRC byte]
```
- LRC = XOR de todos los bytes del payload.
- Payload cifrado con Fernet tras autenticación.

### 5.2 Mensajes Kafka (formato texto)

**Mensajes cifrados** se envuelven como: `ENCRIPTADO,<cp_id>,<contenido_cifrado_base64>`

**Topic `driver_central`** (Driver → Central):
- `REQUEST,<driver_id>,<cp_id>`

**Topic `central_driver`** (Central → Driver):
- `AUTORIZADO,<driver_id>,<cp_id>`
- `DENEGADO,<driver_id>,<cp_id>,<causa>`
- `CARGANDO_DRIVER,<driver_id>,<cp_id>,<kwh>,<coste>`
- `TICKET,<driver_id>,<cp_id>,<kwh>,<coste>`
- `ERROR_INTERRUMPIDO,<driver_id>,<cp_id>,<causa>`
- `ERROR_AVERIA,<driver_id>,<cp_id>,<causa>`

**Topic `central_cp`** (Central → Engine):
- `START,<driver_id>,<precio_kwh>,<cp_id>` (cifrado)
- `PARAR_CP,<cp_id>` (cifrado o plano en emergencia)
- `REANUDAR_CP,<cp_id>` (cifrado)

**Topic `cp_central`** (Engine → Central):
- `CARGANDO,<cp_id>,<driver_id>,<kwh>,<coste>` (cifrado)
- `TICKET,<cp_id>,<driver_id>,<kwh>,<coste>` (cifrado)
- `EVENT,<cp_id>,<driver_id>,<tipo_evento>` (cifrado)

---

## 6. Dependencias Python

```
flask
cryptography
requests
kafka-python
```

---

## 7. Certificados SSL

| Fichero | Usado por | Tipo |
|---------|-----------|------|
| `server_cert.pem` | EV_Central | Certificado servidor (API REST + Socket TLS) |
| `server_key.pem` | EV_Central | Clave privada servidor |
| `registry_cert.pem` | EV_Registry | Certificado servidor (API REST) |
| `registry_key.pem` | EV_Registry | Clave privada servidor |

---

## 8. IPs y Puertos Hardcodeados

| Variable | Valor actual | Fichero | Línea |
|----------|-------------|---------|-------|
| `URL_CENTRAL` | `https://192.168.56.1:5001` | `EV_Registry.py` | 8 |
| `URL_REGISTRY` | `https://192.168.56.110:5000` | `EV_CP_M.py` | 236 |
| `CENTRAL_URL` | `https://192.168.56.1:5001` | `EV_Weather.py` | 12 |
| `API_KEY` (OpenWeather) | *(configurar en `.env`)* | `EV_Weather.py` | 5 |
| API Central port | `5001` | `EV_Central.py` | 752 |
| Registry port | `5000` | `EV_Registry.py` | 72 |
| Engine socket | `127.0.0.1:<puerto>` | `EV_CP_E.py` | 218 |

---

## 9. Plan de Reorganización de Directorios

### 9.1 Estructura Actual (todo plano)

```
EVCharging/
├── EV_Central.py
├── EV_Registry.py
├── EV_CP_E.py
├── EV_CP_M.py
├── EV_Driver.py
├── EV_Weather.py
├── database_manager.py
├── templates/
│   └── index.html
├── certs/
│   ├── server_cert.pem
│   ├── server_key.pem
│   ├── registry_cert.pem
│   └── registry_key.pem
├── .gitignore
└── README.md
```

### 9.2 Estructura Propuesta (por servicio)

```
EVCharging/
├── docker-compose.yml          # Orquestación de todos los contenedores
├── .env                        # Variables de entorno globales (IPs, puertos, API keys)
├── .gitignore
├── README.md
├── SPEC.md                     # Este documento
│
├── services/
│   ├── central/                # Nodo: Host (contenedor "central")
│   │   ├── Dockerfile
│   │   ├── requirements.txt    # flask, cryptography, kafka-python
│   │   ├── EV_Central.py
│   │   ├── database_manager.py
│   │   └── templates/
│   │       └── index.html
│   │
│   ├── registry/               # Nodo: Linux Mint (contenedor "registry")
│   │   ├── Dockerfile
│   │   ├── requirements.txt    # flask, cryptography, requests
│   │   └── EV_Registry.py
│   │
│   ├── engine/                 # Nodo: Debian (contenedor "engine")
│   │   ├── Dockerfile
│   │   ├── requirements.txt    # cryptography, kafka-python
│   │   └── EV_CP_E.py
│   │
│   ├── monitor/                # Nodo: Debian (contenedor "monitor")
│   │   ├── Dockerfile
│   │   ├── requirements.txt    # cryptography, requests
│   │   └── EV_CP_M.py
│   │
│   ├── driver/                 # Nodo: Linux Mint (contenedor "driver")
│   │   ├── Dockerfile
│   │   ├── requirements.txt    # kafka-python
│   │   └── EV_Driver.py
│   │
│   └── weather/                # Nodo: Debian (contenedor "weather")
│       ├── Dockerfile
│       ├── requirements.txt    # requests
│       └── EV_Weather.py
│
├── certs/                      # Certificados compartidos (montados como volumen)
│   ├── server_cert.pem
│   ├── server_key.pem
│   ├── registry_cert.pem
│   └── registry_key.pem
│
└── scripts/
    └── generate_certs.sh       # Script para regenerar certificados autofirmados
```
