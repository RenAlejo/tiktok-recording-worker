# TikTok Recording Worker

Worker independiente que procesa grabaciones de TikTok Live desde una cola Redis.

## 📋 Descripción

Este proyecto contiene **únicamente** la lógica de grabación:
- Motor de grabación TikTok Live completo
- Procesamiento de streams en tiempo real
- Detección de corrupción y reinicio automático
- Gestión de proxies y rotación
- Monitoreo de recursos del sistema

## 🚀 Instalación

1. **Clonar e instalar dependencias:**
```bash
cd tiktok-recording-worker
pip install -r requirements.txt
```

2. **Configurar variables de entorno:**
```bash
cp .env.example .env
# Editar .env con tus configuraciones
```

3. **Configurar archivo telegram.json:**
```bash
cp telegram.json.example telegram.json
# Editar telegram.json con las MISMAS credenciales del bot
```

4. **Configuraciones mínimas requeridas:**
```env
# Redis (para comunicación con bot)
REDIS_URL=redis://localhost:6379/0

# Base de datos
DATABASE_URL=postgresql://user:password@localhost:5432/tiktok_recorder

# Directorio de grabaciones
OUTPUT_DIRECTORY=/recordings

# Worker settings
WORKER_MAX_CONCURRENT_JOBS=5
```

## ▶️ Ejecución

### Worker Independiente:
```bash
cd src
python worker_main.py
```

### Worker con ID personalizado:
```bash
cd src
WORKER_ID=worker_server01 python worker_main.py
```

### Modo CLI (compatible con versión original):
```bash
cd src
python main.py --username tiktoker --mode live
```

## 🏗️ Arquitectura

```
🤖 Bot Server
    ↓
🔄 Redis Queue
    ↓
🎬 Recording Worker (este proyecto)
    ↓
📁 Archivos locales
```

## 📂 Estructura del Proyecto

```
tiktok-recording-worker/
├── src/
│   ├── core/                # Motor de grabación TikTok
│   ├── services/            # Servicios de grabación
│   ├── http_utils/          # HTTP client y proxies
│   ├── utils/               # Utilidades (corrupción, video, etc)
│   ├── worker_main.py       # Entry point worker
│   └── main.py             # CLI mode (compatibilidad)
├── requirements.txt        # Dependencias del worker  
├── proxies.csv            # Lista de proxies HTTP
├── proxiessock5.csv       # Lista de proxies SOCKS5
└── .env.example           # Configuración ejemplo
```

## 🎯 Funcionalidades Principales

### ✅ Grabación Avanzada
- **FFmpeg optimizado** con parámetros de rendimiento
- **Detección universal de corrupción** (Windows/Linux)
- **Reinicio automático** en caso de errores
- **Gestión inteligente de archivos** con límites de tamaño

### ✅ Detección de Problemas
- **Timestamp repetition detection**: Detecta streams congelados
- **Corrupted segment detection**: Análisis post-grabación
- **URL expiration handling**: Renovación automática de URLs
- **Stream validation**: Verificación de que el usuario sigue live

### ✅ Gestión de Proxies
- **Rotación automática** en caso de bloqueos
- **Health checking** de proxies disponibles  
- **Soporte HTTP y SOCKS5**
- **Fallback inteligente** entre proxies

### ✅ Monitoreo de Recursos
- **CPU, memoria y disco** en tiempo real
- **Alertas automáticas** cuando se superan límites
- **Optimización automática** de parámetros FFmpeg

## 🔧 Configuración Avanzada

### Grabación
```env
# Calidad y rendimiento
MAX_FILE_SIZE_GB=1.5
BUFFER_SIZE_KB=512
FFMPEG_LOGLEVEL=error

# Detección de corrupción
ENABLE_TIMESTAMP_FRAME_DETECTION=true
FRAME_REPETITION_THRESHOLD=1
ENABLE_CORRUPTED_SEGMENT_DETECTION=true
```

### Proxies
```env
ENABLE_PROXY=true
HTTP_REQUEST_TIMEOUT_SECONDS=8
MAX_RETRIES=10
RETRY_BACKOFF_FACTOR=0.5
```

### Recursos del Sistema  
```env
ENABLE_RESOURCE_MONITORING=true
CPU_WARNING_THRESHOLD_PERCENT=80
MEMORY_WARNING_THRESHOLD_PERCENT=80
DISK_WARNING_THRESHOLD_PERCENT=90
```

## 📊 Monitoreo del Worker

El worker reporta automáticamente:
- **Heartbeat** cada 30 segundos a Redis
- **Estado de trabajos** (pending, in_progress, completed, failed)
- **Métricas de rendimiento** (CPU, memoria, jobs activos)
- **Errores y excepciones** con contexto completo

## 🔄 Cola de Trabajos

El worker procesa estos tipos de trabajos:
- `RECORDING_REQUEST` - Iniciar nueva grabación
- `STOP_RECORDING` - Detener grabación específica  
- `STATUS_CHECK` - Verificar estado (futuro)

## 📁 Gestión de Archivos

### Estructura de Salida
```
/recordings/
├── 2025.01.15_14-30-25_tiktoker/
│   ├── tiktoker_2025.01.15_14-30-25.mp4
│   ├── tiktoker_cont2_2025.01.15_14-35-12.mp4
│   └── metadata.json
```

### Características
- **Carpetas organizadas** por fecha y usuario
- **Nombres únicos** con timestamps
- **Continuaciones automáticas** (cont2, cont3, etc.)
- **Metadatos JSON** con información de la grabación

## 🚨 Manejo de Errores

### Errores Recuperables
- **Stream corruption** → Reinicio automático con nueva URL
- **Proxy blocked** → Rotación automática de proxy
- **Network timeout** → Retry con backoff exponencial
- **URL expired** → Validación live + nueva URL

### Errores No Recuperables  
- **User not live** → Finalización limpia
- **Rate limit exceeded** → Espera y retry
- **Disk full** → Alerta y pausa del worker
- **Out of memory** → Limpieza y restart

## 🔧 Herramientas CLI

### Grabación Manual
```bash
python main.py --username tiktoker --mode live
```

### Worker Daemon
```bash
python worker_main.py
```

### Verificación de Estado
```bash
python -c "from services.work_queue_service import work_queue_service; print(work_queue_service.get_system_stats())"
```

## 📝 Logs

Los logs incluyen:
- **Worker lifecycle**: Inicio, trabajos, heartbeat, shutdown
- **Grabación detallada**: URLs, errores FFmpeg, corrupción detectada
- **Sistema**: Uso de recursos, warnings, errores críticos

Ubicación: `logs/worker_log_YYYY-MM-DD.txt`

## 🔒 Seguridad

- ✅ **Cookies aisladas** por worker (cookies.json local)
- ✅ **Proxies independientes** (no compartidos entre workers)  
- ✅ **Validación de entrada** en todos los parámetros de trabajo
- ✅ **Cleanup automático** de archivos temporales

## 🚀 Escalabilidad

Cada worker es **completamente independiente**:
- ✅ Deploy en servidores separados
- ✅ Configuración específica por servidor
- ✅ Balanceeo automático de carga via Redis
- ✅ Sin estado compartido entre workers

## 📞 Soporte

Este es el **Recording Worker** - procesa grabaciones independientemente.
Para gestión de usuarios, necesitas también el **Bot Server** (proyecto separado).

El worker funciona de forma autónoma y puede deployarse en cualquier servidor con acceso a Redis y espacio de almacenamiento.