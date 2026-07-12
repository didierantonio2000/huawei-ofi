# OLT Manager — Documentación de Comandos y API

## ¿Qué es este proyecto?

Sistema web para administrar una OLT Huawei (BARCELONA) via protocolo Telnet raw.
Permite visualizar todas las ONTs, ver su potencia óptica RX/TX, autorizar nuevas ONTs y gestionar VLANs.

---

## Instalación y arranque

```bash
# 1. Instalar dependencias (solo la primera vez)
npm install

# 2. Arrancar el servidor
node server.js

# 3. Abrir en el navegador
http://localhost:3000
```

---

## Páginas web

| URL | Descripción |
|-----|-------------|
| `http://localhost:3000/index.html` | Dashboard principal con tabla de todas las ONTs |
| `http://localhost:3000/autorizar.html` | Autorizar ONTs nuevas y ver ONTs pendientes |

---

## API REST — Referencia completa

### `GET /api/healthz`
Verifica que el servidor esté funcionando.
```json
{ "status": "ok", "olt": "45.162.79.228" }
```

---

### `GET /api/status`
Estado actual de la sincronización en background.
```json
{
  "running": false,
  "step": "Completado",
  "progress": 18,
  "total": 18,
  "lastSync": "2024-01-15T10:30:00.000Z",
  "lastError": null
}
```
- `running`: `true` mientras sincroniza
- `step`: descripción del paso actual
- `progress` / `total`: ONTs procesadas vs total
- `lastSync`: fecha/hora de la última sincronización exitosa
- `lastError`: mensaje de error si falló

---

### `POST /api/sync`
Inicia una sincronización completa en background.
Obtiene lista de ONTs + potencia óptica + VLANs y guarda todo en caché JSON.
- Tarda ~3 segundos por ONT para la potencia óptica
- Para 20 ONTs: ~60–90 segundos en total
- Respuesta inmediata, proceso corre en background

```bash
curl -X POST http://localhost:3000/api/sync
```
```json
{ "ok": true, "message": "Sincronización iniciada" }
```

---

### `GET /api/onts`
Lista todas las ONTs con potencia óptica desde caché (respuesta rápida).
```json
{
  "onts": [
    {
      "frame": "0",
      "slot": "1",
      "pon": "0",
      "ontId": "1",
      "sn": "ZTEGC1234567",
      "runState": "online",
      "configState": "normal",
      "matchState": "match",
      "rxPower": "-22.50",
      "txPower": "2.13",
      "temperature": "45",
      "laserBias": "12.3",
      "voltage": "3.30",
      "optTs": 1705312200000
    }
  ],
  "cached": true,
  "cacheAge": 300,
  "ts": 1705312200000
}
```
- `rxPower`: Potencia de recepción en dBm (null si no hay dato)
- `txPower`: Potencia de transmisión en dBm
- `optTs`: Timestamp Unix cuando se midió la potencia óptica
- `cacheAge`: Segundos desde que se actualizó la caché

**Rangos de potencia RX:**
- `-27 dBm o mejor` → 🟢 Buena señal
- `-27 a -30 dBm` → 🟡 Señal débil
- `peor de -30 dBm` → 🔴 Señal crítica

---

### `GET /api/ont/:frame/:slot/:pon/:id`
Obtiene información completa de una ONT específica + potencia óptica en **tiempo real** (no desde caché).
Tarda ~10 segundos. Actualiza automáticamente el caché óptico.

```bash
curl http://localhost:3000/api/ont/0/1/0/1
```
```json
{
  "kv": {
    "F/S/P": "0/1/0",
    "ONT-ID": "1",
    "Control flag": "active",
    "Run state": "online",
    "Config state": "normal",
    "Match state": "match",
    "Protect side": "no",
    "DBA type": "SR",
    "ONT distance(m)": "320",
    "ONT last down cause": "",
    "ONT optinfo profile name": "null"
  },
  "optical": {
    "Rx optical power(dBm)": "-22.50",
    "Tx optical power(dBm)": "2.13",
    "Laser bias current(mA)": "12.3",
    "Temperature(C)": "45",
    "Voltage(V)": "3.30"
  }
}
```

---

### `GET /api/autofind`
Lista las ONTs detectadas por el OLT pero **no autorizadas** aún.
Útil para ver qué equipos nuevos están conectados esperando registro.

```bash
curl http://localhost:3000/api/autofind
```
```json
{
  "onts": [
    {
      "index": "1",
      "frame": "0",
      "slot": "1",
      "pon": "0",
      "sn": "ZTEGC9876543",
      "vendorId": "ZTEG",
      "ontVer": "V5.0"
    }
  ]
}
```

---

### `GET /api/vlans`
Lista las VLANs configuradas en el OLT. Usa caché de 1 hora.

```bash
curl http://localhost:3000/api/vlans
```
```json
{
  "vlans": [101, 200, 300, 400],
  "cached": true
}
```

---

### `POST /api/ont/autorizar`
Autoriza una nueva ONT en el OLT.

**Cuerpo (JSON):**
```json
{
  "frame": "0",
  "slot": "1",
  "pon": "0",
  "ontId": "5",
  "sn": "ZTEGC9876543",
  "lineProfile": "101",
  "serviceProfile": "101",
  "vlan": "101",
  "desc": "Cliente Juan García"
}
```

```bash
curl -X POST http://localhost:3000/api/ont/autorizar \
  -H "Content-Type: application/json" \
  -d '{"slot":"1","pon":"0","ontId":"5","sn":"ZTEGC9876543","desc":"Juan Garcia"}'
```

**Respuesta éxito:**
```json
{ "ok": true, "message": "ONT autorizada correctamente", "raw": "..." }
```

**Respuesta error:**
```json
{ "ok": false, "message": "Posible error — revisa respuesta OLT", "raw": "..." }
```
- `raw`: Salida literal del OLT, útil para diagnóstico

---

### `GET /api/optical/cache`
Devuelve el archivo JSON de caché de potencia óptica completo.
Útil para exportar datos o diagnóstico.

```bash
curl http://localhost:3000/api/optical/cache
```
```json
{
  "0/1/0/1": {
    "rxPower": "-22.50",
    "txPower": "2.13",
    "laserBias": "12.3",
    "temperature": "45",
    "voltage": "3.30",
    "ts": 1705312200000
  }
}
```

---

### `DELETE /api/cache`
Limpia toda la caché (onts.json, optical.json, vlans.json).
Útil para forzar una sincronización desde cero.

```bash
curl -X DELETE http://localhost:3000/api/cache
```

---

## Archivos de caché

El servidor guarda datos en la carpeta `cache/`:

| Archivo | Contenido | Se actualiza |
|---------|-----------|--------------|
| `cache/onts.json` | Lista de todas las ONTs | Cada `POST /api/sync` |
| `cache/optical.json` | Potencia óptica por ONT | Sync y `GET /api/ont/:f/:s/:p/:id` |
| `cache/vlans.json` | VLANs disponibles | Cada sync o cada 1h |

**Clave de cada ONT en optical.json:** `frame/slot/pon/ontId`  
Ejemplo: `"0/1/0/3"` → ONT frame 0, slot 1, pon 0, id 3

---

## Flujo recomendado

```
1. node server.js          → arranca y hace sync automático
2. Esperar ~60–90s         → sync completo en background
3. Abrir index.html        → tabla con todas las ONTs y potencia óptica
4. Clic "Ver Info" + "Actualizar" → potencia en tiempo real para esa ONT
5. Clic "Sincronizar Todo" → actualiza todo nuevamente
6. autorizar.html          → ver ONTs no registradas y autorizarlas
```

---

## Conexión al OLT (técnico)

- **IP:** 45.162.79.228
- **Puerto:** 2333 (Telnet)
- **Usuario:** smartolt
- **Contraseña:** smart2021
- **Protocolo:** net.Socket raw (NO telnet-client)
- **Negociación IAC:** automática (WILL/DO ECHO, NAWS 512x512)
- **Paginación:** `---- More` → envía espacio automáticamente

**Secuencia de login:**
```
>>User name:    → smartolt
>>User password: → smart2021
BARCELONA>      → enable
BARCELONA#      → config
BARCELONA(config)# → listo
```

**Para potencia óptica (requiere contexto interface):**
```
interface gpon 0/<slot>
display ont optical-info <pon> <ontid>
quit
```

---

## Errores comunes

| Error | Causa | Solución |
|-------|-------|----------|
| `Timeout al conectar` | OLT no accesible | Verificar red/VPN |
| `Sin datos — ejecuta POST /api/sync` | No se ha sincronizado | Hacer clic en "Sincronizar Todo" |
| `Posible error al autorizar` | ONT-ID ya existe o SN incorrecto | Revisar respuesta `raw` |
| Potencia muestra `—` en tabla | Sync sin datos ópticos aún | Clic en "Sincronizar Todo" o "Ver Info" individual |
