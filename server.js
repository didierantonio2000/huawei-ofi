"use strict";
const net  = require("net");
const express = require("express");
const cors    = require("cors");
const path    = require("path");
const fs      = require("fs");
const http    = require("http");
const { Server } = require("socket.io");

const app = express();
app.use(cors());
app.use(express.json());
app.use(express.urlencoded({ extended: true }));
app.use(express.static(path.join(__dirname, "public")));

// ─── Servidor HTTP + Websocket ────────────────────────────────────────────────
// socket.io sirve su propio cliente en /socket.io/socket.io.js automaticamente,
// no hace falta instalar nada aparte en el frontend.
const httpServer = http.createServer(app);
const io = new Server(httpServer, { cors: { origin: "*" } });

io.on("connection", (socket) => {
  // Al conectarse un cliente nuevo (o al recargar la pagina), le mandamos el
  // estado actual de una vez, para que no tenga que esperar al proximo cambio.
  socket.emit("sync:status", syncStatus);
});

// ─── Config OLT ──────────────────────────────────────────────────────────────
const OLT = { host: "45.162.79.228", port: 2333, user: "smartolt", pass: "smart2021" };
const AUTO_SYNC_MINUTES = parseInt(process.env.AUTO_SYNC_MINUTES) || 5;
// Por defecto NO se sincroniza sola: ni al arrancar ni cada X minutos.
// Solo corre cuando el usuario presiona el botón "Sincronizar" / "Sincronizar Todo".
// Si en algún momento se quiere volver a la sync automática, arrancar con
// AUTO_SYNC_ENABLED=1 en el entorno.
const AUTO_SYNC_ENABLED = process.env.AUTO_SYNC_ENABLED === "1";

// ─── Cache en disco ───────────────────────────────────────────────────────────
const CACHE_DIR    = path.join(__dirname, "cache");
const ONT_CACHE    = path.join(CACHE_DIR, "onts.json");
const OPT_CACHE    = path.join(CACHE_DIR, "optical.json");
const VLAN_CACHE   = path.join(CACHE_DIR, "vlans.json");
const DETAIL_CACHE = path.join(CACHE_DIR, "detail.json");
const PROFILE_CACHE = path.join(CACHE_DIR, "profiles.json");
if (!fs.existsSync(CACHE_DIR)) fs.mkdirSync(CACHE_DIR, { recursive: true });

function readCache(file) {
  try { return JSON.parse(fs.readFileSync(file, "utf8")); } catch { return null; }
}
function writeCache(file, data) {
  try { fs.writeFileSync(file, JSON.stringify(data, null, 2)); } catch (e) { console.error("Cache write error:", e.message); }
}

let syncStatus = { running: false, step: "idle", progress: 0, total: 0, lastSync: null, lastError: null, mode: null, changes: null };
let syncRunning = false;

// Reemplaza/mezcla syncStatus y de una vez lo empuja a todos los clientes
// conectados por websocket — así el frontend deja de tener que hacer polling
// para saber el progreso del sync.
function setStatus(next) {
  syncStatus = next;
  io.emit("sync:status", syncStatus);
  return syncStatus;
}
function patchStatus(partial) {
  syncStatus = { ...syncStatus, ...partial };
  io.emit("sync:status", syncStatus);
  return syncStatus;
}

// ─── Telnet Session ───────────────────────────────────────────────────────────
class OltSession {
  constructor(timeout = 40000) {
    this.socket = new net.Socket();
    this.buffer = "";
    this.waiter = null;
    this.connectTimeout = timeout;
    this.closed = false; // NEW: bandera para sesiones muertas
  }

  connect() {
    return new Promise((resolve, reject) => {
      const timer = setTimeout(() => {
        this._cleanup();
        reject(new Error("Timeout al conectar"));
      }, this.connectTimeout);

      this.socket.connect(OLT.port, OLT.host, () => {
        this.socket.write(Buffer.from([0xff, 0xfa, 0x1f, 0x02, 0x00, 0x02, 0x00, 0xff, 0xf0]));
      });

      this.socket.on("data", (chunk) => {
        if (this.closed) return;
        const clean = this._iac(chunk);
        const text  = this._strip(clean.toString("binary"));
        if (text.includes("---- More")) {
          this.socket.write(" ");
          this.buffer += text.replace(/----\s*More[^\n]*(\n|$)/g, "");
        } else {
          this.buffer += text;
        }
        if (this.waiter) this.waiter(this.buffer);
      });

      this.socket.on("error", (e) => { 
        clearTimeout(timer);
        this._cleanup();
        reject(e); 
      });
      
      this.socket.on("close", () => { 
        this._cleanup();
      });

      this.waitFor(">>User name:")
        .then(() => { this.send(OLT.user); return this.waitFor(">>User password:"); })
        .then(() => { this.send(OLT.pass); return this.waitFor("BARCELONA>"); })
        .then(() => { this.send("enable");  return this.waitFor("BARCELONA#"); })
        .then(() => { this.send("config");  return this.waitFor("BARCELONA(config)#"); })
        .then(() => { clearTimeout(timer); resolve(); })
        .catch((e) => { 
          clearTimeout(timer);
          this._cleanup();
          reject(e); 
        });
    });
  }

  _cleanup() {
    this.closed = true;
    this.waiter = null;
  }

  _iac(chunk) {
    const out = []; let i = 0;
    while (i < chunk.length) {
      if (chunk[i] !== 0xff) { out.push(chunk[i++]); continue; }
      const cmd = chunk[i+1], opt = chunk[i+2];
      if (cmd === 0xfd) { this.socket.write(Buffer.from([0xff,0xfb,opt])); i+=3; }
      else if (cmd === 0xfb) { this.socket.write(Buffer.from([0xff,0xfd,opt])); i+=3; }
      else if (cmd === 0xfa) {
        i+=3; while (i<chunk.length && !(chunk[i]===0xff && chunk[i+1]===0xf0)) i++; i+=2;
      } else { i+=3; }
    }
    return Buffer.from(out);
  }
  _strip(s) { return s.replace(/\x1b\[[0-9;]*[a-zA-Z]/g,"").replace(/\r/g,""); }

  send(cmd) { 
    if (!this.closed) this.socket.write(cmd + "\r\n"); 
  }

  waitFor(marker, timeout = 22000) {
    return new Promise((resolve, reject) => {
      if (this.closed) return reject(new Error("Session closed"));
      if (this.buffer.includes(marker)) { 
        const r = this.buffer; 
        this.buffer = ""; 
        return resolve(r); 
      }
      
      const timer = setTimeout(() => { 
        this.waiter = null;
        this._cleanup(); // NEW: limpiar sesión en timeout
        reject(new Error("Timeout esperando: " + marker)); 
      }, timeout);
      
      this.waiter = (buf) => {
        if (this.closed) {
          clearTimeout(timer);
          reject(new Error("Session closed"));
          return;
        }
        if (buf.includes(marker)) {
          clearTimeout(timer); 
          this.waiter = null; 
          const r = this.buffer; 
          this.buffer = ""; 
          resolve(r);
        }
      };
    });
  }

  async cmd(command, marker="BARCELONA(config)#", timeout=22000) {
    this.buffer = ""; 
    this.send(command); 
    return this.waitFor(marker, timeout);
  }

  // Igual que waitFor, pero acepta varios markers posibles (ej: el prompt normal
  // O una pregunta de confirmación tipo "are you sure"). Devuelve cuál hizo match.
  waitForAny(markers, timeout = 22000) {
    return new Promise((resolve, reject) => {
      if (this.closed) return reject(new Error("Session closed"));
      const check = () => markers.find(mk => this.buffer.includes(mk));
      const hit = check();
      if (hit) { 
        const r = this.buffer; 
        this.buffer = ""; 
        return resolve({ raw: r, matched: hit }); 
      }
      
      const timer = setTimeout(() => { 
        this.waiter = null;
        this._cleanup(); // NEW: limpiar sesión en timeout
        reject(new Error("Timeout esperando: " + markers.join(" | "))); 
      }, timeout);
      
      this.waiter = (buf) => {
        if (this.closed) {
          clearTimeout(timer);
          reject(new Error("Session closed"));
          return;
        }
        const m = check();
        if (m) { 
          clearTimeout(timer); 
          this.waiter = null; 
          const r = this.buffer; 
          this.buffer = ""; 
          resolve({ raw: r, matched: m }); 
        }
      };
    });
  }

  async cmdAny(command, markers, timeout=22000) {
    this.buffer = ""; 
    this.send(command); 
    return this.waitForAny(markers, timeout);
  }

  async destroy() {
    this._cleanup();
    try { 
      await new Promise((resolve) => {
        this.socket.destroy();
        setTimeout(resolve, 100);
      });
    } catch {}
  }
}

async function withOltRaw(fn, timeout) {
  const s = new OltSession(timeout);
  await s.connect();
  try { return await fn(s); } finally { await s.destroy(); }
}

// ─── Connection Pool para Telnet (MEJORA #2) ──────────────────────────────────
// En lugar de una cola serializada (una conexión a la vez), ahora usamos un pool
// que permite N conexiones simultáneas. Esto acelera sync, autorizaciones, etc.
// sin saturar la OLT. Por defecto: 3 conexiones máximo.
const TELNET_POOL_SIZE = parseInt(process.env.TELNET_POOL_SIZE) || 3;

class TelnetPool {
  constructor(maxConcurrent = 3) {
    this.maxConcurrent = maxConcurrent;
    this.running = 0;
    this.queue = [];
  }

  async run(fn, timeout) {
    // Si ya hay N conexiones activas, esperar a que se libere una
    while (this.running >= this.maxConcurrent) {
      await new Promise(resolve => this.queue.push(resolve));
    }
    
    this.running++;
    try {
      return await withOltRaw(fn, timeout);
    } finally {
      this.running--;
      const next = this.queue.shift();
      if (next) next();
    }
  }
}

const telnetPool = new TelnetPool(TELNET_POOL_SIZE);

function withOlt(fn, timeout) {
  return telnetPool.run(fn, timeout);
}

// ─── Parsers ──────────────────────────────────────────────────────────────────

// El serial real de una ONT (columna "SN/LOID") siempre es un identificador
// hexadecimal (ej: 4857544304CEEA9E). Cuando la OLT corta una descripción
// larga por falta de ancho de terminal, la línea de continuación queda
// pareciendo una fila nueva y el regex de abajo la matchea igual, pero mete
// texto de la descripción/dirección en la columna de serial (ej: "VECINA",
// "MARIA"). Validamos el formato acá y descartamos esas filas fantasma en
// vez de guardar un serial mentiroso.
const SN_RE = /^[0-9A-Fa-f]{8,20}$/;
function looksLikeSn(s) { return !!s && SN_RE.test(s); }

function parseOntTable(raw) {
  const onts = [];
  const seen = new Set();
  const re = /(\d+)\s*\/\s*(\d+)\s*\/\s*(\d+)\s+(\d+)\s+(\S+)\s+(\S+)\s+(online|offline|dying-gasp|los|losi|\S+)\s+(\S+)\s+(\S+)(?:\s+(.+))?/i;
  for (const line of raw.split("\n")) {
    const m = line.match(re);
    if (m) {
      if (!looksLikeSn(m[5])) continue; // fila corrupta (línea de continuación) — descartar
      const key = m[1] + "/" + m[2] + "/" + m[3] + "/" + m[4];
      if (seen.has(key)) continue; // ya tenemos esta ONT, no duplicar
      seen.add(key);
      onts.push({
        frame:       m[1],
        slot:        m[2],
        pon:         m[3],
        ontId:       m[4],
        sn:          m[5],
        controlFlag: m[6],
        runState:    m[7].toLowerCase(),
        configState: m[8],
        matchState:  m[9].toLowerCase(),
        description: m[10] ? m[10].trim() : null,
      });
    }
  }
  return onts;
}

function parseKV(raw) {
  const r = {};
  for (const line of raw.split("\n")) {
    const i = line.indexOf(":");
    if (i > 0) { const k = line.slice(0, i).trim(); const v = line.slice(i + 1).trim(); if (k && v) r[k] = v; }
  }
  return r;
}

function parseOptical(raw) {
  const r = {};
  for (const line of raw.split("\n")) {
    const m = line.match(/^(.+?)\s*:\s*(.+)$/);
    if (m) r[m[1].trim()] = m[2].trim();
  }
  return r;
}

function parseAutofind(raw) {
  const onts = []; let cur = {};
  for (const line of raw.split("\n")) {
    let m;
    if ((m = line.match(/^\s*Number\s*:\s*(\d+)/i)))  { if (cur.sn) onts.push(cur); cur = { index: m[1] }; continue; }
    if ((m = line.match(/F\/S\/P\s*:\s*(\d+)\/(\d+)\/(\d+)/i))) { cur.frame = m[1]; cur.slot = m[2]; cur.pon = m[3]; continue; }
    if ((m = line.match(/Ont SN\s*:\s*(\S+)/i)))      { cur.sn = m[1]; continue; }
    if ((m = line.match(/VendorID\s*:\s*(\S+)/i)))    { cur.vendorId = m[1]; continue; }
    if ((m = line.match(/OntVer\s*:\s*(.+)/i)))       { cur.ontVer = m[1].trim(); continue; }
    if ((m = line.match(/Password\s*:\s*(.+)/i)))     { cur.password = m[1].trim(); continue; }
  }
  if (cur.sn) onts.push(cur);
  return onts;
}

// Parser genérico para "display ont-lineprofile gpon all" / "display ont-srvprofile gpon all"
// Formato típico (Huawei):
//  ------------------------------------------------------------
//   Profile-ID  Profile-name         Bind-times
//  ------------------------------------------------------------
//   0           HW_LINE              0
//   1           default-profile      3
//  ------------------------------------------------------------
function parseProfileList(raw) {
  const profiles = [];
  for (const line of raw.split("\n")) {
    const t = line.trim();
    if (!t || /^-+$/.test(t)) continue;
    if (/profile-id/i.test(t)) continue; // encabezado
    const m = t.match(/^(\d+)\s+(\S.*)$/);
    if (m) {
      // Quitamos columnas extra (ej. bind-times) dejando solo el nombre del perfil
      const rest = m[2].trim().split(/\s{2,}/)[0].trim();
      profiles.push({ id: m[1], name: rest });
    }
  }
  return profiles;
}

async function getProfiles(session) {
  const lineRaw = await session.cmd("display ont-lineprofile gpon all", "BARCELONA(config)#", 20000).catch(() => "");
  const srvRaw  = await session.cmd("display ont-srvprofile gpon all", "BARCELONA(config)#", 20000).catch(() => "");
  return {
    lineProfiles: parseProfileList(lineRaw),
    srvProfiles:  parseProfileList(srvRaw),
  };
}

async function getAllOntsFull(session) {
  // FIX (cruce serial/descripcion): sin esto, la OLT corta las descripciones
  // largas en la salida de "display ont info ... all" y la línea de
  // continuación queda pareciendo una fila nueva al parser, mezclando
  // serial/estado/descripcion entre ONTs distintas. Mismo fix que ya se
  // aplicaba en autorizar/borrar, pero acá faltaba.
  await session.cmd("terminal width 512", "BARCELONA(config)#", 5000).catch(() => {});
  await session.cmd("terminal length 0", "BARCELONA(config)#", 5000).catch(() => {});
  const boardRaw = await session.cmd("display board 0", "BARCELONA(config)#", 30000);
  const slots = [];
  for (const line of boardRaw.split("\n")) {
    const m = line.match(/^\s*(\d+)\s+(\S+)/);
    if (m && /gpon|H80|H85|H801|H802/i.test(m[2])) slots.push(m[1]);
  }
  const targets = slots.length ? slots : ["0","1","2","3","4","5","6","7"];
  const all = [];
  for (const slot of targets) {
    for (let pon = 0; pon <= 15; pon++) {
      try {
        const raw = await session.cmd("display ont info 0 " + slot + " " + pon + " all", "BARCELONA(config)#", 18000);
        if (/^\s*Failure|invalid command|error/im.test(raw)) { if (pon === 0) break; continue; }
        const parsed = parseOntTable(raw);
        if (!parsed.length && pon > 0) break;
        all.push(...parsed);
      } catch { break; }
    }
  }
  return all;
}

const ontKey = (o) => o.frame + "/" + o.slot + "/" + o.pon + "/" + o.ontId;

// Valores que la OLT pone cuando no hay descripción real configurada
const FAKE_DESC = new Set(["no","—","-","n/a","none","null",""," ","\t"]);
function isFakeDesc(d) { return !d || FAKE_DESC.has(d.trim().toLowerCase()); }

// Blindaje extra: si el valor que se parseó como "Description" en realidad es
// una palabra de estado (online/offline/etc.), es señal de que la respuesta de
// la OLT vino mezclada/incompleta — se descarta en vez de guardar un campo
// mentiroso. (La causa raíz era la falta de cola en withOlt, ya corregida arriba,
// pero esto queda como segunda capa de seguridad).
const STATE_WORDS = new Set(["online","offline","dying-gasp","los","losi","up","down"]);
function safeDescription(desc) {
  if (!desc) return null;
  const t = desc.trim();
  if (!t) return null;
  if (STATE_WORDS.has(t.toLowerCase())) return null;
  return t;
}

// ─── QUICK SYNC (Incremental) ─────────────────────────────────────────────────
async function runQuickSync() {
  if (syncRunning) { console.log("[SYNC] Ya en progreso, se omite"); return; }
  syncRunning = true;
  setStatus({ running: true, step: "Escaneando tabla de ONTs...", progress: 0, total: 0, lastSync: null, lastError: null, mode: "quick", changes: null });
  console.log("[QUICK SYNC] Inicio sincronización incremental");
  const t0 = Date.now();

  try {
    const oldCache = readCache(ONT_CACHE);
    const hasCache = !!oldCache;

    // Paso 1: Escaneo bulk (siempre necesario)
    const onts = await withOlt(getAllOntsFull, 60000);
    writeCache(ONT_CACHE, { ts: Date.now(), onts });
    console.log("[QUICK SYNC] " + onts.length + " ONTs en la OLT (escaneo: " + Math.round((Date.now()-t0)/1000) + "s)");
    // La tabla base (sin óptica/detalle todavía) ya está lista — avisar a los
    // clientes conectados para que puedan mostrar de una vez frame/slot/pon/sn.
    io.emit("onts:base", onts);

    const detCache = readCache(DETAIL_CACHE) || {};
    const optCache = readCache(OPT_CACHE) || {};
    const newKeySet = new Set(onts.map(ontKey));

    // Paso 2: Comparar con caché anterior
    let toFetch = [];
    let removedCount = 0;
    let newCount = 0;
    let stateChangeCount = 0;
    let descMissingCount = 0;

    if (!hasCache) {
      console.log("[QUICK SYNC] Sin caché previo — modo completo");
      patchStatus({ mode: "full" });
      toFetch = onts.map(o => ({ ont: o, needOptical: true, needDetail: true }));
      newCount = onts.length;
    } else {
      const oldMap = new Map(oldCache.onts.map(o => [ontKey(o), o]));

      for (const o of onts) {
        const key = ontKey(o);
        const old = oldMap.get(key);
        let needOptical = false, needDetail = false;

        if (!old) {
          needOptical = true;
          needDetail = true;
          newCount++;
        } else {
          const oldState = (old.runState || "").toLowerCase();
          const newState = (o.runState || "").toLowerCase();

          if (oldState !== newState) {
            stateChangeCount++;
            if (newState === "online" && oldState !== "online") {
              needOptical = true;
            }
          }

          const det = detCache[key];
          const detAge = det && det.ts ? Date.now() - det.ts : Infinity;
          const descOk = det && det.description && !isFakeDesc(det.description);
          if (!descOk && detAge > 3600000) {
            needDetail = true;
            descMissingCount++;
          }
        }

        if (needOptical || needDetail) {
          toFetch.push({ ont: o, needOptical, needDetail });
        }
      }

      for (const o of oldCache.onts) {
        const key = ontKey(o);
        if (!newKeySet.has(key)) {
          delete detCache[key];
          delete optCache[key];
          removedCount++;
        }
      }
    }

    // Paso 3: Fetch solo lo necesario
    patchStatus({ total: toFetch.length });
    const skipped = onts.length - toFetch.length;

    if (toFetch.length === 0) {
      patchStatus({ step: "Sin cambios — todo al día" });
      console.log("[QUICK SYNC] Sin cambios detectados (" + Math.round((Date.now()-t0)/1000) + "s)");
    } else {
      patchStatus({ step: "Actualizando " + toFetch.length + " ONTs..." });
      console.log("[QUICK SYNC] " + toFetch.length + " para actualizar (nuevas:" + newCount + " estado:" + stateChangeCount + " sin-desc:" + descMissingCount + ")");

      const groups = {};
      for (const item of toFetch) {
        const o = item.ont;
        const k = o.frame + "/" + o.slot;
        if (!groups[k]) groups[k] = { frame: o.frame, slot: o.slot, items: [] };
        groups[k].items.push(item);
      }

      let done = 0;
      for (const gpKey of Object.keys(groups)) {
        const gp = groups[gpKey];
        try {
          await withOlt(async (session) => {
            await session.cmd("interface gpon " + gp.frame + "/" + gp.slot, "BARCELONA(config-if-gpon", 12000);
            for (const item of gp.items) {
              const o = item.ont;
              const key = ontKey(o);

              if (item.needOptical) {
                try {
                  const raw = await session.cmd("display ont optical-info " + o.pon + " " + o.ontId, "BARCELONA(config-if-gpon", 12000);
                  const parsed = parseOptical(raw);
                  optCache[key] = {
                    rxPower:    parsed["Rx optical power(dBm)"] || parsed["RX optical power(dBm)"] || null,
                    txPower:    parsed["Tx optical power(dBm)"] || parsed["TX optical power(dBm)"] || null,
                    laserBias:  parsed["Laser bias current(mA)"] || null,
                    temperature: parsed["Temperature(C)"] || parsed["Temperature"] || null,
                    voltage:    parsed["Voltage(V)"] || parsed["Voltage"] || null,
                    ts:         Date.now(),
                  };
                } catch {}
              }

              if (item.needDetail) {
                try {
                  const detRaw = await session.cmd("display ont info " + o.pon + " " + o.ontId, "BARCELONA(config-if-gpon", 15000);
                  const kv = parseKV(detRaw);
                  const rawTemp = kv["Temperature"] || "";
                  detCache[key] = {
                    description:    safeDescription(kv["Description"]) || null,
                    distance:       kv["ONT distance(m)"]      || null,
                    matchState:     (kv["Match state"]  || "").toLowerCase() || null,
                    runState:       (kv["Run state"]    || "").toLowerCase() || null,
                    configState:    kv["Config state"]         || null,
                    lastDownCause:  kv["Last down cause"]      || null,
                    onlineDuration: kv["ONT online duration"]  || null,
                    numericTemp:    rawTemp.replace(/\(C\)/g, "").trim() || null,
                    ts:             Date.now(),
                  };
                } catch {}
              }

              done++;
              // Empujar esta ONT ya actualizada a todos los clientes conectados,
              // así la fila se refresca en vivo sin esperar a que termine todo el sync.
              io.emit("ont:update", mergeOntView(o, optCache, detCache));
              patchStatus({ progress: done, step: "Actualizando: " + done + "/" + toFetch.length });
            }
            await session.cmd("quit", "BARCELONA(config)#", 5000).catch(() => {});
          }, 300000);
        } catch (e) { console.error("[QUICK SYNC] Error grupo " + gpKey + ":", e.message); }
        writeCache(OPT_CACHE, optCache);
        writeCache(DETAIL_CACHE, detCache);
      }
    }

    const elapsed = Math.round((Date.now() - t0) / 1000);
    const changes = { total: onts.length, newOnts: newCount, stateChanges: stateChangeCount, descMissing: descMissingCount, removedOnts: removedCount, updated: toFetch.length, skipped: skipped };

    setStatus({
      running: false, step: "Completado", progress: toFetch.length, total: toFetch.length,
      lastSync: new Date().toISOString(), lastError: null,
      mode: hasCache ? "quick" : "full", changes: changes, elapsed: elapsed,
    });
    console.log("[QUICK SYNC] OK en " + elapsed + "s — " + JSON.stringify(changes));

  } catch (e) {
    console.error("[QUICK SYNC] Error:", e.message);
    setStatus({ running: false, step: "Error", progress: 0, total: 0, lastSync: null, lastError: e.message, mode: "quick", changes: null });
  } finally {
    syncRunning = false;
  }
}

// ─── FULL SYNC (forzada, todo) ───────────────────────────────────────────────
async function runFullSync() {
  if (syncRunning) { console.log("[SYNC] Ya en progreso, se omite"); return; }
  syncRunning = true;
  setStatus({ running: true, step: "Conectando al OLT...", progress: 0, total: 0, lastSync: null, lastError: null, mode: "full", changes: null });
  console.log("[FULL SYNC] Inicio sincronización completa");
  const t0 = Date.now();

  try {
    patchStatus({ step: "Obteniendo lista de ONTs..." });
    const onts = await withOlt(getAllOntsFull, 60000);
    writeCache(ONT_CACHE, { ts: Date.now(), onts });
    patchStatus({ total: onts.length });
    console.log("[FULL SYNC] " + onts.length + " ONTs encontradas");
    io.emit("onts:base", onts);

    const detCache = readCache(DETAIL_CACHE) || {};
    const validKeys = new Set(onts.map(ontKey));
    for (const k of Object.keys(detCache)) { if (!validKeys.has(k)) delete detCache[k]; }

    const opticalMap = { ...readCache(OPT_CACHE) || {} };
    const groups = {};
    for (const o of onts) {
      const k = o.frame + "/" + o.slot + "/" + o.pon;
      if (!groups[k]) groups[k] = { frame: o.frame, slot: o.slot, pon: o.pon, onts: [] };
      groups[k].onts.push(o);
    }

    patchStatus({ step: "Sincronizando óptica + detalles..." });
    let done = 0;
    for (const gpKey of Object.keys(groups)) {
      const gp = groups[gpKey];
      try {
        await withOlt(async (session) => {
          await session.cmd("interface gpon " + gp.frame + "/" + gp.slot, "BARCELONA(config-if-gpon", 12000);
          for (const o of gp.onts) {
            const key = ontKey(o);
            try {
              const raw = await session.cmd("display ont optical-info " + gp.pon + " " + o.ontId, "BARCELONA(config-if-gpon", 12000);
              const parsed = parseOptical(raw);
              opticalMap[key] = {
                rxPower:    parsed["Rx optical power(dBm)"] || parsed["RX optical power(dBm)"] || null,
                txPower:    parsed["Tx optical power(dBm)"] || parsed["TX optical power(dBm)"] || null,
                laserBias:  parsed["Laser bias current(mA)"] || null,
                temperature: parsed["Temperature(C)"] || parsed["Temperature"] || null,
                voltage:    parsed["Voltage(V)"] || parsed["Voltage"] || null,
                ts:         Date.now(),
              };
            } catch {}
            try {
              const detRaw = await session.cmd("display ont info " + gp.pon + " " + o.ontId, "BARCELONA(config-if-gpon", 15000);
              const kv = parseKV(detRaw);
              const rawTemp = kv["Temperature"] || "";
              detCache[key] = {
                description:    safeDescription(kv["Description"]) || null,
                distance:       kv["ONT distance(m)"]      || null,
                matchState:     (kv["Match state"]  || "").toLowerCase() || null,
                runState:       (kv["Run state"]    || "").toLowerCase() || null,
                configState:    kv["Config state"]         || null,
                lastDownCause:  kv["Last down cause"]      || null,
                onlineDuration: kv["ONT online duration"]  || null,
                numericTemp:    rawTemp.replace(/\(C\)/g, "").trim() || null,
                ts:             Date.now(),
              };
            } catch {}
            done++;
            io.emit("ont:update", mergeOntView(o, opticalMap, detCache));
            patchStatus({ progress: done, step: "Óptica + Detalles: " + done + "/" + onts.length });
          }
          await session.cmd("quit", "BARCELONA(config)#", 5000).catch(() => {});
        }, 300000);
      } catch (e) { console.error("[FULL SYNC] Error grupo " + gpKey + ":", e.message); }
      writeCache(OPT_CACHE, opticalMap);
      writeCache(DETAIL_CACHE, detCache);
    }

    patchStatus({ step: "Obteniendo VLANs..." });
    try {
      const vlans = await withOlt(async (session) => {
        const raw = await session.cmd("display vlan all", "BARCELONA(config)#", 30000);
        const found = new Set();
        for (const line of raw.split("\n")) {
          const ms = line.match(/\b(\d{2,4})\b/g);
          if (ms) ms.forEach(v => { const n = parseInt(v); if (n >= 100 && n <= 4094) found.add(n); });
        }
        return [...found].sort((a, b) => a - b);
      }, 60000);
      writeCache(VLAN_CACHE, { ts: Date.now(), vlans: vlans });
    } catch (e) { console.error("[FULL SYNC] Error VLANs:", e.message); }

    const elapsed = Math.round((Date.now() - t0) / 1000);
    setStatus({
      running: false, step: "Completado", progress: done, total: onts.length,
      lastSync: new Date().toISOString(), lastError: null,
      mode: "full", changes: { total: onts.length, updated: onts.length, skipped: 0, newOnts: 0, removedOnts: 0, stateChanges: 0, descMissing: 0 }, elapsed: elapsed,
    });
    console.log("[FULL SYNC] OK en " + elapsed + "s");
  } catch (e) {
    console.error("[FULL SYNC] Error fatal:", e.message);
    setStatus({ running: false, step: "Error", progress: 0, total: 0, lastSync: null, lastError: e.message, mode: "full", changes: null });
  } finally {
    syncRunning = false;
  }
}

// ─── Rutas API ────────────────────────────────────────────────────────────────

app.get("/api/healthz", (_, res) => res.json({ status: "ok", olt: OLT.host }));
app.get("/api/status", (_, res) => res.json(syncStatus));

// Quick sync (por defecto — botón verde "Sincronizar")
app.post("/api/sync", (req, res) => {
  if (syncRunning) return res.json({ ok: false, message: "Sincronización en progreso" });
  runQuickSync().catch(e => console.error(e.message));
  res.json({ ok: true, message: "Sincronización rápida iniciada", mode: "quick" });
});

// Full sync (manual — botón amarillo "Sincronizar Todo")
app.post("/api/sync/full", (req, res) => {
  if (syncRunning) return res.json({ ok: false, message: "Sincronización en progreso" });
  runFullSync().catch(e => console.error(e.message));
  res.json({ ok: true, message: "Sincronización completa iniciada", mode: "full" });
});

// Combina la fila cruda de la tabla con lo que haya en cache de óptica/detalle.
// La usa tanto GET /api/onts como los emits de websocket "ont:update", para que
// el objeto que ve el navegador sea siempre igual venga por donde venga.
function mergeOntView(o, optCache, detCache) {
  const key = ontKey(o);
  const opt = optCache[key] || {};
  const det = detCache[key] || {};
  const tableDesc = isFakeDesc(o.description) ? null : o.description;
  return {
    ...o,
    rxPower:     opt.rxPower     || null,
    txPower:     opt.txPower     || null,
    temperature: opt.temperature || det.numericTemp || null,
    laserBias:   opt.laserBias   || null,
    voltage:     opt.voltage     || null,
    optTs:       opt.ts          || null,
    description: tableDesc || det.description || null,
    distance:    det.distance    || null,
    matchState:  det.matchState  || o.matchState  || null,
    runState:    det.runState    || o.runState    || null,
  };
}

app.get("/api/onts", (req, res) => {
  const ontCache = readCache(ONT_CACHE);
  const optCache = readCache(OPT_CACHE) || {};
  const detCache = readCache(DETAIL_CACHE) || {};
  if (!ontCache) return res.json({ onts: [], cached: false, message: "Sin datos — ejecuta POST /api/sync primero" });

  const onts = ontCache.onts.map(o => mergeOntView(o, optCache, detCache));
  res.json({ onts, cached: true, cacheAge: Math.round((Date.now() - ontCache.ts) / 1000), ts: ontCache.ts });
});

app.get("/api/ont/nextid/:f/:s/:p", (req, res) => {
  const { f, s, p } = req.params;
  const profCache = readCache(PROFILE_CACHE);
  const profiles = { lineProfiles: profCache ? profCache.lineProfiles : null, srvProfiles: profCache ? profCache.srvProfiles : null };

  const ontCache = readCache(ONT_CACHE);
  if (!ontCache) return res.json({ nextId: 0, used: [], ...profiles });

  const used = new Set(
    ontCache.onts
      .filter(o => String(o.frame) === String(f) && String(o.slot) === String(s) && String(o.pon) === String(p))
      .map(o => parseInt(o.ontId))
  );
  let next = 0;
  while (used.has(next) && next < 128) next++;
  res.json({ nextId: next, used: [...used].sort((a, b) => a - b), ...profiles });
});

app.get("/api/ont/:f/:s/:p/:id", async (req, res) => {
  const { f, s, p, id } = req.params;
  try {
    const detail = await withOlt(async (session) => {
      const raw = await session.cmd("display ont info " + f + " " + s + " " + p + " " + id, "BARCELONA(config)#", 20000);
      const kv = parseKV(raw);
      let optical = {};
      try {
        await session.cmd("interface gpon " + f + "/" + s, "BARCELONA(config-if-gpon", 10000);
        const optRaw = await session.cmd("display ont optical-info " + p + " " + id, "BARCELONA(config-if-gpon", 15000);
        optical = parseOptical(optRaw);

        const optCache = readCache(OPT_CACHE) || {};
        const key = f + "/" + s + "/" + p + "/" + id;
        optCache[key] = {
          rxPower:    optical["Rx optical power(dBm)"] || optical["RX optical power(dBm)"] || null,
          txPower:    optical["Tx optical power(dBm)"] || optical["TX optical power(dBm)"] || null,
          laserBias:  optical["Laser bias current(mA)"] || null,
          temperature: optical["Temperature(C)"] || optical["Temperature"] || null,
          voltage:    optical["Voltage(V)"] || optical["Voltage"] || null,
          ts:         Date.now(),
        };
        writeCache(OPT_CACHE, optCache);
        await session.cmd("quit", "BARCELONA(config)#", 5000);
      } catch {}

      const detCache = readCache(DETAIL_CACHE) || {};
      const detKey = f + "/" + s + "/" + p + "/" + id;
      const rawTemp = kv["Temperature"] || "";
      detCache[detKey] = {
        description:    safeDescription(kv["Description"]) || null,
        distance:       kv["ONT distance(m)"]      || null,
        matchState:     (kv["Match state"]  || "").toLowerCase() || null,
        runState:       (kv["Run state"]    || "").toLowerCase() || null,
        configState:    kv["Config state"]         || null,
        lastDownCause:  kv["Last down cause"]      || null,
        onlineDuration: kv["ONT online duration"]  || null,
        numericTemp:    rawTemp.replace(/\(C\)/g, "").trim() || null,
        ts:             Date.now(),
      };
      writeCache(DETAIL_CACHE, detCache);
      return { kv, optical };
    });
    res.json(detail);

    // Avisar por websocket a todas las pestañas abiertas (esta u otras) que
    // esta ONT tiene datos nuevos, para que refresquen la fila sin recargar.
    try {
      const ontCache = readCache(ONT_CACHE);
      const row = ontCache && ontCache.onts.find(o => String(o.frame) === String(f) && String(o.slot) === String(s) && String(o.pon) === String(p) && String(o.ontId) === String(id));
      if (row) {
        const optCache = readCache(OPT_CACHE) || {};
        const detCache2 = readCache(DETAIL_CACHE) || {};
        io.emit("ont:update", mergeOntView(row, optCache, detCache2));
      }
    } catch (e2) { console.error("[WS] Error emitiendo ont:update:", e2.message); }
  } catch (e) { res.status(500).json({ error: e.message }); }
});

app.get("/api/autofind", async (req, res) => {
  try {
    const cachedProfiles = readCache(PROFILE_CACHE);
    const profilesFresh = cachedProfiles && (Date.now() - cachedProfiles.ts) < 3600000;

    const result = await withOlt(async (session) => {
      const raw = await session.cmd("display ont autofind all", "BARCELONA(config)#", 35000);
      const onts = parseAutofind(raw);

      let profiles;
      if (profilesFresh) {
        profiles = { lineProfiles: cachedProfiles.lineProfiles, srvProfiles: cachedProfiles.srvProfiles };
      } else {
        profiles = await getProfiles(session);
        writeCache(PROFILE_CACHE, { ts: Date.now(), ...profiles });
      }

      return { onts, ...profiles };
    }, 60000);

    res.json(result);
  } catch (e) { res.status(500).json({ error: e.message }); }
});

// Perfiles solos, por si el frontend los necesita sin correr autofind
app.get("/api/profiles", async (req, res) => {
  const force = req.query.force === "1" || req.query.force === "true";
  const cached = !force ? readCache(PROFILE_CACHE) : null;
  if (cached && (Date.now() - cached.ts) < 3600000) {
    return res.json({ lineProfiles: cached.lineProfiles, srvProfiles: cached.srvProfiles, cached: true });
  }
  try {
    const profiles = await withOlt(getProfiles, 40000);
    writeCache(PROFILE_CACHE, { ts: Date.now(), ...profiles });
    res.json({ ...profiles, cached: false });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

app.get("/api/vlans", async (req, res) => {
  const cached = readCache(VLAN_CACHE);
  if (cached && (Date.now() - cached.ts) < 3600000) return res.json({ vlans: cached.vlans, cached: true });
  try {
    const vlans = await withOlt(async (session) => {
      const raw = await session.cmd("display vlan all", "BARCELONA(config)#", 30000);
      const found = new Set();
      for (const line of raw.split("\n")) {
        const ms = line.match(/\b(\d{2,4})\b/g);
        if (ms) ms.forEach(v => { const n = parseInt(v); if (n >= 100 && n <= 4094) found.add(n); });
      }
      return [...found].sort((a, b) => a - b);
    }, 60000);
    writeCache(VLAN_CACHE, { ts: Date.now(), vlans: vlans });
    res.json({ vlans, cached: false });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

app.post("/api/ont/autorizar", async (req, res) => {
  const {
    frame = "0", slot, pon, ontId, sn,
    lineProfile = "101", serviceProfile = "1",
    desc = "", vlan, authType = "sn-auth",
    // NUEVO: modo de autorización. "router" (default) = la ONT enruta ella
    // misma (WAN por OMCI). "bridge" = la ONT solo transporta el VLAN del
    // cliente hacia el OLT (service-port), sin rutear.
    mode = "router",
    // NUEVO: solo aplica en modo router. "dhcp" (default) o "pppoe".
    wanIpMode = "dhcp",
    pppoeUser = "",
    pppoePass = "",
  } = req.body;

  if (!slot || !pon || !ontId || !sn) {
    return res.status(400).json({ error: "Faltan: slot, pon, ontId, sn" });
  }
  // NUEVO: sin VLAN no hay forma de autorizar servicio real, ni en bridge
  // ni en router — antes se aceptaba vacío y por eso nunca quedaba autorizada.
  if (!vlan) {
    return res.status(400).json({ error: "Falta la VLAN (se obtiene de /api/vlans o se tipea manual)" });
  }

  const bridgeMode = mode === "bridge";

  // 1. Limpiamos comillas internas por seguridad
  const cleanDesc = (desc || sn).replace(/["']/g, "");

  const authMethod = authType === "password-auth" ? "password-auth" : "sn-auth";

  // 2. Alta de la ONT a nivel OMCI/línea (esto YA andaba bien, se deja igual)
  const ontCmd = [
    "ont", "add", pon,
    ontId,
    authMethod, sn,
    "omci",
    "ont-lineprofile-id", lineProfile,
    "ont-srvprofile-id", serviceProfile,
    "desc", `"${cleanDesc}"`
  ].join(" ");

  try {
    const result = await withOlt(async (session) => {

      await session.cmd("terminal width 512", "BARCELONA(config)#", 5000).catch(() => {});
      await session.cmd("terminal length 0", "BARCELONA(config)#", 5000).catch(() => {});

      // Entrar a la interfaz
      await session.cmd("interface gpon " + frame + "/" + slot, "BARCELONA(config-if-gpon", 12000);

      // Alta de la ONT
      const addRaw = await session.cmd(ontCmd, "BARCELONA(config-if-gpon", 25000);
      const addOk = !/failure|error|fail\b/i.test(addRaw);

      // 3. NUEVO: paso que faltaba — sin esto la ONT queda "agregada" pero
      // sin ningún servicio asociado, por eso no autorizaba ni por VLAN ni
      // en modo router.
      let serviceCmd = "";
      let serviceRaw = "";
      let serviceOk = true;

      if (addOk) {
        if (bridgeMode) {
          // ── MODO BRIDGE ──────────────────────────────────────────────
          // service-port se ejecuta a nivel (config)#, no dentro de
          // "interface gpon", así que salimos de la interfaz primero.
          await session.cmd("quit", "BARCELONA(config)#", 5000).catch(() => {});
          serviceCmd = [
            "service-port", "vlan", vlan,
            "gpon", frame + "/" + slot + "/" + pon,
            "ont", ontId, "gemport", "0",
            "multi-service", "user-vlan", vlan
          ].join(" ");
          try {
            serviceRaw = await session.cmd(serviceCmd, "BARCELONA(config)#", 20000);
          } catch (e) { serviceRaw = e.message; serviceOk = false; }
          serviceOk = serviceOk && !/failure|error/i.test(serviceRaw);
        } else {
          // ── MODO ROUTER ──────────────────────────────────────────────
          // wan-config configura la WAN de la propia ONT vía OMCI, dentro
          // de la interfaz gpon. Con DHCP no hace falta usuario/clave; con
          // PPPoE se manda user/pass (por defecto el propio serial si no
          // se especifica).
          const wanParts = [
            "ont", "wan-config", ontId,
            "wan-index", "1",
            "mode", "internet-router",
            "vlan", vlan,
            "ip-mode", wanIpMode
          ];
          if (wanIpMode === "pppoe") {
            wanParts.push("pppoe-user", pppoeUser || sn, "pppoe-password", pppoePass || sn);
          }
          serviceCmd = wanParts.join(" ");
          try {
            serviceRaw = await session.cmd(serviceCmd, "BARCELONA(config-if-gpon", 20000);
          } catch (e) { serviceRaw = e.message; serviceOk = false; }
          serviceOk = serviceOk && !/failure|error/i.test(serviceRaw);
          await session.cmd("quit", "BARCELONA(config)#", 5000).catch(() => {});
        }
      } else {
        await session.cmd("quit", "BARCELONA(config)#", 5000).catch(() => {});
      }

      return {
        addOk, serviceOk,
        raw: addRaw + (serviceRaw ? "\n" + serviceRaw : ""),
        cmd: ontCmd + (serviceCmd ? "  |  " + serviceCmd : ""),
      };
    }, 90000);

    if (result.addOk && result.serviceOk) {
      res.json({
        ok: true,
        message: "ONT autorizada correctamente en modo " + (bridgeMode ? "BRIDGE (VLAN)" : "ROUTER"),
        raw: result.raw, cmd: result.cmd,
      });
    } else if (result.addOk && !result.serviceOk) {
      // La ONT se dio de alta pero el paso de servicio (VLAN/WAN) falló.
      // OJO: la sintaxis exacta de "service-port" / "ont wan-config" puede
      // variar según la versión de firmware de esta OLT puntual. Si esto
      // devuelve error de sintaxis, hay que ajustar serviceCmd con la
      // respuesta cruda (result.raw) que se devuelve acá abajo.
      res.status(207).json({
        ok: false,
        message: "ONT registrada pero falló la config de " + (bridgeMode ? "VLAN (service-port)" : "WAN (modo router)") + " — revisá 'raw' para ver el error exacto de la OLT",
        raw: result.raw, cmd: result.cmd,
      });
    } else {
      res.status(400).json({ ok: false, message: "Posible error — revisa respuesta OLT", raw: result.raw, cmd: result.cmd });
    }
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});
app.delete("/api/ont/:f/:s/:p/:id", async (req, res) => {
  const { f, s, p, id } = req.params;
  try {
    const result = await withOlt(async (session) => {
      // Ajustar ancho de terminal para evitar que la OLT corte el comando
      await session.cmd("terminal width 512", "BARCELONA(config)#", 5000).catch(() => {});
      await session.cmd("terminal length 0", "BARCELONA(config)#", 5000).catch(() => {});

      // Entrar a la interfaz de la ONT que vamos a borrar
      await session.cmd("interface gpon " + f + "/" + s, "BARCELONA(config-if-gpon", 12000);

      // La OLT puede responder directo con el prompt normal, o pedir confirmación
      // ("Are you sure to delete the ONT? (y/n)[n]:") antes de volver al prompt.
      const step1 = await session.cmdAny(
        "ont delete " + p + " " + id,
        ["BARCELONA(config-if-gpon", "are you sure", "Are you sure"],
        15000
      );

      let raw = step1.raw;
      if (/sure/i.test(step1.raw)) {
        session.send("y");
        const step2 = await session.waitFor("BARCELONA(config-if-gpon", 10000).catch(() => "");
        raw += step2;
      }

      await session.cmd("quit", "BARCELONA(config)#", 5000).catch(() => {});

      return {
        success: !/failure|error/i.test(raw),
        raw,
      };
    }, 60000);

    // Limpiar la ONT eliminada de los cachés locales
    const ontCache = readCache(ONT_CACHE);
    if (ontCache) {
      ontCache.onts = ontCache.onts.filter(o => !(String(o.frame) === String(f) && String(o.slot) === String(s) && String(o.pon) === String(p) && String(o.ontId) === String(id)));
      writeCache(ONT_CACHE, ontCache);
    }
    const optCache = readCache(OPT_CACHE) || {};
    delete optCache[f + "/" + s + "/" + p + "/" + id];
    writeCache(OPT_CACHE, optCache);
    const detCache = readCache(DETAIL_CACHE) || {};
    delete detCache[f + "/" + s + "/" + p + "/" + id];
    writeCache(DETAIL_CACHE, detCache);

    if (result.success) {
      io.emit("ont:removed", { frame: f, slot: s, pon: p, ontId: id });
      res.json({ ok: true, message: "ONT " + f + "/" + s + "/" + p + "/" + id + " eliminada", raw: result.raw });
    } else {
      res.status(400).json({ ok: false, message: "Posible error al eliminar — revisa respuesta OLT", raw: result.raw });
    }
  } catch (e) { res.status(500).json({ error: e.message }); }
});

app.get("/api/optical/cache", (_, res) => res.json(readCache(OPT_CACHE) || {}));
app.delete("/api/cache", (_, res) => {
  [ONT_CACHE, OPT_CACHE, VLAN_CACHE, DETAIL_CACHE, PROFILE_CACHE].forEach(f => { try { fs.unlinkSync(f); } catch {} });
  res.json({ ok: true, message: "Caché limpiada" });
});

// ─── Arranque ─────────────────────────────────────────────────────────────────
const PORT = process.env.PORT || 3000;
httpServer.listen(PORT, () => {
  console.log("╔══════════════════════════════════════════════════╗");
  console.log("║  OLT Manager corriendo en :" + PORT + "                     ║");
  console.log("║  http://localhost:" + PORT + "                              ║");
  console.log("║  OLT: " + OLT.host + ":" + OLT.port + "                        ║");
  console.log("║  Websocket: activo (mismo puerto, /socket.io)     ║");
  console.log("║  Auto-sync: " + (AUTO_SYNC_ENABLED ? ("cada " + AUTO_SYNC_MINUTES + " min (incremental)") : "DESACTIVADO — solo manual (botón)") + "       ║");
  console.log("╚══════════════════════════════════════════════════╝");

  if (AUTO_SYNC_ENABLED) {
    const c = readCache(ONT_CACHE);
    if (!c || (Date.now() - c.ts) > 1800000) {
      console.log("\n[AUTO] Primera sync en 3s...");
      setTimeout(() => runQuickSync().catch(e => console.error(e.message)), 3000);
    } else {
      console.log("\n[AUTO] Caché de hace " + Math.round((Date.now()-c.ts)/60000) + "min, próxima auto-sync en " + AUTO_SYNC_MINUTES + " min");
    }

    setInterval(() => {
      if (!syncRunning) {
        console.log("\n[AUTO] Quick sync programada (" + AUTO_SYNC_MINUTES + " min)...");
        runQuickSync().catch(e => console.error(e.message));
      }
    }, AUTO_SYNC_MINUTES * 60000);
  } else {
    console.log("\n[AUTO] Sync automática desactivada. La tabla se sirve desde caché hasta que se presione 'Sincronizar'.");
  }
});