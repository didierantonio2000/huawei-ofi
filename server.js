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
const httpServer = http.createServer(app);
const io = new Server(httpServer, { cors: { origin: "*" } });

// Cada cliente se une a una "room" por OLT (olt:<id>) para recibir solo los
// eventos de la OLT que tiene seleccionada en ese momento. Puede cambiar de
// OLT sin recargar la página, así que también manejamos el cambio de room.
io.on("connection", (socket) => {
  socket.currentOltRoom = null;

  socket.on("olt:join", (oltId) => {
    if (!oltId) return;
    if (socket.currentOltRoom) socket.leave(socket.currentOltRoom);
    const room = "olt:" + oltId;
    socket.join(room);
    socket.currentOltRoom = room;
    socket.emit("sync:status", { oltId, ...ensureStatus(oltId) });
  });
});

// ─── Config multi-OLT ──────────────────────────────────────────────────────
// Antes había una sola OLT fija en el código. Ahora la lista de OLTs vive en
// cache/olts.json y se puede administrar en caliente (agregar/eliminar) desde
// la interfaz, sin tocar el código ni reiniciar el server.
const AUTO_SYNC_MINUTES = parseInt(process.env.AUTO_SYNC_MINUTES) || 5;
const AUTO_SYNC_ENABLED = process.env.AUTO_SYNC_ENABLED === "1";

const CACHE_DIR  = path.join(__dirname, "cache");
const OLTS_FILE  = path.join(CACHE_DIR, "olts.json");
if (!fs.existsSync(CACHE_DIR)) fs.mkdirSync(CACHE_DIR, { recursive: true });

function readCache(file) {
  try { return JSON.parse(fs.readFileSync(file, "utf8")); } catch { return null; }
}
function writeCache(file, data) {
  try { fs.writeFileSync(file, JSON.stringify(data, null, 2)); } catch (e) { console.error("Cache write error:", e.message); }
}

function slugify(s) {
  return (s || "olt").toString().trim().toLowerCase()
    .normalize("NFD").replace(/[\u0300-\u036f]/g, "")
    .replace(/[^a-z0-9]+/g, "-").replace(/^-+|-+$/g, "") || "olt";
}

function loadOlts() {
  let data = readCache(OLTS_FILE);
  if (!data || !Array.isArray(data.olts) || !data.olts.length) {
    // Semilla con la OLT que ya existía antes de multi-OLT, para no perder
    // la config que el usuario ya tenía funcionando.
    data = {
      olts: [
        { id: "barcelona", name: "BARCELONA", host: "45.162.79.228", port: 2333, user: "smartolt", pass: "smart2021", prompt: "BARCELONA" },
      ],
    };
    writeCache(OLTS_FILE, data);
  }
  return data.olts;
}
function saveOlts(list) { writeCache(OLTS_FILE, { olts: list }); }
function getOltById(id) { return loadOlts().find((o) => o.id === id); }

// Cache en disco por OLT (cache/<oltId>/*.json)
function cachePaths(oltId) {
  const dir = path.join(CACHE_DIR, oltId);
  if (!fs.existsSync(dir)) fs.mkdirSync(dir, { recursive: true });
  return {
    ONT:     path.join(dir, "onts.json"),
    OPT:     path.join(dir, "optical.json"),
    VLAN:    path.join(dir, "vlans.json"),
    DETAIL:  path.join(dir, "detail.json"),
    PROFILE: path.join(dir, "profiles.json"),
    OFFLINE: path.join(dir, "offline.json"), // desde cuándo está offline cada ONT (visto por este server)
  };
}

// ─── Estado de sincronización (uno por OLT) ──────────────────────────────────
const syncStatusMap = {};
const syncRunningMap = {};

function ensureStatus(oltId) {
  if (!syncStatusMap[oltId]) {
    syncStatusMap[oltId] = { running: false, step: "idle", progress: 0, total: 0, lastSync: null, lastError: null, mode: null, changes: null };
  }
  return syncStatusMap[oltId];
}
function setStatus(oltId, next) {
  syncStatusMap[oltId] = next;
  io.to("olt:" + oltId).emit("sync:status", { oltId, ...syncStatusMap[oltId] });
  return syncStatusMap[oltId];
}
function patchStatus(oltId, partial) {
  syncStatusMap[oltId] = { ...ensureStatus(oltId), ...partial };
  io.to("olt:" + oltId).emit("sync:status", { oltId, ...syncStatusMap[oltId] });
  return syncStatusMap[oltId];
}

// ─── Telnet Session ───────────────────────────────────────────────────────────
// Ahora recibe la config de la OLT (host/port/user/pass/prompt) en vez de usar
// una constante global — así una misma clase sirve para cualquier OLT Huawei
// registrada, siempre que use el mismo estilo de CLI (usuario/clave + enable +
// config, con el nombre de equipo como prompt).
class OltSession {
  constructor(olt, timeout = 40000) {
    this.olt = olt;
    this.socket = new net.Socket();
    this.buffer = "";
    this.waiter = null;
    this.connectTimeout = timeout;
    this.closed = false;
  }

  connect() {
    const P = this.olt.prompt;
    return new Promise((resolve, reject) => {
      const timer = setTimeout(() => {
        this._cleanup();
        reject(new Error("Timeout al conectar"));
      }, this.connectTimeout);

      this.socket.connect(this.olt.port, this.olt.host, () => {
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
        .then(() => { this.send(this.olt.user); return this.waitFor(">>User password:"); })
        .then(() => { this.send(this.olt.pass); return this.waitFor(P + ">"); })
        .then(() => { this.send("enable");  return this.waitFor(P + "#"); })
        .then(() => { this.send("config");  return this.waitFor(P + "(config)#"); })
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
        this._cleanup();
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

  async cmd(command, marker, timeout=22000) {
    marker = marker || (this.olt.prompt + "(config)#");
    this.buffer = "";
    this.send(command);
    return this.waitFor(marker, timeout);
  }

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
        this._cleanup();
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

async function withOltRaw(olt, fn, timeout) {
  const s = new OltSession(olt, timeout);
  await s.connect();
  try { return await fn(s); } finally { await s.destroy(); }
}

// ─── Connection Pool para Telnet (uno por OLT) ────────────────────────────────
const TELNET_POOL_SIZE = parseInt(process.env.TELNET_POOL_SIZE) || 3;

class TelnetPool {
  constructor(maxConcurrent = 3) {
    this.maxConcurrent = maxConcurrent;
    this.running = 0;
    this.queue = [];
  }

  async run(olt, fn, timeout) {
    while (this.running >= this.maxConcurrent) {
      await new Promise(resolve => this.queue.push(resolve));
    }

    this.running++;
    try {
      return await withOltRaw(olt, fn, timeout);
    } finally {
      this.running--;
      const next = this.queue.shift();
      if (next) next();
    }
  }
}

const telnetPools = {};
function getPool(oltId) {
  if (!telnetPools[oltId]) telnetPools[oltId] = new TelnetPool(TELNET_POOL_SIZE);
  return telnetPools[oltId];
}

// fn recibe (session, olt)
function withOlt(oltId, fn, timeout) {
  const olt = getOltById(oltId);
  if (!olt) return Promise.reject(new Error("OLT no encontrada: " + oltId));
  return getPool(oltId).run(olt, (session) => fn(session, olt), timeout);
}

// ─── Parsers ──────────────────────────────────────────────────────────────────

const SN_RE = /^[0-9A-Fa-f]{8,20}$/;
function looksLikeSn(s) { return !!s && SN_RE.test(s); }

function parseOntTable(raw) {
  const onts = [];
  const seen = new Set();
  const re = /(\d+)\s*\/\s*(\d+)\s*\/\s*(\d+)\s+(\d+)\s+(\S+)\s+(\S+)\s+(online|offline|dying-gasp|los|losi|\S+)\s+(\S+)\s+(\S+)(?:\s+(.+))?/i;
  for (const line of raw.split("\n")) {
    const m = line.match(re);
    if (m) {
      if (!looksLikeSn(m[5])) continue;
      const key = m[1] + "/" + m[2] + "/" + m[3] + "/" + m[4];
      if (seen.has(key)) continue;
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

function parseProfileList(raw) {
  const profiles = [];
  for (const line of raw.split("\n")) {
    const t = line.trim();
    if (!t || /^-+$/.test(t)) continue;
    if (/profile-id/i.test(t)) continue;
    const m = t.match(/^(\d+)\s+(\S.*)$/);
    if (m) {
      const rest = m[2].trim().split(/\s{2,}/)[0].trim();
      profiles.push({ id: m[1], name: rest });
    }
  }
  return profiles;
}

async function getProfiles(session, olt) {
  const P = olt.prompt;
  const lineRaw = await session.cmd("display ont-lineprofile gpon all", P + "(config)#", 20000).catch(() => "");
  const srvRaw  = await session.cmd("display ont-srvprofile gpon all", P + "(config)#", 20000).catch(() => "");
  return {
    lineProfiles: parseProfileList(lineRaw),
    srvProfiles:  parseProfileList(srvRaw),
  };
}

async function getAllOntsFull(session, olt) {
  const P = olt.prompt;
  await session.cmd("terminal width 512", P + "(config)#", 5000).catch(() => {});
  await session.cmd("terminal length 0", P + "(config)#", 5000).catch(() => {});
  const boardRaw = await session.cmd("display board 0", P + "(config)#", 30000);
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
        const raw = await session.cmd("display ont info 0 " + slot + " " + pon + " all", P + "(config)#", 18000);
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

const FAKE_DESC = new Set(["no","—","-","n/a","none","null",""," ","\t"]);
function isFakeDesc(d) { return !d || FAKE_DESC.has(d.trim().toLowerCase()); }

const STATE_WORDS = new Set(["online","offline","dying-gasp","los","losi","up","down"]);
function safeDescription(desc) {
  if (!desc) return null;
  const t = desc.trim();
  if (!t) return null;
  if (STATE_WORDS.has(t.toLowerCase())) return null;
  return t;
}

// ─── Tracking local de "desde cuándo está offline" ───────────────────────────
// La OLT no siempre entrega un campo confiable de "tiempo offline" por ONT,
// así que lo llevamos nosotros: la primera vez que vemos una ONT en estado
// offline guardamos el timestamp; si vuelve a online, se borra. Esto alimenta
// la sección del dashboard de "offline +48h".
function updateOfflineTracking(oltId, onts) {
  const cp = cachePaths(oltId);
  const offlineMap = readCache(cp.OFFLINE) || {};
  const nowKeys = new Set();
  for (const o of onts) {
    const key = ontKey(o);
    nowKeys.add(key);
    const isOffline = (o.runState || "").toLowerCase() === "offline";
    if (isOffline) {
      if (!offlineMap[key]) offlineMap[key] = Date.now();
    } else {
      if (offlineMap[key]) delete offlineMap[key];
    }
  }
  // Limpiar ONTs que ya no existen en la OLT
  for (const key of Object.keys(offlineMap)) {
    if (!nowKeys.has(key)) delete offlineMap[key];
  }
  writeCache(cp.OFFLINE, offlineMap);
  return offlineMap;
}

// ─── QUICK SYNC (Incremental) ─────────────────────────────────────────────────
async function runQuickSync(oltId) {
  if (syncRunningMap[oltId]) { console.log("[SYNC " + oltId + "] Ya en progreso, se omite"); return; }
  syncRunningMap[oltId] = true;
  const cp = cachePaths(oltId);
  setStatus(oltId, { running: true, step: "Escaneando tabla de ONTs...", progress: 0, total: 0, lastSync: null, lastError: null, mode: "quick", changes: null });
  console.log("[QUICK SYNC " + oltId + "] Inicio sincronización incremental");
  const t0 = Date.now();

  try {
    const oldCache = readCache(cp.ONT);
    const hasCache = !!oldCache;

    const onts = await withOlt(oltId, getAllOntsFull, 60000);
    writeCache(cp.ONT, { ts: Date.now(), onts });
    updateOfflineTracking(oltId, onts);
    console.log("[QUICK SYNC " + oltId + "] " + onts.length + " ONTs en la OLT (escaneo: " + Math.round((Date.now()-t0)/1000) + "s)");
    io.to("olt:" + oltId).emit("onts:base", { oltId, onts });

    const detCache = readCache(cp.DETAIL) || {};
    const optCache = readCache(cp.OPT) || {};
    const newKeySet = new Set(onts.map(ontKey));

    let toFetch = [];
    let removedCount = 0;
    let newCount = 0;
    let stateChangeCount = 0;
    let descMissingCount = 0;

    if (!hasCache) {
      console.log("[QUICK SYNC " + oltId + "] Sin caché previo — modo completo");
      patchStatus(oltId, { mode: "full" });
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

    patchStatus(oltId, { total: toFetch.length });
    const skipped = onts.length - toFetch.length;

    if (toFetch.length === 0) {
      patchStatus(oltId, { step: "Sin cambios — todo al día" });
      console.log("[QUICK SYNC " + oltId + "] Sin cambios detectados (" + Math.round((Date.now()-t0)/1000) + "s)");
    } else {
      patchStatus(oltId, { step: "Actualizando " + toFetch.length + " ONTs..." });
      console.log("[QUICK SYNC " + oltId + "] " + toFetch.length + " para actualizar (nuevas:" + newCount + " estado:" + stateChangeCount + " sin-desc:" + descMissingCount + ")");

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
          await withOlt(oltId, async (session, olt) => {
            const P = olt.prompt;
            await session.cmd("interface gpon " + gp.frame + "/" + gp.slot, P + "(config-if-gpon", 12000);
            for (const item of gp.items) {
              const o = item.ont;
              const key = ontKey(o);

              if (item.needOptical) {
                try {
                  const raw = await session.cmd("display ont optical-info " + o.pon + " " + o.ontId, P + "(config-if-gpon", 12000);
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
                  const detRaw = await session.cmd("display ont info " + o.pon + " " + o.ontId, P + "(config-if-gpon", 15000);
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
              io.to("olt:" + oltId).emit("ont:update", { oltId, ont: mergeOntView(o, optCache, detCache) });
              patchStatus(oltId, { progress: done, step: "Actualizando: " + done + "/" + toFetch.length });
            }
            await session.cmd("quit", P + "(config)#", 5000).catch(() => {});
          }, 300000);
        } catch (e) { console.error("[QUICK SYNC " + oltId + "] Error grupo " + gpKey + ":", e.message); }
        writeCache(cp.OPT, optCache);
        writeCache(cp.DETAIL, detCache);
      }
    }

    const elapsed = Math.round((Date.now() - t0) / 1000);
    const changes = { total: onts.length, newOnts: newCount, stateChanges: stateChangeCount, descMissing: descMissingCount, removedOnts: removedCount, updated: toFetch.length, skipped: skipped };

    setStatus(oltId, {
      running: false, step: "Completado", progress: toFetch.length, total: toFetch.length,
      lastSync: new Date().toISOString(), lastError: null,
      mode: hasCache ? "quick" : "full", changes: changes, elapsed: elapsed,
    });
    console.log("[QUICK SYNC " + oltId + "] OK en " + elapsed + "s — " + JSON.stringify(changes));

  } catch (e) {
    console.error("[QUICK SYNC " + oltId + "] Error:", e.message);
    setStatus(oltId, { running: false, step: "Error", progress: 0, total: 0, lastSync: null, lastError: e.message, mode: "quick", changes: null });
  } finally {
    syncRunningMap[oltId] = false;
  }
}

// ─── FULL SYNC (forzada, todo) ───────────────────────────────────────────────
async function runFullSync(oltId) {
  if (syncRunningMap[oltId]) { console.log("[SYNC " + oltId + "] Ya en progreso, se omite"); return; }
  syncRunningMap[oltId] = true;
  const cp = cachePaths(oltId);
  setStatus(oltId, { running: true, step: "Conectando al OLT...", progress: 0, total: 0, lastSync: null, lastError: null, mode: "full", changes: null });
  console.log("[FULL SYNC " + oltId + "] Inicio sincronización completa");
  const t0 = Date.now();

  try {
    patchStatus(oltId, { step: "Obteniendo lista de ONTs..." });
    const onts = await withOlt(oltId, getAllOntsFull, 60000);
    writeCache(cp.ONT, { ts: Date.now(), onts });
    updateOfflineTracking(oltId, onts);
    patchStatus(oltId, { total: onts.length });
    console.log("[FULL SYNC " + oltId + "] " + onts.length + " ONTs encontradas");
    io.to("olt:" + oltId).emit("onts:base", { oltId, onts });

    const detCache = readCache(cp.DETAIL) || {};
    const validKeys = new Set(onts.map(ontKey));
    for (const k of Object.keys(detCache)) { if (!validKeys.has(k)) delete detCache[k]; }

    const opticalMap = { ...readCache(cp.OPT) || {} };
    const groups = {};
    for (const o of onts) {
      const k = o.frame + "/" + o.slot + "/" + o.pon;
      if (!groups[k]) groups[k] = { frame: o.frame, slot: o.slot, pon: o.pon, onts: [] };
      groups[k].onts.push(o);
    }

    patchStatus(oltId, { step: "Sincronizando óptica + detalles..." });
    let done = 0;
    for (const gpKey of Object.keys(groups)) {
      const gp = groups[gpKey];
      try {
        await withOlt(oltId, async (session, olt) => {
          const P = olt.prompt;
          await session.cmd("interface gpon " + gp.frame + "/" + gp.slot, P + "(config-if-gpon", 12000);
          for (const o of gp.onts) {
            const key = ontKey(o);
            try {
              const raw = await session.cmd("display ont optical-info " + gp.pon + " " + o.ontId, P + "(config-if-gpon", 12000);
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
              const detRaw = await session.cmd("display ont info " + gp.pon + " " + o.ontId, P + "(config-if-gpon", 15000);
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
            io.to("olt:" + oltId).emit("ont:update", { oltId, ont: mergeOntView(o, opticalMap, detCache) });
            patchStatus(oltId, { progress: done, step: "Óptica + Detalles: " + done + "/" + onts.length });
          }
          await session.cmd("quit", P + "(config)#", 5000).catch(() => {});
        }, 300000);
      } catch (e) { console.error("[FULL SYNC " + oltId + "] Error grupo " + gpKey + ":", e.message); }
      writeCache(cp.OPT, opticalMap);
      writeCache(cp.DETAIL, detCache);
    }

    patchStatus(oltId, { step: "Obteniendo VLANs..." });
    try {
      const vlans = await withOlt(oltId, async (session, olt) => {
        const raw = await session.cmd("display vlan all", olt.prompt + "(config)#", 30000);
        const found = new Set();
        for (const line of raw.split("\n")) {
          const ms = line.match(/\b(\d{2,4})\b/g);
          if (ms) ms.forEach(v => { const n = parseInt(v); if (n >= 100 && n <= 4094) found.add(n); });
        }
        return [...found].sort((a, b) => a - b);
      }, 60000);
      writeCache(cp.VLAN, { ts: Date.now(), vlans: vlans });
    } catch (e) { console.error("[FULL SYNC " + oltId + "] Error VLANs:", e.message); }

    const elapsed = Math.round((Date.now() - t0) / 1000);
    setStatus(oltId, {
      running: false, step: "Completado", progress: done, total: onts.length,
      lastSync: new Date().toISOString(), lastError: null,
      mode: "full", changes: { total: onts.length, updated: onts.length, skipped: 0, newOnts: 0, removedOnts: 0, stateChanges: 0, descMissing: 0 }, elapsed: elapsed,
    });
    console.log("[FULL SYNC " + oltId + "] OK en " + elapsed + "s");
  } catch (e) {
    console.error("[FULL SYNC " + oltId + "] Error fatal:", e.message);
    setStatus(oltId, { running: false, step: "Error", progress: 0, total: 0, lastSync: null, lastError: e.message, mode: "full", changes: null });
  } finally {
    syncRunningMap[oltId] = false;
  }
}

// Combina la fila cruda de la tabla con lo que haya en cache de óptica/detalle.
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
    onlineDuration: det.onlineDuration || null,
    lastDownCause:  det.lastDownCause  || null,
  };
}

// ─── Rutas de administración de OLTs ─────────────────────────────────────────

app.get("/api/olts", (_, res) => {
  const olts = loadOlts().map(({ pass, ...rest }) => rest); // no exponemos la clave
  res.json({ olts });
});

app.post("/api/olts", (req, res) => {
  const { name, host, port, user, pass, prompt } = req.body || {};
  if (!name || !host || !port || !user || !pass || !prompt) {
    return res.status(400).json({ error: "Faltan datos: name, host, port, user, pass, prompt" });
  }
  const olts = loadOlts();
  let id = slugify(name);
  let n = 2;
  while (olts.some(o => o.id === id)) { id = slugify(name) + "-" + n; n++; }

  const newOlt = { id, name: String(name).trim(), host: String(host).trim(), port: parseInt(port), user: String(user), pass: String(pass), prompt: String(prompt).trim() };
  olts.push(newOlt);
  saveOlts(olts);
  cachePaths(id); // crea la carpeta de caché ya de una vez

  const { pass: _p, ...safe } = newOlt;
  res.json({ ok: true, olt: safe });
});

app.delete("/api/olts/:id", (req, res) => {
  const { id } = req.params;
  const olts = loadOlts();
  const filtered = olts.filter(o => o.id !== id);
  if (filtered.length === olts.length) return res.status(404).json({ error: "OLT no encontrada" });
  saveOlts(filtered);
  delete telnetPools[id];
  delete syncStatusMap[id];
  try {
    const dir = path.join(CACHE_DIR, id);
    fs.rmSync(dir, { recursive: true, force: true });
  } catch {}
  res.json({ ok: true });
});

// ─── Middleware: validar que la OLT exista ───────────────────────────────────
function requireOlt(req, res, next) {
  const olt = getOltById(req.params.oltId);
  if (!olt) return res.status(404).json({ error: "OLT no encontrada: " + req.params.oltId });
  req.olt = olt;
  next();
}

// ─── Rutas API por OLT ────────────────────────────────────────────────────────

app.get("/api/olt/:oltId/healthz", requireOlt, (req, res) => res.json({ status: "ok", olt: req.olt.host }));
app.get("/api/olt/:oltId/status", requireOlt, (req, res) => res.json({ oltId: req.params.oltId, ...ensureStatus(req.params.oltId) }));

app.post("/api/olt/:oltId/sync", requireOlt, (req, res) => {
  const { oltId } = req.params;
  if (syncRunningMap[oltId]) return res.json({ ok: false, message: "Sincronización en progreso" });
  runQuickSync(oltId).catch(e => console.error(e.message));
  res.json({ ok: true, message: "Sincronización rápida iniciada", mode: "quick" });
});

app.post("/api/olt/:oltId/sync/full", requireOlt, (req, res) => {
  const { oltId } = req.params;
  if (syncRunningMap[oltId]) return res.json({ ok: false, message: "Sincronización en progreso" });
  runFullSync(oltId).catch(e => console.error(e.message));
  res.json({ ok: true, message: "Sincronización completa iniciada", mode: "full" });
});

app.get("/api/olt/:oltId/onts", requireOlt, (req, res) => {
  const cp = cachePaths(req.params.oltId);
  const ontCache = readCache(cp.ONT);
  const optCache = readCache(cp.OPT) || {};
  const detCache = readCache(cp.DETAIL) || {};
  if (!ontCache) return res.json({ onts: [], cached: false, message: "Sin datos — ejecuta sincronizar primero" });

  const onts = ontCache.onts.map(o => mergeOntView(o, optCache, detCache));
  res.json({ onts, cached: true, cacheAge: Math.round((Date.now() - ontCache.ts) / 1000), ts: ontCache.ts });
});

// Dashboard: ONTs offline >= 48h, ordenadas con la potencia más crítica primero.
app.get("/api/olt/:oltId/dashboard/offline48", requireOlt, (req, res) => {
  const { oltId } = req.params;
  const cp = cachePaths(oltId);
  const ontCache = readCache(cp.ONT);
  const offlineMap = readCache(cp.OFFLINE) || {};
  const optCache = readCache(cp.OPT) || {};
  const detCache = readCache(cp.DETAIL) || {};
  if (!ontCache) return res.json({ items: [], count: 0 });

  const now = Date.now();
  const THRESH_MS = 48 * 3600 * 1000;
  const items = [];

  for (const o of ontCache.onts) {
    const key = ontKey(o);
    const since = offlineMap[key];
    if (!since) continue;
    const downMs = now - since;
    if (downMs < THRESH_MS) continue;
    const merged = mergeOntView(o, optCache, detCache);
    items.push({ ...merged, offlineSince: since, offlineHours: Math.floor(downMs / 3600000) });
  }

  // Potencia más crítica (más negativa / peor) primero; sin dato de potencia al final.
  items.sort((a, b) => {
    const ar = parseFloat(a.rxPower), br = parseFloat(b.rxPower);
    const aNa = isNaN(ar), bNa = isNaN(br);
    if (aNa !== bNa) return aNa ? 1 : -1;
    if (!aNa && !bNa) return ar - br;
    return b.offlineHours - a.offlineHours;
  });

  res.json({ items, count: items.length });
});

app.get("/api/olt/:oltId/profiles", requireOlt, async (req, res) => {
  const { oltId } = req.params;
  const cp = cachePaths(oltId);
  const force = req.query.force === "1" || req.query.force === "true";
  const cached = !force ? readCache(cp.PROFILE) : null;
  if (cached && (Date.now() - cached.ts) < 3600000) {
    return res.json({ lineProfiles: cached.lineProfiles, srvProfiles: cached.srvProfiles, cached: true });
  }
  try {
    const profiles = await withOlt(oltId, getProfiles, 40000);
    writeCache(cp.PROFILE, { ts: Date.now(), ...profiles });
    res.json({ ...profiles, cached: false });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

app.get("/api/olt/:oltId/vlans", requireOlt, async (req, res) => {
  const { oltId } = req.params;
  const cp = cachePaths(oltId);
  const cached = readCache(cp.VLAN);
  if (cached && (Date.now() - cached.ts) < 3600000) return res.json({ vlans: cached.vlans, cached: true });
  try {
    const vlans = await withOlt(oltId, async (session, olt) => {
      const raw = await session.cmd("display vlan all", olt.prompt + "(config)#", 30000);
      const found = new Set();
      for (const line of raw.split("\n")) {
        const ms = line.match(/\b(\d{2,4})\b/g);
        if (ms) ms.forEach(v => { const n = parseInt(v); if (n >= 100 && n <= 4094) found.add(n); });
      }
      return [...found].sort((a, b) => a - b);
    }, 60000);
    writeCache(cp.VLAN, { ts: Date.now(), vlans: vlans });
    res.json({ vlans, cached: false });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

app.post("/api/olt/:oltId/ont/autorizar", requireOlt, async (req, res) => {
  const { oltId } = req.params;
  const {
    frame = "0", slot, pon, ontId, sn,
    lineProfile = "101", serviceProfile = "1",
    desc = "", vlan, authType = "sn-auth"
  } = req.body;

  if (!slot || !pon || !ontId || !sn) {
    return res.status(400).json({ error: "Faltan: slot, pon, ontId, sn" });
  }

  const cleanDesc = (desc || sn).replace(/["']/g, "");
  const authMethod = authType === "password-auth" ? "password-auth" : "sn-auth";

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
    const result = await withOlt(oltId, async (session, olt) => {
      const P = olt.prompt;
      await session.cmd("terminal width 512", P + "(config)#", 5000).catch(() => {});
      await session.cmd("terminal length 0", P + "(config)#", 5000).catch(() => {});
      await session.cmd("interface gpon " + frame + "/" + slot, P + "(config-if-gpon", 12000);
      const r = await session.cmd(ontCmd, P + "(config-if-gpon", 25000);
      await session.cmd("quit", P + "(config)#", 5000).catch(() => {});

      return {
        success: r.toLowerCase().includes("success") || r.includes("ont-add"),
        raw: r,
        cmd: ontCmd
      };
    }, 90000);

    if (result.success) {
      res.json({ ok: true, message: "ONT autorizada correctamente", raw: result.raw, cmd: result.cmd });
    } else {
      res.status(400).json({ ok: false, message: "Posible error — revisa respuesta OLT", raw: result.raw, cmd: result.cmd });
    }
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});


app.delete("/api/olt/:oltId/ont/:f/:s/:p/:id", requireOlt, async (req, res) => {
  const { oltId, f, s, p, id } = req.params;
  const cp = cachePaths(oltId);
  try {
    const result = await withOlt(oltId, async (session, olt) => {
      const P = olt.prompt;
      await session.cmd("terminal width 512", P + "(config)#", 5000).catch(() => {});
      await session.cmd("terminal length 0", P + "(config)#", 5000).catch(() => {});
      await session.cmd("interface gpon " + f + "/" + s, P + "(config-if-gpon", 12000);

      const step1 = await session.cmdAny(
        "ont delete " + p + " " + id,
        [P + "(config-if-gpon", "are you sure", "Are you sure"],
        15000
      );

      let raw = step1.raw;
      if (/sure/i.test(step1.raw)) {
        session.send("y");
        const step2 = await session.waitFor(P + "(config-if-gpon", 10000).catch(() => "");
        raw += step2;
      }

      await session.cmd("quit", P + "(config)#", 5000).catch(() => {});

      return {
        success: !/failure|error/i.test(raw),
        raw,
      };
    }, 60000);

    const ontCache = readCache(cp.ONT);
    if (ontCache) {
      ontCache.onts = ontCache.onts.filter(o => !(String(o.frame) === String(f) && String(o.slot) === String(s) && String(o.pon) === String(p) && String(o.ontId) === String(id)));
      writeCache(cp.ONT, ontCache);
    }
    const optCache = readCache(cp.OPT) || {};
    delete optCache[f + "/" + s + "/" + p + "/" + id];
    writeCache(cp.OPT, optCache);
    const detCache = readCache(cp.DETAIL) || {};
    delete detCache[f + "/" + s + "/" + p + "/" + id];
    writeCache(cp.DETAIL, detCache);
    const offlineMap = readCache(cp.OFFLINE) || {};
    delete offlineMap[f + "/" + s + "/" + p + "/" + id];
    writeCache(cp.OFFLINE, offlineMap);

    if (result.success) {
      io.to("olt:" + oltId).emit("ont:removed", { oltId, frame: f, slot: s, pon: p, ontId: id });
      res.json({ ok: true, message: "ONT " + f + "/" + s + "/" + p + "/" + id + " eliminada", raw: result.raw });
    } else {
      res.status(400).json({ ok: false, message: "Posible error al eliminar — revisa respuesta OLT", raw: result.raw });
    }
  } catch (e) { res.status(500).json({ error: e.message }); }
});

// Reiniciar (reset) una ONT desde el botón de Diagnóstico.
// Nota importante: "ont reset <pon> <ont-id>" es el comando estándar Huawei
// GPON documentado para reiniciar una ONT dentro de "interface gpon". No lo
// hemos podido confirmar contra el firmware exacto de cada OLT registrada acá,
// así que esta ruta NUNCA asume éxito por sí sola: siempre devuelve la
// respuesta cruda de la OLT (raw) para que se pueda verificar qué contestó
// realmente el equipo, en vez de fingir que funcionó.
app.post("/api/olt/:oltId/ont/:f/:s/:p/:id/reboot", requireOlt, async (req, res) => {
  const { oltId, f, s, p, id } = req.params;
  try {
    const result = await withOlt(oltId, async (session, olt) => {
      const P = olt.prompt;
      await session.cmd("terminal width 512", P + "(config)#", 5000).catch(() => {});
      await session.cmd("terminal length 0", P + "(config)#", 5000).catch(() => {});
      await session.cmd("interface gpon " + f + "/" + s, P + "(config-if-gpon", 12000);

      const step1 = await session.cmdAny(
        "ont reset " + p + " " + id,
        [P + "(config-if-gpon", "are you sure", "Are you sure"],
        15000
      );

      let raw = step1.raw;
      if (/sure/i.test(step1.raw)) {
        session.send("y");
        const step2 = await session.waitFor(P + "(config-if-gpon", 15000).catch(() => "");
        raw += step2;
      }

      await session.cmd("quit", P + "(config)#", 5000).catch(() => {});

      return {
        success: !/failure|error|invalid|unknown command/i.test(raw),
        raw,
      };
    }, 60000);

    if (result.success) {
      res.json({ ok: true, message: "Comando de reinicio enviado a la ONT — verifica el estado en unos minutos", raw: result.raw });
    } else {
      res.status(400).json({ ok: false, message: "La OLT respondió con un posible error. Revisa el detalle antes de asumir que se reinició.", raw: result.raw });
    }
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

app.get("/api/olt/:oltId/ont/nextid/:f/:s/:p", requireOlt, (req, res) => {
  const { oltId, f, s, p } = req.params;
  const cp = cachePaths(oltId);
  const profCache = readCache(cp.PROFILE);
  const profiles = { lineProfiles: profCache ? profCache.lineProfiles : null, srvProfiles: profCache ? profCache.srvProfiles : null };

  const ontCache = readCache(cp.ONT);
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

app.get("/api/olt/:oltId/ont/:f/:s/:p/:id", requireOlt, async (req, res) => {
  const { oltId, f, s, p, id } = req.params;
  const cp = cachePaths(oltId);
  try {
    const detail = await withOlt(oltId, async (session, olt) => {
      const P = olt.prompt;
      const raw = await session.cmd("display ont info " + f + " " + s + " " + p + " " + id, P + "(config)#", 20000);
      const kv = parseKV(raw);
      let optical = {};
      try {
        await session.cmd("interface gpon " + f + "/" + s, P + "(config-if-gpon", 10000);
        const optRaw = await session.cmd("display ont optical-info " + p + " " + id, P + "(config-if-gpon", 15000);
        optical = parseOptical(optRaw);

        const optCache = readCache(cp.OPT) || {};
        const key = f + "/" + s + "/" + p + "/" + id;
        optCache[key] = {
          rxPower:    optical["Rx optical power(dBm)"] || optical["RX optical power(dBm)"] || null,
          txPower:    optical["Tx optical power(dBm)"] || optical["TX optical power(dBm)"] || null,
          laserBias:  optical["Laser bias current(mA)"] || null,
          temperature: optical["Temperature(C)"] || optical["Temperature"] || null,
          voltage:    optical["Voltage(V)"] || optical["Voltage"] || null,
          ts:         Date.now(),
        };
        writeCache(cp.OPT, optCache);
        await session.cmd("quit", P + "(config)#", 5000);
      } catch {}

      const detCache = readCache(cp.DETAIL) || {};
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
      writeCache(cp.DETAIL, detCache);
      return { kv, optical };
    });
    res.json(detail);

    try {
      const ontCache = readCache(cp.ONT);
      const row = ontCache && ontCache.onts.find(o => String(o.frame) === String(f) && String(o.slot) === String(s) && String(o.pon) === String(p) && String(o.ontId) === String(id));
      if (row) {
        const optCache = readCache(cp.OPT) || {};
        const detCache2 = readCache(cp.DETAIL) || {};
        io.to("olt:" + oltId).emit("ont:update", { oltId, ont: mergeOntView(row, optCache, detCache2) });
      }
    } catch (e2) { console.error("[WS] Error emitiendo ont:update:", e2.message); }
  } catch (e) { res.status(500).json({ error: e.message }); }
});

app.get("/api/olt/:oltId/autofind", requireOlt, async (req, res) => {
  const { oltId } = req.params;
  const cp = cachePaths(oltId);
  try {
    const cachedProfiles = readCache(cp.PROFILE);
    const profilesFresh = cachedProfiles && (Date.now() - cachedProfiles.ts) < 3600000;

    const result = await withOlt(oltId, async (session, olt) => {
      const raw = await session.cmd("display ont autofind all", olt.prompt + "(config)#", 35000);
      const onts = parseAutofind(raw);

      let profiles;
      if (profilesFresh) {
        profiles = { lineProfiles: cachedProfiles.lineProfiles, srvProfiles: cachedProfiles.srvProfiles };
      } else {
        profiles = await getProfiles(session, olt);
        writeCache(cp.PROFILE, { ts: Date.now(), ...profiles });
      }

      return { onts, ...profiles };
    }, 60000);

    res.json(result);
  } catch (e) { res.status(500).json({ error: e.message }); }
});

app.get("/api/olt/:oltId/optical/cache", requireOlt, (req, res) => {
  const cp = cachePaths(req.params.oltId);
  res.json(readCache(cp.OPT) || {});
});

app.delete("/api/olt/:oltId/cache", requireOlt, (req, res) => {
  const cp = cachePaths(req.params.oltId);
  [cp.ONT, cp.OPT, cp.VLAN, cp.DETAIL, cp.PROFILE, cp.OFFLINE].forEach(f => { try { fs.unlinkSync(f); } catch {} });
  res.json({ ok: true, message: "Caché limpiada" });
});
// ─── EDITAR DESCRIPCIÓN DE ONT ────────────────────────────────────────────────
// Comando Huawei: ont modify <pon> <ont-id> description "<desc>"
app.patch('/api/olt/:oltId/ont/:f/:s/:p/:id/desc', requireOlt, async (req, res) => {
  const { oltId, f, s, p, id } = req.params;
  const { description } = req.body || {};
  if (description === undefined || description === null) {
    return res.status(400).json({ error: 'Falta el campo description' });
  }
  // Sanitizar: quitar comillas dobles que rompen el comando CLI
  const cleanDesc = String(description).replace(/"/g, '').trim();

  try {
    const result = await withOlt(oltId, async (session, olt) => {
      const P = olt.prompt;
      await session.cmd("terminal width 512", P + "(config)#", 5000).catch(() => {});
      await session.cmd("terminal length 0", P + "(config)#", 5000).catch(() => {});
      await session.cmd("interface gpon " + f + "/" + s, P + "(config-if-gpon", 12000);

      // ── Modificar descripción: comando standard Huawei GPON ──
      // Forma: ont modify <pon> <ont-id> desc <string>
      // NOTA: en firmwares algunos aceptan "description" y otros solo "desc"
      // Probamos primero "desc" (más portable), si falla probamos "description"
      let raw;
      try {
        raw = await session.cmd(
          'ont modify ' + p + ' ' + id + ' desc "' + cleanDesc + '"',
          P + "(config-if-gpon",
          15000
        );
      } catch (e1) {
        // Si falla con "desc", intentamos con "description"
        raw = await session.cmd(
          'ont modify ' + p + ' ' + id + ' description "' + cleanDesc + '"',
          P + "(config-if-gpon",
          15000
        );
      }

      await session.cmd("quit", P + "(config)#", 5000).catch(() => {});
      await session.cmd("commit", P + "(config)#", 8000).catch(() => {}); // algunos firmwares requieren commit

      return {
        success: !/failure|error|invalid|unknown command/i.test(raw),
        raw: raw,
      };
    }, 60000);

    if (result.success) {
      // Actualizar caché local
      const cp = cachePaths(oltId);
      const detCache = readCache(cp.DETAIL) || {};
      const key = f + "/" + s + "/" + p + "/" + id;
      if (detCache[key]) {
        detCache[key].description = cleanDesc || null;
        detCache[key].ts = Date.now();
        writeCache(cp.DETAIL, detCache);
      }
      // También actualizar en la tabla base si tiene descripción那里
      const ontCache = readCache(cp.ONT);
      if (ontCache) {
        const row = ontCache.onts.find(o =>
          String(o.frame) === String(f) && String(o.slot) === String(s) &&
          String(o.pon) === String(p) && String(o.ontId) === String(id)
        );
        if (row) {
          row.description = cleanDesc || null;
          writeCache(cp.ONT, ontCache);
        }
      }

      io.to("olt:" + oltId).emit("ont:update", {
        oltId,
        ont: {
          frame: f, slot: s, pon: p, ontId: id,
          description: cleanDesc || null,
        },
      });

      res.json({ ok: true, message: "Descripción actualizada", raw: result.raw });
    } else {
      res.status(400).json({ ok: false, message: "Posible error — revisa respuesta OLT", raw: result.raw });
    }
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

// ─── MOVER ONT ENTRE PUERTOS PON ─────────────────────────────────────────────
// Flujo: 1) Leer config actual desde caché  2) Eliminar del puerto viejo
//         3) Autorizar en el puerto nuevo con misma config
app.post('/api/olt/:oltId/ont/move', requireOlt, async (req, res) => {
  const { oltId } = req.params;
  const { oldF, oldS, oldP, oldId, newF, newS, newP, sn } = req.body || {};

  if (!oldF || !oldS || !oldP || !oldId || !newF || !newS || !newP || !sn) {
    return res.status(400).json({ error: "Faltan datos: oldF, oldS, oldP, oldId, newF, newS, newP, sn" });
  }

  const cp = cachePaths(oltId);
  const oldKey = oldF + "/" + oldS + "/" + oldP + "/" + oldId;

  // Leer config actual de la ONT desde caché
  const detCache = readCache(cp.DETAIL) || {};
  const ontCache = readCache(cp.ONT) || {};
  const oldDet = detCache[oldKey] || {};
  const oldRow = (ontCache.onts || []).find(o => ontKey(o) === oldKey) || {};

  // Valores para la nueva autorización
  const description = oldDet.description || oldRow.description || sn;
  const cleanDesc = String(description).replace(/"/g, '').trim();

  // Intentar obtener perfiles del detalle o usar defaults
  let lineProfile = "101";
  let serviceProfile = "1";

  // Buscar perfiles en caché de profiles
  const profCache = readCache(cp.PROFILE);
  if (profCache && profCache.lineProfiles && profCache.lineProfiles.length) {
    lineProfile = profCache.lineProfiles[0].id || "101";
  }
  if (profCache && profCache.srvProfiles && profCache.srvProfiles.length) {
    serviceProfile = profCache.srvProfiles[0].id || "1";
  }

  try {
    const result = await withOlt(oltId, async (session, olt) => {
      const P = olt.prompt;
      await session.cmd("terminal width 512", P + "(config)#", 5000).catch(() => {});
      await session.cmd("terminal length 0", P + "(config)#", 5000).catch(() => {});

      // ── Paso 1: Eliminar del puerto viejo ──
      // Comando: undo ont add <pon> <ontId> (estándar Huawei GPON)
      await session.cmd("interface gpon " + oldF + "/" + oldS, P + "(config-if-gpon", 12000);

      // Probar primero con "undo ont add" (más común), luego "ont delete"
      let delRaw;
      try {
        const delStep1 = await session.cmdAny(
          "undo ont add " + oldP + " " + oldId,
          [P + "(config-if-gpon", "are you sure", "Are you sure"],
          15000
        );
        delRaw = delStep1.raw;
        if (/sure/i.test(delStep1.raw)) {
          session.send("y");
          const delStep2 = await session.waitFor(P + "(config-if-gpon", 10000).catch(() => "");
          delRaw += delStep2;
        }
      } catch (eDel1) {
        // Fallback: "ont delete" para firmwares que lo usan
        const delStep1 = await session.cmdAny(
          "ont delete " + oldP + " " + oldId,
          [P + "(config-if-gpon", "are you sure", "Are you sure"],
          15000
        );
        delRaw = delStep1.raw;
        if (/sure/i.test(delStep1.raw)) {
          session.send("y");
          const delStep2 = await session.waitFor(P + "(config-if-gpon", 10000).catch(() => "");
          delRaw += delStep2;
        }
      }
      await session.cmd("quit", P + "(config)#", 5000).catch(() => {});

      const delOk = !/failure|error|invalid/i.test(delRaw);

      // ── Paso 2: Obtener próximo ID disponible en el nuevo puerto ──
      await session.cmd("interface gpon " + newF + "/" + newS, P + "(config-if-gpon", 12000);
      const newIdRaw = await session.cmd("display ont info " + newP + " all", P + "(config-if-gpon", 18000);
      const usedIds = new Set();
      const idRe = /(\d+)\s*\/\s*\d+\s*\/\s*\d+\s+(\d+)\s+/g;
      let m;
      while ((m = idRe.exec(newIdRaw)) !== null) {
        if (m[1] === newP) usedIds.add(parseInt(m[2]));
      }
      let newId = 0;
      while (usedIds.has(newId) && newId < 128) newId++;

      // ── Paso 3: Autorizar en el puerto nuevo ──
      // Comando ont add standard Huawei GPON
      // Formato: ont add <pon> <ont-id> sn-auth <sn> omci ont-lineprofile-id <lp> ont-srvprofile-id <sp> desc "<desc>"
      const addCmd = [
        "ont", "add", newP, String(newId),
        "sn-auth", sn,
        "omci",
        "ont-lineprofile-id", lineProfile,
        "ont-srvprofile-id", serviceProfile
      ].join(" ") + (cleanDesc ? ' desc "' + cleanDesc + '"' : '');

      const addRaw = await session.cmd(addCmd, P + "(config-if-gpon", 25000);
      await session.cmd("quit", P + "(config)#", 5000).catch(() => {});
      await session.cmd("commit", P + "(config)#", 8000).catch(() => {}); // algunos Huawei OLT requieren commit

      const addOk = /success|ont-add/i.test(addRaw.toLowerCase()) || !/failure|error|invalid/i.test(addRaw);

      return {
        delOk: delOk,
        delRaw: delRaw,
        addOk: addOk,
        addRaw: addRaw,
        newId: newId,
        newF: newF,
        newS: newS,
        newP: newP,
        description: cleanDesc,
        lineProfile: lineProfile,
        serviceProfile: serviceProfile,
      };
    }, 120000);

    if (result.addOk) {
      // Limpiar caché vieja
      delete detCache[oldKey];
      writeCache(cp.DETAIL, detCache);

      if (ontCache.onts) {
        ontCache.onts = ontCache.onts.filter(o => ontKey(o) !== oldKey);
        writeCache(cp.ONT, ontCache);
      }

      // Limpiar offline tracking viejo
      const offMap = readCache(cp.OFFLINE) || {};
      delete offMap[oldKey];
      writeCache(cp.OFFLINE, offMap);

      // Notificar vía WebSocket que la ONT vieja fue removida
      io.to("olt:" + oltId).emit("ont:removed", {
        oltId,
        frame: oldF, slot: oldS, pon: oldP, ontId: oldId,
      });

      res.json({
        ok: true,
        message: "ONT movida a " + result.newF + "/" + result.newS + "/" + result.newP + "/" + result.newId,
        newLocation: {
          frame: result.newF, slot: result.newS,
          pon: result.newP, ontId: result.newId,
        },
        delRaw: result.delRaw,
        addRaw: result.addRaw,
      });
    } else {
      res.status(400).json({
        ok: false,
        message: "Error al autorizar en el nuevo puerto — revisa respuesta",
        delRaw: result.delRaw,
        addRaw: result.addRaw,
      });
    }
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});
// ─── Arranque ─────────────────────────────────────────────────────────────────
const PORT = process.env.PORT || 3000;
httpServer.listen(PORT, () => {
  const olts = loadOlts();
  console.log("╔══════════════════════════════════════════════════╗");
  console.log("║  OLT Manager (multi-OLT) corriendo en :" + PORT);
  console.log("║  http://localhost:" + PORT);
  console.log("║  OLTs registradas: " + olts.map(o => o.name + " (" + o.host + ")").join(", "));
  console.log("║  Websocket: activo (mismo puerto, /socket.io)     ║");
  console.log("║  Auto-sync: " + (AUTO_SYNC_ENABLED ? ("cada " + AUTO_SYNC_MINUTES + " min (incremental, todas las OLTs)") : "DESACTIVADO — solo manual (botón)"));
  console.log("╚══════════════════════════════════════════════════╝");

  if (AUTO_SYNC_ENABLED) {
    for (const olt of olts) {
      const cp = cachePaths(olt.id);
      const c = readCache(cp.ONT);
      if (!c || (Date.now() - c.ts) > 1800000) {
        console.log("\n[AUTO] Primera sync de " + olt.id + " en 3s...");
        setTimeout(() => runQuickSync(olt.id).catch(e => console.error(e.message)), 3000);
      }
    }
    setInterval(() => {
      for (const olt of loadOlts()) {
        if (!syncRunningMap[olt.id]) {
          runQuickSync(olt.id).catch(e => console.error(e.message));
        }
      }
    }, AUTO_SYNC_MINUTES * 60000);
  } else {
    console.log("\n[AUTO] Sync automática desactivada. Las tablas se sirven desde caché hasta que se presione 'Sincronizar'.");
  }
});