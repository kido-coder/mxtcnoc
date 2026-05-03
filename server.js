/**
 * ════════════════════════════════════════════════════════════
 *  Helios PV Monitoring System — Unified Production Server v3
 * ════════════════════════════════════════════════════════════
 *
 *  Services:
 *    • Aedes MQTT broker          → port 1883
 *    • Raw TCP socket server      → port 9000
 *    • Express HTTP API + static  → port 3000
 *
 * ════════════════════════════════════════════════════════════
 */

"use strict";

const net = require("net");
const http = require("http");
const path = require("path");
const fs = require("fs");
const express = require("express");
const cors = require("cors");
const mysql = require("mysql2/promise");
const aedes = require("aedes")();
const { WebSocketServer } = require("ws");

// ── Port config ───────────────────────────────────────────────
const MQTT_PORT = parseInt(process.env.MQTT_PORT) || 1883;
const TCP_PORT = parseInt(process.env.TCP_PORT) || 9000;
const HTTP_PORT = parseInt(process.env.HTTP_PORT) || 3000;

// ── MySQL pool ────────────────────────────────────────────────
const db = mysql.createPool({
  host: process.env.DB_HOST || "localhost",
  user: process.env.DB_USER || "root",
  password: process.env.DB_PASS || "Mhtsshataasai123!@$",
  database: process.env.DB_NAME || "solarmonitor",
  waitForConnections: true,
  connectionLimit: 10,
  timezone: "+00:00",
});

// ── Logger ────────────────────────────────────────────────────
const LOG_FILE = path.join(__dirname, "pv_server.log");
function log(tag, msg) {
  const line = `[${new Date().toISOString()}] [${tag}] ${msg}`;
  console.log(line);
  fs.appendFileSync(LOG_FILE, line + "\n");
}

// ── Performance metrics (for perf dashboard) ──────────────────
const metrics = {
  mqtt: { sessions: [], current: null },
  tcp: { sessions: [], current: null },
};
function newSession(proto) {
  return {
    protocol: proto, startTime: Date.now(), endTime: null,
    messagesReceived: 0, bytesReceived: 0, latencySamples: [],
    payloads: [], dbInserted: 0, dbErrors: 0, errors: 0, loss: 0
  };
}
function computeStats(s) {
  const lat = s.latencySamples;
  if (!lat.length) return { avg: 0, min: 0, max: 0, p95: 0, p99: 0 };
  const sorted = [...lat].sort((a, b) => a - b);
  const avg = lat.reduce((a, b) => a + b, 0) / lat.length;
  return {
    avg: +avg.toFixed(2), min: sorted[0], max: sorted[sorted.length - 1],
    p95: sorted[Math.floor(sorted.length * 0.95)] ?? sorted[sorted.length - 1],
    p99: sorted[Math.floor(sorted.length * 0.99)] ?? sorted[sorted.length - 1]
  };
}

// ════════════════════════════════════════════════════════════
//  ACTIVE TCP SOCKET REGISTRY
//  node_id → net.Socket  (so we can push commands back)
// ════════════════════════════════════════════════════════════
const tcpSockets = new Map();   // node_id → socket

// ════════════════════════════════════════════════════════════
//  DATABASE HELPERS
// ════════════════════════════════════════════════════════════

async function insertLog(d) {
  try {
    const [r] = await db.query(
      `INSERT INTO working_log
         (log_node, log_sent_time, log_inserted_time, log_seq,
          log_panel_voltage, log_panel_current, log_battery_voltage, log_lux,
          log_cleaning_stat, log_cleaning_reason, log_con_type)
       VALUES (?, ?, NOW(), ?, ?, ?, ?, ?, ?, ?, ?)`,
      [d.node_id, d.sent_time || new Date(), d.seq ?? null,
      String(d.panel_voltage ?? ""), String(d.panel_current ?? ""),
      String(d.battery_voltage ?? ""), d.lux ?? null,
      d.cleaning_stat ?? 0, d.cleaning_reason ?? "", d.con_type || "UNKNOWN"]
    );
    log("DB", `INSERT log id=${r.insertId} node=${d.node_id} proto=${d.con_type} lux=${d.lux ?? "—"}`);
    return r.insertId;
  } catch (e) {
    log("DB_ERR", `insertLog: ${e.message}`);
    return null;
  }
}

async function upsertLastProto(nodeId, proto, tcpAddr = null) {
  try {
    await db.query(
      `INSERT INTO node_last_proto (node_id, proto, tcp_addr, last_seen)
         VALUES (?, ?, ?, NOW())
       ON DUPLICATE KEY UPDATE proto=VALUES(proto), tcp_addr=VALUES(tcp_addr), last_seen=NOW()`,
      [nodeId, proto, tcpAddr]
    );
  } catch (e) {
    log("DB_ERR", `upsertLastProto: ${e.message}`);
  }
}

async function getLastProto(nodeId) {
  try {
    const [rows] = await db.query(
      `SELECT proto, tcp_addr FROM node_last_proto WHERE node_id=? LIMIT 1`,
      [nodeId]
    );
    return rows[0] || null;
  } catch (e) {
    return null;
  }
}

async function upsertSchedule(nodeId, slot, time, enabled) {
  // time format "HH:MM"
  try {
    await db.query(
      `INSERT INTO node_schedule (node_id, slot, sched_time, enabled, updated_at)
         VALUES (?, ?, ?, ?, NOW())
       ON DUPLICATE KEY UPDATE sched_time=VALUES(sched_time), enabled=VALUES(enabled), updated_at=NOW()`,
      [nodeId, slot, time + ":00", enabled ? 1 : 0]
    );
  } catch (e) {
    log("DB_ERR", `upsertSchedule: ${e.message}`);
  }
}

async function getSchedule(nodeId) {
  try {
    const [rows] = await db.query(
      `SELECT slot, TIME_FORMAT(sched_time,'%H:%i') AS sched_time, enabled
         FROM node_schedule WHERE node_id=? ORDER BY slot`,
      [nodeId]
    );
    // Normalise to always return slots 0 and 1
    const out = [
      { slot: 0, time: "08:00", enabled: false },
      { slot: 1, time: "17:00", enabled: false },
    ];
    rows.forEach(r => {
      if (r.slot === 0 || r.slot === 1) {
        out[r.slot].time = r.sched_time;
        out[r.slot].enabled = r.enabled === 1;
      }
    });
    return out;
  } catch (e) {
    log("DB_ERR", `getSchedule: ${e.message}`);
    return null;
  }
}

async function insertCmd(nodeId, cmdText) {
  try {
    const [r] = await db.query(
      `INSERT INTO cmd_log (cmd_sent, cmd_node, cmd_text) VALUES (NOW(), ?, ?)`,
      [nodeId, cmdText || null]
    );
    log("DB", `INSERT cmd id=${r.insertId} node=${nodeId} cmd=${cmdText}`);
    return r.insertId;
  } catch (e) {
    log("DB_ERR", `insertCmd: ${e.message}`);
    return null;
  }
}

// ════════════════════════════════════════════════════════════
//  COMMAND ROUTING
//  Sends a command string to a node via its last-used protocol.
//  Supported commands:
//    CLEAN
//    SCHED|HH:MM|HH:MM
//    SCHED_OFF|<slot>
//    SCHED_GET
//    TIME|HH:MM
// ════════════════════════════════════════════════════════════
async function sendCommandToNode(nodeId, cmdStr) {
  const proto = await getLastProto(nodeId);

  if (!proto) {
    log("CMD", `No last-proto record for ${nodeId} — cannot route command`);
    return { ok: false, reason: "no_proto_record" };
  }

  log("CMD", `→ ${nodeId} via ${proto.proto}: ${cmdStr}`);

  if (proto.proto === "MQTT") {
    const topic = `cmd/${nodeId}`;
    return new Promise((resolve) => {
      aedes.publish(
        { topic, payload: Buffer.from(cmdStr), qos: 1, retain: false },
        (err) => {
          if (err) { log("CMD_ERR", err.message); resolve({ ok: false, reason: err.message }); }
          else { resolve({ ok: true, proto: "MQTT", topic }); }
        }
      );
    });
  }

  if (proto.proto === "TCP") {
    const sock = tcpSockets.get(nodeId);
    if (!sock || sock.destroyed) {
      log("CMD", `TCP socket for ${nodeId} not connected`);
      return { ok: false, reason: "tcp_not_connected" };
    }
    return new Promise((resolve) => {
      sock.write(cmdStr + "\n", (err) => {
        if (err) { log("CMD_ERR", err.message); resolve({ ok: false, reason: err.message }); }
        else { resolve({ ok: true, proto: "TCP" }); }
      });
    });
  }

  return { ok: false, reason: "unknown_proto" };
}

// Build SCHED command string from two schedule slot objects
function buildSchedCmd(s0, s1) {
  // SCHED|HH:MM|HH:MM  (both slots always sent together)
  return `SCHED|${s0.time}|${s1.time}`;
}

// Send current time to a node (for schedule sync)
function buildTimeCmd() {
  const now = new Date();
  const yy = String(now.getFullYear() % 100).padStart(2, "0");
  const MM = String(now.getMonth() + 1).padStart(2, "0");
  const dd = String(now.getDate()).padStart(2, "0");
  const hh = String(now.getHours()).padStart(2, "0");
  const mm = String(now.getMinutes()).padStart(2, "0");
  const ss = String(now.getSeconds()).padStart(2, "0");
  return `TIME|${yy}${MM}${dd}${hh}${mm}${ss}`;
}

// ════════════════════════════════════════════════════════════
//  INCOMING FRAME HANDLER  (shared by MQTT and TCP parsers)
//  Handles:
//    DATA frame  → insertLog + upsertLastProto
//    SCHED_ACK   → upsertSchedule (confirmation from Arduino)
//    METRICS     → update perf metrics
// ════════════════════════════════════════════════════════════
const parseYYMMDDHHMMSS = (ts) => {
  if (!ts || ts.length !== 12) return null;

  const yy = parseInt(ts.slice(0, 2), 10);
  const mm = parseInt(ts.slice(2, 4), 10) - 1;
  const dd = parseInt(ts.slice(4, 6), 10);
  const hh = parseInt(ts.slice(6, 8), 10);
  const mi = parseInt(ts.slice(8, 10), 10);
  const ss = parseInt(ts.slice(10, 12), 10);

  const fullYear = 2000 + yy; // 14 -> 2014
  return new Date(fullYear, mm, dd, hh, mi, ss);
};

async function handleDataFrame(fields, proto, session, tcpAddr) {
  //  Extended TCP: DATA|node|seq|volt|curr|bat|clean|reason|s0H|s0M|e0|s1H|s1M|e1|ts
  //  JSON (MQTT):  already parsed into fields object
  const nodeId = fields.node_id || fields.node || "unknown";

  if (session) {
    session.messagesReceived++;
    session.bytesReceived += fields._rawLen || 0;
    if (session.payloads.length >= 20) session.payloads.shift();
    session.payloads.push({ ...fields, receivedAt: Date.now() });
  }

  log(proto, `DATA seq=${fields.seq} node=${nodeId} V=${fields.panel_voltage}V I=${fields.panel_current}A bat=${fields.battery_voltage}V lux=${fields.lux ?? "—"} clean=${fields.cleaning_stat}`);

  // Update last-used protocol
  await upsertLastProto(nodeId, proto, tcpAddr || null);

  // If schedule info is included in the frame, sync DB
  if (fields.s0_hour !== undefined) {
    const t0 = `${String(fields.s0_hour).padStart(2, "0")}:${String(fields.s0_min).padStart(2, "0")}`;
    const t1 = `${String(fields.s1_hour).padStart(2, "0")}:${String(fields.s1_min).padStart(2, "0")}`;
    await upsertSchedule(nodeId, 0, t0, fields.s0_ena);
    await upsertSchedule(nodeId, 1, t1, fields.s1_ena);
  }

  const logId = await insertLog({
    node_id: nodeId,
    sent_time: fields.sent_time || new Date(),
    seq: fields.seq,
    panel_voltage: fields.panel_voltage,
    panel_current: fields.panel_current,
    battery_voltage: fields.battery_voltage,
    lux: fields.lux ?? null,
    cleaning_stat: fields.cleaning_stat,
    cleaning_reason: fields.cleaning_reason,
    con_type: proto === "MQTT" ? "MQTT" : "TCP/IP",
  });

  if (session) {
    if (logId) session.dbInserted++;
    else session.dbErrors++;
  }
  broadcastStatus();
}

async function handleSchedAck(parts, proto) {
  // SCHED_ACK|node_id|HH:MM|e0|HH:MM|e1
  if (parts.length < 6) return;
  const nodeId = parts[1];
  const [h0, m0] = parts[2].split(":").map(Number);
  const ena0 = parts[3] === "1";
  const [h1, m1] = parts[4].split(":").map(Number);
  const ena1 = parts[5] === "1";

  const t0 = `${String(h0).padStart(2, "0")}:${String(m0).padStart(2, "0")}`;
  const t1 = `${String(h1).padStart(2, "0")}:${String(m1).padStart(2, "0")}`;

  await upsertSchedule(nodeId, 0, t0, ena0);
  await upsertSchedule(nodeId, 1, t1, ena1);
  log(proto, `SCHED_ACK from ${nodeId}: slot0=${t0}(${ena0}) slot1=${t1}(${ena1})`);
}

// ════════════════════════════════════════════════════════════
//  MQTT BROKER  (port 1883)
// ════════════════════════════════════════════════════════════
const mqttTcpServer = net.createServer(aedes.handle);

aedes.on("client", async (client) => {
  log("MQTT", `Connected: ${client.id}`);
  if (!metrics.mqtt.current) metrics.mqtt.current = newSession("MQTT");

  // Send current time so schedule works immediately
  const timeCmd = buildTimeCmd();
  const nodeId = client.id.replace(/^PV_/, "");
  setTimeout(() => {
    aedes.publish(
      { topic: `cmd/${nodeId}`, payload: Buffer.from(timeCmd), qos: 0, retain: false },
      () => { }
    );
    log("MQTT", `TIME sync → cmd/${nodeId}: ${timeCmd}`);
  }, 500);

  broadcastStatus();
});

aedes.on("clientDisconnect", (client) => {
  log("MQTT", `Disconnected: ${client.id}`);
  if (metrics.mqtt.current) {
    metrics.mqtt.current.endTime = Date.now();
    metrics.mqtt.sessions.push({ ...metrics.mqtt.current });
    metrics.mqtt.current = null;
    broadcastStatus();
  }
});

aedes.on("publish", async (packet, client) => {
  if (!client) return;
  const topic = packet.topic;
  const payload = packet.payload.toString();
  const session = metrics.mqtt.current;

  // pv/<node_id>/data  or  pv/data  (legacy)
  if (topic.startsWith("pv/") && topic.endsWith("/data") || topic === "pv/data") {
    let data = {};
    try { 
      data = JSON.parse(payload); 
    } catch (e) { 
      if (session) 
        session.errors++; 
      return; 
    }
    data._rawLen = packet.payload.length;
    // Map JSON keys to canonical field names
    const fields = {
      node_id: data.node || client.id.replace(/^PV_/, ""),
      seq: data.seq,
      panel_voltage: data.v,
      panel_current: data.i,
      battery_voltage: data.bat ?? data.b,
      lux: data.lux ?? null,
      cleaning_stat: data.clean ?? 0,
      cleaning_reason: data.reason ?? "",
      sent_time: data.ts ? parseYYMMDDHHMMSS(data.ts) : null,
      s0_hour: data.s0 ? parseInt(data.s0.split(":")[0]) : undefined,
      s0_min: data.s0 ? parseInt(data.s0.split(":")[1]) : undefined,
      s0_ena: data.e0,
      s1_hour: data.s1 ? parseInt(data.s1.split(":")[0]) : undefined,
      s1_min: data.s1 ? parseInt(data.s1.split(":")[1]) : undefined,
      s1_ena: data.e1,
      _rawLen: packet.payload.length,
    };
    console.log(fields)
    await handleDataFrame(fields, "MQTT", session, null);
  }

  // pv/echo/request
  else if (topic === "pv/echo/request") {
    aedes.publish({ topic: "pv/echo/response", payload: payload, qos: 0, retain: false }, () => { });
    if (session) session.latencySamples.push(1);
  }

  // SCHED_ACK or CLEAN_ACK (published back by Arduino on topic pv/<id>/data or pv/ack)
  else if (payload.startsWith("SCHED_ACK|") || payload.startsWith("CLEAN_ACK|")) {
    const parts = payload.split("|");
    if (payload.startsWith("SCHED_ACK|")) await handleSchedAck(parts, "MQTT");
  }

  // pv/metrics
  else if (topic === "pv/metrics") {
    if (!session) return;
    try {
      const m = JSON.parse(payload);
      session.arduinoReport = m;
      if (m.avg_rtt_ms > 0) session.latencySamples = Array(m.acked || 1).fill(m.avg_rtt_ms);
      session.loss = m.loss_pct ?? 0;
    } catch (e) { session.errors++; }
    broadcastStatus();
  }
});

mqttTcpServer.listen(MQTT_PORT, () => log("MQTT", `Broker on port ${MQTT_PORT}`));

// ════════════════════════════════════════════════════════════
//  TCP SERVER  (port 9000)
// ════════════════════════════════════════════════════════════
const tcpServer = net.createServer((socket) => {
  const addr = `${socket.remoteAddress}:${socket.remotePort}`;
  log("TCP", `Connected: ${addr}`);

  const session = newSession("TCP");
  metrics.tcp.current = session;
  let rxBuf = "";
  let registeredNodeId = null;  // set on first DATA frame

  socket.on("data", (chunk) => {
    rxBuf += chunk.toString();
    let nl;
    while ((nl = rxBuf.indexOf("\n")) !== -1) {
      const line = rxBuf.slice(0, nl).trim();
      rxBuf = rxBuf.slice(nl + 1);
      if (line) handleTcpFrame(line, socket, session, addr);
    }
  });

  socket.on("close", () => {
    log("TCP", `Disconnected: ${addr}`);
    if (registeredNodeId) tcpSockets.delete(registeredNodeId);
    session.endTime = Date.now();
    metrics.tcp.sessions.push({ ...session });
    metrics.tcp.current = null;
    broadcastStatus();
  });

  socket.on("error", (e) => { log("TCP", `Error [${addr}]: ${e.message}`); session.errors++; });

  // Closure so inner function can update registeredNodeId
  async function handleTcpFrame(line, sock, sess, address) {
    const parts = line.split("|");
    const cmd = parts[0];

    if (cmd === "DATA") {
      //  Basic:    DATA|seq|volt|curr|power|ts
      //  Extended: DATA|node|seq|volt|curr|bat|lux|clean|reason|s0H|s0M|e0|s1H|s1M|e1|ts
      const isBasic = /^\d+$/.test(parts[1]);
      let fields = {};

      if (isBasic) {
        fields = {
          node_id: address,
          seq: parseInt(parts[1], 10),
          panel_voltage: parseFloat(parts[2]),
          panel_current: parseFloat(parts[3]),
          battery_voltage: null,
          cleaning_stat: 0,
          cleaning_reason: "",
          sent_time: parts[5] ? new Date(parseInt(parts[5], 10)) : new Date(),
          _rawLen: line.length,
        };
      } else {
        const nodeId = parts[1];

        // Register socket for this node so commands can be pushed
        if (nodeId && !tcpSockets.has(nodeId)) {
          tcpSockets.set(nodeId, sock);
          registeredNodeId = nodeId;
          log("TCP", `Registered socket for node ${nodeId}`);

          // Send TIME sync on first contact
          sock.write(buildTimeCmd() + "\n");
          log("TCP", `TIME sync → ${nodeId}`);
        }

        fields = {
          node_id: nodeId,
          seq: parseInt(parts[2], 10),
          panel_voltage: parseFloat(parts[3]),
          panel_current: parseFloat(parts[4]),
          battery_voltage: parts[5] !== undefined ? parseFloat(parts[5]) : null,
          lux: parts[6] !== undefined ? parseFloat(parts[6]) : null,
          cleaning_stat: parseInt(parts[7], 10) || 0,
          cleaning_reason: parts[8] || "",
          sent_time: parts[15] ? parseYYMMDDHHMMSS(parts[15]) : new Date(),
          s0_hour: parseInt(parts[9], 10),
          s0_min: parseInt(parts[10], 10),
          s0_ena: parts[11] === "1",
          s1_hour: parseInt(parts[12], 10),
          s1_min: parseInt(parts[13], 10),
          s1_ena: parts[14] === "1",
          _rawLen: line.length,
        };
      }

      sock.write(`ACK|${fields.seq}\n`);
      await handleDataFrame(fields, "TCP", sess, address);

    } else if (cmd === "SCHED_ACK" || cmd === "CLEAN_ACK") {
      await handleSchedAck(parts, "TCP");

    } else if (cmd === "METRICS") {
      const kvs = {};
      for (let i = 2; i < parts.length; i++) {
        const eq = parts[i].indexOf("=");
        if (eq !== -1) { const k = parts[i].slice(0, eq); const v = parts[i].slice(eq + 1); kvs[k] = isNaN(v) ? v : parseFloat(v); }
      }
      sess.arduinoReport = kvs;
      if (kvs.avg_rtt > 0 && kvs.acked > 0) sess.latencySamples = Array(Math.floor(kvs.acked)).fill(kvs.avg_rtt);
      sess.loss = kvs.loss ?? 0;
      broadcastStatus();

    } else {
      log("TCP", `Unknown frame: ${line}`);
      sess.errors++;
    }
  }
});

tcpServer.listen(TCP_PORT, () => log("TCP", `Server on port ${TCP_PORT}`));

// ════════════════════════════════════════════════════════════
//  EXPRESS API
// ════════════════════════════════════════════════════════════
const app = express();
app.use(cors());
app.use(express.json());
app.use(express.static(path.join(__dirname, "public")));

const httpServer = http.createServer(app);

// WebSocket
let wsClients = [];
const wss = new WebSocketServer({ server: httpServer });
wss.on("connection", (ws) => {
  wsClients.push(ws);
  ws.send(JSON.stringify(buildPerfPayload()));
  ws.on("close", () => { wsClients = wsClients.filter(c => c !== ws); });
  ws.on("error", () => { wsClients = wsClients.filter(c => c !== ws); });
});
function broadcastStatus() {
  const d = JSON.stringify(buildPerfPayload());
  wsClients.forEach(ws => { try { ws.send(d); } catch (_) { } });
}

// ── GET /api/health ──────────────────────────────────────────
app.get("/api/health", async (req, res) => {
  try {
    await db.query("SELECT 1");
    res.json({
      status: "ok", db: "connected",
      mqtt: mqttTcpServer.listening ? "up" : "down",
      tcp: tcpServer.listening ? "up" : "down",
      ts: new Date().toISOString()
    });
  } catch (e) {
    res.status(503).json({ status: "error", db: "disconnected", message: e.message });
  }
});

// ── GET /api/nodes ───────────────────────────────────────────
app.get("/api/nodes", async (req, res) => {
  try {
    const [rows] = await db.query(
      `SELECT n.node_id, n.node_name, n.node_long, n.node_lat,
              n.panel_isc, n.panel_voc, n.panel_imp, n.panel_vmp,
              n.battery_voltage, n.battery_current,
              p.proto AS last_proto, p.last_seen
         FROM node_info n
         LEFT JOIN node_last_proto p ON p.node_id = n.node_id
         ORDER BY n.node_id`
    );
    res.json(rows);
  } catch (e) {
    log("API_ERR", `GET /api/nodes: ${e.message}`);
    res.status(500).json({ error: e.message });
  }
});

// ── GET /api/logs ────────────────────────────────────────────
app.get("/api/logs", async (req, res) => {
  const limit = Math.min(parseInt(req.query.limit) || 200, 1000);
  const nodeId = req.query.node_id || null;
  try {
    const [rows] = await db.query(
      nodeId
        ? `SELECT * FROM working_log WHERE log_node=? ORDER BY log_id DESC LIMIT ?`
        : `SELECT * FROM working_log ORDER BY log_id DESC LIMIT ?`,
      nodeId ? [nodeId, limit] : [limit]
    );
    res.json(rows);
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

// ── GET /api/logs/:node_id ───────────────────────────────────
app.get("/api/logs/:node_id", async (req, res) => {
  const limit = Math.min(parseInt(req.query.limit) || 50, 500);
  try {
    const [rows] = await db.query(
      `SELECT * FROM working_log WHERE log_node=? ORDER BY log_id DESC LIMIT ?`,
      [req.params.node_id, limit]
    );
    res.json(rows);
  } catch (e) { res.status(500).json({ error: e.message }); }
});

// ── GET /api/schedule/:node_id ───────────────────────────────
app.get("/api/schedule/:node_id", async (req, res) => {
  const sched = await getSchedule(req.params.node_id);
  if (!sched) return res.status(500).json({ error: "DB error" });
  res.json(sched);
});

// ── POST /api/schedule ───────────────────────────────────────
// Body: { node_id, slots: [ {slot:0,time:"HH:MM",enabled:true}, {slot:1,...} ] }
app.post("/api/schedule", async (req, res) => {
  const { node_id, slots } = req.body;
  if (!node_id || !Array.isArray(slots)) {
    return res.status(400).json({ error: "node_id and slots[] required" });
  }

  // Validate + save to DB
  const saved = [];
  for (const s of slots) {
    if (s.slot !== 0 && s.slot !== 1) continue;
    if (!/^\d{2}:\d{2}$/.test(s.time)) {
      return res.status(400).json({ error: `Invalid time format for slot ${s.slot} — use HH:MM` });
    }
    await upsertSchedule(node_id, s.slot, s.time, s.enabled !== false);
    saved.push(s);
  }

  // Build command and send to Arduino via last-used protocol
  const fullSched = await getSchedule(node_id);
  const cmdStr = buildSchedCmd(fullSched[0], fullSched[1]);
  const result = await sendCommandToNode(node_id, cmdStr);

  // Also send SCHED_OFF for disabled slots
  for (const s of fullSched) {
    if (!s.enabled) {
      await sendCommandToNode(node_id, `SCHED_OFF|${s.slot}`);
    }
  }

  await insertCmd(node_id, cmdStr);

  log("API", `Schedule set for ${node_id}: ${cmdStr} | delivery=${JSON.stringify(result)}`);
  res.json({ node_id, schedule: fullSched, delivery: result });
});

// ── POST /api/cmd ── send command ────────────────────────────
// Body: { node_id, cmd }  (cmd defaults to "CLEAN")
app.post("/api/cmd", async (req, res) => {
  const { node_id, cmd = "CLEAN" } = req.body;
  if (!node_id) return res.status(400).json({ error: "node_id required" });

  const cmdId = await insertCmd(node_id, cmd);
  const result = await sendCommandToNode(node_id, cmd);

  res.json({ cmd_id: cmdId, node_id, cmd, delivery: result });
});

// ── POST /api/log ── manual insert ───────────────────────────
app.post("/api/log", async (req, res) => {
  const { node_id, sent_time, seq, panel_voltage, panel_current,
    battery_voltage, cleaning_stat = 0, cleaning_reason = "", con_type = "HTTP" } = req.body;
  if (!node_id) return res.status(400).json({ error: "node_id required" });
  const logId = await insertLog({
    node_id, sent_time, seq, panel_voltage, panel_current,
    battery_voltage, cleaning_stat, cleaning_reason, con_type
  });
  if (logId) res.json({ inserted: logId });
  else res.status(500).json({ error: "DB insert failed" });
});

// ── GET /api/analytics/:node_id ─────────────────────────────
// Query params: ?date=YYYY-MM-DD  (defaults to today)
app.get("/api/analytics/:node_id", async (req, res) => {
  const { node_id } = req.params;
  const date = req.query.date || new Date().toISOString().slice(0, 10);
  try {
    const [rows] = await db.query(
      `SELECT
         HOUR(log_sent_time)                                        AS hour,
         COUNT(*)                                                   AS count,
         AVG(CAST(log_panel_voltage  AS DECIMAL(10,4)))            AS avg_voltage,
         MIN(CAST(log_panel_voltage  AS DECIMAL(10,4)))            AS min_voltage,
         MAX(CAST(log_panel_voltage  AS DECIMAL(10,4)))            AS max_voltage,
         AVG(CAST(log_panel_current  AS DECIMAL(10,4)))            AS avg_current,
         MIN(CAST(log_panel_current  AS DECIMAL(10,4)))            AS min_current,
         MAX(CAST(log_panel_current  AS DECIMAL(10,4)))            AS max_current,
         AVG(CAST(log_battery_voltage AS DECIMAL(10,4)))           AS avg_battery,
         MIN(CAST(log_battery_voltage AS DECIMAL(10,4)))           AS min_battery,
         MAX(CAST(log_battery_voltage AS DECIMAL(10,4)))           AS max_battery,
         SUM(log_cleaning_stat)                                    AS cleaning_events
       FROM working_log
       WHERE log_node = ? AND DATE(log_sent_time) = ?
       GROUP BY HOUR(log_sent_time)
       ORDER BY hour`,
      [node_id, date]
    );
    res.json(rows);
  } catch (e) {
    log("API_ERR", `GET /api/analytics/${node_id}: ${e.message}`);
    res.status(500).json({ error: e.message });
  }
});

// ── GET /api/cmdlog ──────────────────────────────────────────
// Query params: ?node_id=X&limit=200
app.get("/api/cmdlog", async (req, res) => {
  const limit = Math.min(parseInt(req.query.limit) || 200, 1000);
  const nodeId = req.query.node_id || null;
  try {
    const [rows] = await db.query(
      nodeId
        ? `SELECT c.cmd_id, c.cmd_sent, c.cmd_node, c.cmd_text,
                  n.node_name, n.node_id AS node_id_str
             FROM cmd_log c
             LEFT JOIN node_info n ON n.node_id = c.cmd_node
             WHERE c.cmd_node = ?
             ORDER BY c.cmd_sent DESC LIMIT ?`
        : `SELECT c.cmd_id, c.cmd_sent, c.cmd_node, c.cmd_text,
                  n.node_name, n.node_id AS node_id_str
             FROM cmd_log c
             LEFT JOIN node_info n ON n.node_id = c.cmd_node
             ORDER BY c.cmd_sent DESC LIMIT ?`,
      nodeId ? [nodeId, limit] : [limit]
    );
    res.json(rows);
  } catch (e) {
    log("API_ERR", `GET /api/cmdlog: ${e.message}`);
    res.status(500).json({ error: e.message });
  }
});

// ── GET /api/metrics ─────────────────────────────────────────
app.get("/api/metrics", (req, res) => res.json(buildPerfPayload()));

// ── POST /api/metrics/reset ───────────────────────────────────
app.post("/api/metrics/reset", (req, res) => {
  metrics.mqtt.sessions = []; metrics.tcp.sessions = [];
  metrics.mqtt.current = null; metrics.tcp.current = null;
  broadcastStatus(); res.json({ status: "reset" });
});

// ════════════════════════════════════════════════════════════
//  PERF PAYLOAD BUILDER
// ════════════════════════════════════════════════════════════
function buildPerfPayload() {
  const mqttS = metrics.mqtt.current || metrics.mqtt.sessions.at(-1) || null;
  const tcpS = metrics.tcp.current || metrics.tcp.sessions.at(-1) || null;
  function sum(s) {
    if (!s) return null;
    const st = computeStats(s);
    const el = (s.endTime || Date.now()) - s.startTime;
    return {
      protocol: s.protocol, status: s.endTime ? "completed" : "running",
      messagesReceived: s.messagesReceived, bytesReceived: s.bytesReceived,
      throughput: +(s.messagesReceived / Math.max(el / 1000, 1)).toFixed(2),
      latency: st, loss_pct: +((s.loss || 0)).toFixed(1),
      errors: s.errors, dbInserted: s.dbInserted, dbErrors: s.dbErrors,
      elapsed_ms: el, lastPayloads: s.payloads.slice(-5), arduinoReport: s.arduinoReport
    };
  }
  return {
    ts: new Date().toISOString(), mqtt: sum(mqttS), tcp: sum(tcpS),
    sessions: { mqtt: metrics.mqtt.sessions.length, tcp: metrics.tcp.sessions.length }
  };
}

// ════════════════════════════════════════════════════════════
//  START + STARTUP DB CHECK
// ════════════════════════════════════════════════════════════
httpServer.listen(HTTP_PORT, async () => {
  log("HTTP", `API → http://localhost:${HTTP_PORT}`);
  try {
    await db.query("SELECT 1");
    log("DB", "MySQL OK ✓");
  } catch (e) {
    log("DB", `⚠ MySQL unreachable: ${e.message}`);
  }
});

process.on("SIGINT", () => shutdown("SIGINT"));
process.on("SIGTERM", () => shutdown("SIGTERM"));
function shutdown(sig) {
  log("SYSTEM", `${sig} — shutting down`);
  mqttTcpServer.close(); tcpServer.close();
  httpServer.close(() => { db.end().then(() => process.exit(0)); });
}
