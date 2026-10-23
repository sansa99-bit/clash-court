// Clash Court — serveur de parties privées
// Sert le jeu (dossier public/) et relaie les messages entre l'hôte et l'invité d'un salon.
const http = require('http');
const fs = require('fs');
const path = require('path');
const { WebSocketServer } = require('ws');

const PORT = process.env.PORT || 3000;
// sert public/ s'il existe, sinon les fichiers posés à côté de server.js (version Render)
const PUBLIC = fs.existsSync(path.join(__dirname, 'public')) ? path.join(__dirname, 'public') : __dirname;
const TYPES = { '.html': 'text/html; charset=utf-8', '.js': 'text/javascript', '.css': 'text/css', '.png': 'image/png', '.svg': 'image/svg+xml', '.ico': 'image/x-icon', '.json': 'application/json', '.woff2': 'font/woff2', '.woff': 'font/woff', '.webmanifest': 'application/manifest+json', '.txt': 'text/plain; charset=utf-8' };

const server = http.createServer((req, res) => {
  let url;
  try { url = decodeURIComponent((req.url || '/').split('?')[0]); } catch (e) { res.writeHead(400); res.end(); return; }
  if (url === '/health') { res.writeHead(200); res.end('ok'); return; }
  let file = path.normalize(path.join(PUBLIC, url === '/' ? 'index.html' : url));
  if (!file.startsWith(PUBLIC)) { res.writeHead(403); res.end(); return; }
  fs.readFile(file, (err, data) => {
    if (err) { res.writeHead(404); res.end('Introuvable'); return; }
    const immutable = url.startsWith('/vendor/');
    res.writeHead(200, { 'Content-Type': TYPES[path.extname(file)] || 'application/octet-stream', 'Cache-Control': immutable ? 'public, max-age=604800' : 'no-cache', 'X-Content-Type-Options': 'nosniff' });
    res.end(data);
  });
});

const wss = new WebSocketServer({ server, maxPayload: 256 * 1024 });
const rooms = new Map(); // code -> { host, guest, created }
const LETTERS = 'ABCDEFGHJKLMNPQRSTUVWXYZ';
function newCode() {
  for (let i = 0; i < 1000; i++) {
    let c = ''; for (let j = 0; j < 4; j++) c += LETTERS[Math.floor(Math.random() * LETTERS.length)];
    if (!rooms.has(c)) return c;
  }
  return null;
}
const send = (ws, msg) => { if (ws && ws.readyState === 1) ws.send(typeof msg === 'string' ? msg : JSON.stringify(msg)); };

wss.on('connection', (ws) => {
  ws.isAlive = true; ws.room = null; ws.role = null;
  ws.on('pong', () => { ws.isAlive = true; });
  ws.on('message', (raw, isBinary) => {
    const str = raw.toString();
    // Fast path: relay game traffic without parsing
    if (ws.room && str.charCodeAt(0) === 82 /* 'R' */) {
      const room = rooms.get(ws.room); if (!room) return;
      send(ws.role === 'host' ? room.guest : room.host, str.slice(1));
      return;
    }
    let m; try { m = JSON.parse(str); } catch (e) { return; }
    if (m.t === 'create') {
      if (ws.room) leave(ws);
      const code = newCode(); if (!code) { send(ws, { t: 'error', msg: 'Serveur plein, réessaie.' }); return; }
      rooms.set(code, { host: ws, guest: null, created: Date.now() });
      ws.room = code; ws.role = 'host';
      send(ws, { t: 'created', code });
    } else if (m.t === 'join') {
      const code = String(m.code || '').toUpperCase().trim();
      const room = rooms.get(code);
      if (!room) { send(ws, { t: 'error', msg: 'Aucun salon avec ce code.' }); return; }
      if (room.guest) { send(ws, { t: 'error', msg: 'Ce salon est déjà complet.' }); return; }
      if (ws.room) leave(ws);
      room.guest = ws; ws.room = code; ws.role = 'guest';
      send(ws, { t: 'joined', code });
      send(room.host, { t: 'guest', info: m.info || {} });
    } else if (m.t === 'leave') {
      leave(ws);
    }
  });
  ws.on('close', () => leave(ws));
});

function leave(ws) {
  const code = ws.room; if (!code) return;
  const room = rooms.get(code); ws.room = null;
  if (!room) return;
  if (room.host === ws) {
    send(room.guest, { t: 'peerLeft' });
    if (room.guest) room.guest.room = null;
    rooms.delete(code);
  } else if (room.guest === ws) {
    room.guest = null;
    send(room.host, { t: 'peerLeft' });
  }
}

// Heartbeat + purge des salons abandonnés
setInterval(() => {
  wss.clients.forEach((ws) => { if (!ws.isAlive) return ws.terminate(); ws.isAlive = false; ws.ping(); });
  const now = Date.now();
  for (const [code, room] of rooms) if (!room.guest && now - room.created > 30 * 60 * 1000) { send(room.host, { t: 'error', msg: 'Salon expiré.' }); rooms.delete(code); }
}, 15000);

server.listen(PORT, () => console.log(`Clash Court en ligne sur http://localhost:${PORT}`));
