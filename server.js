// Klash Court — serveur en ligne : sert le jeu, relaie les matchs, gère amis connectés, invitations et file classée
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
  // Réglages publics des comptes joueurs (Supabase) : renseignés dans les variables d'environnement du serveur
  if (url === '/config.js') {
    const cfg = { CC_SUPABASE_URL: process.env.SUPABASE_URL || '', CC_SUPABASE_KEY: process.env.SUPABASE_ANON_KEY || '', CC_PRIVACY: process.env.PRIVACY_URL || '/privacy.html' };
    res.writeHead(200, { 'Content-Type': 'text/javascript; charset=utf-8', 'Cache-Control': 'no-cache', 'X-Content-Type-Options': 'nosniff' });
    res.end(Object.entries(cfg).map(([k, v]) => `window.${k}=window.${k}||${JSON.stringify(v)};`).join('\n'));
    return;
  }
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

// ---- Identité (compte Supabase vérifié), amis en ligne, invitations, file classée ----
const SB_URL = (process.env.SUPABASE_URL || '').replace(/\/$/, ''), SB_KEY = process.env.SUPABASE_ANON_KEY || '';
const DEV_TRUST = process.env.DEV_TRUST === '1' && !SB_URL;   // tests locaux uniquement
const users = new Map();     // uid -> ws
const invites = new Map();   // id -> { from, to, at }
let queue = [];              // [{ ws, info, tr, at }]
async function verify(token) {
  if (!SB_URL || !SB_KEY || !token) return null;
  try {
    const h = { apikey: SB_KEY, Authorization: 'Bearer ' + token };
    const r = await fetch(SB_URL + '/auth/v1/user', { headers: h });
    if (!r.ok) return null;
    const u = await r.json(); if (!u || !u.id) return null;
    let pseudo = (u.user_metadata && u.user_metadata.pseudo) || 'Joueur', tr = 0;
    try {
      const p = await fetch(SB_URL + '/rest/v1/profiles?select=pseudo,trophies&id=eq.' + encodeURIComponent(u.id), { headers: h });
      if (p.ok) { const a = await p.json(); if (a && a[0]) { pseudo = a[0].pseudo || pseudo; tr = a[0].trophies || 0; } }
    } catch (e) {}
    return { uid: u.id, pseudo: String(pseudo).slice(0, 16), tr };
  } catch (e) { return null; }
}
function statusOf(uid) {
  const w = users.get(uid);
  if (!w || w.readyState !== 1) return 'offline';
  const room = w.room && rooms.get(w.room);
  if (room && room.started && room.host && room.guest) return 'ingame';
  return 'online';
}
function unqueue(ws) { queue = queue.filter(q => q.ws !== ws); }
function makeRoom(host, guest, guestInfo, opts) {
  const code = newCode(); if (!code) { send(host, { t: 'error', msg: 'Serveur plein, réessaie.' }); send(guest, { t: 'error', msg: 'Serveur plein, réessaie.' }); return; }
  const token = Math.random().toString(36).slice(2) + Date.now().toString(36);
  const ranked = !!(opts && opts.ranked);
  rooms.set(code, { host, guest, created: Date.now(), token, hostGone: 0, guestInfo, ranked });
  host.room = code; host.role = 'host'; guest.room = code; guest.role = 'guest';
  send(host, { t: 'created', code, token, ranked: ranked ? 1 : 0, opp: guest.pseudo || '' });
  send(guest, { t: 'joined', code, ranked: ranked ? 1 : 0, opp: host.pseudo || '' });
  send(host, { t: 'guest', info: guestInfo });
}

wss.on('connection', (ws) => {
  ws.isAlive = true; ws.room = null; ws.role = null;
  ws.on('pong', () => { ws.isAlive = true; });
  ws.on('message', (raw, isBinary) => {
    const str = raw.toString();
    // Fast path: relay game traffic without parsing
    if (ws.room && str.charCodeAt(0) === 82 /* 'R' */) {
      const room = rooms.get(ws.room); if (!room) return;
      if (!room.started && ws.role === 'host' && str.startsWith('R{"t":"start"')) room.started = true;
      send(ws.role === 'host' ? room.guest : room.host, str.slice(1));
      return;
    }
    let m; try { m = JSON.parse(str); } catch (e) { return; }
    if (m.t === 'ping') { send(ws, { t: 'pong', k: m.k }); return; }
    if (m.t === 'hello') {
      (async () => {
        let id = await verify(m.token);
        if (!id && DEV_TRUST && m.uid) id = { uid: String(m.uid), pseudo: String(m.pseudo || 'Joueur').slice(0, 16), tr: +m.tr || 0 };
        if (!id) { send(ws, { t: 'helloFail' }); return; }
        if (ws.uid && users.get(ws.uid) === ws) users.delete(ws.uid);
        ws.uid = id.uid; ws.pseudo = id.pseudo; ws.tr = Number.isFinite(+m.tr) ? +m.tr : id.tr;
        users.set(id.uid, ws);
        send(ws, { t: 'hello', uid: id.uid, pseudo: id.pseudo });
      })();
      return;
    }
    if (m.t === 'tr') { ws.tr = Math.max(0, Math.min(5000, +m.tr || 0)); return; }
    if (m.t === 'presence') {
      const map = {};
      (Array.isArray(m.uids) ? m.uids.slice(0, 200) : []).forEach(u => { map[u] = statusOf(String(u)); });
      send(ws, { t: 'presence', map });
      return;
    }
    if (m.t === 'invite') {
      if (!ws.uid) { send(ws, { t: 'inviteFail', reason: 'auth' }); return; }
      const to = users.get(String(m.to));
      if (!to || to.readyState !== 1 || to === ws) { send(ws, { t: 'inviteFail', reason: 'offline', to: m.to }); return; }
      if (statusOf(to.uid) === 'ingame') { send(ws, { t: 'inviteFail', reason: 'busy', to: m.to }); return; }
      const id = Math.random().toString(36).slice(2, 10);
      invites.set(id, { from: ws, to, at: Date.now() });
      send(to, { t: 'invite', id, from: { uid: ws.uid, pseudo: ws.pseudo, tr: ws.tr || 0 }, char: String((m.info || {}).char || '') });
      send(ws, { t: 'inviteSent', to: to.uid, pseudo: to.pseudo });
      return;
    }
    if (m.t === 'inviteReply') {
      const inv = invites.get(m.id); invites.delete(m.id);
      if (!inv || inv.to !== ws) { send(ws, { t: 'error', msg: "Cette invitation n'est plus valable." }); return; }
      const host = inv.from;
      if (!m.ok) { send(host, { t: 'inviteDeclined', pseudo: ws.pseudo }); return; }
      if (!host || host.readyState !== 1) { send(ws, { t: 'error', msg: "Ton ami s'est déconnecté." }); return; }
      if (statusOf(host.uid) === 'ingame') { send(ws, { t: 'error', msg: 'Ton ami est déjà en match.' }); return; }
      unqueue(host); unqueue(ws); leave(host, true); leave(ws, true);
      makeRoom(host, ws, m.info || {}, { ranked: false });
      return;
    }
    if (m.t === 'queue') {
      if (!ws.uid) { send(ws, { t: 'error', msg: 'Connecte-toi à ton compte pour jouer en classé.' }); return; }
      unqueue(ws); leave(ws, true);
      const me = { ws, info: m.info || {}, tr: Math.max(0, +m.tr || ws.tr || 0), at: Date.now() };
      let best = -1, bd = Infinity;
      queue.forEach((q, i) => { if (q.ws.uid === ws.uid || q.ws.readyState !== 1) return; const d = Math.abs(q.tr - me.tr); if (d < bd) { bd = d; best = i; } });
      if (best >= 0) { const o = queue.splice(best, 1)[0]; makeRoom(o.ws, ws, me.info, { ranked: true }); }
      else { queue.push(me); send(ws, { t: 'queued', n: queue.length }); }
      return;
    }
    if (m.t === 'unqueue') { unqueue(ws); send(ws, { t: 'unqueued' }); return; }
    if (m.t === 'create') {
      if (ws.room) leave(ws);
      const code = newCode(); if (!code) { send(ws, { t: 'error', msg: 'Serveur plein, réessaie.' }); return; }
      const token = Math.random().toString(36).slice(2) + Date.now().toString(36);
      rooms.set(code, { host: ws, guest: null, created: Date.now(), token, hostGone: 0, guestInfo: null });
      ws.room = code; ws.role = 'host';
      send(ws, { t: 'created', code, token });
    } else if (m.t === 'resume') {
      // l'hôte revient (ex. il est allé envoyer le code dans Messages et iOS a coupé la connexion)
      const code = String(m.code || '').toUpperCase().trim();
      const room = rooms.get(code);
      if (!room || room.token !== m.token) { send(ws, { t: 'resumeFail' }); return; }
      if (room.host && room.host !== ws) { room.host.room = null; try { room.host.terminate(); } catch (e) {} }
      room.host = ws; room.hostGone = 0; ws.room = code; ws.role = 'host';
      send(ws, { t: 'resumed', code });
      if (room.guest) send(ws, { t: 'guest', info: room.guestInfo || {} });
    } else if (m.t === 'join') {
      const code = String(m.code || '').toUpperCase().trim();
      const room = rooms.get(code);
      if (!room) { send(ws, { t: 'error', msg: 'Aucun salon avec ce code.' }); return; }
      if (room.guest) { send(ws, { t: 'error', msg: 'Ce salon est déjà complet.' }); return; }
      if (ws.room) leave(ws);
      room.guest = ws; room.guestInfo = m.info || {}; ws.room = code; ws.role = 'guest';
      send(ws, { t: 'joined', code });
      send(room.host, { t: 'guest', info: room.guestInfo });
    } else if (m.t === 'leave') {
      leave(ws, true);
    }
  });
  ws.on('close', () => {
    leave(ws, false); unqueue(ws);
    if (ws.uid && users.get(ws.uid) === ws) users.delete(ws.uid);
    for (const [id, inv] of invites) if (inv.from === ws || inv.to === ws) { if (inv.to === ws) send(inv.from, { t: 'inviteDeclined', pseudo: ws.pseudo, gone: 1 }); invites.delete(id); }
  });
});

function leave(ws, explicit) {
  const code = ws.room; if (!code) return;
  const room = rooms.get(code); ws.room = null;
  if (!room) return;
  if (room.host === ws && !explicit && !room.started) {
    // coupure involontaire avant le match : on garde le salon 3 minutes pour que l'hôte revienne
    room.host = null; room.hostGone = Date.now();
    return;
  }
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
  for (const [id, inv] of invites) if (now - inv.at > 60000) invites.delete(id);
  queue = queue.filter(q => q.ws.readyState === 1);
  for (const [code, room] of rooms) {
    if (!room.host && room.hostGone && now - room.hostGone > 3 * 60 * 1000) { send(room.guest, { t: 'peerLeft' }); if (room.guest) room.guest.room = null; rooms.delete(code); continue; }
    if (!room.guest && now - room.created > 30 * 60 * 1000) { send(room.host, { t: 'error', msg: 'Salon expiré.' }); rooms.delete(code); }
  }
}, 15000);

server.listen(PORT, () => console.log(`Klash Court en ligne sur http://localhost:${PORT}`));
