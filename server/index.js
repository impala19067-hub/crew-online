/**
 * The Crew Online — Cloud Relay Server
 */

const express = require('express');
const http = require('http');
const { Server } = require('socket.io');
const { v4: uuidv4 } = require('uuid');

const app = express();
const server = http.createServer(app);
const io = new Server(server, {
  cors: { origin: '*', methods: ['GET', 'POST'] }
});

app.use(express.json());

// ─── In-memory state ────────────────────────────────────────────────────────
const rooms = new Map();   // roomCode → { id, name, host, players[], chat[], maxPlayers, isPublic, gameMode }
const players = new Map(); // socketId → { name, roomCode, avatar }

// ─── Helpers ─────────────────────────────────────────────────────────────────
function generateRoomCode() {
  return Math.random().toString(36).substring(2, 8).toUpperCase();
}

function getRoomList() {
  return Array.from(rooms.values())
    .filter(r => r.isPublic)
    .map(({ id, name, host, players, maxPlayers, gameMode, createdAt }) => ({
      id, name, host, playerCount: players.length, maxPlayers, gameMode, createdAt
    }));
}

function broadcastRoomUpdate(roomCode) {
  const room = rooms.get(roomCode);
  if (!room) return;
  io.to(roomCode).emit('room:update', room);
  io.emit('lobby:update', getRoomList());
}

// ─── REST endpoints ───────────────────────────────────────────────────────────
app.get('/', (req, res) => res.json({ status: 'The Crew Online Server Running', rooms: rooms.size }));
app.get('/health', (req, res) => res.json({ status: 'ok', rooms: rooms.size, players: players.size }));
app.get('/rooms', (req, res) => res.json(getRoomList()));

// ─── WebSocket events ────────────────────────────────────────────────────────
io.on('connection', (socket) => {
  console.log(`[+] Connected: ${socket.id}`);

  // Player sets their identity
  socket.on('player:identify', ({ name }) => {
    players.set(socket.id, { name: name || 'Anonymous', roomCode: null });
    socket.emit('player:identified', { name: players.get(socket.id).name });
  });

  // Create a new room
  socket.on('room:create', ({ roomName, maxPlayers = 4, gameMode = 'Free Roam', isPublic = true }) => {
    const player = players.get(socket.id);
    if (!player) return socket.emit('error', 'Identify first');

    if (player.roomCode) leaveRoom(socket, player);

    const code = generateRoomCode();
    const room = {
      id: code,
      name: roomName || `${player.name}'s Crew`,
      host: player.name,
      hostSocketId: socket.id,
      players: [{ id: socket.id, name: player.name, isHost: true }],
      chat: [],
      maxPlayers: Math.min(maxPlayers, 8),
      gameMode,
      isPublic,
      createdAt: new Date().toISOString(),
    };

    rooms.set(code, room);
    player.roomCode = code;
    socket.join(code);

    socket.emit('room:created', room);
    io.emit('lobby:update', getRoomList());
    console.log(`[ROOM] Created: ${code} by ${player.name}`);
  });

  // Join a room by code
  socket.on('room:join', ({ code }) => {
    const player = players.get(socket.id);
    if (!player) return socket.emit('error', 'Identify first');

    const upperCode = code.toUpperCase();
    const room = rooms.get(upperCode);
    if (!room) return socket.emit('room:join:error', 'Room not found. Check your code!');
    if (room.players.length >= room.maxPlayers) return socket.emit('room:join:error', 'Room is full!');

    if (player.roomCode) leaveRoom(socket, player);

    room.players.push({ id: socket.id, name: player.name, isHost: false });
    player.roomCode = upperCode;
    socket.join(upperCode);

    io.to(upperCode).emit('room:player:joined', { name: player.name });
    socket.emit('room:joined', room);
    broadcastRoomUpdate(upperCode);

    socket.emit('chat:history', room.chat.slice(-50));
    console.log(`[ROOM] ${player.name} joined ${upperCode}`);
  });

  // Chat message
  socket.on('chat:message', ({ text }) => {
    const player = players.get(socket.id);
    if (!player || !player.roomCode) return;
    const room = rooms.get(player.roomCode);
    if (!room) return;

    const msg = {
      id: uuidv4(),
      sender: player.name,
      text: text.substring(0, 300),
      timestamp: Date.now(),
    };
    room.chat.push(msg);
    if (room.chat.length > 200) room.chat.shift();
    io.to(player.roomCode).emit('chat:message', msg);
  });

  // Host kicks a player
  socket.on('room:kick', ({ targetSocketId }) => {
    const player = players.get(socket.id);
    if (!player?.roomCode) return;
    const room = rooms.get(player.roomCode);
    if (!room || room.hostSocketId !== socket.id) return;

    const targetSocket = io.sockets.sockets.get(targetSocketId);
    if (targetSocket) {
      targetSocket.emit('room:kicked');
      const targetPlayer = players.get(targetSocketId);
      if (targetPlayer) leaveRoom(targetSocket, targetPlayer);
    }
  });

  // Host starts the game
  socket.on('room:start', () => {
    const player = players.get(socket.id);
    if (!player?.roomCode) return;
    const room = rooms.get(player.roomCode);
    if (!room || room.hostSocketId !== socket.id) return;

    io.to(player.roomCode).emit('room:game:start', {
      message: 'Host is launching the game. Launch your game now!',
    });
  });

  // Leave room
  socket.on('room:leave', () => {
    const player = players.get(socket.id);
    if (player) leaveRoom(socket, player);
  });

  // Disconnect
  socket.on('disconnect', () => {
    const player = players.get(socket.id);
    if (player) leaveRoom(socket, player);
    players.delete(socket.id);
    console.log(`[-] Disconnected: ${socket.id}`);
  });
});

function leaveRoom(socket, player) {
  const code = player.roomCode;
  if (!code) return;
  const room = rooms.get(code);
  if (!room) { player.roomCode = null; return; }

  socket.leave(code);
  room.players = room.players.filter(p => p.id !== socket.id);
  player.roomCode = null;

  if (room.players.length === 0) {
    rooms.delete(code);
    io.emit('lobby:update', getRoomList());
    console.log(`[ROOM] Deleted empty room ${code}`);
  } else {
    if (room.hostSocketId === socket.id) {
      const newHost = room.players[0];
      room.host = newHost.name;
      room.hostSocketId = newHost.id;
      newHost.isHost = true;
      io.to(code).emit('room:host:changed', { name: newHost.name });
    }
    io.to(code).emit('room:player:left', { name: player.name });
    broadcastRoomUpdate(code);
  }
}

// Keep-alive self ping for Render
const SERVER_URL = process.env.RENDER_EXTERNAL_URL || 'https://crew-online.onrender.com';
setInterval(() => {
  http.get(`${SERVER_URL}/health`, (res) => {
    console.log(`[KEEP-ALIVE] Pinged ${SERVER_URL}/health status: ${res.statusCode}`);
  }).on('error', (err) => {
    console.log(`[KEEP-ALIVE] Ping error: ${err.message}`);
  });
}, 10 * 60 * 1000);

const PORT = process.env.PORT || 3000;
server.listen(PORT, () => {
  console.log(`\n🏎️  The Crew Online Relay Server running on port ${PORT}\n`);
});
