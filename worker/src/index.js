// ============================================================
// MEME GAME MULTIPLAYER SERVER — Cloudflare Worker v2
// Dynamic image upload by host before game starts
// ============================================================

export default {
  async fetch(request, env) {
    const url = new URL(request.url);

    const cors = {
      'Access-Control-Allow-Origin': '*',
      'Access-Control-Allow-Methods': 'GET, POST, OPTIONS',
      'Access-Control-Allow-Headers': 'Content-Type',
    };

    if (request.method === 'OPTIONS') {
      return new Response(null, { headers: cors });
    }

    if (url.pathname === '/ws') {
      const roomCode = url.searchParams.get('room');
      if (!roomCode) return new Response('Missing room', { status: 400 });
      const id = env.GAME_ROOM.idFromName(roomCode);
      const room = env.GAME_ROOM.get(id);
      return room.fetch(request);
    }

    if (url.pathname === '/create') {
      const code = Math.floor(1000 + Math.random() * 9000).toString();
      const id = env.GAME_ROOM.idFromName(code);
      const room = env.GAME_ROOM.get(id);
      await room.fetch(new Request(`${url.origin}/init`, { method: 'POST' }));
      return new Response(JSON.stringify({ code }), {
        headers: { ...cors, 'Content-Type': 'application/json' }
      });
    }

    return new Response('Meme Game Server 🎮', { headers: cors });
  }
};

// ============================================================
// DURABLE OBJECT
// ============================================================
export class GameRoom {
  constructor(state, env) {
    this.state = state;
    this.sessions = new Map();
    this.gameState = {
      phase: 'lobby',
      players: {},
      round: 0,
      roundOrder: [],
      currentRound: null,
      submissions: [],
      votes: {},
      voteIdx: 0,
      timerEnd: null,
    };
    // Dynamic images uploaded by host: array of { imgData, topic }
    this.uploadedImages = [];
    this.timerTimeout = null;
  }

  async fetch(request) {
    const url = new URL(request.url);

    if (url.pathname === '/init') {
      return new Response('ok');
    }

    if (request.headers.get('Upgrade') !== 'websocket') {
      return new Response('Expected WebSocket', { status: 426 });
    }

    const name = url.searchParams.get('name') || 'אנונימי';
    const sessionId = crypto.randomUUID();

    const [client, server] = Object.values(new WebSocketPair());
    server.accept();

    const isFirstPlayer = this.sessions.size === 0;
    this.sessions.set(sessionId, { ws: server, name, isHost: isFirstPlayer });

    this.gameState.players[name] = {
      score: this.gameState.players[name]?.score || 0,
      isHost: isFirstPlayer
    };

    server.addEventListener('message', (evt) => {
      try {
        const msg = JSON.parse(evt.data);
        this.handleMessage(sessionId, msg);
      } catch (e) {}
    });

    server.addEventListener('close', () => {
      this.sessions.delete(sessionId);
      if (this.gameState.phase === 'lobby' || this.gameState.phase === 'uploading') {
        delete this.gameState.players[name];
      }
      this.broadcast({ type: 'player_left', name, players: this.gameState.players });
    });

    // Send current state — include upload progress if in uploading phase
    this.sendTo(server, {
      type: 'welcome',
      name,
      isHost: isFirstPlayer,
      state: this.gameState,
      uploadedCount: this.uploadedImages.filter(Boolean).length,
      // Send previews of already uploaded images (thumbnails only, not full data)
      uploadPreviews: this.uploadedImages.map((img, i) =>
        img ? { index: i, topic: img.topic, hasImage: true } : null
      )
    });

    this.broadcast({ type: 'player_joined', name, players: this.gameState.players });

    return new Response(null, { status: 101, webSocket: client });
  }

  handleMessage(sessionId, msg) {
    const session = this.sessions.get(sessionId);
    if (!session) return;

    switch (msg.type) {

      // ── HOST: enter upload phase ──
      case 'begin_upload': {
        if (!session.isHost) return;
        this.gameState.phase = 'uploading';
        this.uploadedImages = [];
        this.broadcast({
          type: 'upload_phase_started',
          hostName: session.name
        });
        break;
      }

      // ── HOST: upload one image ──
      case 'upload_image': {
        if (!session.isHost) return;
        if (this.gameState.phase !== 'uploading') return;

        const { index, imgData, topic } = msg;
        if (index < 0 || index > 4) return;
        if (!imgData || !imgData.startsWith('data:image')) return;

        this.uploadedImages[index] = {
          imgData,
          topic: topic || `תמונה ${index + 1}`
        };

        const uploaded = this.uploadedImages.filter(Boolean).length;

        // Broadcast to all: image slot filled (no image data — just progress)
        this.broadcast({
          type: 'image_uploaded',
          index,
          topic: this.uploadedImages[index].topic,
          uploaded,
          total: 5
        });
        break;
      }

      // ── HOST: remove an uploaded image ──
      case 'remove_image': {
        if (!session.isHost) return;
        const { index } = msg;
        if (index >= 0 && index <= 4) {
          this.uploadedImages[index] = null;
          const uploaded = this.uploadedImages.filter(Boolean).length;
          this.broadcast({ type: 'image_removed', index, uploaded, total: 5 });
        }
        break;
      }

      // ── HOST: start game (all images must be uploaded) ──
      case 'start_game': {
        if (!session.isHost) return;
        const playerNames = Object.keys(this.gameState.players);
        if (playerNames.length < 2) return;

        const ready = this.uploadedImages.filter(Boolean);
        if (ready.length < 5) return; // must have all 5

        this.gameState.phase = 'creation';
        this.gameState.round = 0;
        this.gameState.roundOrder = this.shuffle([0, 1, 2, 3, 4]);
        playerNames.forEach(p => { this.gameState.players[p].score = 0; });

        this.startRound();
        break;
      }

      case 'submit_caption': {
        if (this.gameState.phase !== 'creation') return;
        const { text } = msg;
        if (!text || !text.trim()) return;
        const name = session.name;

        this.gameState.submissions = this.gameState.submissions.filter(s => s.player !== name);
        this.gameState.submissions.push({ player: name, text: text.trim() });

        this.broadcast({
          type: 'submission_update',
          submissions: this.gameState.submissions.map(s => ({ player: s.player })),
          count: this.gameState.submissions.length,
          total: Object.keys(this.gameState.players).length
        });

        const playerCount = Object.keys(this.gameState.players).length;
        if (this.gameState.submissions.length >= playerCount) {
          if (this.timerTimeout) clearTimeout(this.timerTimeout);
          setTimeout(() => this.startVoting(), 800);
        }
        break;
      }

      case 'vote': {
        if (this.gameState.phase !== 'voting') return;
        const { points } = msg;
        const currentMeme = this.gameState.submissions[this.gameState.voteIdx];
        if (!currentMeme) return;

        const voteKey = `${session.name}:${this.gameState.voteIdx}`;
        if (this.gameState.votes[voteKey]) return;
        this.gameState.votes[voteKey] = true;

        const author = currentMeme.player;
        if (!this.gameState.votes[author]) this.gameState.votes[author] = 0;
        this.gameState.votes[author] += points;

        this.broadcast({ type: 'vote_cast', emoji: points === 10 ? '💍' : points === 5 ? '✂️' : '🥱' });
        break;
      }

      case 'next_meme': {
        if (!session.isHost) return;
        this.gameState.voteIdx++;
        if (this.gameState.voteIdx < this.gameState.submissions.length) {
          this.broadcast({
            type: 'show_meme',
            voteIdx: this.gameState.voteIdx,
            meme: { text: this.gameState.submissions[this.gameState.voteIdx].text },
            total: this.gameState.submissions.length
          });
        } else {
          this.showResult();
        }
        break;
      }

      case 'next_round': {
        if (!session.isHost) return;
        this.gameState.round++;
        if (this.gameState.round < 5) {
          this.startRound();
        } else {
          this.showLeaderboard();
        }
        break;
      }

      case 'restart': {
        if (!session.isHost) return;
        this.gameState = {
          phase: 'lobby',
          players: {},
          round: 0,
          roundOrder: [],
          currentRound: null,
          submissions: [],
          votes: {},
          voteIdx: 0,
          timerEnd: null,
        };
        this.uploadedImages = [];
        for (const [sid, sess] of this.sessions) {
          this.gameState.players[sess.name] = { score: 0, isHost: sess.isHost };
        }
        this.broadcast({ type: 'restarted', state: this.gameState });
        break;
      }
    }
  }

  startRound() {
    // Use dynamically uploaded images
    const roundIdx = this.gameState.roundOrder[this.gameState.round];
    const imgEntry = this.uploadedImages[roundIdx];

    if (!imgEntry) {
      // Fallback — skip (shouldn't happen if start_game validated)
      return;
    }

    this.gameState.currentRound = {
      imgData: imgEntry.imgData,  // full base64, sent to all clients
      topic: imgEntry.topic,
      name: `תמונה ${roundIdx + 1}`
    };
    this.gameState.phase = 'creation';
    this.gameState.submissions = [];
    this.gameState.votes = {};
    this.gameState.voteIdx = 0;

    const timerEnd = Date.now() + 60000;
    this.gameState.timerEnd = timerEnd;

    this.broadcast({
      type: 'round_start',
      round: this.gameState.round,
      total: 5,
      currentRound: this.gameState.currentRound,
      timerEnd,
      players: this.gameState.players
    });

    if (this.timerTimeout) clearTimeout(this.timerTimeout);
    this.timerTimeout = setTimeout(() => {
      if (this.gameState.phase === 'creation') this.startVoting();
    }, 62000);
  }

  startVoting() {
    if (this.gameState.submissions.length === 0) {
      const players = Object.keys(this.gameState.players);
      this.gameState.submissions.push({ player: players[0] || 'אנונימי', text: '(לא הוגש כיתוב)' });
    }
    this.gameState.submissions = this.shuffle(this.gameState.submissions);
    this.gameState.phase = 'voting';
    this.gameState.voteIdx = 0;
    this.gameState.votes = {};

    this.broadcast({
      type: 'voting_start',
      meme: { text: this.gameState.submissions[0].text },
      voteIdx: 0,
      total: this.gameState.submissions.length,
      currentRound: this.gameState.currentRound
    });
  }

  showResult() {
    this.gameState.phase = 'result';

    const sorted = this.gameState.submissions.slice().sort((a, b) =>
      (this.gameState.votes[b.player] || 0) - (this.gameState.votes[a.player] || 0)
    );
    const winner = sorted[0];
    const winPts = this.gameState.votes[winner.player] || 0;

    if (this.gameState.players[winner.player]) {
      this.gameState.players[winner.player].score += winPts;
    }

    this.broadcast({
      type: 'result',
      winner: { player: winner.player, text: winner.text, points: winPts },
      allResults: sorted.map(s => ({
        player: s.player,
        text: s.text,
        points: this.gameState.votes[s.player] || 0
      })),
      players: this.gameState.players,
      round: this.gameState.round,
      currentRound: this.gameState.currentRound
    });
  }

  showLeaderboard() {
    this.gameState.phase = 'leaderboard';
    this.broadcast({ type: 'leaderboard', players: this.gameState.players });
  }

  broadcast(msg) {
    const data = JSON.stringify(msg);
    for (const [, session] of this.sessions) {
      try { session.ws.send(data); } catch (e) {}
    }
  }

  sendTo(ws, msg) {
    try { ws.send(JSON.stringify(msg)); } catch (e) {}
  }

  shuffle(arr) {
    const a = arr.slice();
    for (let i = a.length - 1; i > 0; i--) {
      const j = Math.floor(Math.random() * (i + 1));
      [a[i], a[j]] = [a[j], a[i]];
    }
    return a;
  }
}
