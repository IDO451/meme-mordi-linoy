// ============================================================
// MEME GAME MULTIPLAYER SERVER — Cloudflare Worker
// ============================================================

export default {
  async fetch(request, env) {
    const url = new URL(request.url);

    // CORS headers for all responses
    const cors = {
      'Access-Control-Allow-Origin': '*',
      'Access-Control-Allow-Methods': 'GET, POST, OPTIONS',
      'Access-Control-Allow-Headers': 'Content-Type',
    };

    if (request.method === 'OPTIONS') {
      return new Response(null, { headers: cors });
    }

    // Route: /ws?room=XXXX&name=PLAYER — WebSocket upgrade
    if (url.pathname === '/ws') {
      const roomCode = url.searchParams.get('room');
      if (!roomCode) return new Response('Missing room', { status: 400 });

      // Get or create Durable Object for this room
      const id = env.GAME_ROOM.idFromName(roomCode);
      const room = env.GAME_ROOM.get(id);
      return room.fetch(request);
    }

    // Route: /create — create a new room, return 4-digit code
    if (url.pathname === '/create') {
      const code = Math.floor(1000 + Math.random() * 9000).toString();
      const id = env.GAME_ROOM.idFromName(code);
      const room = env.GAME_ROOM.get(id);
      // Init the room
      await room.fetch(new Request(`${url.origin}/init`, { method: 'POST' }));
      return new Response(JSON.stringify({ code }), {
        headers: { ...cors, 'Content-Type': 'application/json' }
      });
    }

    return new Response('Meme Game Server 🎮', { headers: cors });
  }
};

// ============================================================
// DURABLE OBJECT — One instance per game room
// ============================================================
export class GameRoom {
  constructor(state, env) {
    this.state = state;
    this.sessions = new Map(); // sessionId -> { ws, name, isHost }
    this.gameState = {
      phase: 'lobby',      // lobby | creation | voting | result | leaderboard
      players: {},         // name -> { score, isHost }
      round: 0,
      roundOrder: [],
      currentRound: null,  // { imgKey, name, topic }
      submissions: [],     // { player, text }
      votes: {},           // player -> points
      voteIdx: 0,
      timerEnd: null,
    };
    this.timerTimeout = null;
  }

  async fetch(request) {
    const url = new URL(request.url);

    if (url.pathname === '/init') {
      return new Response('ok');
    }

    // WebSocket upgrade
    if (request.headers.get('Upgrade') !== 'websocket') {
      return new Response('Expected WebSocket', { status: 426 });
    }

    const name = url.searchParams.get('name') || 'אנונימי';
    const sessionId = crypto.randomUUID();

    const [client, server] = Object.values(new WebSocketPair());
    server.accept();

    const isFirstPlayer = this.sessions.size === 0;
    this.sessions.set(sessionId, { ws: server, name, isHost: isFirstPlayer });

    // Add player to game state
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
      // Don't remove player from scores if game started
      if (this.gameState.phase === 'lobby') {
        delete this.gameState.players[name];
      }
      this.broadcast({ type: 'player_left', name, players: this.gameState.players });
    });

    // Send current state to new player
    this.sendTo(server, { type: 'welcome', name, isHost: isFirstPlayer, state: this.gameState });
    // Broadcast updated player list
    this.broadcast({ type: 'player_joined', name, players: this.gameState.players });

    return new Response(null, { status: 101, webSocket: client });
  }

  handleMessage(sessionId, msg) {
    const session = this.sessions.get(sessionId);
    if (!session) return;

    switch (msg.type) {

      case 'start_game': {
        if (!session.isHost) return;
        const playerNames = Object.keys(this.gameState.players);
        if (playerNames.length < 2) return;

        this.gameState.phase = 'creation';
        this.gameState.round = 0;
        this.gameState.roundOrder = this.shuffle([0,1,2,3,4]);
        // Reset scores
        playerNames.forEach(p => { this.gameState.players[p].score = 0; });

        this.startRound();
        break;
      }

      case 'submit_caption': {
        if (this.gameState.phase !== 'creation') return;
        const { text } = msg;
        if (!text || !text.trim()) return;
        const name = session.name;

        // Remove previous submission from this player
        this.gameState.submissions = this.gameState.submissions.filter(s => s.player !== name);
        this.gameState.submissions.push({ player: name, text: text.trim() });

        this.broadcast({
          type: 'submission_update',
          submissions: this.gameState.submissions.map(s => ({ player: s.player })), // hide text until voting
          count: this.gameState.submissions.length,
          total: Object.keys(this.gameState.players).length
        });

        // Auto-advance if everyone submitted
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

        // Each player votes once per meme
        const voteKey = `${session.name}:${this.gameState.voteIdx}`;
        if (this.gameState.votes[voteKey]) return; // already voted
        this.gameState.votes[voteKey] = true;

        // Accumulate points for the meme author
        const author = currentMeme.player;
        if (!this.gameState.votes[author]) this.gameState.votes[author] = 0;
        this.gameState.votes[author] += points;

        this.broadcast({ type: 'vote_cast', emoji: points === 10 ? '💍' : points === 5 ? '✂️' : '🥱' });
        break;
      }

      case 'next_meme': {
        if (!session.isHost && this.gameState.phase === 'voting') return;
        this.gameState.voteIdx++;
        if (this.gameState.voteIdx < this.gameState.submissions.length) {
          this.broadcast({
            type: 'show_meme',
            voteIdx: this.gameState.voteIdx,
            meme: {
              text: this.gameState.submissions[this.gameState.voteIdx].text,
              // author hidden until reveal
            },
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
        // Re-add all connected players
        for (const [sid, sess] of this.sessions) {
          this.gameState.players[sess.name] = { score: 0, isHost: sess.isHost };
        }
        this.broadcast({ type: 'restarted', state: this.gameState });
        break;
      }
    }
  }

  startRound() {
    const ROUNDS = [
      { key:'img1', name:'הסמטה המצוירת', topics:['מורדי מגלה מה בדיוק קיבל כמתנה','לינוי בוחרת מתנה לתקציב של מורדי','החתול חושב מה הוא עושה עם הווסט'] },
      { key:'img2', name:'מבצעים ברחוב',  topics:['מורדי גילה כמה עולה האולם','מורדי מחשב את עלות הבר האקטיבי','כשמורדי ראה את מחיר הקייטרינג'] },
      { key:'img3', name:'מתחת לשמיכה',  topics:['מי האישה עם אוזני הארנב בדלת','כשמורדי שכח להזמין מישהו חשוב','בוקר יום החתונה'] },
      { key:'img4', name:'ים המלח',       topics:['הסכם הממון הסודי','ההבטחות שמורדי הבטיח ללינוי','מה הסכימו שם עם הבוץ'] },
      { key:'img5', name:'לינוי הפיצית', topics:['מי מחליט בבית','לינוי מסבירה לאן ירח דבש','מי שולט ביחסים'] }
    ];

    const rd = ROUNDS[this.gameState.roundOrder[this.gameState.round]];
    const topic = rd.topics[Math.floor(Math.random() * rd.topics.length)];
    this.gameState.currentRound = { key: rd.key, name: rd.name, topic };
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

    // Auto-advance when timer expires
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

    // Calculate scores
    const sorted = this.gameState.submissions.slice().sort((a, b) =>
      (this.gameState.votes[b.player] || 0) - (this.gameState.votes[a.player] || 0)
    );
    const winner = sorted[0];
    const winPts = this.gameState.votes[winner.player] || 0;

    // Update cumulative score
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
