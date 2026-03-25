"""Servidor SSE da Sala do Conselho."""

import asyncio
import json
import time
from collections import defaultdict
from dataclasses import dataclass, field
from contextlib import asynccontextmanager
from pathlib import Path
from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse, StreamingResponse
from fastapi.middleware.cors import CORSMiddleware

_STATIC = Path(__file__).parent / "static"


@asynccontextmanager
async def lifespan(app: FastAPI):
    asyncio.create_task(_cleanup_loop())
    yield


app = FastAPI(lifespan=lifespan)
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])

_MAX_LOG      = 500   # evita memory leak em sessões longas
_DEFAULT_ROOM = "default"
_ROOM_TTL     = 7200  # 2h sem atividade → sala removida

# ── Estado por sala: { room_id -> { subscribers, log, counter, last_activity } } ──
@dataclass
class _Room:
    subscribers:   list  = field(default_factory=list)
    log:           list  = field(default_factory=list)
    counter:       int   = 0
    last_activity: float = field(default_factory=time.time)

_rooms: dict[str, _Room] = defaultdict(_Room)


def _get_room(room_id: str | None) -> _Room:
    key = (room_id or _DEFAULT_ROOM).strip().lower() or _DEFAULT_ROOM
    room = _rooms[key]
    room.last_activity = time.time()
    return room


async def _cleanup_loop():
    """Remove salas inativas há mais de 2h a cada 5 minutos."""
    while True:
        await asyncio.sleep(300)
        cutoff = time.time() - _ROOM_TTL
        stale = [k for k, r in list(_rooms.items())
                 if r.last_activity < cutoff and not r.subscribers]
        for k in stale:
            _rooms.pop(k, None)


async def _broadcast(event: dict, room: _Room):
    for q in room.subscribers[:]:
        try:
            q.put_nowait(event)
        except asyncio.QueueFull:
            try:
                room.subscribers.remove(q)
            except ValueError:
                pass


# Retrocompatibilidade garantida: rotas sem ?room= usam sala "default" automaticamente.


def _sanitize(obj, depth=0):
    """Remove \n e \r de strings para prevenir injeção no protocolo SSE."""
    if depth > 8:
        return obj
    if isinstance(obj, str):
        return obj.replace("\r", " ").replace("\n", " ")
    if isinstance(obj, dict):
        return {k: _sanitize(v, depth + 1) for k, v in obj.items() if k != "_id"}
    if isinstance(obj, list):
        return [_sanitize(i, depth + 1) for i in obj]
    return obj


@app.post("/event")
async def receive_event(request: Request):
    room_id = request.query_params.get("room")
    room = _get_room(room_id)
    raw  = await request.json()
    data = _sanitize(raw)
    if data.get("type") not in ("ping", "connected"):
        room.counter += 1
        data["_id"] = room.counter
        room.log.append(data)
        if len(room.log) > _MAX_LOG:
            del room.log[: len(room.log) - _MAX_LOG]
    await _broadcast(data, room)
    return {"ok": True}


@app.get("/health")
async def health():
    rooms_info = {k: {"subs": len(r.subscribers), "log": len(r.log)} for k, r in _rooms.items()}
    return {"ok": True, "rooms": rooms_info}


@app.get("/report")
async def get_report(request: Request):
    room = _get_room(request.query_params.get("room"))
    return {"events": room.log, "count": len(room.log)}


@app.get("/rooms")
async def list_rooms():
    now = time.time()
    return {"rooms": [
        {
            "id": k,
            "subscribers": len(r.subscribers),
            "events": len(r.log),
            "idle_min": int((now - r.last_activity) / 60),
        }
        for k, r in _rooms.items()
    ]}


@app.post("/reset")
async def reset_session(request: Request):
    room = _get_room(request.query_params.get("room"))
    room.log.clear()
    room.counter = 0
    await _broadcast({"type": "reset"}, room)
    return {"ok": True}


@app.get("/stream")
async def sse_stream(request: Request):
    room       = _get_room(request.query_params.get("room"))
    last_id    = request.headers.get("Last-Event-ID")
    replay_from = int(last_id) if last_id and last_id.isdigit() else None

    q: asyncio.Queue = asyncio.Queue(maxsize=200)
    room.subscribers.append(q)

    async def generate():
        try:
            yield 'data: {"type":"connected"}\n\n'
            if replay_from is not None:
                for evt in room.log[:]:
                    if evt.get("_id", 0) > replay_from:
                        eid = evt["_id"]
                        yield f"id: {eid}\ndata: {json.dumps(evt, ensure_ascii=False)}\n\n"
            while True:
                try:
                    event = await asyncio.wait_for(q.get(), timeout=25)
                    eid = event.get("_id")
                    if eid:
                        yield f"id: {eid}\ndata: {json.dumps(event, ensure_ascii=False)}\n\n"
                    else:
                        yield f"data: {json.dumps(event, ensure_ascii=False)}\n\n"
                except asyncio.TimeoutError:
                    yield 'data: {"type":"ping"}\n\n'
        except (asyncio.CancelledError, GeneratorExit):
            pass
        finally:
            if q in room.subscribers:
                room.subscribers.remove(q)

    return StreamingResponse(
        generate(),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
    )


@app.get("/", response_class=HTMLResponse)
async def root():
    html = (_STATIC / "room.html").read_text(encoding="utf-8")
    return HTMLResponse(html)


# ─── legacy: mantido para compatibilidade caso algum código aponte para /room ───
@app.get("/room", response_class=HTMLResponse)
async def room_alias():
    return await root()


# ════════════════════════════════════════════════════════════════════════════════
# ANTIGO HTML EMBUTIDO REMOVIDO — agora servido de static/room.html
# ════════════════════════════════════════════════════════════════════════════════
_REMOVED_HTML = r"""<!DOCTYPE html>
<html lang="pt-br">
<head>
<meta charset="UTF-8">
<title>Sala do Conselho</title>
<link href="https://fonts.googleapis.com/css2?family=Press+Start+2P&family=VT323:wght@400&display=swap" rel="stylesheet">
<style>
*, *::before, *::after { margin:0; padding:0; box-sizing:border-box; image-rendering:pixelated; }

:root {
  --wall:    #3B2E22;
  --wall2:   #2A1F16;
  --brick:   #4A3428;
  --mortar:  #1E1510;
  --floor:   #2C2018;
  --floor2:  #241A12;
  --plank:   #352818;
  --gold:    #C8873A;
  --gold2:   #E8A84A;
  --cream:   #F0E8D0;
  --ink:     #0A0806;
  --panel:   #1A120A;
  --panel2:  #120C06;
  --border:  #3A2A1A;
  --claude:  #A855F7;
  --gemini:  #3B82F6;
  --gpt:     #22C55E;
  --red:     #EF4444;
  --amber:   #F59E0B;
}

body {
  font-family: 'VT323', monospace;
  background: var(--panel2);
  color: var(--cream);
  display: grid;
  grid-template-columns: 220px 1fr 220px;
  grid-template-rows: 1fr 40px;
  height: 100vh;
  overflow: hidden;
  font-size: 18px;
}

::-webkit-scrollbar { width: 4px; }
::-webkit-scrollbar-track { background: var(--panel2); }
::-webkit-scrollbar-thumb { background: var(--border); border-radius: 2px; }

/* ─── Painel esquerdo ───────────────────────────────────────── */
#left {
  grid-row: 1;
  background: var(--panel);
  border-right: 3px solid var(--mortar);
  display: flex; flex-direction: column;
  overflow: hidden;
}
.panel-title {
  font-family: 'Press Start 2P', monospace;
  font-size: 7px;
  color: var(--gold2);
  padding: 8px 10px 6px;
  border-bottom: 2px solid var(--mortar);
  background: var(--panel2);
  text-transform: uppercase;
  letter-spacing: 1px;
  display: flex; align-items: center; gap: 6px;
  flex-shrink: 0;
  width: 100%;
}
.panel-title .icon { font-size: 12px; }
#action-log {
  flex: 1; overflow-y: auto;
  padding: 6px;
  display: flex; flex-direction: column; gap: 3px;
}
.action-entry {
  background: var(--panel2);
  border: 1px solid var(--border);
  border-left: 3px solid var(--claude);
  padding: 4px 6px;
  font-family: 'VT323', monospace;
  font-size: 18px;
  color: #D4C8A8;
  animation: fadeUp 0.2s ease;
  line-height: 1.3;
  white-space: nowrap;
  overflow: hidden;
  text-overflow: ellipsis;
}
.action-entry .ts {
  display: block; font-size: 14px;
  color: var(--gold); margin-bottom: 1px;
  font-family: 'VT323', monospace;
}
.action-entry .short {
  display: block;
  white-space: nowrap;
  overflow: hidden;
  text-overflow: ellipsis;
  max-width: 100%;
}

/* ─── Sala central ──────────────────────────────────────────── */
#room {
  grid-row: 1;
  position: relative;
  overflow: hidden;
  background: var(--wall2);
}

.wall-bg {
  position: absolute; inset: 0 0 38% 0;
  background-color: var(--brick);
  background-image:
    repeating-linear-gradient(
      180deg,
      transparent 0px, transparent 18px,
      var(--mortar) 18px, var(--mortar) 20px
    ),
    repeating-linear-gradient(
      90deg,
      transparent 0px, transparent 38px,
      var(--mortar) 38px, var(--mortar) 40px
    );
  z-index: 1;
}
.wall-bg::after {
  content: '';
  position: absolute; inset: 0;
  background: linear-gradient(180deg, rgba(0,0,0,0.3) 0%, transparent 60%);
}

.floor-bg {
  position: absolute; bottom: 0; left: 0; right: 0; height: 38%;
  background-color: var(--floor);
  background-image:
    repeating-linear-gradient(
      90deg,
      transparent 0px, transparent 39px,
      rgba(0,0,0,0.25) 39px, rgba(0,0,0,0.25) 40px
    ),
    repeating-linear-gradient(
      180deg,
      rgba(255,255,255,0.02) 0px, rgba(255,255,255,0.02) 1px,
      transparent 1px, transparent 8px
    );
  z-index: 1;
}
.floor-bg::before {
  content: '';
  position: absolute; top: 0; left: 0; right: 0; height: 6px;
  background: var(--mortar);
  box-shadow: 0 2px 8px rgba(0,0,0,0.5);
}
.floor-bg::after {
  content: '';
  position: absolute; inset: 0;
  background: linear-gradient(180deg, rgba(0,0,0,0.2) 0%, transparent 50%);
}

/* ── Janela ── */
.window {
  position: absolute;
  top: 18px; left: 50%; transform: translateX(-50%);
  width: 120px; height: 88px;
  background: linear-gradient(135deg, #1A3A5C 0%, #2A5A8C 40%, #1E4A7A 100%);
  border: 4px solid var(--ink);
  box-shadow: inset 0 0 20px rgba(100,180,255,0.15), 0 0 12px rgba(60,120,200,0.2);
  z-index: 3;
}
.window::before {
  content: '';
  position: absolute; inset: 0;
  background:
    linear-gradient(var(--ink) 0, var(--ink) 100%) center/3px 100% no-repeat,
    linear-gradient(var(--ink) 0, var(--ink) 100%) center/100% 3px no-repeat;
}
.window::after {
  content: '';
  position: absolute; top: -4px; left: -4px; right: -4px; bottom: -4px;
  background:
    linear-gradient(to right, #6B2020 0%, #6B2020 16%, transparent 16%),
    linear-gradient(to left,  #6B2020 0%, #6B2020 16%, transparent 16%);
  pointer-events: none;
}
.curtain-rod {
  position: absolute; top: -12px; left: -14px; right: -14px;
  height: 7px; background: var(--gold); border: 2px solid var(--ink);
  border-radius: 3px; z-index: 4;
}

/* ── Quadros na parede ── */
.painting {
  position: absolute; top: 22px;
  border: 3px solid var(--ink);
  box-shadow: 2px 2px 0 var(--ink);
  z-index: 3;
}
.painting-left  { left: 40px;  width: 56px; height: 44px;
  background: linear-gradient(135deg, #2A1A0A, #4A2A1A, #1A2A0A); }
.painting-right { right: 40px; width: 56px; height: 44px;
  background: linear-gradient(135deg, #0A1A2A, #1A2A4A, #2A1A3A); }

/* ── Tochas ── */
.torch { position: absolute; top: 72px; z-index: 4; }
.torch-left  { left: 24px; }
.torch-right { right: 24px; }
.torch-body  { width: 10px; height: 22px; margin: 0 auto;
  background: linear-gradient(180deg, #6B4428, #3A2010);
  border: 2px solid var(--ink); }
.torch-flame { width: 16px; height: 20px; margin: -3px auto 0; position: relative; }
.torch-flame::before { content: '🔥'; font-size: 17px;
  position: absolute; top: -3px; left: -1px;
  animation: flicker 0.18s infinite alternate;
  filter: drop-shadow(0 0 5px #FF6600); }
@keyframes flicker {
  from { transform: scaleX(1) rotate(-3deg); opacity: 1; }
  to   { transform: scaleX(0.85) rotate(3deg); opacity: 0.8; }
}

/* ── Móveis ── */
.bookshelf {
  position: absolute; bottom: 38%; z-index: 3;
  width: 52px; height: 58px;
  background: #3A2810;
  border: 2px solid var(--ink);
  display: grid; grid-template-columns: repeat(3, 1fr);
  grid-template-rows: repeat(3, 1fr);
  gap: 2px; padding: 3px;
}
.bookshelf-left  { left: 18px; }
.bookshelf-right { right: 18px; }
.book { border-radius: 1px; border: 1px solid rgba(0,0,0,0.4); }

.plant {
  position: absolute; bottom: 38%; z-index: 4;
  display: flex; flex-direction: column; align-items: center;
}
.plant-l { left: 82px; }
.plant-r { right: 82px; }
.plant-leaves { font-size: 28px;
  filter: drop-shadow(0 0 3px rgba(0,80,0,0.5));
  animation: sway 4s ease-in-out infinite alternate; }
@keyframes sway { from { transform: rotate(-4deg); } to { transform: rotate(4deg); } }
.plant-pot { width: 24px; height: 16px;
  background: linear-gradient(180deg, #7A3A10, #5A2808);
  border: 2px solid var(--ink);
  clip-path: polygon(10% 0%, 90% 0%, 100% 100%, 0% 100%); }

/* ── Mesa de reunião ── */
.table {
  position: absolute; bottom: 38%; left: 50%; transform: translateX(-50%);
  width: 360px; height: 20px;
  background: linear-gradient(180deg, #7A5020 0%, #4A2C0C 100%);
  border: 3px solid var(--ink);
  border-radius: 2px; z-index: 4;
  box-shadow: 0 5px 0 var(--ink), 0 6px 6px rgba(0,0,0,0.4);
}
.table::before {
  content: '';
  position: absolute; inset: 3px 8px;
  background: repeating-linear-gradient(
    90deg, rgba(255,255,255,0.06) 0, rgba(255,255,255,0.06) 36px,
    transparent 36px, transparent 72px
  );
}

/* ─── P2P Connection Line ───────────────────────────────────── */
#p2p-canvas {
  position: absolute; inset: 0;
  pointer-events: none; z-index: 8;
}

/* ─── Agentes ───────────────────────────────────────────────── */
.agent-wrap {
  position: absolute;
  bottom: 34%;
  display: flex; flex-direction: column; align-items: center;
  transform: translateX(-50%);
  z-index: 10;
  transition: bottom 0.4s ease;
}

/* Balão de fala — max 80 chars, sem scroll */
.bubble {
  position: relative;
  background: #F8F0D8;
  border: 3px solid var(--ink);
  border-radius: 3px;
  padding: 5px 8px;
  font-family: 'VT323', monospace;
  font-size: 16px;
  line-height: 1.3;
  color: #1A1008;
  max-width: 160px; min-width: 50px;
  overflow: hidden;
  margin-bottom: 10px;
  opacity: 0; pointer-events: none;
  transition: opacity 0.25s;
  box-shadow: 3px 3px 0 var(--ink);
  word-break: break-word;
  white-space: pre-wrap;
}
.bubble.show { opacity: 1; pointer-events: auto; }
.bubble::after {
  content: '';
  position: absolute; bottom: -10px; left: 50%; transform: translateX(-50%);
  width: 0; height: 0;
  border: 5px solid transparent; border-top-color: var(--ink);
}
.bubble::before {
  content: '';
  position: absolute; bottom: -6px; left: 50%; transform: translateX(-50%);
  width: 0; height: 0;
  border: 3px solid transparent; border-top-color: #F8F0D8;
  z-index: 1;
}

/* "Respondendo a..." tag */
.reply-tag {
  font-family: 'VT323', monospace;
  font-size: 14px;
  padding: 1px 4px;
  border-radius: 2px;
  color: #FFF;
  margin-bottom: 3px;
  display: none;
}
.reply-tag.show { display: inline-block; }

/* Badge de voto */
.vote-badge {
  position: absolute; top: -38px; right: -16px;
  font-size: 22px; opacity: 0;
  transform: translateY(8px) scale(0.4);
  transition: opacity 0.3s, transform 0.3s;
  z-index: 20;
  filter: drop-shadow(0 2px 3px rgba(0,0,0,0.6));
}
.vote-badge.show { opacity: 1; transform: translateY(0) scale(1); }

/* ── Sprite base ── */
.sprite-wrap { position: relative; }
.sprite { display: flex; flex-direction: column; align-items: center; user-select: none; }

.sprite.thinking { animation: bob 0.28s infinite alternate ease-in-out; }
@keyframes bob { from { transform: translateY(0); } to { transform: translateY(-5px); } }

.think-dots {
  position: absolute; top: -20px; left: 50%; transform: translateX(-50%);
  font-size: 18px; color: var(--gold2);
  display: none; white-space: nowrap;
}
.thinking .think-dots { display: block; animation: blink 0.35s infinite; }
@keyframes blink { 50% { opacity: 0.1; } }

.thinking .sp-leg:first-child  { animation: walk-l 0.36s infinite ease-in-out; transform-origin: top center; }
.thinking .sp-leg:last-child   { animation: walk-r 0.36s infinite ease-in-out; transform-origin: top center; }
.thinking .sp-foot:first-child { animation: walk-l 0.36s infinite ease-in-out; transform-origin: top center; }
.thinking .sp-foot:last-child  { animation: walk-r 0.36s infinite ease-in-out; transform-origin: top center; }
.thinking .sp-arm:first-child  { animation: walk-r 0.36s infinite ease-in-out; transform-origin: top center; }
.thinking .sp-arm:last-child   { animation: walk-l 0.36s infinite ease-in-out; transform-origin: top center; }
@keyframes walk-l { 0%,100% { transform: skewX(-10deg); } 50% { transform: skewX(10deg); } }
@keyframes walk-r { 0%,100% { transform: skewX(10deg);  } 50% { transform: skewX(-10deg); } }

.sp-shadow { width: 32px; height: 5px; background: rgba(0,0,0,0.35); border-radius: 50%; margin-top: 1px; }
.sp-hat    { height: 12px; margin-bottom: -3px; border-radius: 3px 3px 0 0;
  border: 2px solid var(--ink); border-bottom: none; }
.sp-hat-brim { height: 4px; border-radius: 2px; border: 2px solid var(--ink); margin-bottom: -2px; }
.sp-hair   { height: 9px; border-radius: 4px 4px 0 0;
  border: 2px solid var(--ink); border-bottom: none; margin-bottom: -2px; }
.sp-head   {
  width: 30px; height: 28px; border-radius: 4px;
  border: 2px solid var(--ink);
  background: #F0C080;
  position: relative; display: flex; align-items: center; justify-content: center;
}
.sp-head::before, .sp-head::after {
  content: ''; position: absolute;
  width: 4px; height: 5px; background: var(--ink); border-radius: 1px; top: 8px;
}
.sp-head::before { left: 5px; }
.sp-head::after  { right: 5px; }
.sp-smile {
  position: absolute; bottom: 4px; left: 50%; transform: translateX(-50%);
  width: 10px; height: 4px;
  border: 2px solid var(--ink); border-top: none; border-radius: 0 0 5px 5px;
}
.sp-body {
  width: 34px; height: 26px;
  border: 2px solid var(--ink); margin-top: 1px;
  border-radius: 2px; position: relative;
  display: flex; align-items: center; justify-content: center;
}
.sp-body::before { content: '··'; font-size: 8px; color: var(--ink); letter-spacing: 2px; }
.sp-arms { display: flex; gap: 38px; margin-top: -22px; position: relative; z-index: -1; }
.sp-arm  { width: 8px; height: 18px; border: 2px solid var(--ink); border-radius: 2px; }
.sp-legs { display: flex; gap: 4px; margin-top: 1px; }
.sp-leg  { width: 12px; height: 16px; border: 2px solid var(--ink); border-radius: 2px; }
.sp-feet { display: flex; gap: 4px; }
.sp-foot { width: 14px; height: 6px; border: 2px solid var(--ink); border-radius: 0 0 3px 3px; }

/* Label do agente — maior que antes */
.agent-label {
  font-family: 'Press Start 2P', monospace;
  font-size: 7px; margin-top: 5px;
  letter-spacing: 1px; text-shadow: 2px 2px 0 var(--ink);
}

/* ─── Painel direito ────────────────────────────────────────── */
#right {
  grid-row: 1;
  background: var(--panel);
  border-left: 3px solid var(--mortar);
  display: flex; flex-direction: column;
  overflow: hidden;
}
#conv-log {
  flex: 1; overflow-y: auto;
  padding: 6px;
  display: flex; flex-direction: column; gap: 4px;
}
.log-entry {
  background: var(--panel2);
  border: 1px solid var(--border);
  border-radius: 2px;
  padding: 5px 7px;
  color: var(--cream);
  animation: fadeUp 0.2s ease;
  cursor: pointer;
}
.log-entry .who {
  font-family: 'VT323', monospace;
  font-size: 20px; margin-bottom: 2px;
  display: flex; align-items: center; gap: 5px;
  font-weight: normal;
}
.log-entry .who .reply-arrow {
  font-size: 16px; color: #888;
}
.log-entry .body {
  font-family: 'VT323', monospace;
  font-size: 18px;
  color: #C8BCA0;
  line-height: 1.3;
  display: -webkit-box;
  -webkit-line-clamp: 2;
  -webkit-box-orient: vertical;
  overflow: hidden;
  text-overflow: ellipsis;
}
.log-entry.expanded .body {
  display: block;
  overflow: visible;
  -webkit-line-clamp: unset;
}
.log-entry.claude { border-left: 3px solid var(--claude); }
.log-entry.gemini { border-left: 3px solid var(--gemini); }
.log-entry.gpt    { border-left: 3px solid var(--gpt);    }

@keyframes fadeUp {
  from { opacity: 0; transform: translateY(6px); }
  to   { opacity: 1; transform: translateY(0); }
}

/* ─── Barra de status ───────────────────────────────────────── */
#statusbar {
  grid-column: 1 / -1; grid-row: 2;
  background: var(--panel2);
  border-top: 3px solid var(--mortar);
  display: flex; align-items: center;
  padding: 0 12px; gap: 8px;
  font-family: 'VT323', monospace;
  font-size: 20px; color: var(--cream);
}
.status-dot {
  width: 9px; height: 9px; border-radius: 50%;
  background: var(--gpt); border: 2px solid var(--ink);
  box-shadow: 0 0 7px var(--gpt); flex-shrink: 0;
}
.status-dot.busy  { background: var(--amber); box-shadow: 0 0 7px var(--amber); }
.status-dot.error { background: var(--red);   box-shadow: 0 0 7px var(--red); }
#status-text { flex: 1; }
#sse-badge {
  font-family: 'Press Start 2P', monospace; font-size: 6px;
  color: var(--gpt); border: 2px solid currentColor;
  padding: 3px 5px; border-radius: 2px;
}
#sse-badge.off { color: var(--red); }

/* ─── Badges de estado ──────────────────────────────────────── */
#round-badge {
  position: absolute; top: 10px; right: 10px;
  font-family: 'Press Start 2P', monospace; font-size: 6px;
  background: var(--panel2); border: 2px solid var(--gold);
  color: var(--gold2); padding: 4px 7px; border-radius: 2px;
  z-index: 20; opacity: 0; transition: opacity 0.3s;
}
#round-badge.show { opacity: 1; }

/* Parallel badge — oculto por padrão, aparece só quando 2+ pensando */
#parallel-badge {
  position: absolute; top: 10px; left: 10px;
  font-family: 'Press Start 2P', monospace; font-size: 6px;
  background: var(--panel2); border: 2px solid var(--gemini);
  color: var(--gemini); padding: 4px 7px; border-radius: 2px;
  z-index: 20; opacity: 0; transition: opacity 0.3s;
}
#parallel-badge.show {
  opacity: 1;
  animation: pulse-badge 0.8s infinite alternate;
}
@keyframes pulse-badge { from { box-shadow: 0 0 4px var(--gemini); } to { box-shadow: 0 0 12px var(--gemini); } }

/* ── Veredito do Claude ── */
#verdict-box {
  position: absolute; bottom: 42%; left: 50%; transform: translateX(-50%);
  background: var(--panel2); border: 3px solid var(--claude);
  color: var(--cream); font-family: 'VT323', monospace; font-size: 18px;
  padding: 8px 12px; max-width: 300px; width: max-content; line-height: 1.4;
  border-radius: 3px; z-index: 30; text-align: center;
  box-shadow: 0 0 20px rgba(168,85,247,0.4), 3px 3px 0 var(--ink);
  opacity: 0; pointer-events: none; transition: opacity 0.4s;
}
#verdict-box.show { opacity: 1; pointer-events: auto; }
#verdict-box .verdict-title {
  font-family: 'Press Start 2P', monospace; font-size: 6px;
  color: var(--claude); display: block; margin-bottom: 5px;
}
#verdict-box.show::before {
  content: '★ ★ ★';
  display: block; font-size: 12px; color: var(--gold2);
  margin-bottom: 4px; animation: twinkle 0.6s infinite alternate;
}
@keyframes twinkle { from { opacity: 1; } to { opacity: 0.3; } }

/* ─── Novas animações ───────────────────────────────────── */

/* Respiração idle */
.sprite:not(.thinking) { animation: breathe 4s ease-in-out infinite; }
@keyframes breathe {
  0%,100% { transform: scaleY(1) translateY(0); }
  50%      { transform: scaleY(0.97) translateY(1px); }
}

/* Salto ao falar */
@keyframes agentJump {
  0%,100% { transform: translateX(-50%) translateY(0); }
  35%     { transform: translateX(-50%) translateY(-20px) scale(1.06); }
  65%     { transform: translateX(-50%) translateY(-7px); }
}
.agent-wrap.jumping { animation: agentJump 0.55s ease; }

/* Brilho ao falar */
@keyframes speakGlow {
  0%,100% { filter: drop-shadow(0 0 0px transparent); }
  50%     { filter: drop-shadow(0 0 14px var(--glow-color, #fff)); }
}
.agent-wrap.speaking-glow { animation: speakGlow 0.9s ease; }

/* Aceno ao ouvir */
@keyframes nod {
  0%,100% { transform: rotate(0deg) translateY(0); }
  25%     { transform: rotate(-7deg) translateY(-2px); }
  75%     { transform: rotate(7deg) translateY(-2px); }
}
.sprite.nodding { animation: nod 0.6s ease; }

/* Emoji flutuante */
@keyframes floatUp {
  0%   { transform: translateY(0) scale(0.8); opacity: 1; }
  100% { transform: translateY(-80px) scale(1.5); opacity: 0; }
}
.float-emoji { position: fixed; font-size: 22px; z-index: 200; pointer-events: none; animation: floatUp 1.4s ease forwards; }

/* Confetti */
@keyframes confettiFall {
  0%   { transform: translateY(0) rotate(0deg); opacity: 1; }
  80%  { opacity: 0.9; }
  100% { transform: translateY(100vh) rotate(720deg); opacity: 0; }
}

/* Flash de tela */
#screen-flash { position: fixed; inset: 0; pointer-events: none; z-index: 998; opacity: 0; transition: opacity 0.08s; background: rgba(168,85,247,0.18); }
#screen-flash.active { opacity: 1; }

/* Entrada dos agentes no carregamento */
@keyframes agentEnter {
  0%   { transform: translateX(-50%) translateY(60px); opacity: 0; }
  60%  { transform: translateX(-50%) translateY(-8px); opacity: 1; }
  100% { transform: translateX(-50%) translateY(0); }
}
.agent-wrap { opacity: 0; }
.agent-wrap.entered { opacity: 1; animation: agentEnter 0.6s ease forwards; }

/* Pulse no painel de atas quando chega mensagem nova */
@keyframes panelFlash {
  0%,100% { background: var(--panel2); }
  50%     { background: #1e1508; }
}
.log-entry.new-msg { animation: panelFlash 0.4s ease; }
</style>
</head>
<body>

<div id="left">
  <div class="panel-title"><span class="icon">⚡</span>Ações</div>
  <div id="action-log">
    <div class="action-entry">
      <span class="ts">00:00:00</span>
      <span class="short">Sala do Conselho pronta</span>
    </div>
  </div>
</div>

<div id="room">
  <div class="wall-bg"></div>
  <div class="floor-bg"></div>

  <div class="window"><div class="curtain-rod"></div></div>

  <div class="painting painting-left"></div>
  <div class="painting painting-right"></div>

  <div class="torch torch-left">
    <div class="torch-flame"></div>
    <div class="torch-body"></div>
  </div>
  <div class="torch torch-right">
    <div class="torch-flame"></div>
    <div class="torch-body"></div>
  </div>

  <div class="bookshelf bookshelf-left" id="shelf-left"></div>
  <div class="bookshelf bookshelf-right" id="shelf-right"></div>

  <div class="plant plant-l"><div class="plant-leaves">🌿</div><div class="plant-pot"></div></div>
  <div class="plant plant-r"><div class="plant-leaves">🪴</div><div class="plant-pot"></div></div>

  <div class="table"></div>

  <canvas id="p2p-canvas"></canvas>

  <div id="round-badge">ROUND 1</div>
  <div id="parallel-badge">⚡ PARALELO</div>

  <div id="verdict-box">
    <span class="verdict-title">⚖ VEREDITO DO CLAUDE</span>
    <span id="verdict-text"></span>
  </div>
</div>

<div id="right">
  <div class="panel-title"><span class="icon">📜</span>Atas <span id="conv-count" style="margin-left:auto;font-size:6px;color:var(--gold);"></span></div>
  <div id="conv-log"></div>
</div>

<div id="statusbar">
  <div class="status-dot" id="status-dot"></div>
  <span id="status-text">Conectando...</span>
  <span id="sse-badge" class="off">SSE</span>
</div>

<div id="screen-flash"></div>

<script>
// ── Livros coloridos nas estantes ──
const BOOK_COLORS = ['#C0392B','#2980B9','#27AE60','#8E44AD','#D35400','#16A085','#E74C3C','#1ABC9C','#F39C12','#2C3E50'];
['shelf-left','shelf-right'].forEach(id => {
  const el = document.getElementById(id);
  for (let i = 0; i < 9; i++) {
    const b = document.createElement('div');
    b.className = 'book';
    b.style.background = BOOK_COLORS[(i + (id.includes('right') ? 3 : 0)) % BOOK_COLORS.length];
    el.appendChild(b);
  }
});

// ── Definição dos agentes ──
const AGENTS = [
  { key:'claude', name:'Claude', color:'var(--claude)', hex:'#A855F7', x:'20%',
    hat:'#6B21A8', hatBrim:'#9333EA', hair:'#3B0764', hairW:32,
    body:'#7C3AED', legs:'#4C1D95', feet:'#3B0764', arms:'#9333EA' },
  { key:'gemini', name:'Gemini', color:'var(--gemini)', hex:'#3B82F6', x:'50%',
    hat:'#1E3A8A', hatBrim:'#2563EB', hair:'#FCD34D', hairW:32,
    body:'#1D4ED8', legs:'#1E3A8A', feet:'#172554', arms:'#3B82F6' },
  { key:'gpt',    name:'GPT',    color:'var(--gpt)',    hex:'#22C55E', x:'80%',
    hat:'#14532D', hatBrim:'#16A34A', hair:'#92400E', hairW:30,
    body:'#15803D', legs:'#14532D', feet:'#052E16', arms:'#22C55E' },
];

const room = document.getElementById('room');
const canvas = document.getElementById('p2p-canvas');
const ctx = canvas.getContext('2d');

// ── Criar sprites ──
AGENTS.forEach(a => {
  const wrap = document.createElement('div');
  wrap.className = 'agent-wrap';
  wrap.id = `wrap-${a.key}`;
  wrap.style.left = a.x;
  wrap.innerHTML = `
    <div class="reply-tag" id="reply-${a.key}"></div>
    <div class="bubble" id="bubble-${a.key}"></div>
    <div class="sprite-wrap">
      <div class="sprite" id="sprite-${a.key}">
        <span class="think-dots">• • •</span>
        <div class="sp-hat"      style="background:${a.hat}; width:${a.hairW}px;"></div>
        <div class="sp-hat-brim" style="background:${a.hatBrim}; width:${a.hairW+8}px;"></div>
        <div class="sp-head"><div class="sp-smile"></div></div>
        <div class="sp-body"     style="background:${a.body};"></div>
        <div class="sp-arms">
          <div class="sp-arm" style="background:${a.arms};"></div>
          <div class="sp-arm" style="background:${a.arms};"></div>
        </div>
        <div class="sp-legs">
          <div class="sp-leg" style="background:${a.legs};"></div>
          <div class="sp-leg" style="background:${a.legs};"></div>
        </div>
        <div class="sp-feet">
          <div class="sp-foot" style="background:${a.feet};"></div>
          <div class="sp-foot" style="background:${a.feet};"></div>
        </div>
        <div class="sp-shadow"></div>
      </div>
      <div class="vote-badge" id="vote-${a.key}"></div>
    </div>
    <div class="agent-label" style="color:${a.color};">${a.name}</div>
  `;
  room.appendChild(wrap);
});

// ── Canvas P2P resize ──
function resizeCanvas() {
  canvas.width  = room.offsetWidth;
  canvas.height = room.offsetHeight;
}
resizeCanvas();
window.addEventListener('resize', resizeCanvas);

// ── Desenha linha P2P entre dois agentes ──
let _p2pTimer = null;
function drawP2P(fromKey, toKey) {
  if (_p2pTimer) clearTimeout(_p2pTimer);
  const from = document.getElementById(`wrap-${fromKey}`);
  const to   = document.getElementById(`wrap-${toKey}`);
  if (!from || !to) return;

  const roomRect = room.getBoundingClientRect();
  const fr = from.getBoundingClientRect();
  const tr = to.getBoundingClientRect();

  const x1 = fr.left + fr.width / 2 - roomRect.left;
  const y1 = fr.top  + fr.height / 2 - roomRect.top;
  const x2 = tr.left + tr.width / 2 - roomRect.left;
  const y2 = tr.top  + tr.height / 2 - roomRect.top;

  ctx.clearRect(0, 0, canvas.width, canvas.height);
  ctx.save();
  ctx.setLineDash([6, 4]);
  ctx.lineWidth = 2;
  ctx.strokeStyle = AGENTS.find(a => a.key === fromKey)?.hex || '#FFF';
  ctx.globalAlpha = 0.6;
  ctx.beginPath();
  ctx.moveTo(x1, y1);
  ctx.lineTo(x2, y2);
  ctx.stroke();

  const angle = Math.atan2(y2 - y1, x2 - x1);
  ctx.setLineDash([]);
  ctx.globalAlpha = 0.8;
  ctx.fillStyle = AGENTS.find(a => a.key === fromKey)?.hex || '#FFF';
  ctx.beginPath();
  ctx.moveTo(x2, y2);
  ctx.lineTo(x2 - 12 * Math.cos(angle - 0.4), y2 - 12 * Math.sin(angle - 0.4));
  ctx.lineTo(x2 - 12 * Math.cos(angle + 0.4), y2 - 12 * Math.sin(angle + 0.4));
  ctx.closePath();
  ctx.fill();
  ctx.restore();

  _p2pTimer = setTimeout(() => ctx.clearRect(0, 0, canvas.width, canvas.height), 6000);
}

// ── Helpers ──
const agentMap = Object.fromEntries(AGENTS.map(a => [a.key, a]));
const agentNames = { claude: 'Claude', gemini: 'Gemini', gpt: 'GPT-5.4' };

function now() {
  return new Date().toLocaleTimeString('pt-BR', {hour:'2-digit',minute:'2-digit',second:'2-digit'});
}

function setThinking(key, val, replyTo) {
  const sp = document.getElementById(`sprite-${key}`);
  if (!sp) return;
  sp.classList.toggle('thinking', val);

  const rt = document.getElementById(`reply-${key}`);
  if (rt) {
    if (val && replyTo) {
      rt.textContent = `→ ${agentNames[replyTo] || replyTo}`;
      rt.style.background = agentMap[replyTo]?.hex || '#888';
      rt.classList.add('show');
    } else {
      rt.classList.remove('show');
    }
  }

  if (val) _thinkingAgents.add(key);
  else _thinkingAgents.delete(key);

  const pb = document.getElementById('parallel-badge');
  if (_thinkingAgents.size >= 2) {
    pb.classList.add('show');
  } else {
    pb.classList.remove('show');
  }
}

// Bubble: typewriter, max 80 chars displayed, auto-hide after 8s
const _bubbleTimers = {};
const _typewriterTimers = {};
function showBubble(key, text) {
  const b = document.getElementById(`bubble-${key}`);
  if (!b) return;
  if (_bubbleTimers[key]) clearTimeout(_bubbleTimers[key]);
  if (_typewriterTimers[key]) clearInterval(_typewriterTimers[key]);
  b.classList.add('show');
  b.textContent = '';
  const short = (text || '').length > 80 ? text.slice(0, 79) + '…' : (text || '');
  if (!short) return;
  let i = 0;
  const delay = Math.max(20, 1600 / short.length);
  _typewriterTimers[key] = setInterval(() => {
    b.textContent += short[i++];
    if (i >= short.length) { clearInterval(_typewriterTimers[key]); delete _typewriterTimers[key]; }
  }, delay);
  _bubbleTimers[key] = setTimeout(() => {
    b.classList.remove('show');
    b.textContent = '';
  }, 8000);
}

function showVote(key, type) {
  const badge = document.getElementById(`vote-${key}`);
  if (!badge) return;
  badge.textContent = type === 'approve' ? '✅' : '❌';
  badge.classList.add('show');
  setTimeout(() => badge.classList.remove('show'), 5000);
}

// Left panel: keep last 8, max 55 chars per entry, auto-scroll, tooltip
function addAction(text) {
  const log = document.getElementById('action-log');
  const el = document.createElement('div');
  el.className = 'action-entry';
  const fullText = text || '';
  el.title = fullText; // tooltip para ver texto completo
  const ts = document.createElement('span');
  ts.className = 'ts'; ts.textContent = now();
  const sh = document.createElement('span');
  sh.className = 'short';
  sh.textContent = fullText.length > 55 ? fullText.slice(0, 54) + '…' : fullText;
  el.appendChild(ts); el.appendChild(sh);
  log.appendChild(el);
  while (log.children.length > 8) log.removeChild(log.firstChild);
  log.scrollTop = log.scrollHeight;
}

// Right panel: keep last 20, click to expand, timestamp
let _convTotal = 0;
function addConv(key, name, text, color, replyTo) {
  const log = document.getElementById('conv-log');
  _convTotal++;
  const el = document.createElement('div');
  el.className = `log-entry ${key}`;

  const who = document.createElement('div');
  who.className = 'who'; who.style.color = color;
  const whoName = document.createElement('span');
  whoName.textContent = name;
  who.appendChild(whoName);
  if (replyTo) {
    const arr = document.createElement('span');
    arr.className = 'reply-arrow';
    arr.textContent = `→ ${agentNames[replyTo] || replyTo}`;
    who.appendChild(arr);
  }
  // timestamp inline
  const tsBadge = document.createElement('span');
  tsBadge.style.cssText = 'font-size:13px;color:var(--gold);margin-left:auto;flex-shrink:0;';
  tsBadge.textContent = now();
  who.appendChild(tsBadge);

  const body = document.createElement('div');
  body.className = 'body'; body.textContent = text || '';

  el.appendChild(who); el.appendChild(body);
  el.addEventListener('click', () => el.classList.toggle('expanded'));
  log.appendChild(el);
  // Mantém últimas 20 entradas
  while (log.children.length > 20) log.removeChild(log.firstChild);
  log.scrollTop = log.scrollHeight;
  // Atualiza contador no título
  const countEl = document.getElementById('conv-count');
  if (countEl) countEl.textContent = _convTotal > 20 ? `+${_convTotal - 20} ocultas` : '';
}

function setStatus(text, state = 'ok') {
  const statusEl = document.getElementById('status-text');
  const thinking = _thinkingAgents.size;
  const suffix = (state === 'busy' && thinking > 1) ? ` [${thinking} deliberando]` : '';
  statusEl.textContent = text + suffix;
  const dot = document.getElementById('status-dot');
  dot.className = 'status-dot' + (state === 'busy' ? ' busy' : state === 'error' ? ' error' : '');
}
function showRound(n) {
  const badge = document.getElementById('round-badge');
  badge.textContent = `ROUND ${n}`;
  badge.classList.add('show');
  setTimeout(() => badge.classList.remove('show'), 3000);
}

// ── Web Audio API — beep pixel art ──
let _audioCtx = null;
const AGENT_FREQ  = { claude: 523, gemini: 622, gpt: 784 };
const AGENT_FREQ2 = { claude: 659, gemini: 784, gpt: 988 };

function getAudio() {
  if (!_audioCtx) _audioCtx = new (window.AudioContext || window.webkitAudioContext)();
  if (_audioCtx.state === 'suspended') _audioCtx.resume();
  return _audioCtx;
}
document.addEventListener('click', () => { try { getAudio(); } catch(e) {} }, { once: true });

function playBeep(agentKey, type = 'response') {
  try {
    const ctx = getAudio();
    const now = ctx.currentTime;
    const freqs = type === 'verdict'
      ? [AGENT_FREQ.claude, AGENT_FREQ2.claude, 1047]
      : [AGENT_FREQ[agentKey] || 440];

    freqs.forEach((freq, i) => {
      const osc  = ctx.createOscillator();
      const gain = ctx.createGain();
      osc.type = 'square';
      osc.frequency.value = freq;
      const dur = type === 'verdict' ? 0.25 : type === 'thinking' ? 0.06 : 0.12;
      gain.gain.setValueAtTime(0.07, now + i * 0.08);
      gain.gain.exponentialRampToValueAtTime(0.001, now + i * 0.08 + dur);
      osc.connect(gain);
      gain.connect(ctx.destination);
      osc.start(now + i * 0.08);
      osc.stop(now + i * 0.08 + dur);
    });
  } catch(e) {}
}

// ── State ──
let _currentRound = 0;
let _roundResponses = 0;
let _thinkingAgents = new Set();
let _seenIds = new Set(); // deduplicação: evita replay duplicar eventos ao vivo

// ── Entrada animada dos agentes ──
window.addEventListener('load', () => {
  AGENTS.forEach((a, i) => {
    const w = document.getElementById(`wrap-${a.key}`);
    if (!w) return;
    setTimeout(() => w.classList.add('entered'), 300 + i * 200);
  });
});

// ── Salto ao falar ──
function jumpAgent(key) {
  const w = document.getElementById(`wrap-${key}`);
  if (!w) return;
  const a = agentMap[key];
  w.style.setProperty('--glow-color', a?.hex || '#fff');
  w.classList.remove('jumping', 'speaking-glow');
  void w.offsetWidth;
  w.classList.add('jumping', 'speaking-glow');
  setTimeout(() => w.classList.remove('jumping', 'speaking-glow'), 900);
}

// ── Aceno dos outros ──
function nodOthers(speakingKey) {
  AGENTS.forEach(a => {
    if (a.key === speakingKey) return;
    const sp = document.getElementById(`sprite-${a.key}`);
    if (!sp || sp.classList.contains('thinking')) return;
    sp.classList.remove('nodding');
    void sp.offsetWidth;
    sp.classList.add('nodding');
    setTimeout(() => sp.classList.remove('nodding'), 700);
  });
}

// ── Emoji flutuante ──
const REACT_EMOJIS = { response: ['💬','✨','💡','🗣️'], thinking: ['🤔','⚙️','💭'], vote: ['⚖️','👍','👎'], verdict: ['⚖️','🔮','✨'] };
function floatEmoji(key, type = 'response') {
  const w = document.getElementById(`wrap-${key}`);
  if (!w) return;
  const rect = w.getBoundingClientRect();
  const el = document.createElement('div');
  el.className = 'float-emoji';
  const list = REACT_EMOJIS[type] || REACT_EMOJIS.response;
  el.textContent = list[Math.floor(Math.random() * list.length)];
  el.style.left = (rect.left + rect.width / 2 - 11) + 'px';
  el.style.top  = (rect.top + 10) + 'px';
  document.body.appendChild(el);
  setTimeout(() => el.remove(), 1500);
}

// ── Flash de tela ──
function flashScreen() {
  const f = document.getElementById('screen-flash');
  if (!f) return;
  f.classList.add('active');
  setTimeout(() => { f.classList.remove('active');
    setTimeout(() => { f.classList.add('active');
      setTimeout(() => f.classList.remove('active'), 100); }, 250);
  }, 100);
}

// ── Confetti ──
const CONFETTI_COLORS = ['#A855F7','#3B82F6','#22C55E','#F59E0B','#EF4444','#F0E8D0','#FB923C'];
function launchConfetti() {
  for (let i = 0; i < 55; i++) {
    setTimeout(() => {
      const el = document.createElement('div');
      const size = 5 + Math.random() * 9;
      el.style.cssText = `position:fixed;left:${Math.random()*100}vw;top:-${10+Math.random()*20}px;`
        + `width:${size}px;height:${size}px;`
        + `background:${CONFETTI_COLORS[i % CONFETTI_COLORS.length]};`
        + `z-index:200;pointer-events:none;`
        + `border-radius:${Math.random()>0.5?'50%':'2px'};`
        + `animation:confettiFall ${1.5+Math.random()*2}s ease forwards;`;
      document.body.appendChild(el);
      setTimeout(() => el.remove(), 3500);
    }, i * 55);
  }
}

// ── Eventos SSE ──
function handle(data, silent = false) {
  // Deduplicação: ignora evento se já foi processado (evita race condition replay x live)
  if (data._id) {
    if (_seenIds.has(data._id)) return;
    _seenIds.add(data._id);
    if (_seenIds.size > 600) {
      // Limpa IDs antigos mantendo os 500 mais recentes
      const arr = [..._seenIds];
      _seenIds = new Set(arr.slice(arr.length - 500));
    }
  }
  const a = agentMap[data.agent];
  switch (data.type) {
    case 'thinking':
      setThinking(data.agent, true, data.replyTo);
      if (data.replyTo) drawP2P(data.agent, data.replyTo);
      if (!silent) {
        playBeep(data.agent, 'thinking');
        floatEmoji(data.agent, 'thinking');
      }
      setStatus(`${a?.name || data.agent} deliberando...`, 'busy');
      break;

    case 'response': {
      setThinking(data.agent, false, null);
      if (!silent) {
        jumpAgent(data.agent);
        nodOthers(data.agent);
        floatEmoji(data.agent, 'response');
        showBubble(data.agent, data.text);
        playBeep(data.agent, 'response');
      }
      if (a) addConv(a.key, a.name, data.text, a.color, data.replyTo);
      setStatus(`${a?.name} pronunciou-se`);
      if (data.replyTo) {
        _roundResponses++;
        if (_roundResponses === 1) { _currentRound++; showRound(_currentRound); }
        if (_roundResponses >= 2) _roundResponses = 0;
      } else {
        _roundResponses++;
        if (_roundResponses <= 2) showRound(1);
        if (_roundResponses >= 2) { _roundResponses = 0; _currentRound = 1; }
      }
      break;
    }

    case 'vote':
      if (!silent) {
        showVote(data.agent, data.vote);
        floatEmoji(data.agent, 'vote');
      }
      if (a) {
        const label = data.vote === 'approve' ? 'aprovou ✅' : 'rejeitou ❌';
        addConv(a.key, a.name, `[${label}]${data.reason ? ' — ' + data.reason : ''}`, a.color);
      }
      break;

    case 'action':
      addAction(data.text);
      setStatus(`Claude: ${(data.text||'').slice(0,40)}`, 'busy');
      showBubble('claude', data.text);
      break;

    case 'verdict': {
      const vbox = document.getElementById('verdict-box');
      const vtxt = document.getElementById('verdict-text');
      if (vbox && vtxt) {
        vtxt.textContent = data.text;
        vbox.classList.add('show');
        if (vbox._hideTimer) clearTimeout(vbox._hideTimer);
        vbox._hideTimer = setTimeout(() => {
          vbox.classList.remove('show');
          vbox._hideTimer = null;
        }, 12000);
      }
      if (!silent) {
        flashScreen();
        playBeep('claude', 'verdict');
        showBubble('claude', data.text);
        floatEmoji('claude', 'verdict');
        // Todos acenam no veredito
        setTimeout(() => AGENTS.forEach(x => {
          const sp = document.getElementById(`sprite-${x.key}`);
          if (sp) { sp.classList.remove('nodding'); void sp.offsetWidth; sp.classList.add('nodding'); setTimeout(() => sp.classList.remove('nodding'), 700); }
        }), 200);
      }
      addConv('claude', 'Claude (Veredito)', data.text, 'var(--claude)');
      setStatus('Claude emitiu veredito ⚖');
      break;
    }

    case 'done':
      AGENTS.forEach(x => setThinking(x.key, false));
      ctx.clearRect(0, 0, canvas.width, canvas.height);
      _currentRound = 0; _roundResponses = 0;
      setStatus('Sessão encerrada ✓');
      addAction('✓ ' + (data.summary || 'Tarefa concluída'));
      if (!silent) launchConfetti();
      break;

    case 'connected':
      setStatus('Sala do Conselho ativa', 'ok');
      break;

    case 'reset':
      document.getElementById('conv-log').innerHTML = '';
      document.getElementById('action-log').innerHTML =
        '<div class="action-entry"><span class="ts">Sistema</span><span class="short">Sessão reiniciada.</span></div>';
      const vboxReset = document.getElementById('verdict-box');
      if (vboxReset) {
        if (vboxReset._hideTimer) { clearTimeout(vboxReset._hideTimer); vboxReset._hideTimer = null; }
        vboxReset.classList.remove('show');
      }
      ctx.clearRect(0, 0, canvas.width, canvas.height);
      _currentRound = 0; _roundResponses = 0; _thinkingAgents.clear(); _convTotal = 0; _seenIds.clear();
      const countElReset = document.getElementById('conv-count');
      if (countElReset) countElReset.textContent = '';
      AGENTS.forEach(x => {
        setThinking(x.key, false);
        const b = document.getElementById(`bubble-${x.key}`);
        if (b) { b.classList.remove('show'); b.textContent = ''; }
      });
      setStatus('Nova sessão', 'ok');
      break;
  }
}

// ── SSE Connect ──
let _reconnectCount = 0;
function connect() {
  const es = new EventSource('/stream');
  es.onopen = () => {
    _reconnectCount = 0;
    setStatus('Sala do Conselho ativa', 'ok');
    const badge = document.getElementById('sse-badge');
    badge.textContent = 'LIVE'; badge.className = '';
    // Restaura histórico se a página foi recarregada (conv-log vazio)
    if (_convTotal === 0) {
      fetch('/report').then(r => r.json()).then(d => {
        if (d.events && d.events.length) {
          d.events.forEach(evt => handle(evt, true)); // silent: sem beep/salto
        }
      }).catch(() => {});
    }
  };
  es.onmessage = e => {
    try { handle(JSON.parse(e.data)); }
    catch(err) { console.error('SSE parse error:', err); }
  };
  es.onerror = () => {
    _reconnectCount++;
    const delay = Math.min(2000 * _reconnectCount, 10000);
    setStatus(`Reconectando... (${_reconnectCount})`, 'error');
    const badge = document.getElementById('sse-badge');
    badge.textContent = 'OFF'; badge.className = 'off';
    es.close();
    setTimeout(connect, delay);
  };
}
connect();
</script>
</body>
</html>
"""  # fim do _REMOVED_HTML (não usado)


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="127.0.0.1", port=8765, log_level="warning")
