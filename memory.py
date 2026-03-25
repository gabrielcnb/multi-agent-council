"""Memória persistente do Conselho — SQLite + injeção seletiva de contexto."""

import json
import sqlite3
from datetime import datetime
from pathlib import Path

DB_PATH = Path(__file__).parent / "council_memory.db"


def _conn():
    db = sqlite3.connect(DB_PATH)
    db.row_factory = sqlite3.Row
    return db


def _fts_available(db: sqlite3.Connection) -> bool:
    """Verifica se fts5 está compilado no SQLite disponível."""
    try:
        db.execute("CREATE VIRTUAL TABLE IF NOT EXISTS _fts_probe USING fts5(x)")
        db.execute("DROP TABLE IF EXISTS _fts_probe")
        return True
    except sqlite3.OperationalError:
        return False


def init_db():
    with _conn() as db:
        db.executescript("""
        CREATE TABLE IF NOT EXISTS sessions (
            id        INTEGER PRIMARY KEY AUTOINCREMENT,
            date      TEXT NOT NULL,
            topic     TEXT NOT NULL,
            consensus INTEGER NOT NULL DEFAULT 0,  -- 1=consenso, 0=divergência
            verdict   TEXT,
            summary   TEXT
        );
        CREATE TABLE IF NOT EXISTS messages (
            id         INTEGER PRIMARY KEY AUTOINCREMENT,
            session_id INTEGER REFERENCES sessions(id),
            agent      TEXT NOT NULL,
            round      INTEGER NOT NULL DEFAULT 1,
            text       TEXT NOT NULL,
            reply_to   TEXT
        );
        CREATE TABLE IF NOT EXISTS decisions (
            id         INTEGER PRIMARY KEY AUTOINCREMENT,
            date       TEXT NOT NULL,
            topic      TEXT NOT NULL,
            decision   TEXT NOT NULL,   -- texto da decisão aprovada
            approved_by TEXT            -- 'claude'|'gemini'|'gpt'|'all'
        );
        CREATE INDEX IF NOT EXISTS idx_sessions_topic  ON sessions(topic);
        CREATE INDEX IF NOT EXISTS idx_decisions_topic ON decisions(topic);
        CREATE INDEX IF NOT EXISTS idx_messages_sid    ON messages(session_id);
        """)
        # FTS5 para busca rápida em sessões e decisões (fallback silencioso se não disponível)
        if _fts_available(db):
            db.executescript("""
            CREATE VIRTUAL TABLE IF NOT EXISTS fts_sessions
                USING fts5(topic, verdict, content='sessions', content_rowid='id');
            CREATE VIRTUAL TABLE IF NOT EXISTS fts_decisions
                USING fts5(topic, decision, content='decisions', content_rowid='id');
            """)


def _has_fts(db: sqlite3.Connection) -> bool:
    """Verifica se as tabelas FTS foram criadas nesta conexão."""
    row = db.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name='fts_sessions'"
    ).fetchone()
    return row is not None


def save_session(topic: str, transcript: list[dict], verdict: str, consensus: bool) -> int:
    """Salva debate completo. Retorna session_id."""
    init_db()
    summary = f"{len(transcript)} falas · {'consenso' if consensus else 'divergência'}"
    with _conn() as db:
        cur = db.execute(
            "INSERT INTO sessions (date, topic, consensus, verdict, summary) VALUES (?,?,?,?,?)",
            (datetime.now().isoformat(), topic, int(consensus), verdict, summary)
        )
        sid = cur.lastrowid
        for e in transcript:
            db.execute(
                "INSERT INTO messages (session_id, agent, round, text, reply_to) VALUES (?,?,?,?,?)",
                (sid, e.get("agent","?"), e.get("round",1), e.get("response",""), e.get("replyTo"))
            )
        # Atualiza índice FTS se disponível
        if _has_fts(db):
            db.execute(
                "INSERT INTO fts_sessions(rowid, topic, verdict) VALUES (?,?,?)",
                (sid, topic, verdict or "")
            )
    return sid


def save_decision(topic: str, decision: str, approved_by: str = "claude"):
    """Salva uma decisão tomada (ex: feature aprovada, abordagem escolhida)."""
    init_db()
    with _conn() as db:
        cur = db.execute(
            "INSERT INTO decisions (date, topic, decision, approved_by) VALUES (?,?,?,?)",
            (datetime.now().isoformat(), topic, decision, approved_by)
        )
        # Atualiza índice FTS se disponível
        if _has_fts(db):
            db.execute(
                "INSERT INTO fts_decisions(rowid, topic, decision) VALUES (?,?,?)",
                (cur.lastrowid, topic, decision)
            )


def recall(current_topic: str, max_results: int = 4) -> str:
    """Busca memórias relevantes ao tópico atual. Usa FTS5 se disponível, senão LIKE.
    Retorna string formatada para injeção no prompt."""
    init_db()
    keywords = [w.lower() for w in current_topic.split() if len(w) > 3]
    if not keywords:
        return ""

    results: list[dict] = []
    decisions: list[dict] = []

    with _conn() as db:
        if _has_fts(db):
            # ── FTS5: une keywords com OR para match amplo ───────────────────
            fts_query = " OR ".join(keywords[:5])
            try:
                rows = db.execute(
                    "SELECT s.id, s.date, s.topic, s.consensus, s.verdict "
                    "FROM fts_sessions f "
                    "JOIN sessions s ON s.id = f.rowid "
                    "WHERE fts_sessions MATCH ? "
                    "ORDER BY s.id DESC LIMIT ?",
                    (fts_query, max_results * 2)
                ).fetchall()
                results = [dict(r) for r in rows]

                rows = db.execute(
                    "SELECT d.date, d.topic, d.decision, d.approved_by "
                    "FROM fts_decisions f "
                    "JOIN decisions d ON d.id = f.rowid "
                    "WHERE fts_decisions MATCH ? "
                    "ORDER BY d.id DESC LIMIT 6",
                    (fts_query,)
                ).fetchall()
                decisions = [dict(r) for r in rows]
            except sqlite3.OperationalError:
                # FTS query inválida (ex: caracteres especiais) — cai no fallback
                results = []
                decisions = []

        if not results and not decisions:
            # ── Fallback LIKE ────────────────────────────────────────────────
            seen_ids: set[int] = set()
            for kw in keywords[:5]:
                rows = db.execute(
                    "SELECT id, date, topic, consensus, verdict FROM sessions "
                    "WHERE lower(topic) LIKE ? ORDER BY id DESC LIMIT 3",
                    (f"%{kw}%",)
                ).fetchall()
                for r in rows:
                    if r["id"] not in seen_ids:
                        seen_ids.add(r["id"])
                        results.append(dict(r))

            for kw in keywords[:5]:
                rows = db.execute(
                    "SELECT date, topic, decision, approved_by FROM decisions "
                    "WHERE lower(topic) LIKE ? OR lower(decision) LIKE ? ORDER BY id DESC LIMIT 3",
                    (f"%{kw}%", f"%{kw}%")
                ).fetchall()
                decisions.extend([dict(r) for r in rows])

    if not results and not decisions:
        return ""

    lines = ["[MEMÓRIA DO CONSELHO — sessões anteriores relevantes]"]
    for r in results[:max_results]:
        date = r["date"][:10]
        consensus_label = "consenso" if r["consensus"] else "divergência"
        lines.append(f"• {date} | {consensus_label} | Tópico: {r['topic'][:100]}")
        if r["verdict"]:
            lines.append(f"  Veredito: {r['verdict'][:150]}")

    if decisions:
        lines.append("[Decisões anteriores relevantes]")
        seen: set[str] = set()
        for d in decisions[:4]:
            key = d["decision"][:60]
            if key not in seen:
                seen.add(key)
                lines.append(f"• {d['date'][:10]} | {d['decision'][:120]} (por {d['approved_by']})")

    return "\n".join(lines)


def list_sessions(limit: int = 20) -> list[dict]:
    """Lista sessões mais recentes."""
    init_db()
    with _conn() as db:
        rows = db.execute(
            "SELECT id, date, topic, consensus, summary FROM sessions ORDER BY id DESC LIMIT ?",
            (limit,)
        ).fetchall()
    return [dict(r) for r in rows]


def session_count() -> int:
    init_db()
    with _conn() as db:
        return db.execute("SELECT COUNT(*) FROM sessions").fetchone()[0]
