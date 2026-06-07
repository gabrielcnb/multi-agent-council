"""MCP Server multi-agent: Conselho dos Agentes."""

import asyncio
import os
import re
import socket
import subprocess
import sys
from pathlib import Path

import httpx
from mcp.server.fastmcp import FastMCP
from perplexity import ask_perplexity, MODEL_MAP
from memory import recall, save_session, save_decision, list_sessions, session_count

mcp = FastMCP("multi-agent")

ROOM_URL    = "http://127.0.0.1:8765"
_SERVER_DIR = Path(__file__).parent

# Arquivo de lock de manutenção, criado quando alguém está editando o conselho
_MAINT_LOCK = _SERVER_DIR / ".maintenance_lock"

# Tag único por janela Claude Code (baseado no PID deste processo MCP)
_WINDOW_TAG = f"w{os.getpid() % 9999:04d}"


# ─── helpers ────────────────────────────────────────────────────────────────

def _current_room() -> str:
    """Deriva o ID da sala do diretório de trabalho atual (herdado do Claude Code).

    Formato: {projeto}--{window_tag}
    Isso garante salas isoladas por projeto E por janela Claude Code simultânea.
    """
    name = Path.cwd().name.lower()
    slug = re.sub(r"[^a-z0-9-]", "-", name)
    slug = re.sub(r"-+", "-", slug).strip("-") or "default"
    return f"{slug}--{_WINDOW_TAG}"


def _sala_rodando() -> bool:
    try:
        s = socket.create_connection(("127.0.0.1", 8765), timeout=0.5)
        s.close()
        return True
    except OSError:
        return False


def _check_maintenance() -> str | None:
    """Retorna mensagem de aviso se o conselho estiver em manutenção por outra sessão."""
    if not _MAINT_LOCK.exists():
        return None
    try:
        owner = _MAINT_LOCK.read_text().strip()
        room  = _current_room()
        if owner != room:
            return (
                f"⚠️  CONSELHO EM MANUTENÇÃO: sala '{owner}' está editando os arquivos do conselho.\n"
                f"Aguarde a manutenção terminar antes de usar as ferramentas.\n"
                f"(sala atual: '{room}')"
            )
    except Exception:
        pass
    return None


async def _notify(event: dict, room: str | None = None):
    r = room or _current_room()
    payload = {**event, "window": event.get("window", _WINDOW_TAG)}
    try:
        async with httpx.AsyncClient(timeout=2) as c:
            await c.post(f"{ROOM_URL}/event?room={r}", json=payload)
    except Exception:
        pass  # sala fechada, ignora


# ─── retry wrapper ──────────────────────────────────────────────────────────

async def _ask_with_retry(question: str, model: str, max_retries: int = 2) -> str:
    """Chama ask_perplexity com até max_retries tentativas em caso de ERRO."""
    for attempt in range(1, max_retries + 1):
        result = await ask_perplexity(question, model)
        if not result.startswith("ERRO"):
            return result
        if attempt < max_retries:
            await asyncio.sleep(1)
    return result  # retorna o último ERRO se todas as tentativas falharem


# ─── tools ──────────────────────────────────────────────────────────────────

@mcp.tool()
async def convocar_conselho() -> str:
    """🏰 Abre a Sala do Conselho no browser e inicia o servidor se necessário.

    A sala é isolada por projeto (derivada do diretório atual).
    Após convocar, use:
      ask_model(question, model)          → consultar Gemini ou GPT
      debate(topic, rounds)               → debate P2P entre GPT e Gemini
      vote(agent, 'approve'|'reject')     → registrar voto na sala
      broadcast_action(message)           → mostrar o que Claude está fazendo
      finish_task(summary)                → encerrar sessão
    """
    warn = _check_maintenance()
    if warn:
        return warn

    room     = _current_room()
    room_url = f"{ROOM_URL}?room={room}"

    if not _sala_rodando():
        kwargs = {}
        if sys.platform == "win32":
            kwargs["creationflags"] = subprocess.CREATE_NO_WINDOW
        subprocess.Popen(
            [sys.executable, str(_SERVER_DIR / "room_server.py")],
            cwd=str(_SERVER_DIR),
            **kwargs,
        )
        for _ in range(14):
            await asyncio.sleep(0.4)
            if _sala_rodando():
                break
        else:
            return "ERRO: não foi possível iniciar o servidor da sala."

    # Abre o browser na sala correta
    if sys.platform == "win32":
        subprocess.Popen(
            f'start "" "{room_url}"',
            shell=True,
            creationflags=subprocess.CREATE_NO_WINDOW,
        )
    else:
        subprocess.Popen(["xdg-open", room_url], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)

    return (
        f"🏰 Conselho convocado!\n"
        f"  Sala:   {room}\n"
        f"  Janela: {_WINDOW_TAG}\n"
        f"  URL:    {room_url}\n\n"
        "Agentes presentes: Gemini 3.1 Pro · GPT-5.4\n"
        "Eu (Claude) sou o orquestrador: executo no seu computador.\n\n"
        "Ferramentas:\n"
        "  ask_model(question, model)              → consultar Gemini/GPT\n"
        "  debate(topic, rounds)                   → debate P2P\n"
        "  vote(agent, 'approve'|'reject', reason) → votar na sala\n"
        "  broadcast_action(message)               → mostrar minha ação\n"
        "  get_report()                            → ler relatório da sessão\n"
        "  reset_session()                         → limpar sessão\n"
        "  finish_task(summary)                    → encerrar"
    )


@mcp.tool()
async def ask_model(question: str, model: str = "gemini") -> str:
    """Consulta Gemini ou GPT no Perplexity Pro. Resultado aparece na sala.

    model: 'gemini' | 'gpt' | 'sonar' | 'nemotron' | 'best'
    """
    warn = _check_maintenance()
    if warn:
        return warn
    if not question or not question.strip():
        return "ERRO: question não pode ser vazia."
    await _notify({"type": "thinking", "agent": model})
    response = await _ask_with_retry(question, model)
    await _notify({"type": "response", "agent": model, "text": response})
    return response


@mcp.tool()
async def vote(agent: str, decision: str, reason: str = "") -> str:
    """Registra aprovação ou rejeição de um agente na sala.

    agent:    'gemini' | 'gpt' | 'claude'
    decision: 'approve' | 'reject'
    reason:   texto curto exibido no log
    """
    if decision not in ("approve", "reject"):
        return "ERRO: decision deve ser 'approve' ou 'reject'"
    await _notify({"type": "vote", "agent": agent, "vote": decision, "reason": reason})
    return f"{MODEL_MAP.get(agent, agent)} → {decision}"


@mcp.tool()
async def broadcast_action(message: str) -> str:
    """Transmite o que Claude está fazendo para a sala (painel esquerdo).

    Exemplo: broadcast_action("Escrevendo arquivo index.html")
    """
    await _notify({"type": "action", "agent": "claude", "text": message})
    return "OK"


@mcp.tool()
async def finish_task(summary: str = "") -> str:
    """Sinaliza que a tarefa foi concluída na sala visual."""
    await _notify({"type": "done", "summary": summary})
    return "Tarefa finalizada."


@mcp.tool()
async def set_maintenance(on: bool) -> str:
    """Ativa/desativa modo manutenção do conselho.

    Quando ativo, bloqueia outras sessões de usarem as ferramentas do conselho.
    Use ANTES de editar arquivos do conselho (server.py, room.html, etc).
    Use novamente com on=False ao terminar.

    on=True  → ativa manutenção (bloqueia outras sessões)
    on=False → desativa manutenção (libera o corredor)
    """
    room = _current_room()
    if on:
        _MAINT_LOCK.write_text(room)
        return (
            f"🚧 Manutenção ATIVADA para sala '{room}'.\n"
            "Outras sessões verão aviso de bloqueio ao tentar usar o conselho.\n"
            "Lembre de chamar set_maintenance(False) ao terminar."
        )
    else:
        if _MAINT_LOCK.exists():
            owner = _MAINT_LOCK.read_text().strip()
            if owner == room:
                _MAINT_LOCK.unlink()
                return f"✅ Manutenção DESATIVADA. Corredor liberado para outras sessões."
            else:
                return f"⚠️ O lock pertence à sala '{owner}', não à sua ('{room}'). Não foi alterado."
        return "Manutenção já estava desativada."


@mcp.tool()
async def debate(topic: str, agents: list = None, rounds: int = 2) -> str:
    """Debate P2P entre agentes. Round 1 em paralelo, rounds seguintes reagem entre si.

    agents: ['gemini', 'gpt'] (padrão)
    rounds: número de rodadas (mín 1, recomendado 2)

    Fluxo P2P:
      Round 1 → GPT e Gemini respondem ao tópico ao mesmo tempo (paralelo)
      Round 2 → cada um lê o que o outro disse e reage diretamente
      Round N → idem, construindo o debate
    """
    warn = _check_maintenance()
    if warn:
        return warn
    if agents is None:
        agents = ["gemini", "gpt"]
    invalid = [a for a in agents if a not in MODEL_MAP]
    if invalid:
        return f"ERRO: modelos inválidos: {invalid}. Opções: {list(MODEL_MAP.keys())}"

    await broadcast_action(f'Debate P2P: "{topic}"')
    transcript: list[dict] = []  # {agent, model_name, response, round}

    # ── Round 1: paralelo ────────────────────────────────────────────────────
    for agent in agents:
        await _notify({"type": "thinking", "agent": agent})

    # Injeta memória relevante se existir
    memory_ctx = recall(topic)
    mem_block = f"\n\n{memory_ctx}\n" if memory_ctx else ""

    prompts_r1 = {
        agent: (
            f"Debate: {topic}{mem_block}\n"
            f"Você é {MODEL_MAP[agent]}. "
            "Dê sua opinião inicial. Seja direto: máximo 3 frases curtas no total."
        )
        for agent in agents
    }

    responses_r1 = await asyncio.gather(
        *[_ask_with_retry(prompts_r1[a], a) for a in agents]
    )

    for agent, resp in zip(agents, responses_r1):
        transcript.append({"agent": agent, "model_name": MODEL_MAP[agent], "response": resp, "round": 1})
        await _notify({"type": "response", "agent": agent, "text": resp})
        await asyncio.sleep(0.2)

    # ── Rounds 2+: P2P, cada um reage ao outro ──────────────────────────────
    for round_n in range(2, rounds + 1):
        prev_by_agent = {e["agent"]: e for e in transcript if e["round"] == round_n - 1}
        await asyncio.sleep(0.5)

        for i, agent in enumerate(agents):
            other = agents[1 - i]
            other_name = MODEL_MAP[other]
            other_resp  = prev_by_agent[other]["response"]
            model_name  = MODEL_MAP[agent]

            prompt = (
                f"Debate: {topic}\n\n"
                f"{other_name} disse: \"{other_resp[:300]}\"\n\n"
                f"Você é {model_name}. "
                "Concorda ou discorda com o que foi dito acima? Reaja em no máximo 2 frases curtas."
            )

            await _notify({"type": "thinking", "agent": agent, "replyTo": other})
            resp = await _ask_with_retry(prompt, agent)
            transcript.append({"agent": agent, "model_name": model_name, "response": resp, "round": round_n})
            await _notify({"type": "response", "agent": agent, "text": resp, "replyTo": other})

            neg = any(w in resp.lower() for w in [
                "discordo", "não concordo", "porém", "contudo",
                "however", "but", "disagree", "actually",
            ])
            await _notify({
                "type": "vote", "agent": agent,
                "vote": "reject" if neg else "approve",
                "reason": "",
            })
            await asyncio.sleep(0.3)

    # ── Veredito do Claude ───────────────────────────────────────────────────
    verdict = _build_verdict(topic, transcript)
    await _notify({"type": "verdict", "agent": "claude", "text": verdict})

    # ── Salva na memória ─────────────────────────────────────────────────────
    has_consensus = not any(
        w in e["response"].lower()
        for e in transcript if e.get("round", 1) > 1
        for w in ["discordo", "porém", "however", "but", "parcialmente"]
    )
    save_session(topic, transcript, verdict, has_consensus)
    if memory_ctx:
        await broadcast_action(f"Memória: encontrei {memory_ctx.count('•')} sessões relevantes anteriores")

    await _notify({"type": "done", "summary": f"Debate concluído — {len(transcript)} falas"})

    lines = [f"# Debate P2P: {topic}\n"]
    for e in transcript:
        lines += [f"## Round {e['round']} · [{e['model_name']}]", e["response"], ""]
    lines += ["\n## [Veredito Claude]", verdict]
    return "\n".join(lines)


def _build_verdict(topic: str, transcript: list[dict]) -> str:
    """Gera veredito do Claude analisando o transcript do debate."""
    disagreement_entries = [
        e for e in transcript
        if e.get("round", 1) > 1 and any(
            w in e["response"].lower()
            for w in ["discordo", "no entanto", "porém", "contudo", "however", "but", "actually", "parcialmente"]
        )
    ]
    consensus = len(disagreement_entries) == 0

    # Pontos principais de cada agente no Round 1
    r1 = [e for e in transcript if e.get("round") == 1]
    agent_points: dict[str, str] = {}
    for e in r1:
        first = e["response"].split(".")[0].strip()[:120]
        if first:
            agent_points[e["model_name"]] = first

    # Constrói veredito analítico em 2-3 frases concretas
    lines: list[str] = []

    # Frase 1: o que cada agente defendeu
    if len(agent_points) >= 2:
        names = list(agent_points.keys())
        lines.append(
            f"{names[0]} defendeu: \"{agent_points[names[0]]}\". "
            f"{names[1]} posicionou: \"{agent_points[names[1]]}\"."
        )
    elif len(agent_points) == 1:
        name, pt = next(iter(agent_points.items()))
        lines.append(f"{name} argumentou: \"{pt}\".")

    # Frase 2: convergência ou divergência com detalhe
    if consensus:
        lines.append("Os agentes convergiram — não houve contestação entre as rodadas.")
    else:
        dissenting = list({e["model_name"] for e in disagreement_entries})
        lines.append(
            f"Divergência identificada em {len(disagreement_entries)} fala(s) "
            f"({', '.join(dissenting)}) — posições não plenamente alinhadas."
        )

    # Frase 3: encaminhamento concreto
    if consensus:
        lines.append(f"Adotarei a abordagem consensual para '{topic[:60]}'.")
    else:
        lines.append(f"Ponderarei os pontos de conflito antes de decidir sobre '{topic[:60]}'.")

    return " ".join(lines)


@mcp.tool()
async def code_review(code: str, context: str = "", file_path: str = "") -> str:
    """Review de código por GPT e Gemini em paralelo. Retorna análise consolidada.

    code:      código a revisar (string)
    context:   descrição do que o código faz / objetivo
    file_path: caminho do arquivo (opcional, só como referência)

    Retorna reviews estruturadas (bugs / segurança / estilo / patch) e veredito Claude.
    Round 2 automático se as reviews divergirem em severidade.
    """
    warn = _check_maintenance()
    if warn:
        return warn
    label = file_path or "código"
    await broadcast_action(f"Code review: {label}")
    await _notify({"type": "thinking", "agent": "gemini"})
    await _notify({"type": "thinking", "agent": "gpt"})

    review_prompt = lambda model_name: (
        f"Faça uma code review do código abaixo.\n"
        f"Contexto: {context or 'não informado'}\n"
        f"Arquivo: {file_path or 'não informado'}\n\n"
        f"```\n{code[:2000]}\n```\n\n"
        f"Você é {model_name}. Responda EXATAMENTE neste formato (3 linhas):\n"
        f"BUGS: <achados ou 'nenhum'>\n"
        f"SEGURANÇA: <achados ou 'nenhum'>\n"
        f"ESTILO: <sugestão principal ou 'ok'>\n"
        f"PATCH: <linha corrigida ou 'sem patch'>\n"
        f"SEVERIDADE: <baixa|média|alta>"
    )

    # Round 1: paralelo
    r_gemini, r_gpt = await asyncio.gather(
        _ask_with_retry(review_prompt(MODEL_MAP["gemini"]), "gemini"),
        _ask_with_retry(review_prompt(MODEL_MAP["gpt"]),    "gpt"),
    )

    await _notify({"type": "response", "agent": "gemini", "text": r_gemini})
    await _notify({"type": "response", "agent": "gpt",    "text": r_gpt})

    # Detecta divergência de severidade entre as duas reviews
    def _severity(text: str) -> int:
        t = text.lower()
        if "alta" in t:   return 3
        if "média" in t:  return 2
        return 1

    sev_g = _severity(r_gemini)
    sev_p = _severity(r_gpt)
    divergence = abs(sev_g - sev_p) >= 2 or (sev_g >= 2 and sev_p <= 1) or (sev_p >= 2 and sev_g <= 1)

    r2_gemini = r2_gpt = ""
    if divergence:
        await broadcast_action("Reviews divergentes — iniciando Round 2")
        await _notify({"type": "thinking", "agent": "gemini", "replyTo": "gpt"})
        await _notify({"type": "thinking", "agent": "gpt",    "replyTo": "gemini"})

        r2_gemini, r2_gpt = await asyncio.gather(
            _ask_with_retry(
                f"GPT disse sobre o código:\n{r_gpt[:400]}\n\n"
                f"Você é {MODEL_MAP['gemini']}. Concorda com a severidade? "
                "Reavalie em 2 frases e repita sua SEVERIDADE final.",
                "gemini"
            ),
            _ask_with_retry(
                f"Gemini disse sobre o código:\n{r_gemini[:400]}\n\n"
                f"Você é {MODEL_MAP['gpt']}. Concorda com a severidade? "
                "Reavalie em 2 frases e repita sua SEVERIDADE final.",
                "gpt"
            ),
        )
        await _notify({"type": "response", "agent": "gemini", "text": r2_gemini, "replyTo": "gpt"})
        await _notify({"type": "response", "agent": "gpt",    "text": r2_gpt, "replyTo": "gemini"})

    # Veredito Claude
    verdict_lines = [f"Code Review: {label}"]
    verdict_lines.append(f"Gemini (sev={sev_g}): {r_gemini.splitlines()[0][:80]}")
    verdict_lines.append(f"GPT    (sev={sev_p}): {r_gpt.splitlines()[0][:80]}")
    if divergence:
        verdict_lines.append("Reviews divergiram — considerei ambas as perspectivas.")
    else:
        verdict_lines.append("Reviews consistentes — aplicarei os pontos levantados.")
    verdict = " | ".join(verdict_lines)

    await _notify({"type": "verdict", "agent": "claude", "text": verdict})
    await _notify({"type": "done", "summary": f"Review concluída — severidade máx: {max(sev_g, sev_p)}"})

    report = f"# Code Review: {label}\n\n"
    report += f"## Gemini\n{r_gemini}\n\n"
    report += f"## GPT-5.4\n{r_gpt}\n\n"
    if divergence:
        report += f"## Round 2 — Gemini\n{r2_gemini}\n\n"
        report += f"## Round 2 — GPT\n{r2_gpt}\n\n"
    report += f"## Veredito Claude\n{verdict}\n"
    return report


@mcp.tool()
async def get_report() -> str:
    """Lê o relatório completo da sessão atual: o que cada agente disse, votos e ações.

    Use para acompanhar o andamento sem precisar abrir o browser.
    Retorna texto estruturado com toda a atividade registrada.
    """
    room = _current_room()
    try:
        async with httpx.AsyncClient(timeout=3) as c:
            resp = await c.get(f"{ROOM_URL}/report?room={room}")
            data = resp.json()
    except Exception as e:
        return f"Sala não disponível ({e}). Execute convocar_conselho() primeiro."

    events = data.get("events", [])
    if not events:
        return "Nenhuma atividade registrada na sessão atual."

    agent_names = {"claude": "Claude", "gemini": "Gemini 3.1 Pro", "gpt": "GPT-5.4"}
    lines = ["═" * 50, "  RELATÓRIO DA SESSÃO DO CONSELHO", "═" * 50]

    for ev in events:
        t     = ev.get("type")
        agent = ev.get("agent", "?")
        name  = agent_names.get(agent, agent.upper())

        if t == "action":
            lines.append(f"\n⚡ [Claude fez] {ev.get('text')}")
        elif t == "response":
            reply = f" → respondendo {agent_names.get(ev['replyTo'], ev['replyTo'])}" if ev.get("replyTo") else ""
            lines.append(f"\n💬 [{name}{reply}]\n{ev.get('text','')[:1000]}")
        elif t == "vote":
            v = "✅ aprovou" if ev.get("vote") == "approve" else "❌ rejeitou"
            r = f" — {ev['reason']}" if ev.get("reason") else ""
            lines.append(f"\n🗳️  [{name}] {v}{r}")
        elif t == "done":
            lines.append(f"\n{'═'*50}\n✓ SESSÃO ENCERRADA: {ev.get('summary','')}")

    lines.append("\n" + "═" * 50)
    return "\n".join(lines)


@mcp.tool()
async def remember(decision: str, topic: str = "") -> str:
    """Salva uma decisão importante na memória persistente do conselho.

    Exemplo: remember("Usar SQLite para memória, não JSON", topic="arquitetura do multi-agent")
    """
    save_decision(topic or decision[:60], decision)
    await broadcast_action(f"Memória salva: {decision[:80]}")
    return f"Decisão salva: {decision[:100]}"


@mcp.tool()
async def recall_memory(topic: str = "") -> str:
    """Lê memórias relevantes ao tópico: sessões passadas, decisões, vereditos.

    Se topic vazio, mostra as sessões mais recentes.
    """
    if topic:
        ctx = recall(topic)
        return ctx or "Nenhuma memória relevante encontrada para esse tópico."

    sessions = list_sessions(10)
    total = session_count()
    if not sessions:
        return "Nenhuma sessão salva ainda."

    lines = [f"Memória do conselho — {total} sessões totais\n"]
    for s in sessions:
        label = "✅" if s["consensus"] else "⚡"
        lines.append(f"{label} {s['date'][:10]} | {s['topic'][:80]}")
        if s["summary"]:
            lines.append(f"   {s['summary']}")
    return "\n".join(lines)


@mcp.tool()
async def reset_session() -> str:
    """Limpa o log da sessão atual na sala (começa nova conversa)."""
    room = _current_room()
    try:
        async with httpx.AsyncClient(timeout=2) as c:
            await c.post(f"{ROOM_URL}/reset?room={room}")
        return f"Sessão da sala '{room}' resetada. Pronta para nova tarefa."
    except Exception as e:
        return f"Sala não disponível: {e}"


@mcp.tool()
async def list_models() -> str:
    """Lista modelos disponíveis no Perplexity Pro."""
    return "\n".join(f"  {k:10} → {v}" for k, v in MODEL_MAP.items())


if __name__ == "__main__":
    mcp.run(transport="stdio")
