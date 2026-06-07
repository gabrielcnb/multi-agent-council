"""Perplexity Pro: fetch dentro do browser via Playwright (sem automação de UI)."""

import asyncio
import json
import os
import subprocess
import sys
import time
import uuid
from pathlib import Path

from playwright.async_api import async_playwright

PROFILE_DIR     = Path(__file__).parent / "profile"
PROFILE_DIR_STR = str(PROFILE_DIR)
BASE_URL    = "https://www.perplexity.ai"
ASK_URL     = f"{BASE_URL}/rest/sse/perplexity_ask"

# ── Lock de arquivo cross-process (impede dois server.py de abrir o mesmo Chrome) ──
_FLOCK_PATH  = Path(__file__).parent / ".chrome_lock"
_FLOCK_RETRY = 40    # tentativas (40 × 0.5s = 20s máx)
_FLOCK_DELAY = 0.5   # segundos entre tentativas

MODEL_MAP = {
    "sonar":    "Sonar",
    "gpt":      "GPT-5.4",
    "gemini":   "Gemini 3.1 Pro Thinking",
    "claude":   "Claude Sonnet 4.6",
    "nemotron": "Nemotron 3 Super",
    "best":     "Melhor",
}

# IDs internos confirmados por engenharia reversa do Perplexity Pro
_MODEL_PREF = {
    "gpt":      "gpt54",
    "gemini":   "gemini31pro_high",
    "sonar":    "turbo",
    "best":     "pplx_alpha",
    "claude":   "claude37",
    "nemotron": "nemotron",
}

# Estado global: Playwright e browser ficam vivos durante toda a sessão
_pw   = None
_ctx  = None
_page = None
_lock = None


def _get_lock():
    global _lock
    if _lock is None:
        _lock = asyncio.Lock()
    return _lock


# ── File lock cross-process ──────────────────────────────────────────────────
def _acquire_file_lock() -> bool:
    """
    Tenta criar o arquivo de lock com o PID atual.
    Retorna True se adquiriu, False se outro processo está usando.
    """
    pid = os.getpid()
    # Verifica se já tem lock de outro processo vivo
    if _FLOCK_PATH.exists():
        try:
            other_pid = int(_FLOCK_PATH.read_text().strip())
            if other_pid == pid:
                return True  # já é nosso
            # Verifica se o processo ainda existe
            if sys.platform == "win32":
                result = subprocess.run(
                    ['tasklist', '/FI', f'PID eq {other_pid}', '/NH'],
                    capture_output=True, text=True, timeout=3
                )
                if str(other_pid) in result.stdout:
                    return False  # outro processo vivo segura o lock
            else:
                os.kill(other_pid, 0)  # lança se processo morto
                return False
        except (ValueError, ProcessLookupError, FileNotFoundError):
            pass  # lock stale, pode sobrescrever
        except Exception:
            return False

    try:
        _FLOCK_PATH.write_text(str(pid))
        return True
    except Exception:
        return False


def _release_file_lock():
    """Libera o lock de arquivo se for nosso."""
    try:
        if _FLOCK_PATH.exists():
            content = _FLOCK_PATH.read_text().strip()
            if content == str(os.getpid()):
                _FLOCK_PATH.unlink()
    except Exception:
        pass


def _kill_orphan_chrome():
    """Mata qualquer Chrome Playwright usando o mesmo perfil (orphan de sessão anterior)."""
    # Usa a parte final do caminho como filtro (ex: "multi-agent\\profile")
    profile_filter = PROFILE_DIR.name  # "profile"
    try:
        if sys.platform == "win32":
            # WMIC lista processos cujo CommandLine contém o caminho do perfil
            out = subprocess.check_output(
                ['wmic', 'process', 'where',
                 f'name like "%chrome%" and CommandLine like "%{profile_filter}%"',
                 'get', 'ProcessId'],
                stderr=subprocess.DEVNULL, timeout=5, text=True
            )
            for line in out.splitlines():
                line = line.strip()
                if line.isdigit():
                    subprocess.run(['taskkill', '/PID', line, '/F'],
                                   capture_output=True, timeout=3)
        else:
            subprocess.run(
                ['pkill', '-f', f'chrome.*{PROFILE_DIR.name}'],
                capture_output=True, timeout=5
            )
    except Exception:
        pass


async def _reset_all():
    """Fecha e limpa todo o estado Playwright, matando orphan Chrome se necessário."""
    global _pw, _ctx, _page
    try:
        if _ctx is not None:
            await _ctx.close()
    except Exception:
        pass
    try:
        if _pw is not None:
            await _pw.stop()
    except Exception:
        pass
    _pw = None
    _ctx = None
    _page = None
    # Garante que não sobrou Chrome orphan com o perfil bloqueado
    _kill_orphan_chrome()
    await asyncio.sleep(0.5)  # dá tempo pro SO liberar o lockfile


def _ctx_alive() -> bool:
    """Verifica se o contexto ainda está aberto (heurística via .pages)."""
    if _ctx is None:
        return False
    try:
        _ = _ctx.pages  # lança se contexto fechado
        return True
    except Exception:
        return False


async def _ensure_page():
    """Garante que o Playwright e a página estão abertos. Reusa entre chamadas, recria se morto."""
    global _pw, _ctx, _page

    # Reusa a página existente se ainda estiver aberta E o contexto estiver vivo
    if _page and not _page.is_closed() and _ctx_alive():
        return _page

    # Contexto morreu: limpa tudo e recomeça (inclui matar orphan Chrome)
    if not _ctx_alive():
        await _reset_all()

    # Inicia Playwright uma vez
    if _pw is None:
        _pw = await async_playwright().start()

    # Abre o contexto persistente (perfil com sessão salva)
    if _ctx is None:
        _ctx = await _pw.chromium.launch_persistent_context(
            PROFILE_DIR_STR,
            headless=False,
            args=[
                "--no-sandbox",
                "--disable-blink-features=AutomationControlled",
                "--window-position=-32000,-32000",   # fora da tela, não rouba foco
            ],
        )

    _page = _ctx.pages[0] if _ctx.pages else await _ctx.new_page()
    _page.set_default_timeout(120_000)  # 2 min pra não cortar streams longos

    # Navega pro Perplexity se ainda não estiver lá
    if not _page.url.startswith("https://www.perplexity.ai"):
        await _page.goto(BASE_URL, wait_until="domcontentloaded", timeout=30_000)

    return _page


_FETCH_SCRIPT = """
async (args) => {
    const {url, payload, modelPref} = args;

    // Atualiza o localStorage com o modelo escolhido (Perplexity lê isso antes de cada query)
    try {
        const lsKey = 'pplx.local-user-settings.preferredSearchModels-v1';
        localStorage.setItem(lsKey, JSON.stringify({search: modelPref, research: 'pplx_alpha'}));
        localStorage.setItem('pplx.local-user-settings.preferredSearchModelUpdatedAt',
                             JSON.stringify(new Date().toISOString()));
    } catch(e) {}

    const resp = await fetch(url, {
        method: 'POST',
        headers: {'Content-Type': 'application/json'},
        body: JSON.stringify(payload),
        credentials: 'include',
    });
    if (!resp.ok) return {error: resp.status};

    const reader = resp.body.getReader();
    const dec    = new TextDecoder();
    let raw = '';
    const start = Date.now();

    while (Date.now() - start < 120000) {
        const {done, value} = await reader.read();
        if (done) break;
        raw += dec.decode(value, {stream: true});
        if (raw.includes('"final_sse_message": true') ||
            raw.includes('"text_completed": true')) break;
    }
    return {ok: true, raw};
}
"""


def _extract_answer(raw: str) -> str:
    """
    Extrai texto da resposta SSE do Perplexity (formato COPILOT/Pro).

    Estrutura:
      blocks[] { intended_usage: "ask_text", markdown_block: { chunks: [...], chunk_starting_offset: N } }
      O texto final = chunks ordenados por chunk_starting_offset, concatenados.
    """
    all_chunks: dict[int, str] = {}

    for line in raw.splitlines():
        if not line.startswith("data: "):
            continue
        try:
            data = json.loads(line[6:])
        except json.JSONDecodeError:
            continue

        for block in data.get("blocks", []):
            if block.get("intended_usage") != "ask_text":
                continue
            mb  = block.get("markdown_block", {})
            off = mb.get("chunk_starting_offset", 0)
            for i, chunk in enumerate(mb.get("chunks", [])):
                all_chunks[off + i] = chunk

    if all_chunks:
        return "".join(all_chunks[k] for k in sorted(all_chunks)).strip()
    return ""


def _extract_display_model(raw: str) -> str:
    """Extrai o campo display_model da resposta SSE (debug)."""
    for line in raw.splitlines():
        if not line.startswith("data: "):
            continue
        try:
            data = json.loads(line[6:])
            if "display_model" in data:
                return data["display_model"]
        except json.JSONDecodeError:
            continue
    return ""


async def ask_perplexity(question: str, model: str = "gemini") -> str:
    """Consulta um modelo no Perplexity Pro via fetch no browser (sem UI, sem API key)."""
    model_key = model.lower()
    if model_key not in MODEL_MAP:
        return f"ERRO: Modelo '{model}' desconhecido. Opções: {', '.join(MODEL_MAP.keys())}"

    pref = _MODEL_PREF.get(model_key, "turbo")

    payload = {
        "query_str": question,
        "params": {
            "model_preference":            pref,
            "mode":                        "copilot",
            "search_focus":                "internet",
            "is_incognito":                False,
            "frontend_uuid":               str(uuid.uuid4()),
            "language":                    "pt-BR",
            "use_schematized_api":         True,
            "send_back_text_in_streaming_api": False,
            "sources":                     ["web"],
        },
    }

    # ── Aguarda lock cross-process (evita conflito com outra janela do Claude Code) ──
    for attempt in range(_FLOCK_RETRY):
        if _acquire_file_lock():
            break
        await asyncio.sleep(_FLOCK_DELAY)
    else:
        return "ERRO: Outro processo está usando o Perplexity. Tente novamente em instantes."

    async with _get_lock():
        try:
            page = await _ensure_page()
            result = await page.evaluate(_FETCH_SCRIPT, {"url": ASK_URL, "payload": payload, "modelPref": pref})
        except Exception as e:
            # Reset completo: contexto pode estar morto, não apenas a página
            await _reset_all()
            _release_file_lock()
            return f"ERRO ao consultar Perplexity: {e}"
        finally:
            _release_file_lock()

    if isinstance(result, dict) and result.get("error"):
        return f"ERRO HTTP {result['error']} do Perplexity."

    raw    = result.get("raw", "") if isinstance(result, dict) else ""
    answer = _extract_answer(raw)
    return answer or "ERRO: Resposta vazia ou não parseada."
