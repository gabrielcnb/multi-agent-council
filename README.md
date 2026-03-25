# Multi-Agent Council

An MCP (Model Context Protocol) server that orchestrates AI debates between multiple LLMs (GPT, Gemini, Claude) through a real-time visual dashboard. Built to work as a plugin for [Claude Code](https://docs.anthropic.com/en/docs/claude-code), acting as a "council of advisors" that debates decisions before code gets written.

## What it does

Instead of relying on a single AI's opinion, this tool routes questions through multiple models simultaneously via Perplexity Pro, collects their responses, and facilitates structured P2P debates between them. Claude acts as the orchestrator — it calls the council, reads the debate, and makes the final decision.

### Core features

- **P2P Debates** — GPT and Gemini respond in parallel (Round 1), then react to each other's arguments (Round 2+). Claude synthesizes a verdict.
- **Code Review** — Both models review code simultaneously, flag bugs/security/style issues. If severity assessments diverge, an automatic Round 2 forces them to reconcile.
- **Persistent Memory** — SQLite-backed memory with FTS5 full-text search. Past debates and decisions are automatically recalled when relevant topics come up again.
- **Real-time Dashboard** — A browser-based council room (SSE-powered) shows thinking indicators, responses, votes, and verdicts as they happen. Multiple rooms supported for parallel sessions.
- **Session Isolation** — Each Claude Code window gets its own room (derived from project directory + PID), so multiple projects can run councils simultaneously.
- **Maintenance Mode** — Lock mechanism prevents concurrent sessions from conflicting when editing council files.

## Architecture

```
Claude Code (CLI)
    |
    | stdio (MCP protocol)
    v
server.py (MCP Server - FastMCP)
    |
    |--- perplexity.py (Playwright browser → Perplexity Pro SSE API)
    |--- memory.py (SQLite + FTS5 for debate history)
    |--- room_server.py (FastAPI + SSE → real-time dashboard)
              |
              v
         static/room.html (browser dashboard)
```

### How it queries models

The key trick: instead of paying for API keys for each model, it uses **Playwright** to maintain a persistent Chromium session logged into Perplexity Pro. Queries are sent via `fetch()` directly in the browser context, hitting Perplexity's internal SSE endpoint. This gives access to GPT-5.4, Gemini 3.1 Pro, Claude Sonnet 4.6, Nemotron, and Sonar — all through a single Perplexity Pro subscription.

A cross-process file lock prevents multiple Claude Code windows from racing on the same browser instance.

### Available MCP tools

| Tool | Description |
|------|-------------|
| `convocar_conselho()` | Opens the council room and starts the server |
| `ask_model(question, model)` | Query a specific model (gemini, gpt, sonar, nemotron, best) |
| `debate(topic, rounds)` | P2P debate between agents with automatic verdict |
| `code_review(code, context, file_path)` | Parallel code review with severity reconciliation |
| `vote(agent, decision, reason)` | Register approval/rejection |
| `broadcast_action(message)` | Show what Claude is doing in the dashboard |
| `remember(decision, topic)` | Save a decision to persistent memory |
| `recall_memory(topic)` | Search past debates and decisions |
| `get_report()` | Get full session transcript |
| `finish_task(summary)` | Close the session |
| `set_maintenance(on)` | Lock/unlock for file editing |
| `list_models()` | Show available models |

## Setup

### Requirements

- Python 3.11+
- A Perplexity Pro subscription (for multi-model access)
- Claude Code CLI

### Install

```bash
# Clone
git clone https://github.com/gabrielcnb/multi-agent-council.git
cd multi-agent-council

# Install dependencies
pip install -r requirements.txt

# Install Playwright Chromium
playwright install chromium
```

Or run `setup.bat` on Windows.

### Configure Claude Code

Add to your `~/.claude/.mcp.json`:

```json
{
  "mcpServers": {
    "multi-agent": {
      "command": "python",
      "args": ["/path/to/multi-agent-council/server.py"]
    }
  }
}
```

### First run

1. Restart Claude Code to load the MCP server
2. Ask Claude to `convocar_conselho()` — a browser window will open
3. Log into Perplexity Pro in that browser window
4. Close the browser — the session profile is saved in `./profile/`
5. Future queries will reuse the saved session automatically

## Dashboard

The council room is a real-time SSE dashboard at `http://127.0.0.1:8765`. It shows:

- **Left panel**: Claude's actions (what it's doing)
- **Center**: Agent responses, debates, and verdicts
- **Right panel**: Vote log (approvals/rejections)
- **Bottom bar**: Session info and room ID

Rooms are automatically cleaned up after 2 hours of inactivity.

## How memory works

The council remembers past debates via SQLite with FTS5 full-text search:

- **Sessions**: topic, transcript, verdict, consensus flag
- **Decisions**: approved choices with attribution
- **Auto-recall**: When a new debate starts, relevant past sessions are injected into agent prompts

This means the council gets smarter over time — past decisions inform future debates.

## License

MIT
