# AI Spend-Control & Runtime Guardrail Gateway

An asynchronous, low-latency reverse proxy for OpenAI-compatible streaming LLM APIs (`FastAPI` + `httpx` + `Redis Lua`). Designed to enforce financial-grade budget controls, idempotency, and inline runaway-loop circuit breakers for autonomous AI agents.

## Core Architecture & Request Lifecycle

1. **Ingress & Prompt Metering (`src/gateway/main.py`):** Intercepts `/v1/chat/completions` (`stream=true`), counts prompt tokens using `tiktoken`, and extracts `X-Agent-Session-ID` and `X-Idempotency-Key`.
2. **Atomic Pre-Authorization Hold (`src/gateway/ledger.py`):** Executes a single-round-trip Redis Lua script (`PREAUTH_LUA`) that checks idempotency (`HTTP 409` on duplicate retries), verifies available session budget (`HTTP 402` when exhausted), and places a worst-case token hold without distributed locks.
3. **Zero-Copy SSE Streaming & Inline Circuit Breaker (`src/gateway/guardrails.py`):** Proxies upstream Server-Sent Events (SSE) chunks while scanning rolling text and chunk velocity inline. Trips immediately (`CIRCUIT_BREAKER_TRIPPED`) if an agent enters a repetition loop or exceeds velocity limits.
4. **Guaranteed Post-Stream Settlement (`SETTLE_LUA`):** Inside a `finally:` block—executing on normal completion, circuit-breaker cutoff, or client disconnect—releases the pre-auth hold and deducts only the exact tokens streamed.

## Quickstart

```bash
# 1. Start Redis & local LLM (or configure OpenAI/Anthropic in .env)
redis-server --daemonize yes
ollama pull llama3.2

# 2. Install dependencies & run gateway
uv sync
uv run uvicorn gateway.main:app --reload --port 8080

# 3. Run concurrency & idempotency test suite
uv run pytest

# 4. Run concurrent streaming benchmark
uv run python scripts/load_test.py
```

## Benchmark

Completed 100 streaming requests in 6.71s
p50: 1084.21ms | p95: 2328.51ms | p99: 2590.80ms

## Project Status & Next Milestones
- [x] **Milestone 1:** Async SSE streaming proxy with persistent `httpx` connection pooling & TTFT telemetry
- [x] **Milestone 2:** Atomic Redis Lua pre-auth holds, idempotency cache, and post-stream settlement
- [x] **Milestone 3:** Inline `StreamCircuitBreaker` for runaway repetition loops & velocity spikes
- [x] **Milestone 4:** `pytest` concurrency verification suite (`fakeredis[lua]`) & load test harness
- [ ] **Future Enhancements:** Multi-model token pricing tables (USD micro-cent ledgering), OpenTelemetry tracing, and `docker-compose.yml` deployment bundle

