import json
import time
import uuid
from contextlib import asynccontextmanager
from typing import AsyncGenerator

import httpx
import redis.asyncio as redis
import tiktoken
from fastapi import FastAPI, Header, HTTPException, Request
from fastapi.responses import StreamingResponse

from gateway.config import settings
from gateway.guardrails import StreamCircuitBreaker
from gateway.ledger import SpendLedger

ENCODING = tiktoken.get_encoding("o200k_base")


@asynccontextmanager
async def lifespan(app: FastAPI):
    app.state.http_client = httpx.AsyncClient(
        base_url=settings.upstream_base_url,
        timeout=httpx.Timeout(connect=5.0, read=60.0, write=10.0, pool=5.0),
        limits=httpx.Limits(max_keepalive_connections=100, max_connections=500),
    )
    app.state.redis = redis.from_url(settings.redis_url, decode_responses=False)
    app.state.ledger = SpendLedger(app.state.redis, default_session_budget=10_000)
    yield
    await app.state.http_client.aclose()
    await app.state.redis.aclose()


app = FastAPI(title="AI Spend-Control Gateway", lifespan=lifespan)


def count_prompt_tokens(messages: list[dict]) -> int:
    total = 0
    for msg in messages:
        content = msg.get("content", "")
        if isinstance(content, str):
            total += len(ENCODING.encode(content))
    return total


@app.post("/v1/chat/completions")
async def proxy_chat_completions(
    request: Request,
    x_agent_session_id: str = Header(default="default-session"),
    x_idempotency_key: str | None = Header(default=None),
):
    ingress_start = time.perf_counter()
    payload = await request.json()

    if not payload.get("stream", False):
        raise HTTPException(
            status_code=400,
            detail="Gateway enforces stream=true for real-time token metering.",
        )

    idemp_key = x_idempotency_key or str(uuid.uuid4())
    messages = payload.get("messages", [])
    prompt_tokens = count_prompt_tokens(messages)
    max_completion_est = payload.get("max_tokens", settings.default_preauth_tokens)
    estimated_total = prompt_tokens + max_completion_est

    # 1. Atomic Pre-Authorization Hold
    ledger: SpendLedger = request.app.state.ledger
    preauth = await ledger.pre_authorize(
        session_id=x_agent_session_id,
        idempotency_key=idemp_key,
        estimated_tokens=estimated_total,
    )

    if not preauth.approved:
        status_code = 409 if preauth.reason == "DUPLICATE_REQUEST" else 402
        raise HTTPException(
            status_code=status_code,
            detail={
                "error": preauth.reason,
                "remaining_tokens": preauth.remaining_tokens,
            },
        )

    headers = {
        "Authorization": f"Bearer {settings.upstream_api_key}",
        "Content-Type": "application/json",
    }

    async def stream_and_meter() -> AsyncGenerator[bytes, None]:
        completion_text_chunks: list[str] = []
        first_token_logged = False
        client: httpx.AsyncClient = request.app.state.http_client
        breaker = StreamCircuitBreaker(repeat_threshold=3)

        try:
            async with client.stream(
                "POST",
                "/chat/completions",
                json=payload,
                headers=headers,
            ) as upstream_resp:
                if upstream_resp.status_code != 200:
                    error_body = await upstream_resp.aread()
                    raise HTTPException(
                        status_code=upstream_resp.status_code,
                        detail=error_body.decode("utf-8", errors="replace"),
                    )

                async for line in upstream_resp.aiter_lines():
                    if not line:
                        continue

                    if not first_token_logged:
                        ttft_ms = (time.perf_counter() - ingress_start) * 1000
                        print(f"[{x_agent_session_id}] TTFT: {ttft_ms:.2f}ms")
                        first_token_logged = True

                    if line.startswith("data: ") and line != "data: [DONE]":
                        raw_json = line[6:]
                        try:
                            chunk = json.loads(raw_json)
                            delta = chunk["choices"][0].get("delta", {})
                            if "content" in delta and delta["content"]:
                                content_piece = delta["content"]

                                # 2. Inline Runaway Loop & Velocity Guardrail
                                is_safe, trip_reason = breaker.inspect_chunk(content_piece)
                                if not is_safe:
                                    error_event = json.dumps(
                                        {
                                            "error": "CIRCUIT_BREAKER_TRIPPED",
                                            "reason": trip_reason,
                                        }
                                    )
                                    yield f"data: {error_event}\n\n".encode("utf-8")
                                    yield b"data: [DONE]\n\n"
                                    print(
                                        f"[{x_agent_session_id}] CIRCUIT BREAKER TRIPPED: {trip_reason}"
                                    )
                                    break

                                completion_text_chunks.append(content_piece)
                        except (json.JSONDecodeError, KeyError, IndexError):
                            pass

                    yield f"{line}\n\n".encode("utf-8")

        finally:
            # 3. Guaranteed Post-Stream Settlement & Hold Release
            full_output = "".join(completion_text_chunks)
            completion_tokens = len(ENCODING.encode(full_output))
            actual_total = prompt_tokens + completion_tokens

            settlement = await ledger.settle(
                session_id=x_agent_session_id,
                idempotency_key=idemp_key,
                actual_tokens=actual_total,
            )
            print(
                f"[{x_agent_session_id}] Settled: {settlement['settled_tokens']} | "
                f"Released Hold: {settlement['released_hold']} | "
                f"Remaining Budget: {settlement['remaining_budget']}"
            )

    return StreamingResponse(
        stream_and_meter(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "X-Accel-Buffering": "no",
        },
    )

