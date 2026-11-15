import json
import time
from contextlib import asynccontextmanager
from typing import AsyncGenerator

import httpx
import tiktoken
from fastapi import FastAPI, Header, HTTPException, Request
from fastapi.responses import StreamingResponse

from gateway.config import settings

# Reuse tokenizer across requests to avoid initialization overhead
ENCODING = tiktoken.get_encoding("o200k_base")


@asynccontextmanager
async def lifespan(app: FastAPI):
    # Persistent connection pool to upstream LLM provider
    app.state.http_client = httpx.AsyncClient(
        base_url=settings.upstream_base_url,
        timeout=httpx.Timeout(connect=5.0, read=60.0, write=10.0, pool=5.0),
        limits=httpx.Limits(max_keepalive_connections=100, max_connections=500),
    )
    yield
    await app.state.http_client.aclose()


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
            detail="Gateway currently enforces stream=true for real-time token metering.",
        )

    messages = payload.get("messages", [])
    prompt_tokens = count_prompt_tokens(messages)

    # TODO (Weekend 2): Call Redis Lua script here to place pre-auth hold
    # using x_agent_session_id and x_idempotency_key

    headers = {
        "Authorization": f"Bearer {settings.upstream_api_key}",
        "Content-Type": "application/json",
    }

    async def stream_and_meter() -> AsyncGenerator[bytes, None]:
        completion_text_chunks: list[str] = []
        first_token_logged = False
        client: httpx.AsyncClient = request.app.state.http_client

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

                    # Inspect SSE data payload without delaying chunk forwarding
                    if line.startswith("data: ") and line != "data: [DONE]":
                        raw_json = line[6:]
                        try:
                            chunk = json.loads(raw_json)
                            delta = chunk["choices"][0].get("delta", {})
                            if "content" in delta and delta["content"]:
                                completion_text_chunks.append(delta["content"])
                        except (json.JSONDecodeError, KeyError, IndexError):
                            pass

                    yield f"{line}\n\n".encode("utf-8")

        finally:
            # Guaranteed settlement block: executes on normal completion OR client disconnect
            full_output = "".join(completion_text_chunks)
            completion_tokens = len(ENCODING.encode(full_output))
            total_tokens = prompt_tokens + completion_tokens

            # TODO (Weekend 2): Settle exact total_tokens in Redis & release unused hold
            print(
                f"[{x_agent_session_id}] Settled | "
                f"Prompt: {prompt_tokens} | Completion: {completion_tokens} | "
                f"Total: {total_tokens}"
            )

    return StreamingResponse(
        stream_and_meter(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "X-Accel-Buffering": "no",
        },
    )

