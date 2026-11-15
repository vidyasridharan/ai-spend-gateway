import asyncio
import statistics
import time
import uuid
import httpx

GATEWAY_URL = "http://localhost:8080/v1/chat/completions"


async def send_stream_request(client: httpx.AsyncClient, session_id: str) -> float:
    start = time.perf_counter()
    payload = {
        "model": "llama3.2",
        "stream": True,
        "max_tokens": 30,
        "messages": [{"role": "user", "content": "Reply with one word: ok"}],
    }
    headers = {
        "X-Agent-Session-ID": session_id,
        "X-Idempotency-Key": str(uuid.uuid4()),
    }
    async with client.stream("POST", GATEWAY_URL, json=payload, headers=headers) as resp:
        resp.raise_for_status()
        async for _ in resp.aiter_lines():
            pass
    return (time.perf_counter() - start) * 1000


async def run_benchmark(concurrency: int = 20, total_requests: int = 100):
    sem = asyncio.Semaphore(concurrency)
    latencies: list[float] = []

    async with httpx.AsyncClient(timeout=60.0) as client:
        async def worker(idx: int):
            async with sem:
                ms = await send_stream_request(client, f"bench-session-{idx % 10}")
                latencies.append(ms)

        t0 = time.perf_counter()
        await asyncio.gather(*(worker(i) for i in range(total_requests)))
        total_sec = time.perf_counter() - t0

    latencies.sort()
    p50 = statistics.median(latencies)
    p95 = latencies[int(len(latencies) * 0.95)]
    p99 = latencies[int(len(latencies) * 0.99)]

    print(f"Completed {total_requests} streaming requests in {total_sec:.2f}s")
    print(f"p50: {p50:.2f}ms | p95: {p95:.2f}ms | p99: {p99:.2f}ms")


if __name__ == "__main__":
    asyncio.run(run_benchmark())

