import asyncio
import fakeredis.aioredis
import pytest
from gateway.ledger import SpendLedger


@pytest.mark.asyncio
async def test_concurrent_preauth_never_overdraws_budget():
    fake_redis = fakeredis.aioredis.FakeRedis()
    ledger = SpendLedger(fake_redis, default_session_budget=5_000)

    async def attempt_hold(idx: int):
        return await ledger.pre_authorize(
            session_id="concurrent-session",
            idempotency_key=f"req-{idx}",
            estimated_tokens=1_000,
        )

    # Fire 20 parallel requests simultaneously (20,000 tokens requested vs 5,000 budget)
    results = await asyncio.gather(*(attempt_hold(i) for i in range(20)))

    approved = [r for r in results if r.approved]
    rejected = [r for r in results if not r.approved]

    assert len(approved) == 5
    assert len(rejected) == 15
    assert all(r.reason == "INSUFFICIENT_BUDGET" for r in rejected)


@pytest.mark.asyncio
async def test_idempotency_blocks_duplicate_requests():
    fake_redis = fakeredis.aioredis.FakeRedis()
    ledger = SpendLedger(fake_redis, default_session_budget=10_000)

    first = await ledger.pre_authorize("sess-1", "tx-same", 1_000)
    duplicate = await ledger.pre_authorize("sess-1", "tx-same", 1_000)

    assert first.approved is True
    assert duplicate.approved is False
    assert duplicate.reason == "DUPLICATE_REQUEST"


@pytest.mark.asyncio
async def test_settlement_releases_unused_hold():
    fake_redis = fakeredis.aioredis.FakeRedis()
    ledger = SpendLedger(fake_redis, default_session_budget=10_000)

    await ledger.pre_authorize("sess-2", "tx-settle", 2_000)
    settlement = await ledger.settle("sess-2", "tx-settle", actual_tokens=150)

    assert settlement["settled_tokens"] == 150
    assert settlement["released_hold"] == 2_000
    assert settlement["remaining_budget"] == 9_850

