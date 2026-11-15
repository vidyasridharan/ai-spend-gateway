from dataclasses import dataclass
import redis.asyncio as redis

PREAUTH_LUA = """
local budget_key = KEYS[1]
local idemp_key = KEYS[2]
local hold_key = KEYS[3]

local est_tokens = tonumber(ARGV[1])
local default_budget = tonumber(ARGV[2])
local ttl_seconds = tonumber(ARGV[3])

-- 1. Check if this idempotency key already has an active or completed record
local existing_status = redis.call("GET", idemp_key)
if existing_status then
    return {0, "DUPLICATE_REQUEST", 0}
end

-- 2. Initialize session budget if it doesn't exist yet
if redis.call("EXISTS", budget_key) == 0 then
    redis.call("HSET", budget_key,
        "total_budget", default_budget,
        "settled_tokens", 0,
        "active_holds", 0
    )
end

-- 3. Calculate available balance
local vals = redis.call("HMGET", budget_key, "total_budget", "settled_tokens", "active_holds")
local total_budget = tonumber(vals[1])
local settled_tokens = tonumber(vals[2])
local active_holds = tonumber(vals[3])
local available = total_budget - settled_tokens - active_holds

if available < est_tokens then
    return {0, "INSUFFICIENT_BUDGET", available}
end

-- 4. Place pre-auth hold & record idempotency state
redis.call("HINCRBY", budget_key, "active_holds", est_tokens)
redis.call("SET", hold_key, est_tokens, "EX", ttl_seconds)
redis.call("SET", idemp_key, "IN_FLIGHT", "EX", ttl_seconds)

return {1, "APPROVED", available - est_tokens}
"""

SETTLE_LUA = """
local budget_key = KEYS[1]
local idemp_key = KEYS[2]
local hold_key = KEYS[3]

local actual_tokens = tonumber(ARGV[1])
local ttl_seconds = tonumber(ARGV[2])

local held_tokens = tonumber(redis.call("GET", hold_key) or "0")

if held_tokens > 0 then
    redis.call("HINCRBY", budget_key, "active_holds", -held_tokens)
    redis.call("DEL", hold_key)
end

redis.call("HINCRBY", budget_key, "settled_tokens", actual_tokens)
redis.call("SET", idemp_key, "SETTLED:" .. actual_tokens, "EX", ttl_seconds)

local vals = redis.call("HMGET", budget_key, "total_budget", "settled_tokens", "active_holds")
local remaining = tonumber(vals[1]) - tonumber(vals[2]) - tonumber(vals[3])
return {actual_tokens, held_tokens, remaining}
"""


@dataclass
class PreAuthResult:
    approved: bool
    reason: str
    remaining_tokens: int


class SpendLedger:
    def __init__(self, redis_client: redis.Redis, default_session_budget: int = 10_000):
        self.redis = redis_client
        self.default_session_budget = default_session_budget
        self._preauth_script = self.redis.register_script(PREAUTH_LUA)
        self._settle_script = self.redis.register_script(SETTLE_LUA)

    async def pre_authorize(
        self,
        session_id: str,
        idempotency_key: str,
        estimated_tokens: int,
        ttl_seconds: int = 3600,
    ) -> PreAuthResult:
        keys = [
            f"session:{session_id}:budget",
            f"session:{session_id}:idemp:{idempotency_key}",
            f"session:{session_id}:hold:{idempotency_key}",
        ]
        res = await self._preauth_script(
            keys=keys,
            args=[estimated_tokens, self.default_session_budget, ttl_seconds],
        )
        return PreAuthResult(
            approved=bool(res[0]),
            reason=res[1].decode() if isinstance(res[1], bytes) else str(res[1]),
            remaining_tokens=int(res[2]),
        )

    async def settle(
        self,
        session_id: str,
        idempotency_key: str,
        actual_tokens: int,
        ttl_seconds: int = 3600,
    ) -> dict:
        keys = [
            f"session:{session_id}:budget",
            f"session:{session_id}:idemp:{idempotency_key}",
            f"session:{session_id}:hold:{idempotency_key}",
        ]
        res = await self._settle_script(
            keys=keys,
            args=[actual_tokens, ttl_seconds],
        )
        return {
            "settled_tokens": int(res[0]),
            "released_hold": int(res[1]),
            "remaining_budget": int(res[2]),
        }

