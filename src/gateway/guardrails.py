import time


class StreamCircuitBreaker:
    """
    Inline runtime guardrail for streaming LLM responses.
    Detects:
      1. Runaway repetition loops (repeated character n-grams across rolling text)
      2. Token velocity spikes (chunks/sec exceeding session threshold)
    """

    def __init__(
        self,
        phrase_len: int = 40,
        repeat_threshold: int = 3,
        max_tokens_per_sec: float = 500.0,
    ):
        self.phrase_len = phrase_len
        self.repeat_threshold = repeat_threshold
        self.max_tokens_per_sec = max_tokens_per_sec

        self._rolling_text = ""
        self._start_time = time.perf_counter()
        self._chunk_count = 0

    def inspect_chunk(self, text_delta: str) -> tuple[bool, str | None]:
        self._chunk_count += 1
        self._rolling_text = (self._rolling_text + text_delta)[-800:]

        # 1. Velocity check (after 25-chunk warmup)
        if self._chunk_count > 25:
            elapsed = max(time.perf_counter() - self._start_time, 0.001)
            velocity = self._chunk_count / elapsed
            if velocity > self.max_tokens_per_sec:
                return False, f"VELOCITY_EXCEEDED ({velocity:.1f} chunks/s)"

        # 2. Rolling substring repetition check (immune to token boundary splits)
        if len(self._rolling_text) >= self.phrase_len * self.repeat_threshold:
            tail_phrase = self._rolling_text[-self.phrase_len :]
            if tail_phrase.strip() and self._rolling_text.count(tail_phrase) >= self.repeat_threshold:
                return False, "RUNAWAY_REPETITION_LOOP"

        return True, None

