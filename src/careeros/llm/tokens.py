"""Provider-neutral token estimation (~4 characters per token for English text)."""

import math


def estimate_tokens(text: str) -> int:
    return max(1, math.ceil(len(text) / 4))
