"""Test payload generation.

Payload bytes are created once per message *before* the protocol adapter is
involved, so what the adapter transmits is exactly what was generated.
"""

from __future__ import annotations

import random

from eccom.core.config import PayloadConfig


class PayloadGenerator:
    def __init__(self, config: PayloadConfig) -> None:
        self._size = config.size_bytes
        if config.content == "zeros":
            # One shared immutable bytes object: zero per-message cost.
            self._zeros: bytes | None = bytes(self._size)
            self._rng: random.Random | None = None
        else:
            self._zeros = None
            # Seeded PRNG: each call yields different bytes, but the whole
            # sequence is reproducible across runs with the same seed.
            self._rng = random.Random(config.seed)

    def generate(self, size_bytes: int | None = None) -> bytes:
        """One payload of ``size_bytes`` (the configured size when None)."""
        size = self._size if size_bytes is None else size_bytes
        if self._zeros is not None:
            return self._zeros if size == self._size else bytes(size)
        assert self._rng is not None
        return self._rng.randbytes(size)
