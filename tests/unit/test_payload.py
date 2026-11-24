from eccom.core.config import PayloadConfig
from eccom.generators.payload import PayloadGenerator


def test_zero_payload_has_requested_size_and_content() -> None:
    generator = PayloadGenerator(PayloadConfig(size_bytes=1024, content="zeros"))
    payload = generator.generate()
    assert len(payload) == 1024
    assert payload == bytes(1024)
    # zeros are shared, so repeated generation is free and identical
    assert generator.generate() is payload


def test_random_payload_is_reproducible_across_generators() -> None:
    first = PayloadGenerator(PayloadConfig(size_bytes=256, content="random", seed=42))
    second = PayloadGenerator(PayloadConfig(size_bytes=256, content="random", seed=42))
    sequence_a = [first.generate() for _ in range(3)]
    sequence_b = [second.generate() for _ in range(3)]
    assert sequence_a == sequence_b
    assert all(len(p) == 256 for p in sequence_a)
    # consecutive payloads differ (fresh randomness per message)
    assert sequence_a[0] != sequence_a[1]


def test_random_payload_differs_across_seeds() -> None:
    a = PayloadGenerator(PayloadConfig(size_bytes=256, content="random", seed=1))
    b = PayloadGenerator(PayloadConfig(size_bytes=256, content="random", seed=2))
    assert a.generate() != b.generate()


def test_generate_takes_a_per_message_size() -> None:
    zeros = PayloadGenerator(PayloadConfig(size_bytes=16, content="zeros"))
    assert zeros.generate(64) == bytes(64)
    assert zeros.generate() is zeros.generate(16)
    random_payloads = PayloadGenerator(PayloadConfig(size_bytes=16, content="random", seed=7))
    assert len(random_payloads.generate(4096)) == 4096
    assert len(random_payloads.generate()) == 16
