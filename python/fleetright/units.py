from __future__ import annotations

from dataclasses import dataclass

_MAX_I64 = (1 << 63) - 1
_PPM = 1_000_000


def _integer(value: int, name: str, maximum: int = _MAX_I64) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 0 or value > maximum:
        raise ValueError(f"{name} must be a bounded nonnegative integer")
    return value


def multiply_ppm_floor(value: int, *factors: int) -> int:
    """Multiply bounded parts-per-million factors and round down once."""
    _integer(value, "value", (1 << 64) - 1)
    if not 1 <= len(factors) <= 3 or any(
        isinstance(factor, bool) or not isinstance(factor, int) or not 0 < factor <= _PPM
        for factor in factors
    ):
        raise ValueError("one to three PPM factors in (0, 1_000_000] are required")
    numerator = value
    for factor in factors:
        numerator *= factor
    return numerator // (_PPM ** len(factors))


@dataclass(frozen=True, order=True)
class CpuMillicores:
    value: int

    def __post_init__(self) -> None:
        _integer(self.value, "cpu_millicores")


@dataclass(frozen=True, order=True)
class MemoryMiB:
    value: int

    def __post_init__(self) -> None:
        _integer(self.value, "memory_mib")


@dataclass(frozen=True, order=True)
class NetworkMbps:
    value: int

    def __post_init__(self) -> None:
        _integer(self.value, "network_mbps")


@dataclass(frozen=True, order=True)
class RequestsPerSecondMilli:
    value: int

    def __post_init__(self) -> None:
        _integer(self.value, "requests_per_second_milli", (1 << 64) - 1)


@dataclass(frozen=True, order=True)
class Concurrency:
    value: int

    def __post_init__(self) -> None:
        _integer(self.value, "concurrency", (1 << 64) - 1)


@dataclass(frozen=True, order=True)
class LatencyMs:
    value: int

    def __post_init__(self) -> None:
        _integer(self.value, "latency_ms", (1 << 64) - 1)


@dataclass(frozen=True)
class ResourceVector:
    cpu: CpuMillicores
    memory: MemoryMiB
    network: NetworkMbps

    @classmethod
    def from_units(cls, cpu_millicores: int, memory_mib: int, network_mbps: int) -> ResourceVector:
        return cls(CpuMillicores(cpu_millicores), MemoryMiB(memory_mib), NetworkMbps(network_mbps))
