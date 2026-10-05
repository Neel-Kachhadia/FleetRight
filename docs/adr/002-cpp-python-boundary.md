# ADR-002: C++ and Python boundary

Status: Accepted

C++20 owns canonical units, planning, placement, invariant evaluation, and replay. Python owns
measurement orchestration, telemetry/profile statistics, synthetic traces, baselines, simulation,
and reports. Both exchange versioned Protocol Buffers; C++ uses generated types, while Python calls
the installed `protoc` encoder/decoder to avoid requiring an unavailable Python protobuf runtime.
