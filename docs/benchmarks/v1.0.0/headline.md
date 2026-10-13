# Headline comparison (SIMULATED, paired by seed)

Cell = `relative node-cost difference | mean SLO-violation-window difference | verdict` for FleetRight minus the column policy. Negative favours FleetRight. Verdicts use the paired 95% bootstrap CIs; see comparison.md for intervals and per-policy tables. The last table is the forecast ablation: predictive at the reactive policy's own knob.

## fleetright_reactive

| scenario | static_peak_reserve | naive_threshold | hpa_like |
|---|---|---|---|
| S01 steady | +0% \| +0.0 \| no significant difference | -20% \| +0.0 \| FR better | -20% \| +0.0 \| FR better |
| S02 gradual increase | -1% \| -50.7 \| FR better (cost and SLO) | -15% \| +0.0 \| FR better | -9% \| +0.0 \| FR better |
| S03 daily peaks (compressed 12-window day) | -19% \| +25.1 \| trade-off | -25% \| +14.9 \| trade-off | -3% \| +22.2 \| trade-off |
| S04 flash crowd 5x | -65% \| +0.0 \| FR better | -15% \| -4.5 \| FR better (cost and SLO) | -15% \| +0.0 \| FR better |
| S05 CPU-intensive workload | -12% \| +12.3 \| trade-off | +46% \| -17.6 \| trade-off | +26% \| -17.6 \| trade-off |
| S06 memory-intensive workload | +8% \| +0.0 \| FR worse | -19% \| +0.0 \| FR better | -7% \| +0.0 \| FR better |
| S08 slow-starting replicas (150 s) | -15% \| +115.0 \| trade-off | -30% \| +80.2 \| trade-off | -14% \| +43.2 \| trade-off |
| S09 multiple tenants, different SLOs | -10% \| +51.6 \| trade-off | +26% \| -20.8 \| trade-off | +8% \| -20.8 \| trade-off |
| S10 noisy-neighbour demand | +1% \| +0.0 \| no significant difference | -13% \| -0.1 \| FR better | -5% \| -0.2 \| FR better (cost and SLO) |
| S11 correlated surges | -47% \| -6.0 \| FR better (cost and SLO) | -39% \| -6.0 \| FR better (cost and SLO) | -17% \| +0.0 \| FR better |
| S12 forecast underprediction (+80% level shift) | -11% \| -29.1 \| FR better (cost and SLO) | -28% \| +13.6 \| trade-off | -16% \| +23.8 \| trade-off |
| S13 forecast overprediction (-50% level shift) | -23% \| +8.8 \| trade-off | -0% \| +5.3 \| FR worse | +25% \| +7.7 \| FR worse |
| S14 stale telemetry (1 of 3 windows missing) | -17% \| +45.0 \| trade-off | -17% \| +45.0 \| trade-off | +5% \| +29.7 \| FR worse |
| S15 capacity profile drift (-20% true capacity) | -19% \| +66.0 \| trade-off | -35% \| +67.5 \| trade-off | -19% \| +69.9 \| trade-off |
| S16 one node lost (1 replica per tenant) | +0% \| +0.0 \| no significant difference | -20% \| +2.0 \| trade-off | -20% \| +2.0 \| trade-off |
| **tally** | FR better: 1; FR better (cost and SLO): 3; FR worse: 1; no significant difference: 3; trade-off: 7 | FR better: 4; FR better (cost and SLO): 2; FR worse: 1; trade-off: 8 | FR better: 5; FR better (cost and SLO): 1; FR worse: 2; trade-off: 7 |

## fleetright_predictive

| scenario | static_peak_reserve | naive_threshold | hpa_like |
|---|---|---|---|
| S01 steady | -1% \| +12.7 \| trade-off | -21% \| +12.7 \| trade-off | -21% \| +12.7 \| trade-off |
| S02 gradual increase | -11% \| -30.4 \| FR better (cost and SLO) | -25% \| +20.3 \| trade-off | -19% \| +20.3 \| trade-off |
| S03 daily peaks (compressed 12-window day) | -13% \| +21.4 \| trade-off | -20% \| +11.3 \| trade-off | +4% \| +18.6 \| FR worse |
| S04 flash crowd 5x | -58% \| +10.1 \| trade-off | +2% \| +5.6 \| FR worse | +1% \| +10.1 \| FR worse |
| S05 CPU-intensive workload | +10% \| +3.5 \| FR worse | +83% \| -26.4 \| trade-off | +57% \| -26.4 \| trade-off |
| S06 memory-intensive workload | -3% \| +21.1 \| trade-off | -27% \| +21.1 \| trade-off | -16% \| +21.1 \| trade-off |
| S08 slow-starting replicas (150 s) | -2% \| +52.5 \| trade-off | -19% \| +17.8 \| trade-off | -0% \| -19.2 \| FR better |
| S09 multiple tenants, different SLOs | +8% \| +3.6 \| FR worse | +51% \| -68.8 \| trade-off | +30% \| -68.8 \| trade-off |
| S10 noisy-neighbour demand | -1% \| +12.0 \| FR worse | -15% \| +11.9 \| trade-off | -7% \| +11.8 \| trade-off |
| S11 correlated surges | -40% \| +6.4 \| trade-off | -31% \| +6.4 \| trade-off | -5% \| +12.4 \| trade-off |
| S12 forecast underprediction (+80% level shift) | -3% \| -13.3 \| FR better (cost and SLO) | -22% \| +29.4 \| trade-off | -9% \| +39.5 \| trade-off |
| S13 forecast overprediction (-50% level shift) | -21% \| +6.8 \| trade-off | +2% \| +3.3 \| FR worse | +28% \| +5.7 \| FR worse |
| S14 stale telemetry (1 of 3 windows missing) | -11% \| +34.5 \| trade-off | -11% \| +34.5 \| trade-off | +12% \| +19.1 \| FR worse |
| S15 capacity profile drift (-20% true capacity) | -13% \| +44.1 \| trade-off | -30% \| +45.6 \| trade-off | -14% \| +48.0 \| trade-off |
| S16 one node lost (1 replica per tenant) | -1% \| +12.7 \| trade-off | -21% \| +14.7 \| trade-off | -21% \| +14.7 \| trade-off |
| **tally** | FR better (cost and SLO): 2; FR worse: 3; trade-off: 10 | FR worse: 2; trade-off: 13 | FR better: 1; FR worse: 4; trade-off: 10 |

## fleetright_predictive

| scenario | fleetright_reactive | fleetright_predictive_same_headroom |
|---|---|---|
| S01 steady | -1% \| +12.7 \| trade-off | -1% \| +12.7 \| trade-off |
| S02 gradual increase | -11% \| +20.3 \| trade-off | -13% \| +20.3 \| trade-off |
| S03 daily peaks (compressed 12-window day) | +7% \| -3.6 \| trade-off | -14% \| +16.4 \| trade-off |
| S04 flash crowd 5x | +20% \| +10.1 \| FR worse | -1% \| +10.1 \| trade-off |
| S05 CPU-intensive workload | +25% \| -8.8 \| trade-off | -12% \| +1.6 \| trade-off |
| S06 memory-intensive workload | -10% \| +21.1 \| trade-off | -21% \| +21.1 \| trade-off |
| S08 slow-starting replicas (150 s) | +16% \| -62.5 \| trade-off | -8% \| +13.5 \| trade-off |
| S09 multiple tenants, different SLOs | +20% \| -48.0 \| trade-off | -13% \| +7.0 \| trade-off |
| S10 noisy-neighbour demand | -2% \| +12.0 \| trade-off | -9% \| +12.0 \| trade-off |
| S11 correlated surges | +14% \| +12.4 \| FR worse | -3% \| +12.4 \| trade-off |
| S12 forecast underprediction (+80% level shift) | +8% \| +15.8 \| FR worse | -13% \| +35.6 \| trade-off |
| S13 forecast overprediction (-50% level shift) | +2% \| -2.0 \| trade-off | -6% \| +5.5 \| trade-off |
| S14 stale telemetry (1 of 3 windows missing) | +6% \| -10.6 \| trade-off | -12% \| +18.2 \| trade-off |
| S15 capacity profile drift (-20% true capacity) | +7% \| -21.9 \| trade-off | -14% \| +30.7 \| trade-off |
| S16 one node lost (1 replica per tenant) | -1% \| +12.7 \| trade-off | -1% \| +12.7 \| trade-off |
| **tally** | FR worse: 3; trade-off: 12 | trade-off: 15 |

## fleetright_predictive_same_headroom

| scenario | fleetright_reactive |
|---|---|
| S01 steady | +0% \| +0.0 \| no significant difference |
| S02 gradual increase | +2% \| +0.0 \| FR worse |
| S03 daily peaks (compressed 12-window day) | +24% \| -20.1 \| trade-off |
| S04 flash crowd 5x | +21% \| +0.0 \| FR worse |
| S05 CPU-intensive workload | +42% \| -10.4 \| trade-off |
| S06 memory-intensive workload | +13% \| +0.0 \| FR worse |
| S08 slow-starting replicas (150 s) | +27% \| -76.0 \| trade-off |
| S09 multiple tenants, different SLOs | +38% \| -54.9 \| trade-off |
| S10 noisy-neighbour demand | +7% \| +0.0 \| FR worse |
| S11 correlated surges | +18% \| +0.0 \| FR worse |
| S12 forecast underprediction (+80% level shift) | +25% \| -19.9 \| trade-off |
| S13 forecast overprediction (-50% level shift) | +9% \| -7.5 \| trade-off |
| S14 stale telemetry (1 of 3 windows missing) | +22% \| -28.8 \| trade-off |
| S15 capacity profile drift (-20% true capacity) | +24% \| -52.5 \| trade-off |
| S16 one node lost (1 replica per tenant) | +0% \| +0.0 \| no significant difference |
| **tally** | FR worse: 5; no significant difference: 2; trade-off: 8 |

