# Paired policy comparison (SIMULATED)

Run `20261013T070105Z`, commit `93ae19557cf428bfa222d5f50b47b3a9c1e828ff`. Seeds 100-119 (n=20 paired traces per scenario). Costs are cost-model-v1 node-micros (synthetic prices, replicas packed on 'small' nodes); latency is the declared simulator's per-window p99, not measured traffic.

Tuned knobs (chosen on separate tuning seeds/load): fleetright_predictive=700000, fleetright_reactive=600000, hpa_like=500000, naive_threshold=500000, static_peak_reserve=50000

## S01 steady

| policy | node cost | soft viol. | hard viol. (seeds) | max p99 ms | scale acts | idle CPU |
|---|---:|---:|---:|---:|---:|---:|
| static_peak_reserve | 800,000 | 0.0 | 0.0 (0) | 101 | 0.0 | 0.25 |
| naive_threshold | 1,000,000 | 0.0 | 0.0 (0) | 77 | 0.0 | 0.28 |
| hpa_like | 1,000,000 | 0.0 | 0.0 (0) | 77 | 0.0 | 0.28 |
| fleetright_reactive | 800,000 | 0.0 | 0.0 (0) | 101 | 0.0 | 0.19 |
| fleetright_predictive | 789,500 | 12.7 | 0.0 (0) | 101 | 22.2 | 0.24 |
| fleetright_predictive_same_headroom | 800,000 | 0.0 | 0.0 (0) | 101 | 0.0 | 0.19 |
| offline_lookahead_oracle | 820,167 | 0.0 | 0.0 (0) | 100 | 86.8 | 0.25 |

| FleetRight vs | cost diff [95% CI] | rel. | SLO-violation windows diff [95% CI] | hard diff [95% CI] | verdict |
|---|---:|---:|---:|---:|---|
| FR-reactive vs static_peak_reserve | +0.0 [+0.0, +0.0] | +0.0% | +0.0 [+0.0, +0.0] | +0.0 [+0.0, +0.0] | no significant difference |
| FR-reactive vs naive_threshold | -200,000.0 [-200,000.0, -200,000.0]* | -20.0% | +0.0 [+0.0, +0.0] | +0.0 [+0.0, +0.0] | FR better |
| FR-reactive vs hpa_like | -200,000.0 [-200,000.0, -200,000.0]* | -20.0% | +0.0 [+0.0, +0.0] | +0.0 [+0.0, +0.0] | FR better |
| FR-reactive vs fleetright_predictive | +10,500.0 [+9,083.5, +12,000.0]* | +1.3% | -12.7 [-14.5, -11.0]* | +0.0 [+0.0, +0.0] | trade-off |
| FR-reactive vs fleetright_predictive_same_headroom | +0.0 [+0.0, +0.0] | +0.0% | +0.0 [+0.0, +0.0] | +0.0 [+0.0, +0.0] | no significant difference |
| FR-reactive vs offline_lookahead_oracle | -20,166.8 [-21,583.3, -18,791.8]* | -2.5% | +0.0 [+0.0, +0.0] | +0.0 [+0.0, +0.0] | FR better |
| FR-predictive vs static_peak_reserve | -10,500.0 [-11,958.4, -9,041.6]* | -1.3% | +12.7 [+11.0, +14.4]* | +0.0 [+0.0, +0.0] | trade-off |
| FR-predictive vs naive_threshold | -210,500.0 [-212,000.0, -209,083.3]* | -21.1% | +12.7 [+10.9, +14.4]* | +0.0 [+0.0, +0.0] | trade-off |
| FR-predictive vs hpa_like | -210,500.0 [-212,000.0, -209,083.3]* | -21.1% | +12.7 [+11.0, +14.4]* | +0.0 [+0.0, +0.0] | trade-off |
| FR-predictive vs fleetright_reactive | -10,500.0 [-11,958.4, -9,041.8]* | -1.3% | +12.7 [+11.1, +14.4]* | +0.0 [+0.0, +0.0] | trade-off |
| FR-predictive vs fleetright_predictive_same_headroom | -10,500.0 [-12,000.0, -9,041.7]* | -1.3% | +12.7 [+10.9, +14.4]* | +0.0 [+0.0, +0.0] | trade-off |
| FR-predictive vs offline_lookahead_oracle | -30,666.8 [-32,708.5, -28,708.4]* | -3.7% | +12.7 [+10.9, +14.4]* | +0.0 [+0.0, +0.0] | trade-off |
| FR-predictive_same_headroom vs fleetright_reactive | +0.0 [+0.0, +0.0] | +0.0% | +0.0 [+0.0, +0.0] | +0.0 [+0.0, +0.0] | no significant difference |

## S02 gradual increase

| policy | node cost | soft viol. | hard viol. (seeds) | max p99 ms | scale acts | idle CPU |
|---|---:|---:|---:|---:|---:|---:|
| static_peak_reserve | 1,000,000 | 50.7 | 0.0 (0) | 164 | 0.0 | 0.23 |
| naive_threshold | 1,174,625 | 0.0 | 0.0 (0) | 92 | 10.0 | 0.27 |
| hpa_like | 1,089,042 | 0.0 | 0.0 (0) | 101 | 9.8 | 0.27 |
| fleetright_reactive | 994,917 | 0.0 | 0.0 (0) | 123 | 41.1 | 0.21 |
| fleetright_predictive | 885,042 | 20.3 | 0.0 (0) | 167 | 25.0 | 0.21 |
| fleetright_predictive_same_headroom | 1,017,000 | 0.0 | 0.0 (0) | 118 | 24.9 | 0.22 |
| offline_lookahead_oracle | 1,080,792 | 0.0 | 0.0 (0) | 100 | 51.8 | 0.27 |

| FleetRight vs | cost diff [95% CI] | rel. | SLO-violation windows diff [95% CI] | hard diff [95% CI] | verdict |
|---|---:|---:|---:|---:|---|
| FR-reactive vs static_peak_reserve | -5,083.3 [-6,291.7, -3,874.9]* | -0.5% | -50.7 [-51.6, -49.8]* | +0.0 [+0.0, +0.0] | FR better (cost and SLO) |
| FR-reactive vs naive_threshold | -179,708.4 [-181,833.5, -177,583.5]* | -15.3% | +0.0 [+0.0, +0.0] | +0.0 [+0.0, +0.0] | FR better |
| FR-reactive vs hpa_like | -94,124.9 [-95,999.9, -92,333.3]* | -8.6% | +0.0 [+0.0, +0.0] | +0.0 [+0.0, +0.0] | FR better |
| FR-reactive vs fleetright_predictive | +109,875.1 [+107,416.9, +112,208.6]* | +12.4% | -20.3 [-22.8, -17.8]* | +0.0 [+0.0, +0.0] | trade-off |
| FR-reactive vs fleetright_predictive_same_headroom | -22,083.2 [-24,458.2, -19,666.6]* | -2.2% | +0.0 [+0.0, +0.0] | +0.0 [+0.0, +0.0] | FR better |
| FR-reactive vs offline_lookahead_oracle | -85,874.9 [-87,625.0, -84,125.0]* | -7.9% | +0.0 [+0.0, +0.0] | +0.0 [+0.0, +0.0] | FR better |
| FR-predictive vs static_peak_reserve | -114,958.4 [-117,041.8, -112,916.8]* | -11.5% | -30.4 [-33.1, -27.9]* | +0.0 [+0.0, +0.0] | FR better (cost and SLO) |
| FR-predictive vs naive_threshold | -289,583.5 [-292,333.5, -286,875.2]* | -24.7% | +20.3 [+17.8, +22.8]* | +0.0 [+0.0, +0.0] | trade-off |
| FR-predictive vs hpa_like | -204,000.1 [-206,250.0, -201,791.8]* | -18.7% | +20.3 [+17.8, +22.8]* | +0.0 [+0.0, +0.0] | trade-off |
| FR-predictive vs fleetright_reactive | -109,875.1 [-112,250.1, -107,500.1]* | -11.0% | +20.3 [+17.8, +22.6]* | +0.0 [+0.0, +0.0] | trade-off |
| FR-predictive vs fleetright_predictive_same_headroom | -131,958.4 [-134,499.9, -129,541.8]* | -13.0% | +20.3 [+17.7, +22.7]* | +0.0 [+0.0, +0.0] | trade-off |
| FR-predictive vs offline_lookahead_oracle | -195,750.1 [-197,583.5, -194,041.8]* | -18.1% | +20.3 [+17.8, +22.8]* | +0.0 [+0.0, +0.0] | trade-off |
| FR-predictive_same_headroom vs fleetright_reactive | +22,083.2 [+19,666.6, +24,541.5]* | +2.2% | +0.0 [+0.0, +0.0] | +0.0 [+0.0, +0.0] | FR worse |

## S03 daily peaks (compressed 12-window day)

| policy | node cost | soft viol. | hard viol. (seeds) | max p99 ms | scale acts | idle CPU |
|---|---:|---:|---:|---:|---:|---:|
| static_peak_reserve | 800,000 | 0.0 | 0.0 (0) | 100 | 0.0 | 0.25 |
| naive_threshold | 866,667 | 10.1 | 0.0 (0) | 160 | 191.9 | 0.28 |
| hpa_like | 669,625 | 2.9 | 0.0 (0) | 139 | 139.3 | 0.27 |
| fleetright_reactive | 650,000 | 25.1 | 0.0 (0) | 139 | 160.0 | 0.24 |
| fleetright_predictive | 694,292 | 21.4 | 0.0 (0) | 238 | 119.9 | 0.21 |
| fleetright_predictive_same_headroom | 806,042 | 5.0 | 0.0 (0) | 136 | 156.2 | 0.22 |
| offline_lookahead_oracle | 594,458 | 0.0 | 0.0 (0) | 100 | 168.8 | 0.21 |

| FleetRight vs | cost diff [95% CI] | rel. | SLO-violation windows diff [95% CI] | hard diff [95% CI] | verdict |
|---|---:|---:|---:|---:|---|
| FR-reactive vs static_peak_reserve | -150,000.0 [-150,000.0, -150,000.0]* | -18.8% | +25.1 [+23.9, +26.1]* | +0.0 [+0.0, +0.0] | trade-off |
| FR-reactive vs naive_threshold | -216,667.0 [-216,667.0, -216,667.0]* | -25.0% | +14.9 [+14.1, +15.8]* | +0.0 [+0.0, +0.0] | trade-off |
| FR-reactive vs hpa_like | -19,625.0 [-22,791.7, -15,999.9]* | -2.9% | +22.2 [+20.9, +23.5]* | +0.0 [+0.0, +0.0] | trade-off |
| FR-reactive vs fleetright_predictive | -44,291.8 [-46,208.4, -42,416.8]* | -6.4% | +3.6 [+1.5, +5.7]* | +0.0 [+0.0, +0.0] | trade-off |
| FR-reactive vs fleetright_predictive_same_headroom | -156,041.6 [-160,250.0, -151,875.0]* | -19.4% | +20.1 [+18.6, +21.4]* | +0.0 [+0.0, +0.0] | trade-off |
| FR-reactive vs offline_lookahead_oracle | +55,541.7 [+54,000.1, +56,916.8]* | +9.3% | +25.1 [+24.0, +26.1]* | +0.0 [+0.0, +0.0] | FR worse |
| FR-predictive vs static_peak_reserve | -105,708.2 [-107,624.9, -103,833.2]* | -13.2% | +21.4 [+19.4, +23.4]* | +0.0 [+0.0, +0.0] | trade-off |
| FR-predictive vs naive_threshold | -172,375.2 [-174,292.0, -170,417.0]* | -19.9% | +11.3 [+9.3, +13.3]* | +0.0 [+0.0, +0.0] | trade-off |
| FR-predictive vs hpa_like | +24,666.8 [+20,458.5, +29,416.8]* | +3.7% | +18.6 [+16.4, +20.8]* | +0.0 [+0.0, +0.0] | FR worse |
| FR-predictive vs fleetright_reactive | +44,291.8 [+42,416.8, +46,250.1]* | +6.8% | -3.6 [-5.7, -1.5]* | +0.0 [+0.0, +0.0] | trade-off |
| FR-predictive vs fleetright_predictive_same_headroom | -111,749.9 [-114,500.0, -108,916.6]* | -13.9% | +16.4 [+15.0, +17.9]* | +0.0 [+0.0, +0.0] | trade-off |
| FR-predictive vs offline_lookahead_oracle | +99,833.4 [+97,125.1, +102,500.1]* | +16.8% | +21.4 [+19.4, +23.4]* | +0.0 [+0.0, +0.0] | FR worse |
| FR-predictive_same_headroom vs fleetright_reactive | +156,041.6 [+151,833.3, +160,249.9]* | +24.0% | -20.1 [-21.4, -18.6]* | +0.0 [+0.0, +0.0] | trade-off |

## S04 flash crowd 5x

| policy | node cost | soft viol. | hard viol. (seeds) | max p99 ms | scale acts | idle CPU |
|---|---:|---:|---:|---:|---:|---:|
| static_peak_reserve | 2,400,000 | 3.0 | 0.0 (0) | 287 | 0.0 | 0.25 |
| naive_threshold | 1,000,000 | 0.0 | 7.5 (20) | 115,013 | 0.0 | 0.28 |
| hpa_like | 1,005,833 | 0.0 | 3.0 (20) | 115,013 | 9.0 | 0.27 |
| fleetright_reactive | 850,833 | 0.0 | 3.0 (20) | 145,854 | 25.0 | 0.18 |
| fleetright_predictive | 1,019,958 | 10.1 | 3.0 (20) | 145,854 | 67.8 | 0.23 |
| fleetright_predictive_same_headroom | 1,028,333 | 0.0 | 3.0 (20) | 145,854 | 50.0 | 0.18 |
| offline_lookahead_oracle | 820,167 | 1.0 | 0.0 (0) | 287 | 89.5 | 0.25 |

| FleetRight vs | cost diff [95% CI] | rel. | SLO-violation windows diff [95% CI] | hard diff [95% CI] | verdict |
|---|---:|---:|---:|---:|---|
| FR-reactive vs static_peak_reserve | -1,549,167.0 [-1,549,167.0, -1,549,167.0]* | -64.5% | +0.0 [+0.0, +0.0] | +3.0 [+3.0, +3.0]* | trade-off (hard SLO worse) |
| FR-reactive vs naive_threshold | -149,167.0 [-149,167.0, -149,167.0]* | -14.9% | -4.5 [-4.7, -4.2]* | -4.5 [-4.7, -4.2]* | FR better (cost and SLO) |
| FR-reactive vs hpa_like | -155,000.0 [-155,000.0, -155,000.0]* | -15.4% | +0.0 [+0.0, +0.0] | +0.0 [+0.0, +0.0] | FR better |
| FR-reactive vs fleetright_predictive | -169,125.3 [-170,375.4, -167,833.6]* | -16.6% | -10.1 [-11.7, -8.6]* | +0.0 [+0.0, +0.0] | FR better (cost and SLO) |
| FR-reactive vs fleetright_predictive_same_headroom | -177,500.0 [-177,500.0, -177,500.0]* | -17.3% | +0.0 [+0.0, +0.0] | +0.0 [+0.0, +0.0] | FR better |
| FR-reactive vs offline_lookahead_oracle | +30,666.2 [+29,249.6, +32,041.2]* | +3.7% | +2.0 [+2.0, +2.0]* | +3.0 [+3.0, +3.0]* | FR worse (hard SLO worse) |
| FR-predictive vs static_peak_reserve | -1,380,041.7 [-1,381,333.5, -1,378,833.3]* | -57.5% | +10.1 [+8.6, +11.6]* | +3.0 [+3.0, +3.0]* | trade-off (hard SLO worse) |
| FR-predictive vs naive_threshold | +19,958.3 [+18,625.0, +21,166.7]* | +2.0% | +5.6 [+4.2, +7.1]* | -4.5 [-4.7, -4.2]* | trade-off |
| FR-predictive vs hpa_like | +14,125.3 [+12,833.6, +15,375.2]* | +1.4% | +10.1 [+8.6, +11.6]* | +0.0 [+0.0, +0.0] | FR worse |
| FR-predictive vs fleetright_reactive | +169,125.3 [+167,833.6, +170,333.6]* | +19.9% | +10.1 [+8.6, +11.7]* | +0.0 [+0.0, +0.0] | FR worse |
| FR-predictive vs fleetright_predictive_same_headroom | -8,374.7 [-9,708.0, -7,166.3]* | -0.8% | +10.1 [+8.6, +11.7]* | +0.0 [+0.0, +0.0] | trade-off |
| FR-predictive vs offline_lookahead_oracle | +199,791.5 [+197,916.6, +201,583.2]* | +24.4% | +12.1 [+10.6, +13.6]* | +3.0 [+3.0, +3.0]* | FR worse (hard SLO worse) |
| FR-predictive_same_headroom vs fleetright_reactive | +177,500.0 [+177,500.0, +177,500.0]* | +20.9% | +0.0 [+0.0, +0.0] | +0.0 [+0.0, +0.0] | FR worse |

## S05 CPU-intensive workload

| policy | node cost | soft viol. | hard viol. (seeds) | max p99 ms | scale acts | idle CPU |
|---|---:|---:|---:|---:|---:|---:|
| static_peak_reserve | 1,000,000 | 0.0 | 0.0 (0) | 104 | 0.0 | 0.75 |
| naive_threshold | 600,000 | 24.1 | 5.8 (20) | 24,583 | 0.0 | 0.75 |
| hpa_like | 698,958 | 24.1 | 5.8 (20) | 24,583 | 98.2 | 0.75 |
| fleetright_reactive | 878,542 | 12.3 | 0.0 (0) | 205 | 98.5 | 0.75 |
| fleetright_predictive | 1,098,917 | 2.9 | 0.6 (9) | 2,333 | 55.6 | 0.75 |
| fleetright_predictive_same_headroom | 1,247,708 | 1.9 | 0.0 (0) | 162 | 59.1 | 0.75 |
| offline_lookahead_oracle | 663,333 | 0.0 | 0.0 (0) | 81 | 79.6 | 0.75 |

| FleetRight vs | cost diff [95% CI] | rel. | SLO-violation windows diff [95% CI] | hard diff [95% CI] | verdict |
|---|---:|---:|---:|---:|---|
| FR-reactive vs static_peak_reserve | -121,458.4 [-125,791.6, -116,916.6]* | -12.1% | +12.3 [+11.2, +13.4]* | +0.0 [+0.0, +0.0] | trade-off |
| FR-reactive vs naive_threshold | +278,541.7 [+274,291.6, +282,958.3]* | +46.4% | -17.6 [-19.1, -16.1]* | -5.8 [-6.7, -5.0]* | trade-off |
| FR-reactive vs hpa_like | +179,583.2 [+177,916.4, +181,333.2]* | +25.7% | -17.6 [-19.1, -16.1]* | -5.8 [-6.7, -5.0]* | trade-off |
| FR-reactive vs fleetright_predictive | -220,375.0 [-236,375.0, -204,875.0]* | -20.1% | +8.8 [+7.7, +10.2]* | -0.6 [-0.9, -0.3]* | FR better (cost and SLO) |
| FR-reactive vs fleetright_predictive_same_headroom | -369,166.7 [-387,333.3, -351,666.5]* | -29.6% | +10.4 [+9.3, +11.4]* | +0.0 [+0.0, +0.0] | trade-off |
| FR-reactive vs offline_lookahead_oracle | +215,208.3 [+212,208.2, +218,208.4]* | +32.4% | +12.3 [+11.2, +13.4]* | +0.0 [+0.0, +0.0] | FR worse |
| FR-predictive vs static_peak_reserve | +98,916.7 [+83,625.1, +114,833.3]* | +9.9% | +3.5 [+2.6, +4.4]* | +0.6 [+0.3, +0.9]* | FR worse (hard SLO worse) |
| FR-predictive vs naive_threshold | +498,916.7 [+484,125.0, +514,666.7]* | +83.2% | -26.4 [-28.1, -24.8]* | -5.2 [-6.0, -4.5]* | trade-off |
| FR-predictive vs hpa_like | +399,958.3 [+384,625.0, +416,083.3]* | +57.2% | -26.4 [-28.1, -24.8]* | -5.2 [-6.0, -4.5]* | trade-off |
| FR-predictive vs fleetright_reactive | +220,375.0 [+204,833.4, +237,000.0]* | +25.1% | -8.8 [-10.2, -7.7]* | +0.6 [+0.3, +0.9]* | FR worse (hard SLO worse) |
| FR-predictive vs fleetright_predictive_same_headroom | -148,791.6 [-153,874.9, -143,208.1]* | -11.9% | +1.6 [+1.1, +2.0]* | +0.6 [+0.3, +0.9]* | trade-off (hard SLO worse) |
| FR-predictive vs offline_lookahead_oracle | +435,583.3 [+420,500.0, +451,499.9]* | +65.7% | +3.5 [+2.5, +4.4]* | +0.6 [+0.3, +0.9]* | FR worse (hard SLO worse) |
| FR-predictive_same_headroom vs fleetright_reactive | +369,166.7 [+350,958.5, +387,208.2]* | +42.0% | -10.4 [-11.4, -9.4]* | +0.0 [+0.0, +0.0] | trade-off |

## S06 memory-intensive workload

| policy | node cost | soft viol. | hard viol. (seeds) | max p99 ms | scale acts | idle CPU |
|---|---:|---:|---:|---:|---:|---:|
| static_peak_reserve | 600,000 | 0.0 | 0.0 (0) | 125 | 0.0 | 0.75 |
| naive_threshold | 800,000 | 0.0 | 0.0 (0) | 85 | 0.0 | 0.75 |
| hpa_like | 695,292 | 0.0 | 0.0 (0) | 123 | 120.0 | 0.75 |
| fleetright_reactive | 650,000 | 0.0 | 0.0 (0) | 125 | 120.0 | 0.75 |
| fleetright_predictive | 582,417 | 21.1 | 0.0 (0) | 234 | 42.2 | 0.75 |
| fleetright_predictive_same_headroom | 734,958 | 0.0 | 0.0 (0) | 125 | 53.1 | 0.75 |
| offline_lookahead_oracle | 696,125 | 0.0 | 0.0 (0) | 100 | 119.0 | 0.75 |

| FleetRight vs | cost diff [95% CI] | rel. | SLO-violation windows diff [95% CI] | hard diff [95% CI] | verdict |
|---|---:|---:|---:|---:|---|
| FR-reactive vs static_peak_reserve | +50,000.0 [+50,000.0, +50,000.0]* | +8.3% | +0.0 [+0.0, +0.0] | +0.0 [+0.0, +0.0] | FR worse |
| FR-reactive vs naive_threshold | -150,000.0 [-150,000.0, -150,000.0]* | -18.8% | +0.0 [+0.0, +0.0] | +0.0 [+0.0, +0.0] | FR better |
| FR-reactive vs hpa_like | -45,291.7 [-45,916.8, -44,624.9]* | -6.5% | +0.0 [+0.0, +0.0] | +0.0 [+0.0, +0.0] | FR better |
| FR-reactive vs fleetright_predictive | +67,583.4 [+65,708.3, +69,541.6]* | +11.6% | -21.1 [-23.4, -18.9]* | +0.0 [+0.0, +0.0] | trade-off |
| FR-reactive vs fleetright_predictive_same_headroom | -84,958.4 [-90,500.0, -79,250.0]* | -11.6% | +0.0 [+0.0, +0.0] | +0.0 [+0.0, +0.0] | FR better |
| FR-reactive vs offline_lookahead_oracle | -46,124.9 [-46,749.9, -45,458.2]* | -6.6% | +0.0 [+0.0, +0.0] | +0.0 [+0.0, +0.0] | FR better |
| FR-predictive vs static_peak_reserve | -17,583.3 [-19,500.1, -15,625.0]* | -2.9% | +21.1 [+18.8, +23.4]* | +0.0 [+0.0, +0.0] | trade-off |
| FR-predictive vs naive_threshold | -217,583.4 [-219,500.0, -215,666.8]* | -27.2% | +21.1 [+18.9, +23.4]* | +0.0 [+0.0, +0.0] | trade-off |
| FR-predictive vs hpa_like | -112,875.1 [-114,833.4, -110,916.6]* | -16.2% | +21.1 [+18.9, +23.5]* | +0.0 [+0.0, +0.0] | trade-off |
| FR-predictive vs fleetright_reactive | -67,583.4 [-69,541.6, -65,666.6]* | -10.4% | +21.1 [+18.9, +23.4]* | +0.0 [+0.0, +0.0] | trade-off |
| FR-predictive vs fleetright_predictive_same_headroom | -152,541.7 [-156,458.6, -148,458.4]* | -20.8% | +21.1 [+18.8, +23.4]* | +0.0 [+0.0, +0.0] | trade-off |
| FR-predictive vs offline_lookahead_oracle | -113,708.2 [-115,624.8, -111,708.1]* | -16.3% | +21.1 [+18.8, +23.4]* | +0.0 [+0.0, +0.0] | trade-off |
| FR-predictive_same_headroom vs fleetright_reactive | +84,958.4 [+79,249.9, +90,583.4]* | +13.1% | +0.0 [+0.0, +0.0] | +0.0 [+0.0, +0.0] | FR worse |

## S08 slow-starting replicas (150 s)

| policy | node cost | soft viol. | hard viol. (seeds) | max p99 ms | scale acts | idle CPU |
|---|---:|---:|---:|---:|---:|---:|
| static_peak_reserve | 800,000 | 0.0 | 0.0 (0) | 100 | 0.0 | 0.25 |
| naive_threshold | 964,250 | 34.7 | 0.0 (0) | 295 | 213.1 | 0.25 |
| hpa_like | 784,583 | 71.1 | 0.6 (9) | 566 | 197.8 | 0.29 |
| fleetright_reactive | 677,083 | 99.5 | 15.4 (20) | 15,707 | 160.0 | 0.26 |
| fleetright_predictive | 784,042 | 44.4 | 8.1 (20) | 10,664 | 157.2 | 0.24 |
| fleetright_predictive_same_headroom | 856,708 | 34.0 | 4.9 (20) | 10,252 | 119.0 | 0.25 |
| offline_lookahead_oracle | 594,125 | 55.9 | 0.0 (0) | 192 | 168.4 | 0.21 |

| FleetRight vs | cost diff [95% CI] | rel. | SLO-violation windows diff [95% CI] | hard diff [95% CI] | verdict |
|---|---:|---:|---:|---:|---|
| FR-reactive vs static_peak_reserve | -122,916.7 [-124,083.4, -121,750.1]* | -15.4% | +115.0 [+114.2, +115.7]* | +15.4 [+14.7, +16.1]* | trade-off (hard SLO worse) |
| FR-reactive vs naive_threshold | -287,166.7 [-292,250.2, -282,125.0]* | -29.8% | +80.2 [+79.3, +81.2]* | +15.4 [+14.7, +16.1]* | trade-off (hard SLO worse) |
| FR-reactive vs hpa_like | -107,500.1 [-109,166.8, -105,791.6]* | -13.7% | +43.2 [+42.0, +44.5]* | +14.8 [+14.1, +15.7]* | trade-off (hard SLO worse) |
| FR-reactive vs fleetright_predictive | -106,958.4 [-112,250.1, -101,708.3]* | -13.6% | +62.5 [+58.4, +66.0]* | +7.3 [+6.3, +8.3]* | trade-off (hard SLO worse) |
| FR-reactive vs fleetright_predictive_same_headroom | -179,625.0 [-185,791.6, -173,333.4]* | -21.0% | +76.0 [+72.0, +79.3]* | +10.6 [+9.6, +11.4]* | trade-off (hard SLO worse) |
| FR-reactive vs offline_lookahead_oracle | +82,958.4 [+80,958.4, +84,958.3]* | +14.0% | +59.1 [+58.0, +60.1]* | +15.4 [+14.8, +16.1]* | FR worse (hard SLO worse) |
| FR-predictive vs static_peak_reserve | -15,958.4 [-21,625.0, -10,708.3]* | -2.0% | +52.5 [+48.9, +56.6]* | +8.1 [+7.0, +9.1]* | trade-off (hard SLO worse) |
| FR-predictive vs naive_threshold | -180,208.3 [-188,208.2, -172,083.3]* | -18.7% | +17.8 [+13.9, +22.1]* | +8.1 [+7.0, +9.1]* | trade-off (hard SLO worse) |
| FR-predictive vs hpa_like | -541.7 [-6,000.1, +4,916.6] | -0.1% | -19.2 [-22.7, -15.3]* | +7.5 [+6.3, +8.7]* | FR worse (hard SLO worse) |
| FR-predictive vs fleetright_reactive | +106,958.4 [+101,416.8, +112,208.4]* | +15.8% | -62.5 [-66.1, -58.2]* | -7.3 [-8.3, -6.3]* | trade-off |
| FR-predictive vs fleetright_predictive_same_headroom | -72,666.6 [-75,499.9, -69,749.9]* | -8.5% | +13.5 [+12.2, +14.8]* | +3.2 [+2.5, +4.0]* | trade-off (hard SLO worse) |
| FR-predictive vs offline_lookahead_oracle | +189,916.7 [+184,333.5, +195,333.3]* | +32.0% | -3.4 [-7.2, +1.1] | +8.1 [+7.0, +9.1]* | FR worse (hard SLO worse) |
| FR-predictive_same_headroom vs fleetright_reactive | +179,625.0 [+173,249.9, +185,833.4]* | +26.5% | -76.0 [-79.3, -72.1]* | -10.6 [-11.4, -9.6]* | trade-off |

## S09 multiple tenants, different SLOs

| policy | node cost | soft viol. | hard viol. (seeds) | max p99 ms | scale acts | idle CPU |
|---|---:|---:|---:|---:|---:|---:|
| static_peak_reserve | 1,400,000 | 12.8 | 0.0 (0) | 164 | 0.0 | 0.24 |
| naive_threshold | 1,000,000 | 73.0 | 12.2 (20) | 64,304 | 0.0 | 0.28 |
| hpa_like | 1,168,042 | 73.0 | 12.2 (20) | 64,304 | 298.9 | 0.28 |
| fleetright_reactive | 1,262,083 | 45.6 | 18.8 (20) | 34,855 | 313.6 | 0.28 |
| fleetright_predictive | 1,514,083 | 14.5 | 1.9 (18) | 14,767 | 188.1 | 0.26 |
| fleetright_predictive_same_headroom | 1,743,042 | 8.3 | 1.2 (17) | 14,424 | 225.2 | 0.27 |
| offline_lookahead_oracle | 966,000 | 0.0 | 0.0 (0) | 100 | 331.4 | 0.26 |

| FleetRight vs | cost diff [95% CI] | rel. | SLO-violation windows diff [95% CI] | hard diff [95% CI] | verdict |
|---|---:|---:|---:|---:|---|
| FR-reactive vs static_peak_reserve | -137,916.6 [-156,833.4, -119,375.0]* | -9.9% | +51.6 [+48.1, +55.0]* | +18.8 [+17.9, +19.8]* | trade-off (hard SLO worse) |
| FR-reactive vs naive_threshold | +262,083.4 [+243,625.0, +280,791.7]* | +26.2% | -20.8 [-24.1, -17.8]* | +6.6 [+5.5, +7.8]* | FR worse (hard SLO worse) |
| FR-reactive vs hpa_like | +94,041.8 [+80,458.4, +107,541.8]* | +8.1% | -20.8 [-24.1, -17.8]* | +6.6 [+5.5, +7.8]* | FR worse (hard SLO worse) |
| FR-reactive vs fleetright_predictive | -252,000.0 [-266,208.3, -237,916.7]* | -16.6% | +48.0 [+45.9, +50.0]* | +16.9 [+15.9, +17.9]* | trade-off (hard SLO worse) |
| FR-reactive vs fleetright_predictive_same_headroom | -480,958.2 [-495,666.5, -465,916.7]* | -27.6% | +54.9 [+53.2, +56.7]* | +17.6 [+16.7, +18.6]* | trade-off (hard SLO worse) |
| FR-reactive vs offline_lookahead_oracle | +296,083.3 [+280,291.8, +312,041.6]* | +30.7% | +64.4 [+62.9, +66.0]* | +18.8 [+17.9, +19.8]* | FR worse (hard SLO worse) |
| FR-predictive vs static_peak_reserve | +114,083.4 [+94,708.4, +132,583.5]* | +8.1% | +3.6 [+0.1, +6.8]* | +1.9 [+1.4, +2.5]* | FR worse (hard SLO worse) |
| FR-predictive vs naive_threshold | +514,083.3 [+494,791.8, +532,750.1]* | +51.4% | -68.8 [-72.1, -65.3]* | -10.2 [-11.3, -9.2]* | trade-off |
| FR-predictive vs hpa_like | +346,041.8 [+329,666.8, +361,583.3]* | +29.6% | -68.8 [-72.0, -65.3]* | -10.2 [-11.3, -9.2]* | trade-off |
| FR-predictive vs fleetright_reactive | +252,000.0 [+237,833.4, +265,583.3]* | +20.0% | -48.0 [-50.0, -45.9]* | -16.9 [-17.9, -15.9]* | trade-off |
| FR-predictive vs fleetright_predictive_same_headroom | -228,958.2 [-233,374.9, -224,583.1]* | -13.1% | +7.0 [+6.0, +7.9]* | +0.8 [+0.4, +1.1]* | trade-off (hard SLO worse) |
| FR-predictive vs offline_lookahead_oracle | +548,083.3 [+530,874.9, +564,958.3]* | +56.7% | +16.4 [+15.1, +17.9]* | +1.9 [+1.4, +2.5]* | FR worse (hard SLO worse) |
| FR-predictive_same_headroom vs fleetright_reactive | +480,958.2 [+465,708.3, +495,624.8]* | +38.1% | -54.9 [-56.7, -53.2]* | -17.6 [-18.6, -16.7]* | trade-off |

## S10 noisy-neighbour demand

| policy | node cost | soft viol. | hard viol. (seeds) | max p99 ms | scale acts | idle CPU |
|---|---:|---:|---:|---:|---:|---:|
| static_peak_reserve | 990,000 | 0.0 | 0.0 (0) | 107 | 0.0 | 0.29 |
| naive_threshold | 1,146,458 | 0.1 | 0.0 (0) | 102 | 14.9 | 0.27 |
| hpa_like | 1,047,667 | 0.2 | 0.0 (0) | 108 | 35.0 | 0.25 |
| fleetright_reactive | 997,458 | 0.0 | 0.0 (0) | 103 | 61.1 | 0.29 |
| fleetright_predictive | 975,208 | 12.0 | 0.0 (0) | 104 | 56.4 | 0.29 |
| fleetright_predictive_same_headroom | 1,066,500 | 0.0 | 0.0 (0) | 103 | 44.1 | 0.31 |
| offline_lookahead_oracle | 934,833 | 0.0 | 0.0 (0) | 100 | 176.1 | 0.28 |

| FleetRight vs | cost diff [95% CI] | rel. | SLO-violation windows diff [95% CI] | hard diff [95% CI] | verdict |
|---|---:|---:|---:|---:|---|
| FR-reactive vs static_peak_reserve | +7,458.2 [-37,625.1, +56,499.8] | +0.8% | +0.0 [+0.0, +0.0] | +0.0 [+0.0, +0.0] | no significant difference |
| FR-reactive vs naive_threshold | -149,000.0 [-160,958.1, -136,916.8]* | -13.0% | -0.1 [-0.2, +0.0] | +0.0 [+0.0, +0.0] | FR better |
| FR-reactive vs hpa_like | -50,208.4 [-80,916.7, -18,041.7]* | -4.8% | -0.2 [-0.5, -0.1]* | +0.0 [+0.0, +0.0] | FR better (cost and SLO) |
| FR-reactive vs fleetright_predictive | +22,249.8 [+14,208.2, +30,791.3]* | +2.3% | -12.0 [-13.5, -10.4]* | +0.0 [+0.0, +0.0] | trade-off |
| FR-reactive vs fleetright_predictive_same_headroom | -69,041.7 [-79,624.9, -57,500.1]* | -6.5% | +0.0 [+0.0, +0.0] | +0.0 [+0.0, +0.0] | FR better |
| FR-reactive vs offline_lookahead_oracle | +62,624.9 [+46,458.2, +79,458.1]* | +6.7% | +0.0 [+0.0, +0.0] | +0.0 [+0.0, +0.0] | FR worse |
| FR-predictive vs static_peak_reserve | -14,791.6 [-54,708.3, +28,333.5] | -1.5% | +12.0 [+10.5, +13.4]* | +0.0 [+0.0, +0.0] | FR worse |
| FR-predictive vs naive_threshold | -171,249.9 [-183,583.3, -158,083.2]* | -14.9% | +11.9 [+10.4, +13.3]* | +0.0 [+0.0, +0.0] | trade-off |
| FR-predictive vs hpa_like | -72,458.2 [-98,166.5, -45,124.9]* | -6.9% | +11.8 [+10.2, +13.2]* | +0.0 [+0.0, +0.0] | trade-off |
| FR-predictive vs fleetright_reactive | -22,249.8 [-30,666.5, -14,208.1]* | -2.2% | +12.0 [+10.4, +13.4]* | +0.0 [+0.0, +0.0] | trade-off |
| FR-predictive vs fleetright_predictive_same_headroom | -91,291.6 [-104,333.2, -77,500.1]* | -8.6% | +12.0 [+10.5, +13.4]* | +0.0 [+0.0, +0.0] | trade-off |
| FR-predictive vs offline_lookahead_oracle | +40,375.1 [+28,041.7, +52,291.8]* | +4.3% | +12.0 [+10.5, +13.5]* | +0.0 [+0.0, +0.0] | FR worse |
| FR-predictive_same_headroom vs fleetright_reactive | +69,041.7 [+57,250.1, +79,875.1]* | +6.9% | +0.0 [+0.0, +0.0] | +0.0 [+0.0, +0.0] | FR worse |

## S11 correlated surges

| policy | node cost | soft viol. | hard viol. (seeds) | max p99 ms | scale acts | idle CPU |
|---|---:|---:|---:|---:|---:|---:|
| static_peak_reserve | 1,600,000 | 9.0 | 0.0 (0) | 267 | 0.0 | 0.28 |
| naive_threshold | 1,397,500 | 0.0 | 9.0 (20) | 48,934 | 8.0 | 0.30 |
| hpa_like | 1,020,000 | 0.0 | 3.0 (20) | 25,112 | 14.9 | 0.27 |
| fleetright_reactive | 845,833 | 1.0 | 2.0 (20) | 53,016 | 19.1 | 0.19 |
| fleetright_predictive | 964,917 | 12.4 | 3.0 (20) | 53,016 | 53.6 | 0.25 |
| fleetright_predictive_same_headroom | 998,333 | 1.0 | 2.0 (20) | 53,016 | 39.7 | 0.20 |
| offline_lookahead_oracle | 833,292 | 0.0 | 0.0 (0) | 100 | 89.8 | 0.25 |

| FleetRight vs | cost diff [95% CI] | rel. | SLO-violation windows diff [95% CI] | hard diff [95% CI] | verdict |
|---|---:|---:|---:|---:|---|
| FR-reactive vs static_peak_reserve | -754,167.0 [-754,167.0, -754,167.0]* | -47.1% | -6.0 [-6.0, -6.0]* | +2.0 [+2.0, +2.0]* | trade-off (hard SLO worse) |
| FR-reactive vs naive_threshold | -551,667.0 [-551,667.0, -551,667.0]* | -39.5% | -6.0 [-6.0, -6.0]* | -7.0 [-7.0, -7.0]* | FR better (cost and SLO) |
| FR-reactive vs hpa_like | -174,167.0 [-174,167.0, -174,167.0]* | -17.1% | +0.0 [+0.0, +0.0] | -1.0 [-1.0, -1.0]* | FR better (cost and SLO) |
| FR-reactive vs fleetright_predictive | -119,083.6 [-120,375.2, -117,625.2]* | -12.3% | -12.4 [-14.2, -10.8]* | -1.0 [-1.0, -1.0]* | FR better (cost and SLO) |
| FR-reactive vs fleetright_predictive_same_headroom | -152,500.0 [-152,500.0, -152,500.0]* | -15.3% | +0.0 [+0.0, +0.0] | +0.0 [+0.0, +0.0] | FR better |
| FR-reactive vs offline_lookahead_oracle | +12,541.4 [+11,124.8, +13,916.4]* | +1.5% | +3.0 [+3.0, +3.0]* | +2.0 [+2.0, +2.0]* | FR worse (hard SLO worse) |
| FR-predictive vs static_peak_reserve | -635,083.4 [-636,541.8, -633,791.7]* | -39.7% | +6.4 [+4.8, +8.2]* | +3.0 [+3.0, +3.0]* | trade-off (hard SLO worse) |
| FR-predictive vs naive_threshold | -432,583.4 [-434,041.7, -431,291.8]* | -31.0% | +6.4 [+4.8, +8.2]* | -6.0 [-6.0, -6.0]* | FR better (cost and SLO) |
| FR-predictive vs hpa_like | -55,083.4 [-56,541.8, -53,791.7]* | -5.4% | +12.4 [+10.8, +14.2]* | +0.0 [+0.0, +0.0] | trade-off |
| FR-predictive vs fleetright_reactive | +119,083.6 [+117,583.6, +120,333.7]* | +14.1% | +12.4 [+10.8, +14.2]* | +1.0 [+1.0, +1.0]* | FR worse (hard SLO worse) |
| FR-predictive vs fleetright_predictive_same_headroom | -33,416.4 [-34,916.4, -32,124.8]* | -3.3% | +12.4 [+10.8, +14.2]* | +1.0 [+1.0, +1.0]* | trade-off (hard SLO worse) |
| FR-predictive vs offline_lookahead_oracle | +131,625.0 [+129,791.5, +133,458.4]* | +15.8% | +15.4 [+13.8, +17.2]* | +3.0 [+3.0, +3.0]* | FR worse (hard SLO worse) |
| FR-predictive_same_headroom vs fleetright_reactive | +152,500.0 [+152,500.0, +152,500.0]* | +18.0% | +0.0 [+0.0, +0.0] | +0.0 [+0.0, +0.0] | FR worse |

## S12 forecast underprediction (+80% level shift)

| policy | node cost | soft viol. | hard viol. (seeds) | max p99 ms | scale acts | idle CPU |
|---|---:|---:|---:|---:|---:|---:|
| static_peak_reserve | 1,000,000 | 56.1 | 0.0 (0) | 190 | 0.0 | 0.23 |
| naive_threshold | 1,248,417 | 13.4 | 0.0 (0) | 174 | 199.1 | 0.28 |
| hpa_like | 1,058,750 | 3.3 | 0.0 (0) | 177 | 245.1 | 0.27 |
| fleetright_reactive | 893,917 | 27.1 | 0.0 (0) | 208 | 284.0 | 0.24 |
| fleetright_predictive | 967,750 | 42.9 | 0.0 (0) | 1,534 | 195.3 | 0.23 |
| fleetright_predictive_same_headroom | 1,114,792 | 7.2 | 0.0 (0) | 187 | 213.3 | 0.24 |
| offline_lookahead_oracle | 871,625 | 0.0 | 0.0 (0) | 100 | 364.4 | 0.25 |

| FleetRight vs | cost diff [95% CI] | rel. | SLO-violation windows diff [95% CI] | hard diff [95% CI] | verdict |
|---|---:|---:|---:|---:|---|
| FR-reactive vs static_peak_reserve | -106,083.4 [-107,583.4, -104,791.7]* | -10.6% | -29.1 [-30.2, -28.0]* | +0.0 [+0.0, +0.0] | FR better (cost and SLO) |
| FR-reactive vs naive_threshold | -354,500.0 [-357,166.7, -351,875.0]* | -28.4% | +13.6 [+12.1, +15.1]* | +0.0 [+0.0, +0.0] | trade-off |
| FR-reactive vs hpa_like | -164,833.5 [-168,125.4, -161,542.0]* | -15.6% | +23.8 [+22.1, +25.4]* | +0.0 [+0.0, +0.0] | trade-off |
| FR-reactive vs fleetright_predictive | -73,833.3 [-79,666.6, -68,083.4]* | -7.6% | -15.8 [-18.5, -13.2]* | +0.0 [+0.0, +0.0] | FR better (cost and SLO) |
| FR-reactive vs fleetright_predictive_same_headroom | -220,875.0 [-228,000.0, -213,541.8]* | -19.8% | +19.9 [+18.2, +21.4]* | +0.0 [+0.0, +0.0] | trade-off |
| FR-reactive vs offline_lookahead_oracle | +22,291.6 [+20,333.3, +24,166.5]* | +2.6% | +27.1 [+25.4, +28.7]* | +0.0 [+0.0, +0.0] | FR worse |
| FR-predictive vs static_peak_reserve | -32,250.1 [-38,416.8, -26,458.4]* | -3.2% | -13.3 [-16.0, -10.8]* | +0.0 [+0.0, +0.0] | FR better (cost and SLO) |
| FR-predictive vs naive_threshold | -280,666.7 [-286,000.0, -275,250.1]* | -22.5% | +29.4 [+26.9, +31.8]* | +0.0 [+0.0, +0.0] | trade-off |
| FR-predictive vs hpa_like | -91,000.2 [-97,083.4, -85,208.4]* | -8.6% | +39.5 [+37.1, +42.1]* | +0.0 [+0.0, +0.0] | trade-off |
| FR-predictive vs fleetright_reactive | +73,833.3 [+67,916.7, +79,624.9]* | +8.3% | +15.8 [+13.2, +18.4]* | +0.0 [+0.0, +0.0] | FR worse |
| FR-predictive vs fleetright_predictive_same_headroom | -147,041.7 [-149,333.4, -144,583.3]* | -13.2% | +35.6 [+33.6, +37.9]* | +0.0 [+0.0, +0.0] | trade-off |
| FR-predictive vs offline_lookahead_oracle | +96,124.9 [+90,249.9, +101,874.7]* | +11.0% | +42.9 [+40.4, +45.5]* | +0.0 [+0.0, +0.0] | FR worse |
| FR-predictive_same_headroom vs fleetright_reactive | +220,875.0 [+213,375.1, +228,125.0]* | +24.7% | -19.9 [-21.4, -18.3]* | +0.0 [+0.0, +0.0] | trade-off |

## S13 forecast overprediction (-50% level shift)

| policy | node cost | soft viol. | hard viol. (seeds) | max p99 ms | scale acts | idle CPU |
|---|---:|---:|---:|---:|---:|---:|
| static_peak_reserve | 800,000 | 0.0 | 0.0 (0) | 99 | 0.0 | 0.25 |
| naive_threshold | 618,833 | 3.5 | 0.0 (0) | 150 | 93.7 | 0.22 |
| hpa_like | 493,625 | 1.1 | 0.0 (0) | 137 | 53.2 | 0.18 |
| fleetright_reactive | 617,500 | 8.8 | 0.0 (0) | 137 | 56.0 | 0.30 |
| fleetright_predictive | 632,375 | 6.8 | 0.0 (0) | 215 | 50.9 | 0.28 |
| fleetright_predictive_same_headroom | 672,000 | 1.4 | 0.0 (0) | 125 | 77.0 | 0.26 |
| offline_lookahead_oracle | 468,167 | 0.0 | 0.0 (0) | 99 | 61.3 | 0.16 |

| FleetRight vs | cost diff [95% CI] | rel. | SLO-violation windows diff [95% CI] | hard diff [95% CI] | verdict |
|---|---:|---:|---:|---:|---|
| FR-reactive vs static_peak_reserve | -182,500.0 [-182,500.0, -182,500.0]* | -22.8% | +8.8 [+8.1, +9.6]* | +0.0 [+0.0, +0.0] | trade-off |
| FR-reactive vs naive_threshold | -1,333.0 [-4,833.0, +2,417.0] | -0.2% | +5.3 [+4.8, +5.9]* | +0.0 [+0.0, +0.0] | FR worse |
| FR-reactive vs hpa_like | +123,874.9 [+122,124.9, +125,541.6]* | +25.1% | +7.7 [+7.0, +8.4]* | +0.0 [+0.0, +0.0] | FR worse |
| FR-reactive vs fleetright_predictive | -14,875.0 [-15,791.8, -13,875.0]* | -2.4% | +2.0 [+1.2, +2.9]* | +0.0 [+0.0, +0.0] | trade-off |
| FR-reactive vs fleetright_predictive_same_headroom | -54,500.0 [-57,208.3, -51,874.9]* | -8.1% | +7.5 [+6.7, +8.3]* | +0.0 [+0.0, +0.0] | trade-off |
| FR-reactive vs offline_lookahead_oracle | +149,333.4 [+148,375.1, +150,250.0]* | +31.9% | +8.8 [+8.1, +9.6]* | +0.0 [+0.0, +0.0] | FR worse |
| FR-predictive vs static_peak_reserve | -167,625.0 [-168,583.5, -166,666.8]* | -21.0% | +6.8 [+5.8, +7.8]* | +0.0 [+0.0, +0.0] | trade-off |
| FR-predictive vs naive_threshold | +13,542.0 [+10,042.0, +17,041.8]* | +2.2% | +3.3 [+2.5, +4.2]* | +0.0 [+0.0, +0.0] | FR worse |
| FR-predictive vs hpa_like | +138,749.9 [+136,583.3, +140,666.5]* | +28.1% | +5.7 [+4.7, +6.7]* | +0.0 [+0.0, +0.0] | FR worse |
| FR-predictive vs fleetright_reactive | +14,875.0 [+13,875.0, +15,791.6]* | +2.4% | -2.0 [-2.9, -1.2]* | +0.0 [+0.0, +0.0] | trade-off |
| FR-predictive vs fleetright_predictive_same_headroom | -39,625.1 [-41,500.1, -37,708.4]* | -5.9% | +5.5 [+4.7, +6.3]* | +0.0 [+0.0, +0.0] | trade-off |
| FR-predictive vs offline_lookahead_oracle | +164,208.3 [+162,916.8, +165,333.4]* | +35.1% | +6.8 [+5.8, +7.8]* | +0.0 [+0.0, +0.0] | FR worse |
| FR-predictive_same_headroom vs fleetright_reactive | +54,500.0 [+51,791.6, +57,124.9]* | +8.8% | -7.5 [-8.3, -6.7]* | +0.0 [+0.0, +0.0] | trade-off |

## S14 stale telemetry (1 of 3 windows missing)

| policy | node cost | soft viol. | hard viol. (seeds) | max p99 ms | scale acts | idle CPU |
|---|---:|---:|---:|---:|---:|---:|
| static_peak_reserve | 800,000 | 0.0 | 0.0 (0) | 100 | 0.0 | 0.25 |
| naive_threshold | 800,000 | 0.0 | 0.0 (0) | 100 | 0.0 | 0.25 |
| hpa_like | 635,625 | 15.3 | 0.0 (0) | 139 | 168.8 | 0.24 |
| fleetright_reactive | 666,667 | 45.0 | 0.0 (0) | 139 | 160.0 | 0.22 |
| fleetright_predictive | 709,667 | 34.5 | 0.0 (0) | 292 | 97.3 | 0.21 |
| fleetright_predictive_same_headroom | 810,042 | 16.2 | 0.0 (0) | 138 | 136.4 | 0.23 |
| offline_lookahead_oracle | 594,458 | 0.0 | 0.0 (0) | 100 | 168.8 | 0.21 |

| FleetRight vs | cost diff [95% CI] | rel. | SLO-violation windows diff [95% CI] | hard diff [95% CI] | verdict |
|---|---:|---:|---:|---:|---|
| FR-reactive vs static_peak_reserve | -133,333.0 [-133,333.0, -133,333.0]* | -16.7% | +45.0 [+44.0, +46.1]* | +0.0 [+0.0, +0.0] | trade-off |
| FR-reactive vs naive_threshold | -133,333.0 [-133,333.0, -133,333.0]* | -16.7% | +45.0 [+44.0, +46.1]* | +0.0 [+0.0, +0.0] | trade-off |
| FR-reactive vs hpa_like | +31,042.0 [+30,583.7, +31,500.4]* | +4.9% | +29.7 [+28.9, +30.5]* | +0.0 [+0.0, +0.0] | FR worse |
| FR-reactive vs fleetright_predictive | -42,999.7 [-45,208.0, -40,791.3]* | -6.1% | +10.6 [+8.2, +12.9]* | +0.0 [+0.0, +0.0] | trade-off |
| FR-reactive vs fleetright_predictive_same_headroom | -143,374.7 [-146,207.9, -140,583.1]* | -17.7% | +28.8 [+27.5, +30.1]* | +0.0 [+0.0, +0.0] | trade-off |
| FR-reactive vs offline_lookahead_oracle | +72,208.6 [+70,708.6, +73,583.6]* | +12.1% | +45.0 [+44.0, +46.1]* | +0.0 [+0.0, +0.0] | FR worse |
| FR-predictive vs static_peak_reserve | -90,333.3 [-92,499.9, -88,125.0]* | -11.3% | +34.5 [+32.3, +36.7]* | +0.0 [+0.0, +0.0] | trade-off |
| FR-predictive vs naive_threshold | -90,333.3 [-92,500.0, -88,166.6]* | -11.3% | +34.5 [+32.2, +36.6]* | +0.0 [+0.0, +0.0] | trade-off |
| FR-predictive vs hpa_like | +74,041.8 [+71,916.9, +76,166.8]* | +11.6% | +19.1 [+16.9, +21.2]* | +0.0 [+0.0, +0.0] | FR worse |
| FR-predictive vs fleetright_reactive | +42,999.7 [+40,874.7, +45,208.1]* | +6.4% | -10.6 [-12.9, -8.2]* | +0.0 [+0.0, +0.0] | trade-off |
| FR-predictive vs fleetright_predictive_same_headroom | -100,375.0 [-101,666.6, -99,041.8]* | -12.4% | +18.2 [+16.5, +19.9]* | +0.0 [+0.0, +0.0] | trade-off |
| FR-predictive vs offline_lookahead_oracle | +115,208.4 [+112,624.9, +117,833.2]* | +19.4% | +34.5 [+32.3, +36.6]* | +0.0 [+0.0, +0.0] | FR worse |
| FR-predictive_same_headroom vs fleetright_reactive | +143,374.7 [+140,624.8, +146,249.6]* | +21.5% | -28.8 [-30.2, -27.4]* | +0.0 [+0.0, +0.0] | trade-off |

## S15 capacity profile drift (-20% true capacity)

| policy | node cost | soft viol. | hard viol. (seeds) | max p99 ms | scale acts | idle CPU |
|---|---:|---:|---:|---:|---:|---:|
| static_peak_reserve | 800,000 | 5.3 | 0.0 (0) | 152 | 0.0 | 0.25 |
| naive_threshold | 995,833 | 3.8 | 0.0 (0) | 157 | 205.4 | 0.28 |
| hpa_like | 806,000 | 1.4 | 0.0 (0) | 147 | 185.0 | 0.29 |
| fleetright_reactive | 650,000 | 71.2 | 0.0 (0) | 333 | 160.0 | 0.24 |
| fleetright_predictive | 694,292 | 48.0 | 1.4 (17) | 2,572 | 119.9 | 0.21 |
| fleetright_predictive_same_headroom | 806,042 | 18.7 | 0.0 (0) | 306 | 156.2 | 0.22 |
| offline_lookahead_oracle | 594,458 | 5.2 | 0.0 (0) | 149 | 168.8 | 0.21 |

| FleetRight vs | cost diff [95% CI] | rel. | SLO-violation windows diff [95% CI] | hard diff [95% CI] | verdict |
|---|---:|---:|---:|---:|---|
| FR-reactive vs static_peak_reserve | -150,000.0 [-150,000.0, -150,000.0]* | -18.8% | +66.0 [+65.0, +66.8]* | +0.0 [+0.0, +0.0] | trade-off |
| FR-reactive vs naive_threshold | -345,833.0 [-345,833.0, -345,833.0]* | -34.7% | +67.5 [+66.3, +68.6]* | +0.0 [+0.0, +0.0] | trade-off |
| FR-reactive vs hpa_like | -156,000.0 [-159,666.6, -152,249.9]* | -19.4% | +69.9 [+68.8, +71.0]* | +0.0 [+0.0, +0.0] | trade-off |
| FR-reactive vs fleetright_predictive | -44,291.8 [-46,208.2, -42,375.2]* | -6.4% | +21.9 [+19.3, +24.4]* | -1.4 [-1.8, -1.0]* | FR better (cost and SLO) |
| FR-reactive vs fleetright_predictive_same_headroom | -156,041.6 [-160,291.6, -151,875.0]* | -19.4% | +52.5 [+50.5, +54.6]* | +0.0 [+0.0, +0.0] | trade-off |
| FR-reactive vs offline_lookahead_oracle | +55,541.7 [+54,041.7, +56,875.1]* | +9.3% | +66.0 [+64.6, +67.3]* | +0.0 [+0.0, +0.0] | FR worse |
| FR-predictive vs static_peak_reserve | -105,708.2 [-107,624.9, -103,791.6]* | -13.2% | +44.1 [+41.4, +47.0]* | +1.4 [+1.0, +1.8]* | trade-off (hard SLO worse) |
| FR-predictive vs naive_threshold | -301,541.2 [-303,457.9, -299,624.5]* | -30.3% | +45.6 [+42.6, +48.7]* | +1.4 [+1.0, +1.8]* | trade-off (hard SLO worse) |
| FR-predictive vs hpa_like | -111,708.2 [-115,499.8, -107,916.5]* | -13.9% | +48.0 [+45.2, +51.0]* | +1.4 [+1.0, +1.8]* | trade-off (hard SLO worse) |
| FR-predictive vs fleetright_reactive | +44,291.8 [+42,375.1, +46,208.3]* | +6.8% | -21.9 [-24.4, -19.2]* | +1.4 [+1.0, +1.8]* | FR worse (hard SLO worse) |
| FR-predictive vs fleetright_predictive_same_headroom | -111,749.9 [-114,541.6, -108,916.6]* | -13.9% | +30.7 [+29.2, +32.2]* | +1.4 [+1.0, +1.8]* | trade-off (hard SLO worse) |
| FR-predictive vs offline_lookahead_oracle | +99,833.4 [+97,166.7, +102,541.7]* | +16.8% | +44.2 [+41.4, +47.2]* | +1.4 [+1.0, +1.8]* | FR worse (hard SLO worse) |
| FR-predictive_same_headroom vs fleetright_reactive | +156,041.6 [+151,958.4, +160,291.7]* | +24.0% | -52.5 [-54.6, -50.5]* | +0.0 [+0.0, +0.0] | trade-off |

## S16 one node lost (1 replica per tenant)

| policy | node cost | soft viol. | hard viol. (seeds) | max p99 ms | scale acts | idle CPU |
|---|---:|---:|---:|---:|---:|---:|
| static_peak_reserve | 800,000 | 2.0 | 0.0 (0) | 264 | 0.0 | 0.25 |
| naive_threshold | 1,000,000 | 0.0 | 0.0 (0) | 104 | 0.0 | 0.28 |
| hpa_like | 1,000,000 | 0.0 | 0.0 (0) | 104 | 0.0 | 0.28 |
| fleetright_reactive | 800,000 | 2.0 | 0.0 (0) | 264 | 0.0 | 0.19 |
| fleetright_predictive | 789,500 | 14.7 | 0.0 (0) | 264 | 22.2 | 0.24 |
| fleetright_predictive_same_headroom | 800,000 | 2.0 | 0.0 (0) | 264 | 0.0 | 0.19 |
| offline_lookahead_oracle | 820,167 | 1.8 | 0.0 (0) | 236 | 86.8 | 0.25 |

| FleetRight vs | cost diff [95% CI] | rel. | SLO-violation windows diff [95% CI] | hard diff [95% CI] | verdict |
|---|---:|---:|---:|---:|---|
| FR-reactive vs static_peak_reserve | +0.0 [+0.0, +0.0] | +0.0% | +0.0 [+0.0, +0.0] | +0.0 [+0.0, +0.0] | no significant difference |
| FR-reactive vs naive_threshold | -200,000.0 [-200,000.0, -200,000.0]* | -20.0% | +2.0 [+2.0, +2.0]* | +0.0 [+0.0, +0.0] | trade-off |
| FR-reactive vs hpa_like | -200,000.0 [-200,000.0, -200,000.0]* | -20.0% | +2.0 [+2.0, +2.0]* | +0.0 [+0.0, +0.0] | trade-off |
| FR-reactive vs fleetright_predictive | +10,500.0 [+9,083.2, +12,000.0]* | +1.3% | -12.7 [-14.5, -11.0]* | +0.0 [+0.0, +0.0] | trade-off |
| FR-reactive vs fleetright_predictive_same_headroom | +0.0 [+0.0, +0.0] | +0.0% | +0.0 [+0.0, +0.0] | +0.0 [+0.0, +0.0] | no significant difference |
| FR-reactive vs offline_lookahead_oracle | -20,166.8 [-21,583.3, -18,791.8]* | -2.5% | +0.2 [+0.1, +0.4]* | +0.0 [+0.0, +0.0] | trade-off |
| FR-predictive vs static_peak_reserve | -10,500.0 [-12,000.0, -9,041.9]* | -1.3% | +12.7 [+11.1, +14.4]* | +0.0 [+0.0, +0.0] | trade-off |
| FR-predictive vs naive_threshold | -210,500.0 [-211,999.9, -209,083.4]* | -21.1% | +14.7 [+12.9, +16.4]* | +0.0 [+0.0, +0.0] | trade-off |
| FR-predictive vs hpa_like | -210,500.0 [-211,958.4, -209,041.8]* | -21.1% | +14.7 [+13.0, +16.4]* | +0.0 [+0.0, +0.0] | trade-off |
| FR-predictive vs fleetright_reactive | -10,500.0 [-11,958.4, -9,041.6]* | -1.3% | +12.7 [+11.0, +14.5]* | +0.0 [+0.0, +0.0] | trade-off |
| FR-predictive vs fleetright_predictive_same_headroom | -10,500.0 [-11,958.4, -9,041.7]* | -1.3% | +12.7 [+10.9, +14.5]* | +0.0 [+0.0, +0.0] | trade-off |
| FR-predictive vs offline_lookahead_oracle | -30,666.8 [-32,708.5, -28,666.9]* | -3.7% | +12.9 [+11.2, +14.7]* | +0.0 [+0.0, +0.0] | trade-off |
| FR-predictive_same_headroom vs fleetright_reactive | +0.0 [+0.0, +0.0] | +0.0% | +0.0 [+0.0, +0.0] | +0.0 [+0.0, +0.0] | no significant difference |

`*` = 95% paired-bootstrap CI (10,000 resamples over seeds) excludes 0. Diff = FleetRight minus other; a negative cost/violation diff favours FleetRight.
