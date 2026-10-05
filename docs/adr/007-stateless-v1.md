# ADR-007: Stateless replicated services in v1

Status: Accepted

Prompt 1 models stateless replicated services only. Their replicas may be recreated during
evacuation, and the simulator can treat queued work as an explicit model state. Stateful storage,
data movement, GPU scheduling, multi-cluster placement, and live Kubernetes mutation are excluded.
