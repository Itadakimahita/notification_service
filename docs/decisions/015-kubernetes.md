# 015. Kustomize, KEDA on consumer lag, and API autoscaling on CPU

**Status:** accepted (not yet run on a cluster)

## Context
Different services need different scaling signals, and the platform must scale and roll out without dropping work.

## Decision
- Kustomize base with a Deployment per service, hardened pods (non-root, read-only filesystem, dropped capabilities), probes, rolling updates with no unavailable pods and PodDisruptionBudgets.
- The API scales on CPU with an HPA. The router, status service and workers scale with KEDA on Kafka consumer lag, up to the topic's partition count.
- Relay, scheduler and campaign run a fixed number of replicas and coordinate through DynamoDB leases.
- A local overlay for kind carries its own Kafka, DynamoDB Local, Redis, Mailpit and mock provider.

## Why this is better
- **Scale on what the work actually is.** A consumer is overloaded when it falls behind, not necessarily when its CPU is high, especially for I/O-bound sending. Lag reacts to the real backlog.
- **No wasted replicas.** More consumers than partitions would sit idle, so the maximum equals the partition count.
- **Safe rollouts.** Zero unavailable pods and PDBs keep capacity during deploys. Leases make extra relay and scheduler pods safe instead of dangerous.
- **Plain YAML.** Kustomize avoids a templating layer for a repo of this size.

## What we give up
- KEDA and metrics-server must be installed.
- Resource requests are guesses until measured.
- A production overlay (MSK, ElastiCache, IAM access, secrets) is not written.

## Evidence
Measured: the manifests render and pass consistency tests (selectors, targets, references, security settings), and an image builds and starts as a non-root user. **Not done:** applying anything to a cluster or exercising HPA and KEDA.

## Revisit when
The first real deployment: tune requests and thresholds from measurements, then add the production overlay.
