# Kubernetes

Manifests are Kustomize (`deploy/k8s`). `base/` is environment-neutral; `overlays/local/` targets a kind cluster and brings its own single-node Kafka, DynamoDB Local, Redis, Mailpit and the mock provider.

## Verification status

| Item | Status |
|---|---|
| Image build (`Dockerfile`, distroless, non-root) | Built and started locally |
| `kubectl kustomize` renders base and local overlay | Verified; `deploy/k8s/manifests_test.go` checks selectors, Service/PDB/HPA/ScaledObject targets, ConfigMap and Secret references, security context, probes |
| Applied to a real cluster | **Not done.** No cluster tooling (kind, minikube) was available |
| HPA and KEDA scaling behaviour | **Not exercised** |
| Production overlay | **Not provided** (see below) |

Treat the local overlay as a starting point that has not yet met a cluster.

## Local (kind)

```sh
for s in api relay router scheduler status inapp campaign lagexporter worker mockprovider migrate; do
  docker build --build-arg SERVICE=$s -t notif-$s:dev .      # or: make images
done
kind create cluster --config deploy/k8s/kind-config.yaml
for s in api relay router scheduler status inapp campaign lagexporter worker mockprovider migrate; do
  kind load docker-image notif-$s:dev
done
kubectl apply -k deploy/k8s/overlays/local
kubectl -n notifications wait --for=condition=complete job/migrate --timeout=180s
curl localhost:8081/healthz   # api is exposed on NodePort 30081, mapped to host port 8081
```

The Job `migrate` creates tables and topics. Workers and the router retry their dependencies on startup, so the first minute may show restarts while Kafka comes up.

## Scaling model

- **api**: HPA on CPU (3 to 50 replicas). Stateless; one instance handled about 2,000 req/s in local tests, see `benchmarks.md`.
- **router, status, workers**: KEDA `ScaledObject` on consumer-group lag (workers on both the high and low lane of their channel). Maximum replicas equal the topic partition counts (48 for requested and high lanes, 24 for status), because extra consumers beyond that would idle.
- **relay, scheduler, campaign**: fixed replicas. They coordinate through DynamoDB leases, so extra replicas are safe and give failover; they do not add throughput beyond the number of stream shards or schedule shards.
- **PodDisruptionBudgets** allow one voluntary disruption at a time for every multi-replica service.

## Before using this in production

1. Create a production overlay: remove the local deps, set `DYNAMO_ENDPOINT` and `S3_ENDPOINT` empty (real AWS), point `KAFKA_BROKERS` and the KEDA `bootstrapServers` at MSK, and set `REDIS_ADDR` to ElastiCache.
2. Supply the `notif-secrets` Secret (`API_KEYS`, `WEBHOOK_KEYS`) from a secret manager. The base deliberately does not define it.
3. Give pods AWS access through IRSA or Pod Identity (DynamoDB, Streams, S3). No static keys.
4. Install KEDA and metrics-server, and set resource requests from measured load rather than the defaults here, which are guesses.
5. Provision Kafka topics through the `migrate` Job or your own IaC with replication factor 3.
