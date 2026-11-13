# Kubernetes Deployment

Kubernetes manifests for deploying the dipper service.

## Prerequisites

- PostgreSQL database accessible from the cluster
- Configuration secrets managed separately
- IISA service reachable at startup (dipper health-checks IISA and exits if it is unreachable)

## Files

- `deployment.yaml` - Dipper deployment with health probes and security context. It sets no log level: dipper logs at `info` unless `RUST_LOG` is set, for example to `dipper_service=debug,info`, which an environment can supply
- `service.yaml` - ClusterIP service exposing Admin RPC (8545)
- `network-policy.yaml` - Refuses connections to dipper from other pods (see [Network policy](#network-policy))
- `configmap-example.yaml` - Example ConfigMap showing config.json structure (do not apply directly)

## Deployment

1. Deploy the IISA service first (see subgraph-dips-indexer-selection repo)
2. Create `dipper-config` ConfigMap with actual values
3. Apply dipper manifests

Dipper requires IISA at startup: it runs an IISA health check and exits if IISA is not reachable after a few attempts. Deploy and verify IISA before starting dipper.

## Configuration

The `configmap-example.yaml` shows the required config.json structure. Create a ConfigMap named `dipper-config` with your actual values:

```bash
kubectl create configmap dipper-config --from-file=config.json=your-config.json
```

Or apply a ConfigMap manifest with the values injected from your secrets management system.

## IISA Service Discovery

Dipper finds IISA via Kubernetes DNS. The config.json `iisa.endpoint` should be set to:

```
http://iisa:8080
```

This assumes IISA is deployed with a Service named `iisa` in the same namespace.

## Ports

| Service | Port | Protocol | Purpose |
|---------|------|----------|---------|
| Admin RPC | 8545 | TCP | JSON-RPC for admin operations |
| Health | 8546 | TCP | HTTP `GET /health` for the startup and liveness probes |

The health port is deliberately absent from `service.yaml`: the kubelet probes the pod directly, so
the endpoint does not need to be reachable from elsewhere in the cluster.

## Network policy

`network-policy.yaml` refuses connections to dipper from other pods, on any port. The admin
port's read calls need no signature, so without it any pod in the cluster could read every
request and agreement. Operators reach the admin port with `kubectl port-forward`, and the
kubelet probes the pod; both still work, because Kubernetes never blocks traffic from a pod's own
node or its localhost. For the same reason a pod on dipper's node that shares the node's
network, which only a privileged pod can, still gets through. An environment with an in-cluster caller of the admin port adds
its own policy that allows just that caller, since policies add up. The policy only takes effect
on a cluster that enforces network policies.

Outgoing traffic is not limited. The database, chain RPC and Kafka addresses differ by
environment and may be private, and the only addresses an outsider chooses, the indexer URLs,
are checked in dipper's code whenever it calls one (see `indexer_urls.allow_private_addresses`).
