# Kubernetes Deployment

Kubernetes manifests for deploying the dipper service.

## Prerequisites

- PostgreSQL database accessible from the cluster
- Configuration secrets managed separately
- IISA service reachable at startup (dipper health-checks IISA and exits if it is unreachable)

## Files

- `deployment.yaml` - Dipper deployment with health probes and security context
- `service.yaml` - ClusterIP service exposing Admin RPC (8545)
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
