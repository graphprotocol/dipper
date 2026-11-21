# Kubernetes Deployment

Kubernetes manifests for deploying the dipper service.

## Prerequisites

- PostgreSQL database accessible from the cluster. Ask for an encrypted, verified connection in `db.url`, for example `postgres://<host>:5432/dipper?sslmode=verify-full&sslrootcert=/config/db-ca.pem` with the server's CA mounted next to `config.json`, where `<host>` is a name the server's certificate covers (a managed database may give one, such as Cloud SQL's instance DNS name with its shared CA). `sslmode=verify-ca` checks the name as well for now, so it is no way round a certificate that does not name the address dipper uses; reach such a database through a proxy that authenticates for dipper instead, such as the Cloud SQL Auth Proxy. `db.password` can be left out where the database asks for none, as through the Cloud SQL Auth Proxy with automatic IAM database authentication
- Configuration secrets managed separately
- IISA service reachable at startup (dipper health-checks IISA, waits up to `iisa.startup_wait`, 180s by default, and exits if it stays unreachable)

## Files

- `deployment.yaml` - Dipper deployment with health probes and security context. It sets no log level: dipper logs at `info` unless `RUST_LOG` is set, for example to `dipper_service=debug,info`, which an environment can supply
- `service.yaml` - ClusterIP service exposing Admin RPC (8545)
- `network-policy.yaml` - Refuses connections to dipper from other pods (see [Network policy](#network-policy))
- `configmap-example.yaml` - Example ConfigMap showing config.json structure (do not apply directly)

## Deployment

1. Deploy the IISA service first (see subgraph-dips-indexer-selection repo)
2. Create `dipper-config` ConfigMap with actual values
3. Apply dipper manifests

Dipper requires IISA at startup: it runs an IISA health check, retrying with growing pauses for up to `iisa.startup_wait` (180s by default, enough for IISA's own rollout), and exits if IISA is still not reachable. It waits for the database the same way, for up to `db.connect_wait` (120s by default), but exits at once if the database refuses it, as with a wrong password. Deploy and verify IISA before starting dipper.

Run exactly 1 replica. The signer's nonce and the locks that keep 2 jobs from acting on the same agreement live in the process, so 2 pods side by side could send transactions on the same nonce or act on an agreement twice. `deployment.yaml` sets `strategy: Recreate` for that reason: a rollout stops the old pod before the new one starts, at the cost of a gap while the new pod starts up. Keep it if you deploy with other manifests.

`Recreate` only covers rollouts. When a pod is evicted, deleted or its node drained, Kubernetes starts the replacement while the old pod is still shutting down, and the old pod's worker keeps taking jobs until its turn in the stop sequence comes. Until dipper holds a lock of its own for as long as it runs, keep such disruptions to planned windows: set a maintenance window on the cluster, and drain a node only after scaling dipper to 0.

## Configuration

The `configmap-example.yaml` shows the required config.json structure. Create a ConfigMap named `dipper-config` with your actual values:

```bash
kubectl create configmap dipper-config --from-file=config.json=your-config.json
```

Dipper reads every file named in its `args`, merged in order (see the main README), so secrets need not sit in the ConfigMap. Leave them out of `config.json` and put them in a `secrets.json` that holds only the secret fields, in the same shape:

```json
{
  "signer": { "secret_key": "0x…" },
  "event_streaming_config": { "kafka": { "sasl_password": "…" } },
  "chain_client": { "providers": ["https://arb-mainnet.g.alchemy.com/v2/…"] },
  "alerts": { "slack_webhook_url": "https://hooks.slack.com/services/…" }
}
```

Keep that file in a Secret, for example one External Secrets renders from your secret store, mount it, and name it after `config.json`:

```yaml
args: [/config/config.json, /secrets/secrets.json]
volumeMounts:
  - { name: config, mountPath: /config, readOnly: true }
  - { name: secrets, mountPath: /secrets, readOnly: true }
volumes:
  - { name: config, configMap: { name: dipper-config } }
  - { name: secrets, secret: { secretName: dipper-secrets } }
```

A list in a later file replaces the earlier one whole, so `chain_client.providers` goes in 1 file or the other. Dipper reads the files once, at start: restart it to take a changed secret, for example with a checksum of the Secret in a pod annotation. Through the Cloud SQL Auth Proxy with automatic IAM database authentication, `db.password` is left out altogether.

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
| Health | 8546 | TCP | HTTP `GET /health` for the startup and liveness probes, and `GET /metrics` for Prometheus |

The health port is deliberately absent from `service.yaml`: the kubelet probes the pod directly, so
the endpoint does not need to be reachable from elsewhere in the cluster. A metrics scraper reaches
`/metrics` on the pod directly as well (for example through a PodMonitor), and
`network-policy.yaml` refuses it like any other pod until you allow it: add a policy admitting your
scraper's namespace to the `health` port only.

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
