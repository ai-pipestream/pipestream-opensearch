# opensearch-krick — dedicated single-node OpenSearch for the dev pipeline

Single-node OpenSearch tuned for **vector (k-NN/HNSW) ingest** on a dedicated
fast box: many-core CPU, 64 GB RAM, NVMe storage, reached directly over the
LAN. Sibling of the NAS-hosted stack
(`dev-tools/dev-assets/docker/opensearch-nas` — that one sits behind Traefik
and shares the NAS's 8 cores with everything else; this one owns its host).

## Why a dedicated node

HNSW vector indexing is CPU-bound graph construction, and the graphs must fit
in **native** (off-heap) memory. A NAS-class box sharing cores with the rest
of the home-lab stack tops out fast; measurements showed the indexing pipeline
healthy but the node itself as the ceiling. The 64 GB split here:
24 GB heap / ~24 GB k-NN graphs (60% circuit breaker) / remainder page cache.

## Deploy

```bash
cd deploy/opensearch-krick
cp .env.example .env        # set heap, ports, data dir
# For real use, switch the volume to a bind mount on the NVMe (see the
# DATA VOLUME comment at the bottom of docker-compose.yml).
docker compose up -d
curl -s http://localhost:9200/_cluster/health | jq .status   # green
```

Portainer: paste `docker-compose.yml` as a stack and set the env vars in the
stack editor instead of `.env`.

## Point the dev pipeline at it

In `~/.pipeline/dev` env (consumed by opensearch-manager's
`%dev.opensearch.hosts`):

```
OPENSEARCH_HOST=<LAN IP or name of this host>
OPENSEARCH_PORT=9200
OPENSEARCH_PROTOCOL=http
```

Restart `opensearch-manager`. Direct LAN access is intentional — no proxy
hop. If a browser-friendly HTTPS name is wanted, add a route on the NAS
Traefik pointing at this host; don't front this stack with its own proxy.

## Ops notes

- **Keep the index population lean.** A single node with thousands of stale
  dev shards spends its life on cluster-state churn and heap pressure
  (observed: 304 indices / 2,241 shards ground the NAS node to a crawl).
  Perf-test indices (`idx-perf-*`) clean themselves up via the module-test
  teardown; crawl indices (`idx-pipeline-crawl-*`) accumulate — purge
  periodically.
- The `init-templates` sidecar applies a low-priority default template
  (shards/replicas/refresh/codec) on every stack start; plan-provisioned
  indices override it.
- Dashboards: use the NAS stack's dashboards with
  `data_source.enabled: true` and register this node as a data source —
  no need to run a second dashboards container here.
