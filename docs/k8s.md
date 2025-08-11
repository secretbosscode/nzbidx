# Kubernetes

The `k8s/` directory contains an example manifest for running NZBIdx on a
Kubernetes cluster. It provides a minimal `Deployment` and `Service` for the
combined API and ingest service and exposes configuration via environment
variables.

Persistent storage is required for the backing Postgres and OpenSearch
services; use `PersistentVolumeClaim` resources or managed cloud services and
set `DATABASE_URL` and `OPENSEARCH_URL` accordingly.

Apply the manifest:

```bash
kubectl apply -f k8s/api.yaml
```

Adjust images, replicas and resources to suit your environment.
