# Kubernetes

The `k8s/` directory contains example manifests for running NZBIdx on a
Kubernetes cluster. They provide minimal `Deployment` and `Service` resources
for the API and ingest workers and expose configuration via environment
variables.

Persistent storage is required for the backing Postgres and OpenSearch
services; use `PersistentVolumeClaim` resources or managed cloud services and
set `DATABASE_URL` and `OPENSEARCH_URL` accordingly.

Apply the manifests:

```bash
kubectl apply -f k8s/api.yaml
kubectl apply -f k8s/ingest.yaml
```

Adjust images, replicas and resources to suit your environment.
