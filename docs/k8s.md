# Kubernetes

The `k8s/` directory contains an example manifest for running NZBidx on a
Kubernetes cluster. It provides minimal `Deployment` and `Service` resources for
the API (which also runs the ingest worker) and exposes configuration via
environment variables.

Persistent storage is required for the backing Postgres and OpenSearch
services; use `PersistentVolumeClaim` resources or managed cloud services and
set `DATABASE_URL` and `OPENSEARCH_URL` accordingly.

Apply the manifest:

```bash
kubectl apply -f k8s/api.yaml
```

Adjust image, replicas and resources to suit your environment.
