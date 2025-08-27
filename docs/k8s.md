# Kubernetes

The `k8s/` directory contains an example manifest for running NZBidx on a
Kubernetes cluster. It provides minimal `Deployment` and `Service` resources for
the API (which also runs the ingest worker) and exposes configuration via
environment variables.

Persistent storage is required for the backing Postgres service; use
`PersistentVolumeClaim` resources or managed cloud services and set
`DATABASE_URL` accordingly.

The example manifest also sets `NNTP_TOTAL_TIMEOUT` and
`NZB_TIMEOUT_SECONDS`. Adjust these to suit your deployment, ensuring
`NZB_TIMEOUT_SECONDS` is greater than or equal to `NNTP_TOTAL_TIMEOUT` to avoid
premature API timeouts during NZB generation.

Database maintenance, including vacuuming, analyzing, reindexing and pruning
releases outside configured size limits, runs automatically via
`scripts/db_maintenance.py`.

Apply the manifest:

```bash
kubectl apply -f k8s/api.yaml
```

Adjust image, replicas and resources to suit your environment.
