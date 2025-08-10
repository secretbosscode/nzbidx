#!/usr/bin/env python3
"""Register an OpenSearch snapshot repository if configured.

Env variables:
  OS_SNAP_REPO      Repository name
  OS_S3_BUCKET      S3 bucket (optional)
  OS_S3_REGION      S3 region (optional)
  OS_S3_ROLE_ARN    IAM role ARN (optional)
  OS_GCS_BUCKET     GCS bucket (optional)
  OS_GCS_CLIENT     GCS client name (optional)
"""

from __future__ import annotations

import json
import os
import urllib.request


def main() -> int:
    repo = os.getenv("OS_SNAP_REPO")
    if not repo:
        print("OS_SNAP_REPO not set; skipping")
        return 0
    url = os.getenv("OPENSEARCH_URL", "http://opensearch:9200")
    s3_bucket = os.getenv("OS_S3_BUCKET")
    gcs_bucket = os.getenv("OS_GCS_BUCKET")
    if s3_bucket:
        body: dict[str, object] = {"type": "s3", "settings": {"bucket": s3_bucket}}
        if region := os.getenv("OS_S3_REGION"):
            body["settings"]["region"] = region
        if role := os.getenv("OS_S3_ROLE_ARN"):
            body["settings"]["role_arn"] = role
    elif gcs_bucket:
        body = {"type": "gcs", "settings": {"bucket": gcs_bucket}}
        if client := os.getenv("OS_GCS_CLIENT"):
            body["settings"]["client"] = client
    else:
        print("No snapshot backend configured; skipping")
        return 0
    req = urllib.request.Request(
        f"{url}/_snapshot/{repo}",
        data=json.dumps(body).encode("utf-8"),
        headers={"Content-Type": "application/json"},
        method="PUT",
    )
    try:
        urllib.request.urlopen(req, timeout=5)
        print("snapshot repository registered")
    except Exception as exc:  # pragma: no cover - network errors
        print(f"failed to register snapshot repo: {exc}")
    return 0


if __name__ == "__main__":  # pragma: no cover - script entry
    raise SystemExit(main())
