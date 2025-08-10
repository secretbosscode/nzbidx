# SBOM generation

The security audit workflow emits CycloneDX SBOMs for each service. To
regenerate locally:

```bash
pip install cyclonedx-bom
pip install -e services/api
cyclonedx-bom -e -o sbom-api.json

pip install -e services/ingest
cyclonedx-bom -e -o sbom-ingest.json
```

The resulting files can be inspected or fed into downstream tooling.
