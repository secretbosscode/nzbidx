# SBOM generation

The security audit workflow emits a CycloneDX SBOM covering all services. To
regenerate locally:

```bash
pip install cyclonedx-py
pip install -e services/api -e services/ingest
cyclonedx-py environment -o sbom.json
```

The resulting file can be inspected or fed into downstream tooling.
