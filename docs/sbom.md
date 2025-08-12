# SBOM generation

The security audit workflow emits a CycloneDX SBOM covering the application. To
regenerate locally:

```bash
pip install cyclonedx-py
pip install -e services/api
cyclonedx-py environment -o sbom.json
```

The resulting file can be inspected or fed into downstream tooling.
