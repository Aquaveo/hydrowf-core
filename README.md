# hydrowf-core

The hydrowf library: what a lego is, and how legos turn into runs. No web framework, no services.

| Package | Contents | Status |
|---|---|---|
| `hydrowf_core.models` | `ToolLego`, `DataLego`, `Port` (pydantic v2): validation and JSON Schema for forms, OGC process descriptions and agent tools | started |
| `hydrowf_core.importers` | CWL (cwl-utils) and Argo templates → legos | planned |
| `hydrowf_core.adapters` | runtime adapters: Argo (from hydrowf-platform's translator), HPC later | planned |
| `hydrowf_core.drivers` | data drivers: Iceberg, STAC, files, external (optional extras) | planned |

Lego definitions themselves (tools, pipelines, datasets) live in
[hydrowf-tools](https://github.com/Aquaveo/hydrowf-tools); the backend services (OGC API, auth,
registry store) in [hydrowf-platform](https://github.com/Aquaveo/hydrowf-platform). Nothing in
this package knows a domain (ngen, FIM, …): that knowledge belongs in lego definitions.

## Develop

```sh
uv run pytest -v
uv run flake8 --max-line-length 120 src tests
```
