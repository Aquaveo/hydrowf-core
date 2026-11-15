# hydrowf-core

The hydrowf library: what a lego is, and how legos turn into runs. No web framework, no services.

| Package | Contents | Status |
|---|---|---|
| `hydrowf_core.models` | `ToolLego`, `DataLego`, `Pipeline`, `Port` (pydantic v2): validation and JSON Schema for forms, OGC process descriptions and agent tools | in use |
| `hydrowf_core.registry` | checks across legos (references, port and source compatibility) | in use |
| `hydrowf_core.adapters` | `argo.render` (pipeline lego → Argo Workflow); `argo_runtime.ArgoRuntime` (submit, status, outputs; `argo` extra); HPC later | Argo in use |
| `hydrowf_core.importers` | CWL and Argo templates → legos | optional, not planned yet |
| `hydrowf_core.drivers` | data drivers: Iceberg, STAC, files, external (optional extras) | planned |

Lego definitions themselves (tools, pipelines, datasets) live in
[hydrowf-tools](https://github.com/Aquaveo/hydrowf-tools); the backend services (OGC API, auth,
registry store) in [hydrowf-platform](https://github.com/Aquaveo/hydrowf-platform). Nothing in
this package knows a domain (ngen, FIM, …): that knowledge belongs in lego definitions.

## Develop

```sh
uv run --all-extras pytest -v
uv run --all-extras flake8 --max-line-length 120 src tests
```

`tests/test_argo_live.py` submits a small pipeline to a real Argo server when `HYDROWF_ARGO_HOST`
is set (see the file).
