# Step 3: tool-declared formats and data legos as inputs

Status: agreed (2026-11-20); PR 1 (formats) merged (#16); PR 2 (data drivers) in progress. Part of the data MVP:

| Step | What | State |
|---|---|---|
| 1 | Outputs stored as files | merged (core #15, ogc #23) |
| 2 | STAC Items from each output's manifest | in review (ogc `stac-items`, tools `cng-manifest-0.2`) |
| **3** | **Tool-declared output formats; data legos as inputs; NGIAB round trip** | **this document** |
| 4 | TEEHR evaluation (`teehr-timeseries`, `teehr-evaluate`) | after 3, builds on its formats |

## 1. Goals

1. **A tool says which formats it can write, and the pipeline picks one.** A format is a data lego
   (`ngen-cng`, `teehr-timeseries`). One tool may write several; there is no converter lego type and no
   step the platform inserts on its own. Where a tool can't write a format (an upstream image such as
   `awiciroh/ciroh-ngen-image`), the pipeline author wires an ordinary tool that does.
2. **Data legos can be inputs.** Today `render()` refuses them: a `DataSource` or a `dataset` input is
   `UnsupportedForArgo("... need a data driver")` (adapters/argo.py `pipeline_values`, `task_inputs`) and
   `process_description` refuses dataset inputs (adapters/ogc.py). Step 3 adds the drivers for
   `external` and `stac` data.
3. **An NGIAB run survives the trip to cloud-native files and back.** `ngiab-to-cng` carries what a rerun
   needs; a new `cng-to-ngen` tool rebuilds the run folder; a test proves the rerun gives the same output.

Non-goals: per-user uploads (later, as STAC files); the Iceberg driver's reads and writes; private
pipelines and data in /ogc; a multi-choice port type (step 4 uses a string with a pattern); a STAC catalog.

The domain-agnostic test still holds: none of this names ngen. A new model with its own tools and data
legos uses every mechanism here with registry entries only.

## 2. Part A: tool-declared output formats

### 2.1 Model (models/lego.py)

An output port lists the formats it can write. `data` stays as the one-format shorthand, so every
existing lego is unchanged.

```yaml
- kind: tool
  id: ngiab-to-cng
  version: 0.3.0
  args: ["{ngen_run}", --output, "{workdir}/out", --format, "{out_format}"]
  outputs:
    - name: out
      type: directory
      formats: [ngen-cng, teehr-timeseries]   # the first is the default
      path: out
```

- `Port.formats: list[LegoId]`: output ports of type file or directory only; at least 2 entries, no
  repeats; set **instead of** `data` (a port has one or the other). `Port.choices` returns `formats`, or
  `[data]`.
- Each multi-format output `<name>` gives the tool a placeholder `{<name>_format}`: the chosen data lego
  id. `check_tool` reserves the name (an input can't be called `out_format` when output `out` has
  formats) and accepts the placeholder in `args` and `env`, not in paths (like `{run}`: a path is
  fixed before the choice is made). A tool with formats must use its placeholder, or it could not
  know what to write.
- `Step.formats: dict[str, LegoId]`: output name -> the chosen format. Optional: an output not listed
  writes its first format.

### 2.2 Checks (registry/validate.py)

- Every id in `formats` is a registered data lego (as `data` is today).
- `Step.formats` names an output of the step's tool that has `formats`, and a format in its list.
- **Wiring uses the chosen format.** `source_problem` compares `(upstream.type, effective data)` with
  the port, where effective data = `step.formats.get(output)` or the port's first choice. The rule
  stays exact equality: a link from a step that writes `ngen-cng` can't feed a port that wants
  `teehr-timeseries`.
- `chosen_format(step, port)` (models) is the one rule for "the step's choice, else the first format":
  wiring, render and the OGC description all use it. `output_formats(pipeline, legos)` (adapters/argo.py)
  gives each pipeline output's data lego as `id@version`, and is what the annotation (2.3) records.
- A tool must pass `{<name>_format}` in plain args or env: inside an ArgGroup alone it could be left out,
  and the tool would never hear the choice.

### 2.3 Rendering (adapters/argo.py)

- `container_template`: `values["<name>_format"]` = the chosen id, so `argv` fills it.
- `output_artifact` takes the chosen data lego (its `data` argument), not `port.data`: a format's
  `archive` (`none`/`tgz`) is the chosen data lego's. `latest_data` gives the one version render uses for
  both the archive and the annotation. The newest version of a data lego decides, whatever its driver: if it isn't
  STAC, the output is stored as files (before, the newest STAC version decided the archive).
- The workflow records each output's format in a new annotation, `hydrowf/formats`
  (`{"cng": "ngen-cng@1.0.0"}`: id **and** the data lego version current at render time). This also
  closes step 2's known gap: hydrowf-ogc reads the Item's `item_from` from the version the job ran with
  instead of the latest one.

### 2.4 hydrowf-ogc

- `manager.output_data` reads `hydrowf/formats` (falling back to today's lookup for older runs). Until
  then it reads `port.data`, so a multi-format output gets no STAC Item: don't publish multi-format tools
  to a live registry before hydrowf-ogc PR 3.
- (Process descriptions already show each output's chosen format: core's `process_description`.)

## 3. Part B: data legos as inputs

What can feed a step input today, and what step 3 adds:

| Source | Port type | Today | Step 3 |
|---|---|---|---|
| Value, pipeline scalar input | scalars | yes | unchanged |
| Pipeline file/directory input (an `s3://` URL) | file, directory | yes: an earlier job's output or a key under `shared_inputs` | **the URL's data must match the port's** (3.3) |
| Link to another step's output | file, directory | yes | uses the chosen format (2.2) |
| `DataSource` of an **external** data lego | dataset | refused | **the tool gets the URL** (3.1) |
| `DataSource` of a **managed STAC** data lego | file, directory | refused | **staged from its location** (3.2) |
| `DataSource` of an Iceberg data lego | dataset | refused | `check()` accepts the wiring; `render()` refuses (no Argo driver yet) |

### 3.1 External driver

`ExternalStorage` already holds a public `s3://` or `https://` URL (anonymous only; `public_host` refuses
private, loopback and link-local hosts). For a `dataset` port fed by an external data lego:

- render passes the data lego's `url` as the input's value (`{name}` in args/env). Nothing is copied:
  the tool reads it in place, as the Port docstring says datasets are.
- `check()` and render refuse external data to a tool without `network: true` (it reads from the
  internet). Nothing stops a `network: false` tool from reaching the network at run time until the new
  cluster's egress policy exists.
- The URL may not hold `{`, `}`, `$`, spaces or control characters (Argo expands `{{...}}` and Kubernetes
  `$(VAR)` in args and env): anyone may publish external data, so its URL is checked like caller text.
- The URL comes from the registry, never from the caller: a caller can't point a tool at an address of
  their choosing. (Caller-given dataset URLs stay refused: decision 1.)

### 3.2 STAC driver for managed data

A managed STAC data lego is data the platform keeps (today the conus hydrofabric gpkg sits under the
`hydrofabric/` shared prefix and is typed only by the port it is passed to). Step 3 gives it a place:

- `StacStorage.location: str | None`: for `data_kind: managed`, the key it lives at in the artifact
  bucket (a file key or a prefix ending in `/`), e.g. `managed/hydrofabric-gpkg/2.2.0/conus_nextgen.gpkg`.
  Required for managed STAC data, refused for output data (an output's key is per run). It must be a plain
  key **under `managed/`** (A-Z a-z 0-9 . _ / -, no empty, `.` or `..` part): job outputs live under
  `<workflow name>/`, so a managed location can never point into a job's outputs.
- A `DataSource` of such a lego feeds a file or directory port by **staging** (an Argo artifact from that
  key, as a pipeline file input is today). The key's form says which: ending in `/` is a directory, else a
  file; `check()` refuses a mismatch.
- **Not a dataset port.** Staging uses Argo's credentials; tool pods have none for the platform bucket, so
  an `s3://` URL to managed data couldn't be read. Giving tools read access to the bucket is a trust
  decision for later (it would let any tool read every managed key).
- render refuses a workflow named `managed`: its outputs (`<workflow name>/...`) would land under
  `managed/`. hydrowf-ogc's job ids are UUIDs, but render is a public API.
- Who may publish managed data stays as decided: `tool-authors` only (hydrowf-registry). Getting the bytes to
  `location` is an admin task for now (uploads come later).
- hydrowf-ogc then needs fewer `shared_inputs`: pipelines name `{data: {id: hydrofabric-gpkg, version: 2.2.0}}`
  instead of asking every caller for the URL. `shared_inputs` stays for older pipelines.

### 3.3 Caller-given job outputs carry their data type

A caller may pass an earlier job's output as a file or directory input (hydrowf-ogc `check_inputs`
already limits it to the caller's own jobs and shared prefixes). Today nothing checks **what** it is: an
`ngen-cng` folder passed where `ngen-run` is wanted fails inside the tool. With `hydrowf/formats` (2.3),
hydrowf-ogc knows each output's data lego and refuses a mismatch with a 400 before anything runs. Keys
under `shared_inputs` aren't typed and stay unchecked.

## 4. Part C: the NGIAB round trip

- **ngiab-to-cng 0.3.0** copies what a rerun needs into the CNG output, unchanged: `config/realization.json`,
  `config/troute.yaml`, `config/cat_config/` and the run folder's `partitions_*.json`, under `run/`, listed in the
  manifest with their sha256 (the manifest already records input hashes). The gpkg is not copied: its
  layers are the GeoParquet files.
- **cng-to-ngen 0.1.0** (new, hydrowf-tools): `ngen-cng` -> `ngen-run` (`archive: tgz`). It writes the
  configs back, rebuilds the gpkg from the GeoParquet layers (icefabric schemas, same layer names) and
  `forcings/forcings.nc` from `timeseries/forcings.parquet`.
- **Proof** (a test in hydrowf-tools, run in the tools' containers): the test run folder ->
  ngiab-to-cng -> cng-to-ngen -> ngiab-run again; t-route's `flow` matches the first run's within float32
  rounding, and the configs are byte-identical. Run once on kind as a pipeline too.
- `ngen-cng` gets a new version (2.0.0) whose description says it is rerunnable.

`teehr-timeseries` as a second format of ngiab-to-cng is step 4; this step only needs the formats
mechanism to exist (it is tested with fixture legos).

## 5. Compatibility

- Legos with `data` on every port validate and render exactly as before: the golden Argo files
  (tests/fixtures/argo) must not change except for the new `hydrowf/formats` annotation.
  One edge case changes: a data id whose newest version isn't STAC (none in today's registry) is stored
  as files rather than by an older STAC version's `archive`.
- Runs without `hydrowf/formats` (older ones) keep working in hydrowf-ogc through today's lookup.
- The registry service stores legos as JSON, so the new fields need no migration; its validation comes
  from core (pin bump).
- **Deploy order:** the models forbid unknown fields, so a service pinned to an older core can't load a
  lego that uses `formats`. Bump the core pin in hydrowf-ogc and hydrowf-registry before any such lego is
  published.
- **Stricter data legos (PR 2):** managed STAC data now needs a `location`, and an external URL may not hold
  `{ } $`, whitespace or control characters, nor name an internal https host. A lego that validated before
  and breaks one of these is left out of hydrowf-ogc's registry snapshot (logged). None in today's
  registries does.

## 6. Security notes

- Formats are chosen in the pipeline lego, not per request: a caller can't change what a tool writes.
- External URLs only come from registered data legos (`public_host`, anonymous). The tool must declare
  `network: true` (checked in `check()` and again in render). For `https://` URLs, `public_host` also
  refuses one-label names and cluster/LAN suffixes (`.local`, `.internal`, `.cluster.local`, `.svc`,
  `.lan`, `.home.arpa`); for `s3://` the "host" is a bucket name and isn't checked. A public name can
  still resolve to an internal address; the new cluster's egress policy is the real fence (noted in the
  deploy work).
- Managed `location` keys are plain keys under `managed/`; staging uses Argo's own credentials, as pipeline
  file inputs do today, and no job writes under `managed/` (job keys start with the workflow name, and
  render refuses the name `managed`).

## 7. Work breakdown

| PR | Repo | Contents | Depends on |
|---|---|---|---|
| 1 | hydrowf-core | `Port.formats`, `Step.formats`, `{<output>_format}`, checks, render, `hydrowf/formats`, `output_formats` | |
| 2 | hydrowf-core | external and managed-STAC drivers (`DataSource`, external on dataset ports, managed staged, `StacStorage.location`); caller dataset inputs stay refused | 1 |
| 3 | hydrowf-ogc | pin core; results read `hydrowf/formats`; inputs checked against their data (3.3); registry legos updated | 1, 2 |
| 4 | hydrowf-tools | ngiab-to-cng 0.3.0 (configs), cng-to-ngen 0.1.0, round-trip test | |
| 5 | hydrowf-ogc | pin the two new images; `ngen-cng@2.0.0`; a pipeline that reruns from CNG | 3, 4 |

Each PR: tests first, gates, an independent review (the core PRs change a public API).

## 8. Decisions (2026-11-20)

1. **Dataset URLs come from registered data legos only.** A caller can't pass their own URL in step 3;
   users bring their own data with uploads (later), into our bucket under checks we control.
2. **The format is chosen per pipeline**, in `Step.formats`, so a process's outputs are known when it is
   published and wiring is checked before any run. A per-run choice can be added later (a `Step.formats`
   value taken from a pipeline input) without breaking this.
3. **Managed data is put at its `location` by an admin script** in hydrowf-deploy until uploads exist.
