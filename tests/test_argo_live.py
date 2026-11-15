"""Submit a tiny two-step pipeline to a real Argo server. Skipped unless HYDROWF_ARGO_HOST is set.

    kubectl --context kind-hydrowf -n argo port-forward svc/argo-server 2746:2746 &
    HYDROWF_ARGO_HOST=https://localhost:2746 HYDROWF_ARGO_INSECURE=1 uv run pytest tests/test_argo_live.py -v
"""
import os
import time

import pytest
import yaml

from hydrowf_core.adapters.argo import render
from hydrowf_core.models import LEGOS

HOST = os.environ.get("HYDROWF_ARGO_HOST")
pytestmark = pytest.mark.skipif(not HOST, reason="set HYDROWF_ARGO_HOST to run against an Argo server")

HELLO = yaml.safe_load("""
- {kind: data, id: note, version: 1.0.0, title: A text file, data_kind: output, storage: {driver: stac, format: other}}
- kind: tool
  id: write-note
  version: 1.0.0
  title: Write a note
  image: busybox:1.36
  command: [sh, -c]
  args: ['echo "{text}" > {workdir}/note.txt']
  inputs: [{name: text, type: string}]
  outputs: [{name: note, type: file, data: note, path: note.txt}]
- kind: tool
  id: copy-note
  version: 1.0.0
  title: Copy a note
  image: busybox:1.36
  command: [sh, -c]
  args: ['cp {note} {workdir}/copy.txt && cat {workdir}/copy.txt']
  inputs: [{name: note, type: file, data: note}]
  outputs: [{name: copy, type: file, data: note, path: copy.txt}]
- kind: pipeline
  id: hello-note
  version: 1.0.0
  title: Write a note, then copy it
  inputs: [{name: text, type: string}]
  steps:
    - {name: write, tool: {id: write-note, version: 1.0.0}, inputs: {text: {input: text}}}
    - {name: copy, tool: {id: copy-note, version: 1.0.0}, inputs: {note: {step: write, output: note}}}
  outputs: {copy: {step: copy, output: copy}}
""")


def test_submit_status_outputs_on_a_real_server():
    from hydrowf_core.adapters.argo_runtime import ArgoRuntime     # needs the argo extra

    legos = LEGOS.validate_python(HELLO)
    runtime = ArgoRuntime(HOST, bucket=os.environ.get("HYDROWF_ARGO_BUCKET", "my-bucket"),
                          verify_ssl=not os.environ.get("HYDROWF_ARGO_INSECURE"))
    name = runtime.submit(render(legos[-1], legos, {"text": "hello lego"}))

    deadline = time.monotonic() + 300
    while not (status := runtime.status(name)).finished and time.monotonic() < deadline:
        time.sleep(3)
    assert (status.phase, status.steps) == ("Succeeded", {"write": "Succeeded", "copy": "Succeeded"}), status
    outputs = runtime.outputs(name)
    assert list(outputs) == ["copy"] and outputs["copy"].startswith(f"s3://my-bucket/{name}/"), outputs
