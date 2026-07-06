# DANDI Cache: `<cache-name>`

`<A short description of what this cache contains and how it is derived.>`

Updated frequently.

Primarily for use by developers.

> **Note:** Throughout this template, `<cache-name>` refers to the hyphenated repository name (e.g., `my-cache`) and `<cache_name>` refers to the underscored form used for file and variable names (e.g., `my_cache`).



## One-time use

If you only plan to use this cache infrequently or from disparate locations, you can directly download the latest version of the cache as a compressed [JSON Lines](https://jsonlines.org/) file from the `dist` branch:

### Python API (recommended)

```python
import gzip
import json

import requests

url = "https://raw.githubusercontent.com/dandi-cache/<cache-name>/refs/heads/dist/derivatives/<cache_name>.jsonl.gz"
response = requests.get(url)
lines = gzip.decompress(data=response.content).decode("utf-8").splitlines()
<cache_name> = [json.loads(line) for line in lines]
```

### Save to file

```bash
curl https://raw.githubusercontent.com/dandi-cache/<cache-name>/refs/heads/dist/derivatives/<cache_name>.jsonl.gz -o <cache_name>.jsonl.gz
```



## Repeated use

If you plan on using this cache regularly, clone the `dist` branch of this repository:

```bash
git clone --branch dist https://github.com/dandi-cache/<cache-name>.git
```

Or, if you prefer [DataLad](https://www.datalad.org/):

```bash
datalad clone https://github.com/dandi-cache/<cache-name>.git --branch derivatives
```

Then set up a CRON on your system to pull the latest version of the cache at your desired frequency.

For example, through `crontab -e`, add:

```bash
0 0 * * * git -C /path/to/<cache-name> pull
```

This will minimize data overhead by only loading the most recent changes.



## How it works

This cache template demonstrates how generated results of the code branch and records every update with full provenance.

It uses three branches:

- **`main`** holds only the code of the update logic, the runtime container definition, and the CI workflows (including building and distributing the container images).
- [**`derivatives`**](https://github.com/dandi-cache/cache-template/tree/derivatives) is a persistent [DataLad](https://www.datalad.org/) dataset on its own branch. Each update is recorded there with `datalad containers-run`, so every revision carries full provenance of the exact command, the input subdataset commit, the output diff, and the runtime container image digest.
- **`dist`** is the lightweight publication artifact consumed by downstream users and preferred for one-time downloads.

The processing runs inside a published container image (`ghcr.io/dandi-cache/<cache-name>:latest`) that holds only the pinned runtime environment.

The orchestration lives in [`code/update_pipeline.sh`](code/update_pipeline.sh); the actual cache logic lives in [`code/update.py`](code/update.py).

The repository is described as a [BIDS study dataset](https://bids-specification.readthedocs.io/en/stable/common-principles.html#study-dataset) via [`dataset_description.json`](dataset_description.json) (`DatasetType: "study"`). Future enhancements may improve the provenance tracking through this mechanism in line with BEP028.



### Input modes

A cache's inputs can come from one of three first-class sources. Select the one that fits in [`code/update_pipeline.sh`](code/update_pipeline.sh) (the `INPUT_SUBDATASET_URL` TODO) and [`code/update.py`](code/update.py):

1. **Upstream DataLad dataset.** Set `INPUT_SUBDATASET_URL` to register an upstream dataset as an input subdataset. It is cloned into the `derivatives` dataset and pinned via `--input` in the provenance of every run, so each result records the exact input commit it was computed from.
2. **Local `sourcedata` directory.** Inputs live under the dataset's own `sourcedata/` (e.g. committed fixtures). Leave `INPUT_SUBDATASET_URL` empty.
3. **First-in-chain / no input dataset.** The cache fetches its own inputs over the network at run time (e.g. it queries a remote API or archive). Leave `INPUT_SUBDATASET_URL` empty: there is no input dataset to pin, so no `--input` provenance is declared. Because the inputs are pulled at run time, **the processing container requires outbound network access** — the runtime environment must allow the container to reach the upstream source.



## Repository setup

After generating a repository from this template:

1. Replace every `<cache-name>` / `<cache_name>` placeholder and resolve the `TODO` markers (the update schedule, the cache logic, the input dataset, the notification recipients). Fill in the placeholder fields in [`dataset_description.json`](dataset_description.json) (`Name`, `Authors`; `License` defaults to `CC-BY-4.0` — change it if this cache uses a different license).
2. Add this cache's processing dependencies to [`envs/pyproject.toml`](envs/pyproject.toml).
3. Specify the [`code/update.py`](code/update.py) protocol.
4. Remove the template scaffolding from the local README — these sections document the template itself, not the generated cache. Delete the **How it works** section (including its **Input modes** subsection, once you have chosen and wired up an input mode) and this **Repository setup** section (including the **Local development** subsection below).



### Local development

The container image is the authoritative runtime, but you can recreate the environment locally with [uv](https://docs.astral.sh/uv/) for debugging:

```bash
uv run --project envs python code/update.py
```
