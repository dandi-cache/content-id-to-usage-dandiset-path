# DANDI Cache: `content-id-to-usage-dandiset-path`

A one-to-one mapping from content IDs to a single (dandiset ID, asset path) pair, resolved from the multi-valued entries in [`dandi-cache/content-id-to-dandiset-paths`](https://github.com/dandi-cache/content-id-to-dandiset-paths).

When a content ID maps to multiple dandisets, the dandiset that came into existence first is preferred; when it maps to multiple paths within one dandiset, the asset path that was created first is preferred. This approach is entirely heuristic, is technically 'not true', but is also not 'any more false' than what we currently have.

This cache may be retired when or if full audit tracking or watermark enforcement is ever fully integrated.

Updated frequently.

Primarily for use by developers.



## One-time use

If you only plan to use this cache infrequently or from disparate locations, you can directly download the latest version of the cache as a compressed [JSON Lines](https://jsonlines.org/) file from the `dist` branch:

### Python API (recommended)

```python
import gzip
import json

import requests

url = "https://raw.githubusercontent.com/dandi-cache/content-id-to-usage-dandiset-path/refs/heads/dist/derivatives/content_id_to_usage_dandiset_path.jsonl.gz"
response = requests.get(url)
lines = gzip.decompress(data=response.content).decode("utf-8").splitlines()
content_id_to_usage_dandiset_path = [json.loads(line) for line in lines]
```

Each line is a record of the form:

```json
{"<content_id>": {"<dandiset_id>": "<path>"}}
```

### Save to file

```bash
curl https://raw.githubusercontent.com/dandi-cache/content-id-to-usage-dandiset-path/refs/heads/dist/derivatives/content_id_to_usage_dandiset_path.jsonl.gz -o content_id_to_usage_dandiset_path.jsonl.gz
```



## Repeated use

If you plan on using this cache regularly, clone the `dist` branch of this repository:

```bash
git clone --branch dist https://github.com/dandi-cache/content-id-to-usage-dandiset-path.git
```

Or, if you prefer [DataLad](https://www.datalad.org/):

```bash
datalad clone https://github.com/dandi-cache/content-id-to-usage-dandiset-path.git --branch derivatives
```

Then set up a CRON on your system to pull the latest version of the cache at your desired frequency.

For example, through `crontab -e`, add:

```bash
0 0 * * * git -C /path/to/content-id-to-usage-dandiset-path pull
```

This will minimize data overhead by only loading the most recent changes.



## How it works

This cache uses three branches:

- **`main`** holds only the code of the update logic, the runtime container definition, and the CI workflows (including building and distributing the container images).
- **`derivatives`** is a persistent [DataLad](https://www.datalad.org/) dataset on its own branch. Each update is recorded there with `datalad containers-run`, so every revision carries full provenance of the exact command, the input subdataset commit, the output diff, and the runtime container image digest.
- **`dist`** is the lightweight publication artifact consumed by downstream users and preferred for one-time downloads.

The input is the [`dandi-cache/content-id-to-dandiset-paths`](https://github.com/dandi-cache/content-id-to-dandiset-paths) cache (its `derivatives` branch), registered as an input subdataset under `sourcedata/` in the `derivatives` dataset and pinned in the provenance of every run. The update logic additionally queries the [DANDI Archive](https://dandiarchive.org/) API at run time to resolve the non-unique entries (dandiset and asset creation times).

The processing runs inside a published container image (`ghcr.io/dandi-cache/content-id-to-usage-dandiset-path:latest`) that holds only the pinned runtime environment.

The orchestration lives in [`code/update_pipeline.sh`](code/update_pipeline.sh); the actual cache logic lives in [`code/update.py`](code/update.py).

The repository is described as a [BIDS study dataset](https://bids-specification.readthedocs.io/en/stable/common-principles.html#study-dataset) via [`dataset_description.json`](dataset_description.json) (`DatasetType: "study"`). Future enhancements may improve the provenance tracking through this mechanism in line with BEP028.



### Local development

The container image is the authoritative runtime, but you can recreate the environment locally with [uv](https://docs.astral.sh/uv/) for debugging:

```bash
uv run --project envs python code/update.py
```
