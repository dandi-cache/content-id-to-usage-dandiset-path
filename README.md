# DANDI Cache: `content-id-to-usage-dandiset-path`

A one-to-one mapping from content IDs to a single (dandiset ID, asset path) pair, resolved from the multi-valued entries in [`dandi-cache/content-id-to-dandiset-paths`](https://github.com/dandi-cache/content-id-to-dandiset-paths).

When a content ID maps to multiple dandisets, the dandiset that came into existence first is preferred; when it maps to multiple paths within one dandiset, the asset path that was created first is preferred.
This approach is entirely heuristic, is technically 'not true', but is also not 'any more false' than what we currently have.

This cache may be retired when or if full audit tracking or watermark enforcement is ever fully integrated.

Updated frequently.

Primarily for use by developers.

A second output, `content_id_to_resolved_from`, records a short digest of the upstream entry each heuristic answer was resolved from.
It is bookkeeping rather than data, and it is what makes the cache accumulative: a content ID's answer depends on nothing but its own upstream entry, so an unchanged digest means the recorded answer is still correct and costs no further network round trips.
A content ID that gains a Dandiset or a path gets a different digest and is resolved again.
Content IDs that were already unique upstream need no heuristic and so have no entry here.



## One-time use

`dist` publishes each output as one compressed [JSON Lines](https://jsonlines.org/) file.
The cache itself is `derivatives/content_id_to_usage_dandiset_path.jsonl.gz`, with the other output beside it as `derivatives/content_id_to_resolved_from.jsonl.gz`.

### Python API (recommended)

```python
import gzip
import json
import urllib.request

url = "https://raw.githubusercontent.com/dandi-cache/content-id-to-usage-dandiset-path/refs/heads/dist/derivatives/content_id_to_usage_dandiset_path.jsonl.gz"
with urllib.request.urlopen(url) as response:
    lines = gzip.decompress(data=response.read()).decode("utf-8").splitlines()
content_id_to_usage_dandiset_path = {key: value for line in lines if line for key, value in json.loads(line).items()}
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

### Local development

The container image is the authoritative runtime, but you can recreate the environment locally with [uv](https://docs.astral.sh/uv/) for debugging:

```bash
uv run --project envs python code/update.py
```

Each output is one compressed JSON Lines file in the clone of `dist`:

```python
import gzip
import json
import pathlib

path = pathlib.Path("content-id-to-usage-dandiset-path/derivatives/content_id_to_usage_dandiset_path.jsonl.gz")
lines = gzip.decompress(data=path.read_bytes()).decode("utf-8").splitlines()
content_id_to_usage_dandiset_path = {key: value for line in lines if line for key, value in json.loads(line).items()}
```


## Reading the `derivatives` branch

Each output is kept on the `derivatives` branch as a [Zarr](https://zarr.dev/) v3 store of JSON Lines: a one-dimensional `uint8` array whose bytes are the lines, padded with newlines to the end of its last chunk.
A key recorded more than once keeps its last line, which the dictionary below does by itself.

```python
import json

import zarr  # zarr>=3, with fsspec and aiohttp to read over HTTP

url = "https://raw.githubusercontent.com/dandi-cache/content-id-to-usage-dandiset-path/derivatives/derivatives/content_id_to_usage_dandiset_path.zarr"
stream = zarr.open_group(url, mode="r")["stream"]
lines = stream[:].tobytes().decode("utf-8").splitlines()
content_id_to_usage_dandiset_path = {key: value for line in lines if line for key, value in json.loads(line).items()}
```

The other output, `content_id_to_resolved_from.zarr`, is a store beside it under the same `derivatives/` path.

Without Zarr, each chunk is a plain file of whole lines at `derivatives/content_id_to_usage_dandiset_path.zarr/stream/c/<n>`, read in order of `n`.


## How this cache is built

The orchestration, the runtime library and the CI all come from elsewhere, so this repository holds only what is specific to this cache: `cache.toml` (what it is), `code/update.py` (the two resolution heuristics), `envs/pyproject.toml` (its dependencies, which the base image now supplies) and the schedule in `.github/workflows/update.yml`.

The pipeline is [`dandi-cache-utils`](https://github.com/dandi-cache/dandi-cache-utils), vendored into the runtime image this cache is built `FROM`, and the workflows call the shared actions in [`dandi-cache-action`](https://github.com/dandi-cache/dandi-cache-action).
A gap in any of them is fixed there, where every cache gets the fix, rather than worked around here.
