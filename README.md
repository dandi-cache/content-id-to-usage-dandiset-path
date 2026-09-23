# DANDI Cache: `content-id-to-usage-dandiset-path`

A one-to-one mapping from content IDs to a single (dandiset ID, asset path) pair, resolved from the multi-valued entries in [`dandi-cache/content-id-to-dandiset-paths`](https://github.com/dandi-cache/content-id-to-dandiset-paths).

When a content ID maps to multiple dandisets, the dandiset that came into existence first is preferred; when it maps to multiple paths within one dandiset, the asset path that was created first is preferred.
This approach is entirely heuristic, is technically 'not true', but is also not 'any more false' than what we currently have.

This cache may be retired when or if full audit tracking or watermark enforcement is ever fully integrated.

Updated frequently.

Primarily for use by developers.



## One-time use

If you only plan to use this cache infrequently or from disparate locations, you can directly download the latest version of the cache as a compressed [JSON Lines](https://jsonlines.org/) file from the `dist` branch:

### Python API (recommended)

```python
import gzip
import json
import urllib.request

url = "https://raw.githubusercontent.com/dandi-cache/content-id-to-usage-dandiset-path/refs/heads/dist/derivatives/content_id_to_usage_dandiset_path.jsonl.gz"
with urllib.request.urlopen(url) as response:
    lines = gzip.decompress(data=response.read()).decode("utf-8").splitlines()
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



### Local development

The container image is the authoritative runtime, but you can recreate the environment locally with [uv](https://docs.astral.sh/uv/) for debugging:

```bash
uv run --project envs python code/update.py
```


## How this cache is built

The orchestration, the runtime library and the CI all come from elsewhere, so this repository holds only what is specific to this cache: `cache.toml` (what it is), `code/update.py` (the two resolution heuristics), `envs/pyproject.toml` (its dependencies, which the base image now supplies) and the schedule in `.github/workflows/update.yml`.

The pipeline is [`dandi-cache-utils`](https://github.com/dandi-cache/dandi-cache-utils), vendored into the runtime image this cache is built `FROM`, and the workflows call the shared actions in [`dandi-cache-action`](https://github.com/dandi-cache/dandi-cache-action).
A gap in any of them is fixed there, where every cache gets the fix, rather than worked around here.
