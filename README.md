# DANDI Cache: `content-id-to-usage-dandiset-path`

A one-to-one mapping from content IDs to a single (dandiset ID, asset path) pair, resolved from the multi-valued entries in [`dandi-cache/content-id-to-dandiset-paths`](https://github.com/dandi-cache/content-id-to-dandiset-paths).

When a content ID maps to multiple dandisets, the dandiset that came into existence first is preferred; when it maps to multiple paths within one dandiset, the asset path that was created first is preferred.
This approach is entirely heuristic, is technically 'not true', but is also not 'any more false' than what we currently have.

This cache may be retired when or if full audit tracking or watermark enforcement is ever fully integrated.

Updated frequently.

Primarily for use by developers.

A second file, `derivatives/content_id_to_resolved_from.jsonl`, records a short digest of the upstream entry each heuristic answer was resolved from.
It is bookkeeping rather than data, and it is what makes the cache accumulative: a content ID's answer depends on nothing but its own upstream entry, so an unchanged digest means the recorded answer is still correct and costs no further network round trips.
A content ID that gains a Dandiset or a path gets a different digest and is resolved again.
Content IDs that were already unique upstream need no heuristic and so have no entry here.



## One-time use

If you only plan to use this cache infrequently or from disparate locations, you can directly download the latest version of the cache as a compressed [JSON Lines](https://jsonlines.org/) file from the `dist` branch:

### Python API (recommended)

```python
import gzip
import json
import urllib.request

base = "https://raw.githubusercontent.com/dandi-cache/content-id-to-usage-dandiset-path/refs/heads/dist/derivatives"
content_id_to_usage_dandiset_path = []
for digit in "0123456789abcdef":
    with urllib.request.urlopen(f"{base}/content_id_to_usage_dandiset_path_{digit}.jsonl.gz") as response:
        lines = gzip.decompress(data=response.read()).decode("utf-8").splitlines()
    content_id_to_usage_dandiset_path += [json.loads(line) for line in lines]
```

The cache is split across sixteen files by the first hexadecimal digit of the content ID, `content_id_to_usage_dandiset_path_0.jsonl.gz` to `content_id_to_usage_dandiset_path_f.jsonl.gz`, because as one file it outgrew GitHub's 100 MiB limit.
A content ID's entry is in the file named by its first digit.

Each line is a record of the form:

```json
{"<content_id>": {"<dandiset_id>": "<path>"}}
```

### Save to file

```bash
for digit in 0 1 2 3 4 5 6 7 8 9 a b c d e f; do
  curl -O "https://raw.githubusercontent.com/dandi-cache/content-id-to-usage-dandiset-path/refs/heads/dist/derivatives/content_id_to_usage_dandiset_path_${digit}.jsonl.gz"
done
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
