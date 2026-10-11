# DANDI Cache: `content-id-to-usage-dandiset-path`

A one-to-one mapping from content IDs to a single (dandiset ID, asset path) pair, resolved from the multi-valued entries in [`dandi-cache/content-id-to-dandiset-paths`](https://github.com/dandi-cache/content-id-to-dandiset-paths).

When a content ID maps to multiple dandisets, the dandiset that came into existence first is preferred; when it maps to multiple paths within one dandiset, the asset path that was created first is preferred.
This approach is entirely heuristic, is technically 'not true', but is also not 'any more false' than what we currently have.

This cache may be retired when or if full audit tracking or watermark enforcement is ever fully integrated.

Updated frequently.

Primarily for use by developers.

A second output, `derivatives/content_id_to_resolved_from.zarr`, records a short digest of the upstream entry each heuristic answer was resolved from.
It is bookkeeping rather than data, and it is what makes the cache accumulative: a content ID's answer depends on nothing but its own upstream entry, so an unchanged digest means the recorded answer is still correct and costs no further network round trips.
A content ID that gains a Dandiset or a path gets a different digest and is resolved again.
Content IDs that were already unique upstream need no heuristic and so have no entry here.



## One-time use

`dist` publishes each output as a [Zarr](https://zarr.dev/) store: the cache itself is `derivatives/content_id_to_usage_dandiset_path.zarr/`, with the cache's other outputs beside it as stores of the same kind (`derivatives/content_id_to_resolved_from.zarr/`).
A store is one array of [JSON Lines](https://jsonlines.org/) text cut into chunk files, each holding whole lines and gzip-compressed.
Where a key appears more than once, the last line is its current entry, and the published copy has no repeats.
It can be read with the standard library alone, or opened remotely with any Zarr library.

### Python API (recommended)

The chunk files are gzip-compressed [JSON Lines](https://jsonlines.org/): list them, then read them in order as you would any JSONL files.

```python
import gzip, json, urllib.request

chunks = "https://api.github.com/repos/dandi-cache/content-id-to-usage-dandiset-path/contents/derivatives/content_id_to_usage_dandiset_path.zarr/stream/c?ref=dist"
files = sorted((entry["download_url"] for entry in json.load(urllib.request.urlopen(chunks))), key=lambda url: int(url.rsplit("/", 1)[1]))
content_id_to_usage_dandiset_path = {key: value for url in files for line in gzip.decompress(urllib.request.urlopen(url).read()).decode().splitlines() if line for key, value in json.loads(line).items()}
```

Each line is a record of the form:

```json
{"<content_id>": {"<dandiset_id>": "<path>"}}
```

An entry is not filed under its key: the chunks hold the entries in the order they were recorded, so looking up one means reading through them.
To look up many, read it once as above, or clone the `derivatives` branch.

### Remote Zarr store

Anything that opens Zarr can open the store over HTTP, reading only the chunks it needs.
The group's `zarr.json` carries the array's metadata, so opening it takes one request:

```bash
pip install "zarr>=3" fsspec aiohttp
```

```python
import json

import zarr

url = "https://raw.githubusercontent.com/dandi-cache/content-id-to-usage-dandiset-path/refs/heads/dist/derivatives/content_id_to_usage_dandiset_path.zarr"
stream = zarr.open_group(url, mode="r")["stream"]
print(stream.shape, stream.chunks)  # Bytes in all, and bytes per chunk.

# The first chunk, fetched and decompressed on its own: whole lines, each `{"<key>": <value>}`.
first_chunk = stream[: stream.chunks[0]].tobytes().decode("utf-8")
for line in first_chunk.splitlines():
    if line.strip():
        print(json.loads(line))
        break
```

Reading `stream[:]` returns the whole text; to read it in pieces, step through it by `stream.chunks[0]`, as each chunk is fetched whole.

### Save to file

A raw URL cannot be listed, so to fetch every chunk at once clone the `dist` branch, which holds nothing else:

```bash
git clone --branch dist --depth 1 https://github.com/dandi-cache/content-id-to-usage-dandiset-path.git
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

Each output is a store in the clone, with every chunk gzip-compressed:

```python
import gzip, json, pathlib

chunks = sorted(pathlib.Path("content-id-to-usage-dandiset-path/derivatives/content_id_to_usage_dandiset_path.zarr/stream/c").iterdir(), key=lambda path: int(path.name))
content_id_to_usage_dandiset_path = {key: value for chunk in chunks for line in gzip.decompress(chunk.read_bytes()).decode().splitlines() if line for key, value in json.loads(line).items()}
```


## How this cache is built

The orchestration, the runtime library and the CI all come from elsewhere, so this repository holds only what is specific to this cache: `cache.toml` (what it is), `code/update.py` (the two resolution heuristics), `envs/pyproject.toml` (its dependencies, which the base image now supplies) and the schedule in `.github/workflows/update.yml`.

The pipeline is [`dandi-cache-utils`](https://github.com/dandi-cache/dandi-cache-utils), vendored into the runtime image this cache is built `FROM`, and the workflows call the shared actions in [`dandi-cache-action`](https://github.com/dandi-cache/dandi-cache-action).
A gap in any of them is fixed there, where every cache gets the fix, rather than worked around here.
