# DANDI Cache: Content ID → 'Usage' Dandiset paths

A one-to-one mapping from content IDs to a single (dandiset ID, asset path) pair, resolved from the non-unique entries in [`dandi-cache/content-id-to-unique-dandiset-path`](https://github.com/dandi-cache/content-id-to-unique-dandiset-path).

This approach is entirely heuristic, is technically 'not true', but is also not any more false than what we have currently anyhow.

This cache may be retired when or if fully audit tracking or watermark enforcement is ever fully integrated.

Updated frequently.

Primarily for use by developers.



## One-time use

If you only plan to use this cache infrequently or from disparate locations, you can directly download the latest version of the cache as a minified and compressed JSON file:

### Python API (recommended)

```python
import gzip
import json

import requests

url = "https://raw.githubusercontent.com/dandi-cache/content-id-to-usage-dandiset-path/refs/heads/min/derivatives/content_id_to_usage_dandiset_path.min.json.gz"
response = requests.get(url)
content_id_to_usage_dandiset_path = json.loads(gzip.decompress(data=response.content))
```

### Save to file

```bash
curl https://raw.githubusercontent.com/dandi-cache/content-id-to-usage-dandiset-path/refs/heads/min/derivatives/content_id_to_usage_dandiset_path.min.json.gz -o content_id_to_usage_dandiset_path.min.json.gz
```



## Repeated use

If you plan on using this cache regularly, clone this repository:

```bash
git clone https://github.com/dandi-cache/content-id-to-usage-dandiset-path.git
```

Then set up a CRON on your system to pull the latest version of the cache at your desired frequency.

For example, through `crontab -e`, add:

```bash
0 0 * * * git -C /path/to/content-id-to-usage-dandiset-path pull
```

This will minimize data overhead by only loading the most recent changes.
