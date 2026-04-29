# DANDI Cache: `<cache-name>`

`<A short description of what this cache contains and how it is derived.>`

Updated frequently.

Primarily for use by developers.



## One-time use

If you only plan to use this cache infrequently or from disparate locations, you can directly download the latest version of the cache as a minified and compressed JSON file:

> **Note:** Throughout this template, `<cache-name>` refers to the hyphenated repository name (e.g., `my-cache`) and `<cache_name>` refers to the underscored form used for file and variable names (e.g., `my_cache`).

### Python API (recommended)

```python
import gzip
import json

import requests

url = "https://raw.githubusercontent.com/dandi-cache/<cache-name>/refs/heads/min/derivatives/<cache_name>.min.json.gz"
response = requests.get(url)
<cache_name> = json.loads(gzip.decompress(data=response.content))
```

### Save to file

```bash
curl https://raw.githubusercontent.com/dandi-cache/<cache-name>/refs/heads/min/derivatives/<cache_name>.min.json.gz -o <cache_name>.min.json.gz
```



## Repeated use

If you plan on using this cache regularly, clone this repository:

```bash
git clone https://github.com/dandi-cache/<cache-name>.git
```

Then set up a CRON on your system to pull the latest version of the cache at your desired frequency.

For example, through `crontab -e`, add:

```bash
0 0 * * * git -C /path/to/<cache-name> pull
```

This will minimize data overhead by only loading the most recent changes.
