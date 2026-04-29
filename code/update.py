import pathlib
import sys
import time
from datetime import datetime

import requests
import yaml

DANDI_API_BASE = "https://api.dandiarchive.org/api"
_REQUEST_TIMEOUT = 30
_MAX_RETRIES = 5
_RETRY_BACKOFF = 2.0


def _get(url: str, params: dict | None = None) -> dict:
    """Make a GET request to the DANDI API with retries."""
    for attempt in range(_MAX_RETRIES):
        try:
            response = requests.get(url, params=params, timeout=_REQUEST_TIMEOUT)
            if response.status_code == 429:
                retry_after = float(response.headers.get("Retry-After", _RETRY_BACKOFF * (attempt + 1)))
                time.sleep(retry_after)
                continue
            response.raise_for_status()
            return response.json()
        except requests.RequestException as exc:
            if attempt == _MAX_RETRIES - 1:
                raise
            time.sleep(_RETRY_BACKOFF * (attempt + 1))
            print(f"  Retrying ({attempt + 1}/{_MAX_RETRIES}) after error: {exc}", flush=True)
    raise RuntimeError(f"All {_MAX_RETRIES} retries exhausted for {url}")


def _get_dandiset_created(dandiset_id: str, cache: dict[str, datetime]) -> datetime:
    """Return the creation datetime for a dandiset, using a local cache to avoid redundant calls."""
    if dandiset_id not in cache:
        padded = dandiset_id.zfill(6)
        data = _get(f"{DANDI_API_BASE}/dandisets/{padded}/")
        created_str = data["created"]
        cache[dandiset_id] = datetime.fromisoformat(created_str.replace("Z", "+00:00"))
    return cache[dandiset_id]


def _get_earliest_asset_path(dandiset_id: str, paths: list[str], cache: dict[tuple[str, str], datetime | None]) -> str:
    """Return the path from *paths* whose asset was created earliest in *dandiset_id*."""
    padded = dandiset_id.zfill(6)
    earliest_path = paths[0]
    earliest_created: datetime | None = None

    for path in paths:
        key = (dandiset_id, path)
        if key not in cache:
            data = _get(
                f"{DANDI_API_BASE}/dandisets/{padded}/versions/draft/assets/",
                params={"path": path},
            )
            results = data.get("results", [])
            if results:
                created_str = results[0]["created"]
                cache[key] = datetime.fromisoformat(created_str.replace("Z", "+00:00"))
            else:
                cache[key] = None

        created = cache[key]
        if created is not None and (earliest_created is None or created < earliest_created):
            earliest_created = created
            earliest_path = path

    return earliest_path


def _run(base_directory: pathlib.Path, input_directory: pathlib.Path, /) -> None:
    """Resolve non-unique content-ID mappings and write a one-to-one output mapping.

    Parameters
    ----------
    base_directory:
        Root of *this* repository (output is written to ``derivatives/`` here).
    input_directory:
        Path to the ``derivatives/`` folder of a local clone of
        ``dandi-cache/content-id-to-unique-dandiset-path``.
    """
    multiple_dandisets_path = input_directory / "multiple_dandisets.yaml"
    multiple_paths_path = input_directory / "multiple_paths_same_dandiset.yaml"

    if not multiple_dandisets_path.exists():
        raise FileNotFoundError(f"Input file not found: {multiple_dandisets_path}")
    if not multiple_paths_path.exists():
        raise FileNotFoundError(f"Input file not found: {multiple_paths_path}")

    with multiple_dandisets_path.open(mode="r") as file_stream:
        multiple_dandisets: dict = yaml.safe_load(file_stream) or {}

    with multiple_paths_path.open(mode="r") as file_stream:
        multiple_paths_same_dandiset: dict = yaml.safe_load(file_stream) or {}

    cache: dict[str, dict[str, str]] = {}
    dandiset_created_cache: dict[str, datetime] = {}
    asset_created_cache: dict[tuple[str, str], datetime | None] = {}

    # Resolve entries where the same content-ID appears in multiple dandisets.
    # Heuristic: prefer the dandiset that came into existence first.
    print(f"Resolving {len(multiple_dandisets)} multiple-dandiset entries...", flush=True)
    for idx, (content_id, dandisets) in enumerate(multiple_dandisets.items(), start=1):
        if idx % 100 == 0:
            print(f"  {idx}/{len(multiple_dandisets)}", flush=True)

        # dandisets is a dict of {dandiset_id: [list of paths]}.
        # PyYAML may parse numeric keys as integers, so normalize to strings.
        normalized: dict[str, list[str]] = {str(k): [str(p) for p in v] for k, v in dandisets.items()}
        earliest_dandiset_id = min(
            normalized.keys(),
            key=lambda d: _get_dandiset_created(d, dandiset_created_cache),
        )

        paths: list[str] = normalized[earliest_dandiset_id]
        if len(paths) == 1:
            path = paths[0]
        else:
            path = _get_earliest_asset_path(earliest_dandiset_id, paths, asset_created_cache)

        cache[content_id] = {earliest_dandiset_id: path}

    # Resolve entries where the same content-ID appears in multiple paths within one dandiset.
    # Heuristic: prefer the asset path that was created first.
    print(f"Resolving {len(multiple_paths_same_dandiset)} multiple-path entries...", flush=True)
    for idx, (content_id, dandisets) in enumerate(multiple_paths_same_dandiset.items(), start=1):
        if idx % 100 == 0:
            print(f"  {idx}/{len(multiple_paths_same_dandiset)}", flush=True)

        if not dandisets:
            raise ValueError(f"Empty dandisets mapping for content_id={content_id!r}")
        dandiset_id, paths_raw = next(iter(dandisets.items()))
        dandiset_id = str(dandiset_id)
        paths = [str(p) for p in paths_raw]

        path = _get_earliest_asset_path(dandiset_id, paths, asset_created_cache)
        cache[content_id] = {dandiset_id: path}

    output_file_path = base_directory / "derivatives" / "content_id_to_usage_dandiset_path.yaml"
    print(f"Writing {len(cache)} entries to {output_file_path}", flush=True)
    with output_file_path.open(mode="w") as file_stream:
        yaml.safe_dump(data=cache, stream=file_stream)


if __name__ == "__main__":
    repo_head = pathlib.Path(__file__).parent.parent

    if len(sys.argv) > 1:
        input_dir = pathlib.Path(sys.argv[1])
    else:
        input_dir = repo_head / "sourcedata" / "content-id-to-unique-dandiset-path" / "derivatives"

    _run(repo_head, input_dir)
