import pathlib
import sys
from datetime import datetime

import yaml
from dandi.dandiapi import DandiAPIClient, RemoteDandiset
from dandi.exceptions import NotFoundError


def _get_dandiset(
    client: DandiAPIClient, dandiset_id: str, cache: dict[str, RemoteDandiset | None]
) -> RemoteDandiset | None:
    """Return a RemoteDandiset for *dandiset_id*, or None if the dandiset no longer exists."""
    if dandiset_id not in cache:
        try:
            dandiset = client.get_dandiset(dandiset_id)
            # Access .created to trigger the lazy load and detect a missing dandiset early.
            _ = dandiset.created
            cache[dandiset_id] = dandiset
        except NotFoundError:
            print(f"  Warning: dandiset {dandiset_id} not found, skipping", flush=True)
            cache[dandiset_id] = None
    return cache[dandiset_id]


def _get_earliest_asset_path(
    dandiset: RemoteDandiset,
    paths: list[str],
    cache: dict[tuple[str, str], datetime | None],
) -> str:
    """Return the path from *paths* whose asset was created earliest in *dandiset*.

    Falls back to the first path if no asset timestamps can be retrieved.
    """
    dandiset_id = dandiset.identifier
    earliest_path = paths[0]
    earliest_created: datetime | None = None

    for path in paths:
        key = (dandiset_id, path)
        if key not in cache:
            try:
                asset = dandiset.get_asset_by_path(path)
                cache[key] = asset.created
            except NotFoundError:
                print(f"  Warning: asset {path!r} not found in dandiset {dandiset_id}", flush=True)
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
    dandiset_cache: dict[str, RemoteDandiset | None] = {}
    asset_created_cache: dict[tuple[str, str], datetime | None] = {}

    client = DandiAPIClient()

    # Resolve entries where the same content-ID appears in multiple dandisets.
    # Heuristic: prefer the dandiset that came into existence first.
    print(f"Resolving {len(multiple_dandisets)} multiple-dandiset entries...", flush=True)
    for idx, (content_id, dandisets) in enumerate(multiple_dandisets.items(), start=1):
        if idx % 100 == 0:
            print(f"  {idx}/{len(multiple_dandisets)}", flush=True)

        # dandisets is a dict of {dandiset_id: [list of paths]}.
        # PyYAML may parse numeric keys as integers, so normalize to strings.
        normalized: dict[str, list[str]] = {str(k): [str(p) for p in v] for k, v in dandisets.items()}

        # Resolve dandiset objects; exclude dandisets that have been deleted.
        ds_by_id = {d: _get_dandiset(client, d, dandiset_cache) for d in normalized}
        available = {d: paths for d, paths in normalized.items() if ds_by_id[d] is not None}
        if not available:
            print(f"  Warning: all dandisets for content_id={content_id!r} not found, skipping", flush=True)
            continue

        earliest_dandiset_id = min(
            available.keys(),
            key=lambda d: ds_by_id[d].created,  # type: ignore[union-attr]
        )

        paths: list[str] = available[earliest_dandiset_id]
        if len(paths) == 1:
            path = paths[0]
        else:
            path = _get_earliest_asset_path(ds_by_id[earliest_dandiset_id], paths, asset_created_cache)  # type: ignore[arg-type]

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

        # Skip if the dandiset has been deleted.
        dandiset = _get_dandiset(client, dandiset_id, dandiset_cache)
        if dandiset is None:
            print(f"  Warning: dandiset {dandiset_id} for content_id={content_id!r} not found, skipping", flush=True)
            continue

        path = _get_earliest_asset_path(dandiset, paths, asset_created_cache)
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
