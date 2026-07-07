import argparse
import datetime
import itertools
import json
import pathlib

import dandi.dandiapi
import dandi.exceptions

# Testing mode processes only this many entries of each category (already-unique,
# multiple-dandisets, multiple-paths) and writes to its own designated files
# (`derivatives/testing.jsonl` and `testing_`-prefixed logs), leaving the real cache
# untouched.
_TESTING_LIMIT = 10
_CACHE_FILE_NAME = "content_id_to_usage_dandiset_path.jsonl"
_TESTING_FILE_NAME = "testing.jsonl"


def _get_earliest_asset_path(
    *,
    dandiset: dandi.dandiapi.RemoteDandiset,
    paths: list[str],
    asset_created_cache: dict[tuple[str, str], datetime.datetime | None],
    resolution_failures: list[str],
    processing_step: str,
) -> str:
    """
    Return the path from *paths* whose asset was created earliest in *dandiset*.

    Falls back to the first path if no asset timestamps can be retrieved.
    """
    dandiset_id = dandiset.identifier
    earliest_path = paths[0]
    earliest_created: datetime.datetime | None = None

    for path in paths:
        key = (dandiset_id, path)
        if key not in asset_created_cache:
            try:
                asset = dandiset.get_asset_by_path(path)
                asset_created_cache[key] = asset.created
            except dandi.exceptions.NotFoundError:
                resolution_failures.append(
                    f"Asset not found: dandiset_id={dandiset_id!r}, "
                    f"path={path!r}, processing_step={processing_step!r}"
                )
                asset_created_cache[key] = None
                continue

        created = asset_created_cache[key]
        if created is not None and (earliest_created is None or created < earliest_created):
            earliest_created = created
            earliest_path = path

    return earliest_path


def _run(base_directory: pathlib.Path, testing: bool) -> None:
    """Resolve non-unique content-ID mappings and write a one-to-one output mapping."""
    input_file_path = (
        base_directory
        / "sourcedata"
        / "content-id-to-dandiset-paths"
        / "derivatives"
        / "content_id_to_dandiset_paths.jsonl"
    )
    if not input_file_path.exists():
        raise FileNotFoundError(f"Input file not found: {input_file_path}")

    # Each line is a single-entry mapping of {content_id: {dandiset_id: [paths, ...]}}.
    content_id_to_dandiset_paths: dict[str, dict[str, list[str]]] = {}
    with input_file_path.open(mode="r") as file_stream:
        for line in file_stream:
            content_id_to_dandiset_paths.update(json.loads(line))

    # Split the entries into the already-unique mappings and the two non-unique cases.
    content_id_to_usage_dandiset_path: dict[str, dict[str, str]] = {}
    multiple_dandisets: dict[str, dict[str, list[str]]] = {}
    multiple_paths_same_dandiset: dict[str, dict[str, list[str]]] = {}
    for content_id, dandisets in content_id_to_dandiset_paths.items():
        if not dandisets:
            raise ValueError(f"Empty dandisets mapping for content_id={content_id!r}")
        if len(dandisets) > 1:
            multiple_dandisets[content_id] = dandisets
            continue

        dandiset_id, paths = next(iter(dandisets.items()))
        if len(paths) > 1:
            multiple_paths_same_dandiset[content_id] = {dandiset_id: paths}
            continue

        content_id_to_usage_dandiset_path[content_id] = {dandiset_id: paths[0]}

    if testing:
        # Testing run: keep only the first few entries of each category, so the run is fast
        # but still exercises the passthrough and both resolution heuristics. The upfront
        # dandiset pass below still runs in full (it is required for resolution and is the
        # genuine DANDI API interaction being smoke-tested).
        content_id_to_usage_dandiset_path = dict(
            itertools.islice(content_id_to_usage_dandiset_path.items(), _TESTING_LIMIT)
        )
        multiple_dandisets = dict(itertools.islice(multiple_dandisets.items(), _TESTING_LIMIT))
        multiple_paths_same_dandiset = dict(itertools.islice(multiple_paths_same_dandiset.items(), _TESTING_LIMIT))

    asset_created_cache: dict[tuple[str, str], datetime.datetime | None] = {}
    resolution_failures: list[str] = []
    dandiset_failures: list[str] = []

    client = dandi.dandiapi.DandiAPIClient()

    # One initial pass over all dandisets to collect creation times.
    # Dandisets that have been deleted will simply be absent from this mapping.
    print("Fetching all dandiset creation times...", flush=True)
    dandiset_created_on: dict[str, datetime.datetime] = {}
    dandiset_by_id: dict[str, dandi.dandiapi.RemoteDandiset] = {}
    for dandiset in client.get_dandisets():
        dandiset_by_id[dandiset.identifier] = dandiset
        dandiset_created_on[dandiset.identifier] = dandiset.created
    print(f"  Found {len(dandiset_created_on)} dandisets", flush=True)

    # Resolve entries where the same content-ID appears in multiple dandisets.
    # Heuristic: prefer the dandiset that came into existence first.
    print(f"Resolving {len(multiple_dandisets)} multiple-dandiset entries...", flush=True)
    for idx, (content_id, dandisets) in enumerate(multiple_dandisets.items(), start=1):
        if idx % 100 == 0:
            print(f"  {idx}/{len(multiple_dandisets)}", flush=True)

        # Exclude dandisets that have been deleted (absent from the upfront pass).
        available = {d: paths for d, paths in dandisets.items() if d in dandiset_created_on}
        if not available:
            dandiset_failures.append(f"No dandiset found for content_id={content_id!r}")
            continue

        earliest_dandiset_id = min(available.keys(), key=lambda d: dandiset_created_on[d])

        paths: list[str] = available[earliest_dandiset_id]
        if len(paths) == 1:
            path = paths[0]
        else:
            path = _get_earliest_asset_path(
                dandiset=dandiset_by_id[earliest_dandiset_id],
                paths=paths,
                asset_created_cache=asset_created_cache,
                resolution_failures=resolution_failures,
                processing_step="dandiset came first",
            )

        content_id_to_usage_dandiset_path[content_id] = {earliest_dandiset_id: path}

    # Resolve entries where the same content-ID appears in multiple paths within one dandiset.
    # Heuristic: prefer the asset path that was created first.
    print(f"Resolving {len(multiple_paths_same_dandiset)} multiple-path entries...", flush=True)
    for idx, (content_id, dandisets) in enumerate(multiple_paths_same_dandiset.items(), start=1):
        if idx % 100 == 0:
            print(f"  {idx}/{len(multiple_paths_same_dandiset)}", flush=True)

        dandiset_id, paths = next(iter(dandisets.items()))

        # Skip if the dandiset has been deleted.
        if dandiset_id not in dandiset_created_on:
            dandiset_failures.append(f'Dandiset "{dandiset_id!r}" not found in `dandiset_created_on`!')
            continue

        path = _get_earliest_asset_path(
            dandiset=dandiset_by_id[dandiset_id],
            paths=paths,
            asset_created_cache=asset_created_cache,
            resolution_failures=resolution_failures,
            processing_step="asset came first",
        )
        content_id_to_usage_dandiset_path[content_id] = {dandiset_id: path}

    records = [
        {"content_id": content_id, "dandiset_id": dandiset_id, "path": path}
        for content_id, mapping in sorted(content_id_to_usage_dandiset_path.items())
        for dandiset_id, path in mapping.items()
    ]

    derivatives_directory = base_directory / "derivatives"
    derivatives_directory.mkdir(parents=True, exist_ok=True)

    # Testing runs write to their own designated files, so the real cache is never touched.
    output_file_path = derivatives_directory / (_TESTING_FILE_NAME if testing else _CACHE_FILE_NAME)
    print(f"Writing {len(records)} entries to {output_file_path}", flush=True)
    with output_file_path.open(mode="w") as file_stream:
        file_stream.writelines(f"{json.dumps(record)}\n" for record in records)

    # The failure logs are rewritten in full on every run so they always reflect the
    # current state of the upstream data, and are saved into the derivatives dataset
    # alongside the output for provenance.
    logs_directory = derivatives_directory / "logs"
    logs_directory.mkdir(parents=True, exist_ok=True)
    log_file_prefix = "testing_" if testing else ""
    with (logs_directory / f"{log_file_prefix}dandiset_failures.txt").open(mode="w") as file_stream:
        file_stream.writelines(f"{line}\n" for line in dandiset_failures)
    with (logs_directory / f"{log_file_prefix}resolution_failures.txt").open(mode="w") as file_stream:
        file_stream.writelines(f"{line}\n" for line in resolution_failures)


if __name__ == "__main__":
    default_base_directory = pathlib.Path(__file__).parent.parent

    parser = argparse.ArgumentParser(description="Update the content-id-to-usage-dandiset-path DANDI cache.")
    parser.add_argument(
        "--base-directory",
        type=pathlib.Path,
        default=default_base_directory,
        help=(
            "The directory containing the `sourcedata` and `derivatives` directories. "
            "Set to the mounted dataset path when run inside the pipeline container; "
            "defaults to the repository root."
        ),
    )
    parser.add_argument(
        "--testing",
        action="store_true",
        help=(
            f"Run in testing mode: process only the first {_TESTING_LIMIT} entries of each category "
            f"and write `derivatives/{_TESTING_FILE_NAME}` (and `testing_`-prefixed logs) instead of "
            "the real cache, leaving it untouched. Omit for a complete update."
        ),
    )
    args = parser.parse_args()

    _run(base_directory=args.base_directory, testing=args.testing)
