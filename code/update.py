import argparse
import concurrent.futures
import datetime
import functools
import itertools
import json
import pathlib

import boto3
import botocore
import botocore.config
import botocore.exceptions
import dandi.dandiapi
import dandi.exceptions

# Testing mode processes only this many entries of each category (already-unique,
# multiple-dandisets, multiple-paths) and writes to its own designated files
# (`derivatives/testing.jsonl` and `testing_`-prefixed logs), leaving the real cache
# untouched.
_TESTING_LIMIT = 10
_CACHE_FILE_NAME = "content_id_to_usage_dandiset_path.jsonl"
_TESTING_FILE_NAME = "testing.jsonl"

# Dandiset creation times come from the public DANDI S3 bucket, not the REST API. Each
# dandiset publishes a `dandiset.jsonld` manifest whose `dateCreated` is the creation time,
# and reading it directly recovers dandisets that the API's dandiset-listing endpoint omits
# even though they still exist (e.g. 000403, 000561), which the listing-based approach dropped.
#
# Asset creation times are NOT published in the S3 manifests: `assets.jsonld` carries
# `dateModified` / `blobDateModified` / `datePublished`, but not the asset's `created`
# timestamp. So the per-asset tie-break (multiple paths within one dandiset) still queries the
# REST API. The container therefore requires outbound network access to both S3 and the API.
_BUCKET = "dandiarchive"
_REGION = "us-east-2"
_DANDISET_MANIFEST_KEY = "dandisets/{dandiset_id}/draft/dandiset.jsonld"


def _build_s3_client(max_pool_connections: int) -> "botocore.client.BaseClient":
    # `dandiarchive` is a public bucket, so requests are sent unsigned (anonymous). The
    # connection pool holds one connection per worker so the surplus workers do not redo the
    # TCP/TLS handshake on every request.
    config = botocore.config.Config(
        signature_version=botocore.UNSIGNED,
        max_pool_connections=max_pool_connections,
        retries={"mode": "standard"},
    )
    return boto3.client("s3", region_name=_REGION, config=config)


def _get_dandiset_created(s3_client: "botocore.client.BaseClient", dandiset_id: str) -> datetime.datetime | None:
    """Return the dandiset's creation time from its S3 `dandiset.jsonld`, or None if unavailable."""
    key = _DANDISET_MANIFEST_KEY.format(dandiset_id=dandiset_id)
    try:
        response = s3_client.get_object(Bucket=_BUCKET, Key=key)
    except botocore.exceptions.ClientError as error:
        error_code = error.response.get("Error", {}).get("Code", "")
        # A deleted dandiset has no manifest (NoSuchKey); an embargoed one denies anonymous
        # reads (AccessDenied). Both mean the dandiset cannot be placed in time, so it is
        # treated as absent, exactly as a deleted dandiset was under the listing-based pass.
        if error_code in ("AccessDenied", "NoSuchKey"):
            return None
        raise
    body = response["Body"].read()
    metadata = json.loads(body) if body.strip() else {}

    date_created = metadata.get("dateCreated")
    if date_created is None:
        return None
    return datetime.datetime.fromisoformat(date_created)


def _collect_dandiset_created_on(
    s3_client: "botocore.client.BaseClient", dandiset_ids: set[str], max_workers: int
) -> dict[str, datetime.datetime]:
    """Fetch creation times for the given dandiset IDs concurrently; omit those without one."""
    ordered_ids = sorted(dandiset_ids)
    get_created = functools.partial(_get_dandiset_created, s3_client)
    created_on: dict[str, datetime.datetime] = {}
    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        for dandiset_id, created in zip(ordered_ids, executor.map(get_created, ordered_ids)):
            if created is not None:
                created_on[dandiset_id] = created
    return created_on


def _get_remote_dandiset(
    client: dandi.dandiapi.DandiAPIClient,
    dandiset_id: str,
    remote_dandiset_cache: dict[str, dandi.dandiapi.RemoteDandiset | None],
) -> dandi.dandiapi.RemoteDandiset | None:
    """Return the API's RemoteDandiset for asset lookups, or None if the API cannot resolve it."""
    if dandiset_id not in remote_dandiset_cache:
        try:
            remote_dandiset_cache[dandiset_id] = client.get_dandiset(dandiset_id)
        except dandi.exceptions.NotFoundError:
            remote_dandiset_cache[dandiset_id] = None
    return remote_dandiset_cache[dandiset_id]


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


def _resolve_earliest_path(
    *,
    client: dandi.dandiapi.DandiAPIClient,
    dandiset_id: str,
    paths: list[str],
    remote_dandiset_cache: dict[str, dandi.dandiapi.RemoteDandiset | None],
    asset_created_cache: dict[tuple[str, str], datetime.datetime | None],
    resolution_failures: list[str],
    processing_step: str,
) -> str:
    """Pick the earliest-created path among *paths*, falling back to the first path."""
    if len(paths) == 1:
        return paths[0]

    remote_dandiset = _get_remote_dandiset(client, dandiset_id, remote_dandiset_cache)
    if remote_dandiset is None:
        # The dandiset has a creation time from S3 but the REST API cannot resolve its assets
        # (e.g. it is absent from the API), so the tie-break falls back to the first path.
        resolution_failures.append(
            f"Dandiset not resolvable via API: dandiset_id={dandiset_id!r}, " f"processing_step={processing_step!r}"
        )
        return paths[0]

    return _get_earliest_asset_path(
        dandiset=remote_dandiset,
        paths=paths,
        asset_created_cache=asset_created_cache,
        resolution_failures=resolution_failures,
        processing_step=processing_step,
    )


def _run(base_directory: pathlib.Path, testing: bool, max_workers: int) -> None:
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
        # but still exercises the passthrough and both resolution heuristics. The dandiset and
        # asset lookups below are then scoped to just those entries.
        content_id_to_usage_dandiset_path = dict(
            itertools.islice(content_id_to_usage_dandiset_path.items(), _TESTING_LIMIT)
        )
        multiple_dandisets = dict(itertools.islice(multiple_dandisets.items(), _TESTING_LIMIT))
        multiple_paths_same_dandiset = dict(itertools.islice(multiple_paths_same_dandiset.items(), _TESTING_LIMIT))

    asset_created_cache: dict[tuple[str, str], datetime.datetime | None] = {}
    remote_dandiset_cache: dict[str, dandi.dandiapi.RemoteDandiset | None] = {}
    resolution_failures: list[str] = []
    dandiset_failures: list[str] = []

    client = dandi.dandiapi.DandiAPIClient()

    # Collect creation times only for the dandisets actually referenced by the non-unique
    # entries (the already-unique passthrough needs none), read from their S3 dandiset.jsonld.
    # Dandisets that have been deleted are simply absent from this mapping.
    referenced_dandiset_ids: set[str] = set()
    for dandisets in multiple_dandisets.values():
        referenced_dandiset_ids.update(dandisets.keys())
    for dandisets in multiple_paths_same_dandiset.values():
        referenced_dandiset_ids.update(dandisets.keys())

    print(f"Fetching creation times for {len(referenced_dandiset_ids)} dandisets from S3...", flush=True)
    s3_client = _build_s3_client(max_pool_connections=max_workers)
    dandiset_created_on = _collect_dandiset_created_on(s3_client, referenced_dandiset_ids, max_workers=max_workers)
    print(f"  Resolved {len(dandiset_created_on)} dandiset creation times", flush=True)

    # Resolve entries where the same content-ID appears in multiple dandisets.
    # Heuristic: prefer the dandiset that came into existence first.
    print(f"Resolving {len(multiple_dandisets)} multiple-dandiset entries...", flush=True)
    for idx, (content_id, dandisets) in enumerate(multiple_dandisets.items(), start=1):
        if idx % 100 == 0:
            print(f"  {idx}/{len(multiple_dandisets)}", flush=True)

        # Exclude dandisets that have been deleted (no creation time from S3).
        available = {d: paths for d, paths in dandisets.items() if d in dandiset_created_on}
        if not available:
            dandiset_failures.append(f"No dandiset found for content_id={content_id!r}")
            continue

        earliest_dandiset_id = min(available.keys(), key=lambda d: dandiset_created_on[d])

        path = _resolve_earliest_path(
            client=client,
            dandiset_id=earliest_dandiset_id,
            paths=available[earliest_dandiset_id],
            remote_dandiset_cache=remote_dandiset_cache,
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

        path = _resolve_earliest_path(
            client=client,
            dandiset_id=dandiset_id,
            paths=paths,
            remote_dandiset_cache=remote_dandiset_cache,
            asset_created_cache=asset_created_cache,
            resolution_failures=resolution_failures,
            processing_step="asset came first",
        )
        content_id_to_usage_dandiset_path[content_id] = {dandiset_id: path}

    # One JSON value per line: `{"<content_id>": {"<dandiset_id>": "<path>"}}`.
    records = [
        {content_id: content_id_to_usage_dandiset_path[content_id]}
        for content_id in sorted(content_id_to_usage_dandiset_path)
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
        "--max-workers",
        type=int,
        default=16,
        help="Number of concurrent workers used to fetch dandiset creation times from S3.",
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

    _run(base_directory=args.base_directory, testing=args.testing, max_workers=args.max_workers)
