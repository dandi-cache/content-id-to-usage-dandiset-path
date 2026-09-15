"""Update the content-id-to-usage-dandiset-path DANDI cache.

Reduce the upstream `content-id-to-dandiset-paths` cache, where one content ID may appear in
several Dandisets and under several paths, to a single `(dandiset ID, asset path)` per content ID.
Two heuristics do the reducing, and both prefer whatever came into existence first:

- a content ID in several Dandisets is attributed to the Dandiset created earliest;
- several paths within one Dandiset are reduced to the asset created earliest.

This is a pure filter over its input with nothing to resume, so it recomputes the whole mapping
each run. Everything shared with the other caches -- argument parsing, logging, the batch cap, the
output paths and testing mode -- comes from `dandi_cache_utils`.
"""

import itertools

import dandi.exceptions
import dandi_cache_utils as dandi_cache

#: Sized together with the S3 client's connection pool, which is what the shared client does.
MAX_WORKERS = 16


def split_by_uniqueness(dandiset_paths: dict, /) -> tuple[dict, dict, dict]:
    """Separate the entries that are already unique from the two kinds that are not."""
    unique: dict[str, dict[str, str]] = {}
    several_dandisets: dict[str, dict[str, list[str]]] = {}
    several_paths: dict[str, dict[str, list[str]]] = {}

    for content_id, dandisets in dandiset_paths.items():
        if not dandisets:
            raise ValueError(f"Empty dandisets mapping for content_id={content_id!r}")
        if len(dandisets) > 1:
            several_dandisets[content_id] = dandisets
            continue

        dandiset_id, paths = next(iter(dandisets.items()))
        if len(paths) > 1:
            several_paths[content_id] = {dandiset_id: paths}
            continue

        unique[content_id] = {dandiset_id: paths[0]}

    return unique, several_dandisets, several_paths


def earliest_path(resolver, dandiset_id: str, paths: list[str], /, *, failures, step: str) -> str:
    """The path whose asset was created first, falling back to the first path.

    Every failure to place an asset in time is recorded rather than raised: one unresolvable asset
    should not lose the whole content ID, and the fallback is the same one the cache has always
    used.
    """
    if len(paths) == 1:
        return paths[0]

    try:
        # Asked once, before the per-path lookups: an unresolvable Dandiset is one problem, not one
        # per path. Both raise `NotFoundError`, so they are only distinguishable by asking apart.
        resolver.dandiset(dandiset_id)
    except dandi.exceptions.NotFoundError:
        failures.append(f"Dandiset not resolvable via API: dandiset_id={dandiset_id!r}, processing_step={step!r}")
        return paths[0]

    chosen, earliest = paths[0], None
    for path in paths:
        try:
            created = resolver.created(dandiset_id, path)
        except dandi.exceptions.NotFoundError:
            failures.append(f"Asset not found: dandiset_id={dandiset_id!r}, path={path!r}, processing_step={step!r}")
            continue
        if created is not None and (earliest is None or created < earliest):
            chosen, earliest = path, created

    return chosen


def build_mapping(dataset, *, limit: int | None) -> list:
    """Resolve every content ID to one usage location, and return the records to publish."""
    dandiset_paths = dataset.read_input()
    unique, several_dandisets, several_paths = split_by_uniqueness(dandiset_paths)

    if limit is not None:
        # Cap each category rather than the result, so a bounded run still exercises the
        # passthrough and both heuristics rather than only the first of them.
        unique = dict(itertools.islice(unique.items(), limit))
        several_dandisets = dict(itertools.islice(several_dandisets.items(), limit))
        several_paths = dict(itertools.islice(several_paths.items(), limit))

    dandiset_failures = dandi_cache.ErrorLog(dataset.logs_directory / f"{dataset.log_prefix}dandiset_failures.txt")
    resolution_failures = dandi_cache.ErrorLog(dataset.logs_directory / f"{dataset.log_prefix}resolution_failures.txt")
    unresolved: list[str] = []

    # Only the non-unique entries need a creation time; the passthrough needs none.
    referenced = {
        dandiset_id
        for dandisets in itertools.chain(several_dandisets.values(), several_paths.values())
        for dandiset_id in dandisets
    }
    ordered = sorted(referenced)
    dandi_cache.logger.info("Fetching creation times for %d dandisets from S3.", len(ordered))
    client = dandi_cache.s3.anonymous_client(max_pool_connections=MAX_WORKERS)
    created_on = {
        dandiset_id: created
        for dandiset_id, created in zip(
            ordered,
            dandi_cache.s3.concurrent_map(
                lambda dandiset_id: dandi_cache.s3.dandiset_created(client, dandiset_id),
                ordered,
                max_workers=MAX_WORKERS,
            ),
        )
        if created is not None
    }
    dandi_cache.logger.info("Resolved %d dandiset creation times.", len(created_on))

    resolver = dandi_cache.api.AssetResolver()

    dandi_cache.logger.info("Resolving %d content IDs seen in several dandisets.", len(several_dandisets))
    for content_id, dandisets in several_dandisets.items():
        # A Dandiset with no creation time has been deleted or embargoed, so it cannot be placed
        # in time and is excluded, exactly as it was under the old listing-based pass.
        available = {dandiset_id: paths for dandiset_id, paths in dandisets.items() if dandiset_id in created_on}
        if not available:
            unresolved.append(content_id)
            dandiset_failures.append(f"No dandiset found for content_id={content_id!r}")
            continue

        first = min(available, key=lambda dandiset_id: created_on[dandiset_id])
        path = earliest_path(
            resolver, first, available[first], failures=resolution_failures, step="dandiset came first"
        )
        unique[content_id] = {first: path}

    dandi_cache.logger.info("Resolving %d content IDs seen under several paths.", len(several_paths))
    for content_id, dandisets in several_paths.items():
        dandiset_id, paths = next(iter(dandisets.items()))
        if dandiset_id not in created_on:
            unresolved.append(content_id)
            dandiset_failures.append(f"Dandiset {dandiset_id!r} has no creation time, for content_id={content_id!r}")
            continue

        path = earliest_path(resolver, dandiset_id, paths, failures=resolution_failures, step="asset came first")
        unique[content_id] = {dandiset_id: path}

    if unresolved:
        dandi_cache.logger.warning("%d content IDs could not be placed in any dandiset.", len(unresolved))
    return [{content_id: unique[content_id]} for content_id in sorted(unique)]


def main() -> None:
    dataset, arguments = dandi_cache.open_dataset()
    limit = dandi_cache.effective_limit(testing=dataset.testing, limit=arguments.limit)

    # The failure logs describe the current state of the upstream data rather than a history, so
    # each run starts them afresh. The shared `ErrorLog` appends and caps its own size, which is
    # what an incremental cache wants; a full rebuild wants neither.
    for name in ("dandiset_failures.txt", "resolution_failures.txt"):
        (dataset.logs_directory / f"{dataset.log_prefix}{name}").unlink(missing_ok=True)

    dandi_cache.run_full_rebuild(dataset, build=lambda: build_mapping(dataset, limit=limit))


if __name__ == "__main__":
    main()
