"""Update the content-id-to-usage-dandiset-path DANDI cache.

Reduce the upstream `content-id-to-dandiset-paths` cache, where one content ID may appear in
several Dandisets and under several paths, to a single `(dandiset ID, asset path)` per content ID.
Two heuristics do the reducing, and both prefer whatever came into existence first:

- a content ID in several Dandisets is attributed to the Dandiset created earliest;
- several paths within one Dandiset are reduced to the asset created earliest.

Most content IDs need neither heuristic: they appear once, under one path, and reducing them is a
passthrough with no work in it at all. The rest cost a Dandiset creation time from S3 and an asset
creation time per candidate path from the DANDI API, and that is the whole cost of this cache.

So the cache accumulates those resolutions rather than recomputing them. What makes that sound is
that a content ID's answer depends on nothing but its own upstream entry: `resolution_key`
fingerprints that entry, and a recorded answer is reused only while the fingerprint still matches.
A content ID whose upstream entry gained a Dandiset or a path is therefore resolved again, and one
that was unique and still is never costs anything. `limit` then means what it means everywhere
else -- how many ambiguous content IDs one run resolves -- and each run publishes the complete
mapping, this run's work included.

Everything shared with the other caches -- argument parsing, logging, the incremental frontier,
the batch cap, the error logs, the output paths and testing mode -- comes from `dandi_cache_utils`.
"""

import hashlib
import json

import dandi.exceptions
import dandi_cache_utils as dandi_cache

#: The side output: the fingerprint each recorded answer was resolved from, which is what makes
#: reusing it safe. Published rather than kept in `logs/`, so anyone can check an answer is current.
RESOLVED_FROM = "content_id_to_resolved_from.jsonl"

#: Sized together with the S3 client's connection pool, which is what the shared client does.
MAX_WORKERS = 16

# Placing a Dandiset in time and placing an asset within it fail for unrelated reasons, and a week
# of failures is only triageable when each kind has its own log.
STAGES = {
    "placing the Dandiset in time": "dandiset_lookup_errors.txt",
    "placing the asset in time": "asset_lookup_errors.txt",
}


def split_by_uniqueness(dandiset_paths: dict, /) -> tuple[dict, dict]:
    """Separate the entries that are already unique from the ones a heuristic has to reduce."""
    unique: dict[str, dict[str, str]] = {}
    ambiguous: dict[str, dict[str, list[str]]] = {}

    for content_id, dandisets in dandiset_paths.items():
        if not dandisets:
            raise ValueError(f"Empty dandisets mapping for content_id={content_id!r}")
        if len(dandisets) > 1:
            ambiguous[content_id] = dandisets
            continue

        dandiset_id, paths = next(iter(dandisets.items()))
        if len(paths) > 1:
            ambiguous[content_id] = {dandiset_id: paths}
            continue

        unique[content_id] = {dandiset_id: paths[0]}

    return unique, ambiguous


def resolution_key(dandisets: dict, /) -> str:
    """A short, stable fingerprint of the upstream entry an answer was resolved from.

    The heuristics read nothing but this entry, so an unchanged fingerprint means an unchanged
    answer and no reason to spend the network round trips again. It is a digest rather than the
    entry itself because the side output would otherwise be as large as the input.
    """
    canonical = json.dumps(
        {dandiset_id: sorted(paths) for dandiset_id, paths in sorted(dandisets.items())},
        separators=(",", ":"),
    )
    return hashlib.sha256(canonical.encode(encoding="utf-8")).hexdigest()[:16]


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


def creation_times(dandiset_ids: set, /) -> dict:
    """When each of these Dandisets was created, leaving out the ones that cannot be placed."""
    if not dandiset_ids:
        return {}

    ordered = sorted(dandiset_ids)
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
    return created_on


def main() -> None:
    dataset, arguments = dandi_cache.open_dataset()

    dandiset_paths = dataset.read_input()
    unique, ambiguous = split_by_uniqueness(dandiset_paths)
    dandi_cache.logger.info("%d content IDs are already unique; %d need a heuristic.", len(unique), len(ambiguous))

    records = dataset.read_split_output_lookup(dataset.config.cache_file_name)
    resolved_from = dataset.read_output_lookup(RESOLVED_FROM)

    # A content ID the upstream no longer lists is no longer this cache's to publish. Dropping it
    # here is what keeps the accumulation from carrying an entry forever after its source went.
    for content_id in records.keys() - dandiset_paths.keys():
        del records[content_id]
        resolved_from.pop(content_id, None)

    # The passthrough costs nothing, so it is redone in full every run rather than accumulated: a
    # content ID that has become unique since it was last resolved is corrected here, immediately.
    records.update(unique)
    for content_id in unique:
        resolved_from.pop(content_id, None)

    # An answer is current when it was resolved from the upstream entry that is there now. Those
    # are the ones this run has no reason to touch; everything else ambiguous is the frontier.
    current = {
        content_id
        for content_id, dandisets in ambiguous.items()
        if resolved_from.get(content_id) == resolution_key(dandisets)
    }
    batch = dandi_cache.select_new(ambiguous, current, limit=dataset.limit(arguments.limit))

    # Only the Dandisets this batch actually needs, rather than every one the upstream mentions.
    created_on = creation_times({dandiset_id for content_id in batch for dandiset_id in ambiguous[content_id]})

    dandiset_failures = dandi_cache.ErrorLog(dataset.logs_directory / f"{dataset.log_prefix}dandiset_failures.txt")
    resolution_failures = dandi_cache.ErrorLog(dataset.logs_directory / f"{dataset.log_prefix}resolution_failures.txt")
    resolver = dandi_cache.api.AssetResolver()

    def resolve(content_id, item):
        """The one Dandiset and path this content ID is attributed to, or nothing if it cannot be."""
        dandisets = ambiguous[content_id]
        item.context["candidates"] = sum(len(paths) for paths in dandisets.values())

        item.stage = "placing the Dandiset in time"
        # A Dandiset with no creation time has been deleted or embargoed, so it cannot be placed in
        # time and is excluded, exactly as it was under the old listing-based pass.
        available = {dandiset_id: paths for dandiset_id, paths in dandisets.items() if dandiset_id in created_on}
        if not available:
            dandiset_failures.append(f"No dandiset found for content_id={content_id!r}")
            # Left unrecorded rather than recorded wrongly: the Dandiset may come back, and the
            # fingerprint is not stamped either, so the next run tries again.
            return dandi_cache.NOTHING

        first = min(available, key=lambda dandiset_id: created_on[dandiset_id])
        step = "dandiset came first" if len(dandisets) > 1 else "asset came first"

        item.stage = "placing the asset in time"
        path = earliest_path(resolver, first, available[first], failures=resolution_failures, step=step)

        resolved_from[content_id] = resolution_key(dandisets)
        return {first: path}

    _records, result = dandi_cache.run_incremental_update(
        dataset,
        batch=batch,
        process=resolve,
        recorded=records,
        # The network round trips behind a resolution fail transiently far more often than they
        # fail permanently, and the answer is not "unresolvable" -- it is "not yet". Leave the
        # content ID for a later run rather than recording an attribution it did not earn.
        on_failure=dandi_cache.SKIP,
        on_write=lambda: dataset.write_output_lookup(resolved_from, RESOLVED_FROM),
        stages=STAGES,
        describe=lambda location: f"attributed to {next(iter(location))}",
        checkpoint_every=50,
        # Split across sixteen files, since as one it would pass GitHub's 100 MiB limit for a file.
        split=True,
    )

    dandi_cache.logger.info(
        "Resolved %d ambiguous content IDs; %d of %d are now current, leaving %d for later runs.",
        result.succeeded,
        len(current) + result.succeeded,
        len(ambiguous),
        len(ambiguous) - len(current) - result.succeeded,
    )


if __name__ == "__main__":
    main()
