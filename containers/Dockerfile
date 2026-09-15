# syntax=docker/dockerfile:1

# The pinned runtime environment for this cache.
#
# Everything shared comes from the base image: Debian, git-annex, the virtual environment, the
# `dandi_cache_utils` library that `code/update.py` imports, and the vendored `update_pipeline.sh`
# that CI extracts and runs on the runner.
#
# `:latest` already carries this cache's two processing dependencies, boto3 and the DANDI client,
# through the library's `s3` and `archive` extras, so there is nothing to install here.
#
# The image holds only the environment, not this cache's code. The code is mounted at run time, so
# a single image serves any revision of it.
#
# The container needs outbound network access at run time: it reads Dandiset manifests from the
# public S3 bucket and resolves asset creation times through the DANDI REST API.
FROM ghcr.io/dandi-cache/dandi-cache-utils:latest

# The source label associates the image on the GitHub Container Registry with this repository.
LABEL org.opencontainers.image.source="https://github.com/dandi-cache/content-id-to-usage-dandiset-path"
LABEL org.opencontainers.image.description="The pinned runtime environment for the DANDI Cache content-id-to-usage-dandiset-path update pipeline."

CMD ["python", "--version"]
