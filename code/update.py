import pathlib

import yaml


def _run(base_directory: pathlib.Path, /) -> None:
    # TODO: implement the update logic for this cache
    # Read source data, compute the cache, and write it to the derivatives directory

    cache: dict = dict()

    output_file_path = base_directory / "derivatives" / "<cache_name>.yaml"
    with output_file_path.open(mode="w") as file_stream:
        yaml.safe_dump(data=cache, stream=file_stream)


if __name__ == "__main__":
    repo_head = pathlib.Path(__file__).parent.parent

    _run(repo_head)
