import argparse
import json
import re
from importlib import import_module
from pathlib import Path
from typing import Optional, Self

import numpy as np
import yaml
from paramiko import SFTPClient, SSHClient
from pydantic import BaseModel, ConfigDict, field_validator, model_validator

SIZE_PAT = re.compile("[0-9]+(K|M|G|T)(B)?")


def yaml_glob(loader, node):
    try:
        data = loader.construct_mapping(node)
        dir = data["in-files"]
        glob = data["glob"]
    except yaml.constructor.ConstructorError:
        dir, glob = loader.construct_scalar(node).split()

    dir = Path(dir)
    files = [Path(p) for p in sorted(dir.glob(glob), key=file_sorter)]
    assert len(files) > 0, f"no files were found in glob {dir / glob}"
    return files


def parse_num(v: str, strict: bool = True) -> int | float | str:
    try:
        return int(v)
    except ValueError:
        pass
    try:
        return float(v)
    except ValueError:
        if strict is True:
            raise ValueError("{v} could not be interpreted as an int or float")
    return v


# npt.NDArray[np.floating | np.intp]


def yaml_range(loader, node) -> list[int | float]:
    params = loader.construct_scalar(node).split()
    start, stop, step = None, None, None
    match len(params):
        case 1:
            (stop,) = map(parse_num, params)
        case 2:
            start, stop = map(parse_num, params)
        case 3:
            start, stop, step = map(parse_num, params)
        case _:
            raise ValueError(f"!range: must specify 1, 2, or 3 params. got: {params}")
    if isinstance(step, float):
        raise ValueError(f"step must be an int. got: {step}")
    if stop is None:
        raise ValueError("stop must be specified")
    if start is None:
        return np.arange(stop).tolist()
    if step is None:
        return np.arange(start, stop).tolist()
    return np.arange(start, stop, step).tolist()


def yaml_linspace(loader, node) -> list[float]:
    params = loader.construct_scalar(node).split()
    start, stop, num = None, None, None
    if len(params) == 3:
        start, stop, num = map(parse_num, params)
    else:
        raise ValueError(f"!linspace: must specify 3 params. got: {params}")
    if isinstance(num, float):
        raise ValueError(f"num must be an int. got: {num}")
    return np.linspace(start, stop, num).tolist()


def yaml_repeat(loader, node) -> list[str | int | float]:
    if (m := re.match(r"\((![0-9a-z ]+)\)", node.value)) is not None:
        nested_constructor = m.group(1).split()
        tag, args = nested_constructor[0], nested_constructor[1:]
        val = loader.yaml_constructors[tag](
            loader, yaml.ScalarNode(tag=tag, value=" ".join(args))
        )
        n = parse_num(node.value[m.span()[-1] :].strip())
    else:
        raw = node.value.split()
        if len(raw) != 2:
            raise ValueError(f"!repeat: expect 2 arguments, got {raw}")
        val, n = raw
        n = parse_num(n)
    if not isinstance(n, int):
        raise ValueError(f"!repeat: number of repeats must be an int, got {n}")
    return [val] * n


yaml.SafeLoader.add_constructor("!glob", yaml_glob)
yaml.SafeLoader.add_constructor("!range", yaml_range)
yaml.SafeLoader.add_constructor("!linspace", yaml_linspace)
yaml.SafeLoader.add_constructor("!repeat", yaml_repeat)


def file_sorter(path: Path, split_chars: list[str] = ["-", "_", ":", "."]):
    keys = re.split(rf"[{''.join(split_chars)}]", path.name)
    return tuple(map(lambda v: parse_num(v, strict=False), keys))


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("job_yaml", type=Path, help="input job description yaml")
    return parser.parse_args()


class ProgrammaticJobParams(BaseModel):
    model_config = ConfigDict(arbitrary_types_allowed=True)

    in_files: Optional[list[Path]] = None
    params: Optional[dict[str, list[str | int | float | list[str | int | float]]]] = (
        None
    )


class ClusterJob(BaseModel):
    model_config = ConfigDict(arbitrary_types_allowed=True)
    name: str
    memory: str
    disk: str
    cores: int
    entrypoint: str
    docker_image: str
    job_params: Optional[ProgrammaticJobParams] = None
    grouped_job_params: Optional[dict[str, list[dict] | ProgrammaticJobParams]] = None
    n_jobs: int = 0
    n_params: int = 0

    @field_validator("disk", "memory")
    @classmethod
    def contains_units(cls, v: str) -> str:
        assert not v.startswith("-"), "value can not be negative"
        assert (
            SIZE_PAT.match(v) is not None
        ), f"value must be a valid size, matching {SIZE_PAT.pattern}"
        return v

    @model_validator(mode="after")
    def verify_params(self) -> Self:
        assert not (self.job_params is None) and (
            self.grouped_job_params is None
        ), "job_params or grouped_job_params must be specified"
        if (
            (job_params := self.job_params) is not None
            and job_params.in_files is not None
            and job_params.params is not None
        ):
            for k, v in job_params.params.items():
                if (n_params := len(v)) != (n_files := len(job_params.in_files)):
                    raise ValueError(
                        "number of params must match number of files, there are "
                        f"{n_files} in files and {n_params} params for {k}"
                    )
                self.n_jobs = n_params
            self.n_params = len(list(job_params.params))
        return self

    # @field_validator(""):
    # @classmethod
    # def path_exists(cls, v: str) -> Path:
    #     p = Path(v)
    #     assert p.exists(), f"{p} does not exist"


def read_and_validate(in_yaml: Path, schema: ClusterJob) -> ClusterJob:
    with open(in_yaml, "r") as fp:
        return schema(**yaml.safe_load(fp))
        # return yaml.safe_load(fp)

def chtc_ssh_client() -> SSHClient:
    client = SSHClient()
    client.load_system_host_keys()
    # TODO: config
    client.connect("ap2002.chtc.wisc.edu", username="lkirk2")
    return client


# def mkdir(client: SSHClient, path: Path) -> None:
#     _, stdout, _ = client.exec_command(f'mkdir {path}')
#     stdout.channel.set_combine_stderr(True)
#     msg = "".join(stdout.readlines())  # drain output so cmd completes
#     if stdout.channel.exit_status:
#         raise Exception(msg)


def mkdir(client: SFTPClient, path: Path) -> None:
    try:
        client.stat(str(path))
        raise Exception(f"{path} exists on remote server")
    except FileNotFoundError:
        pass
    try:
        client.mkdir(str(path))
    except FileNotFoundError:
        raise Exception(f"{path.parent} does not exist on remote server")


def write_file(client: SFTPClient, dest: Path, data: str) -> None:
    with client.open(str(dest), "w") as fp:
        fp.write(data)


def copy_file(client: SFTPClient, source: Path, dest: Path) -> None:
    client.put(str(source), str(dest), confirm=True)


def main():
    args = parse_args()
    cluster_dir = Path("cluster-out")
    if not args.job_yaml.exists() and args.job_yaml.is_file():
        raise ValueError(f"{args.job_yaml} does not exist")
    job_descr = read_and_validate(args.job_yaml, ClusterJob)
    # module, function = job_descr.entrypoint.split(":")
    # entrypoint = getattr(import_module(module), function)
    client = chtc_ssh_client()
    with client.open_sftp() as sftp:
        if (job_params := job_descr.job_params) is not None:
            assert job_params.params is not None  # mypy
            assert job_params.in_files is not None  # mypy
            # for row in zip(job_params.in_files, p.keys(), *p.values()):
            mkdir(sftp, input_dir := cluster_dir / "input")
            for j in range(job_descr.n_jobs):
                params = {k: job_params.params[k][j] for k in job_params.params}
                copy_file(sftp, job_params.in_files[j], input_dir / job_params.in_files[j].name)
                print(f'copied {job_params.in_files[j]}')
                write_file(sftp, input_dir / f"params_{j}.json", json.dumps(params, indent=2))
                print(f"wrote params for job {j}")


if __name__ == "__main__":
    main()
