import argparse
import re
from importlib import import_module
from pathlib import Path
from typing import Callable, Optional, TypeAlias

import numpy as np
import numpy.typing as npt
import yaml
from pydantic import BaseModel, ConfigDict, field_validator

SIZE_PAT = re.compile("[0-9]+(K|M|G|T)(B)?")

# class FileGlob:
#     def __init__(self, dir: Path | str, glob: str, sort: Optional[Callable] = None):
#         dir = Path(dir)
#         self._val = [{"path": p} for p in sorted(dir.glob(glob), key=sort)]
#         assert len(self._val) > 0, f"no files were found in glob {dir / glob}"

#     @classmethod
#     def yaml_constructor(cls, loader, node):
#         return FileGlob(**loader.construct_mapping(node))

# yaml.SafeLoader.add_constructor(u'!glob', FileGlob.yaml_constructor)


# class Range:
#     def __init__(
#         self,
#         start: Optional[int] = None,
#         stop: Optional[int] = None,
#         step: Optional[int] = None,
#         dtype: Optional[npt.DTypeLike] = None,
#     ):
#         if stop is None:
#             raise ValueError("stop must be specified")
#         if start is None:
#             self._val = np.arange(stop)
#         if step is None:
#             self._val = np.arange(start, stop)
#         self._val = np.arange(start, stop, step)
#         if dtype is not None:
#             self._val = self._val.astype(dtype)


# class LinSpace:
#     def __init__(
#         self,
#         start: int | float,
#         stop: int | float,
#         num: int,
#         dtype: Optional[npt.DTypeLike] = None,
#     ):
#         self._val = np.linspace(start, stop, num)
#         if dtype is not None:
#             self._val = self._val.astype(dtype)


# class Repeat:
#     def __init__(self, val: int | float | str, n: int):
#         self._val = [val] * n


def yaml_glob(loader, node):
    try:
        data = loader.construct_mapping(node)
        dir = data["in-files"]
        glob = data["glob"]
    except yaml.constructor.ConstructorError:
        dir, glob = loader.construct_scalar(node).split()

    dir = Path(dir)
    files = [Path(p) for p in sorted(dir.glob(glob))]
    assert len(files) > 0, f"no files were found in glob {dir / glob}"
    return files


def parse_num(v):
    try:
        return int(v)
    except ValueError:
        pass
    try:
        return float(v)
    except ValueError:
        raise ValueError("{v} could not be interpreted as an int or float")


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


def yaml_linspace(loader, node) -> npt.NDArray[np.floating | np.intp]:
    params = loader.construct_scalar(node).split()
    start, stop, num = None, None, None
    if len(params) == 3:
        start, stop, num = map(parse_num, params)
    else:
        raise ValueError(f"!linspace: must specify 3 params. got: {params}")
    if isinstance(num, float):
        raise ValueError(f"num must be an int. got: {num}")
    return np.linspace(start, stop, num)


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


class ProgrammaticJobParams(BaseModel):
    model_config = ConfigDict(arbitrary_types_allowed=True)

    in_files: Optional[list[Path]] = None
    params: Optional[dict[str, list[str | int | float | list[str | int | float]]]] = (
        None
    )
    # params: Optional[dict[str, npt.NDArray[np.floating | np.intp] | list[str]]] = None


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("job_yaml", type=Path, help="input job description yaml")
    return parser.parse_args()


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

    @field_validator("disk", "memory")
    @classmethod
    def contains_units(cls, v: str) -> str:
        assert not v.startswith("-"), "value can not be negative"
        assert (
            SIZE_PAT.match(v) is not None
        ), f"value must be a valid size, matching {SIZE_PAT.pattern}"
        return v

    # @field_validator(""):
    # @classmethod
    # def path_exists(cls, v: str) -> Path:
    #     p = Path(v)
    #     assert p.exists(), f"{p} does not exist"


def read_and_validate(in_yaml: Path, schema: ClusterJob) -> dict:
    with open(in_yaml, "r") as fp:
        return schema(**yaml.safe_load(fp)).dict()
        # return yaml.safe_load(fp)


def main():
    args = parse_args()
    if not args.job_yaml.exists() and args.job_yaml.is_file():
        raise ValueError(f"{args.job_yaml} does not exist")
    job_descr = read_and_validate(args.job_yaml, ClusterJob)
    module, function = job_descr['entrypoint'].split(':')
    entrypoint = getattr(import_module(module), function)
    entrypoint(job_descr)


if __name__ == "__main__":
    main()
