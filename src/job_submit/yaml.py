import re
from pathlib import Path

import numpy as np
import yaml

from htcluster.validators import ClusterJob, ImplicitOut


def file_sorter(path: Path, split_chars: list[str] = ["-", "_", ":", "."]):
    keys = re.split(rf"[{''.join(split_chars)}]", path.name)
    return tuple(map(lambda v: parse_num(v, strict=False), keys))


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
        # TODO: figure out typing here
        assert not isinstance(stop, str)  # mypy
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
    # TODO: figure out typing here
    assert not isinstance(stop, str)  # mypy
    assert not isinstance(start, str)  # mypy
    assert not isinstance(num, str)  # mypy
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


def yaml_implicit_out(loader, node) -> ImplicitOut:
    raw = node.value.split()
    if len(raw) > 1:
        raise ValueError(f"!implicit_out: expect 1 arguments, got {raw}")
    return ImplicitOut(raw[0])


yaml.SafeLoader.add_constructor("!glob", yaml_glob)
yaml.SafeLoader.add_constructor("!range", yaml_range)
yaml.SafeLoader.add_constructor("!linspace", yaml_linspace)
yaml.SafeLoader.add_constructor("!repeat", yaml_repeat)
yaml.SafeLoader.add_constructor("!implicit_out", yaml_implicit_out)


def read_and_validate_job_yaml(in_yaml: Path) -> ClusterJob:
    with open(in_yaml, "r") as fp:
        return ClusterJob(**yaml.safe_load(fp))
