import re
from pathlib import Path

import numpy as np
import yaml


class ImplicitOut:
    suffix: str
    idx: int | None

    def __init__(self, suffix: str, idx: int | None = None):
        self.suffix = suffix
        self.idx = idx


def parse_num(v: str) -> int | float | str:
    try:
        return int(v)
    except ValueError:
        pass
    try:
        return float(v)
    except ValueError:
        pass
    return v


def file_sorter(path: Path, split_chars: list[str] = ["-", "_", ":", "."]):
    keys = re.split(rf"[{''.join(split_chars)}]", path.name)
    return tuple(map(lambda v: parse_num(v), keys))


def construct_mapping_with_required_args(
    loader: yaml.Loader, node: yaml.MappingNode, required: set
) -> dict:
    args = loader.construct_mapping(node)
    if len(extra_or_missing := args.keys() ^ required):
        req = " ".join([f"'{f}'" for f in required])
        raise ValueError(f"{req} are required. got: {extra_or_missing}")

    return args


def yaml_glob(loader: yaml.Loader, node: yaml.MappingNode):
    args = construct_mapping_with_required_args(loader, node, {"dir", "glob"})
    dir = Path(args["dir"])
    files = [Path(p) for p in sorted(dir.glob(args["glob"]), key=file_sorter)]
    assert len(files) > 0, f"no files were found in glob {dir / args['glob']}"
    return files


def yaml_range(loader: yaml.Loader, node: yaml.SequenceNode) -> list[int | float]:
    params = loader.construct_sequence(node)
    start, stop, step = None, None, None
    match len(params):
        case 1:
            (stop,) = params
        case 2:
            start, stop = params
        case 3:
            start, stop, step = params
        case _:
            raise ValueError(f"!range: must specify 1, 2, or 3 params. got: {params}")
    if stop is None:
        raise ValueError("stop must be specified")
    if start is None:
        return np.arange(stop).tolist()
    if step is None:
        return np.arange(start, stop).tolist()
    return np.arange(start, stop, step).tolist()


def yaml_linspace(loader: yaml.Loader, node: yaml.SequenceNode) -> list[float]:
    params = loader.construct_sequence(node)
    start, stop, num = None, None, None
    if len(params) == 3:
        start, stop, num = params
    else:
        raise ValueError(f"!linspace: must specify 3 params. got: {params}")
    if isinstance(num, float):
        raise ValueError(f"num must be an int. got: {num}")
    return np.linspace(start, stop, num).tolist()


def yaml_repeat(loader: yaml.Loader, node: yaml.MappingNode) -> list[str | int | float]:
    args = construct_mapping_with_required_args(loader, node, {"rep", "n"})
    return [args["rep"]] * args["n"]


def yaml_flatten(
    loader: yaml.Loader, node: yaml.MappingNode
) -> list[str | int | float]:
    args = construct_mapping_with_required_args(loader, node, {"arr"})
    # ignore typing here, let numpy deal with it
    return np.array(args["arr"]).flatten()  # type: ignore


def yaml_implicit_out(loader: yaml.Loader, node: yaml.MappingNode) -> ImplicitOut:
    raw = node.value.split()
    match len(raw):
        case 1:
            return ImplicitOut(raw[0])
        case 2:
            try:
                idx = int(raw[1])
                return ImplicitOut(raw[0], idx)
            except ValueError as e:
                raise ValueError("!implicit out: expect idx to be an integer") from e
        case _:
            raise ValueError(f"!implicit_out: expect 1 or 2 arguments, got {raw}")


def yaml_file_range(loader: yaml.Loader, node: yaml.MappingNode) -> list[str]:
    args = construct_mapping_with_required_args(loader, node, {"fmt", "num"})
    return [args["fmt"].format(i) for i in range(args["num"])]


yaml.SafeLoader.add_constructor("!glob", yaml_glob)
yaml.SafeLoader.add_constructor("!range", yaml_range)
yaml.SafeLoader.add_constructor("!linspace", yaml_linspace)
yaml.SafeLoader.add_constructor("!repeat", yaml_repeat)
yaml.SafeLoader.add_constructor("!flatten", yaml_flatten)
yaml.SafeLoader.add_constructor("!implicit_out", yaml_implicit_out)
yaml.SafeLoader.add_constructor("!file_range", yaml_file_range)
