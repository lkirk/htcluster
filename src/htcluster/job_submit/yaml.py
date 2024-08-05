import re
from itertools import product
from pathlib import Path
from typing import Hashable

import numpy as np
import yaml


class ImplicitOut:
    suffix: str

    def __init__(self, suffix: str):
        self.suffix = suffix


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
    args = loader.construct_mapping(node, deep=True)
    if len(extra_or_missing := args.keys() ^ required):
        req = " ".join([f"'{f}'" for f in required])
        raise ValueError(f"{req} are required. got: {extra_or_missing}")

    return args


def yaml_randint_32(loader: yaml.Loader, node: yaml.MappingNode) -> list[int]:
    args = construct_mapping_with_required_args(loader, node, {"seed", "size"})
    rng = np.random.RandomState(args["seed"])
    return rng.randint(0, 2**32, size=args["size"]).tolist()


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


def yaml_logspace(loader: yaml.Loader, node: yaml.SequenceNode) -> list[float]:
    params = loader.construct_sequence(node)
    start, stop, num = None, None, None
    if len(params) == 3:
        start, stop, num = params
    else:
        raise ValueError(f"!linspace: must specify 3 params. got: {params}")
    if isinstance(num, float):
        raise ValueError(f"num must be an int. got: {num}")
    return np.logspace(start, stop, num).tolist()


def yaml_product(loader: yaml.Loader, node: yaml.MappingNode) -> list[dict]:
    args = loader.construct_mapping(node, deep=True)
    return [dict(zip(args.keys(), row)) for row in product(*args.values())]


def yaml_product_transposed(
    loader: yaml.Loader, node: yaml.MappingNode
) -> dict[Hashable, list]:
    args = loader.construct_mapping(node, deep=True)
    prod = [dict(zip(args.keys(), row)) for row in product(*args.values())]
    keys = prod[0].keys()
    for d in prod:
        # assert that all dict keys in list are same
        assert len(d.keys() ^ keys) == 0, f"keys differ: {d.keys()}, {keys}"
    return {k: [d[k] for d in prod] for k in keys}


def yaml_repeat(loader: yaml.Loader, node: yaml.MappingNode) -> list[str | int | float]:
    args = construct_mapping_with_required_args(loader, node, {"rep", "n"})
    return [args["rep"]] * args["n"]


def yaml_flatten(
    loader: yaml.Loader, node: yaml.SequenceNode
) -> list[str | int | float]:
    params = loader.construct_sequence(node, deep=True)
    return np.array(params).flatten().tolist()


def yaml_implicit_out(loader: yaml.Loader, node: yaml.MappingNode) -> ImplicitOut:
    raw = node.value.split()
    if len(raw) > 1:
        raise ValueError(f"!implicit_out: expect 1 arguments, got {raw}")
    return ImplicitOut(raw[0])


def yaml_file_range(loader: yaml.Loader, node: yaml.MappingNode) -> list[str]:
    args = construct_mapping_with_required_args(loader, node, {"fmt", "num"})
    return [args["fmt"].format(i) for i in range(args["num"])]


def yaml_zip(loader: yaml.Loader, node: yaml.SequenceNode) -> list[list]:
    params = loader.construct_sequence(node, deep=True)
    first_len = len(params[0])
    for l in params:
        if len(l) != first_len:
            raise ValueError(
                "!zip: expect all arguments to be of the same length, first element "
                f"is of length {first_len}, {l} is of length {len(l)}"
            )
    return list(map(list, zip(*params)))


def yaml_merge(loader: yaml.Loader, node: yaml.SequenceNode) -> dict:
    params = loader.construct_sequence(node, deep=True)
    out = dict()
    for p in params:
        if not isinstance(p, dict):
            raise ValueError(f"!merge: all arguments must be dict, got {p}")
        out = {**out, **p}
    return out


yaml.SafeLoader.add_constructor("!randint_32", yaml_randint_32)
yaml.SafeLoader.add_constructor("!glob", yaml_glob)
yaml.SafeLoader.add_constructor("!range", yaml_range)
yaml.SafeLoader.add_constructor("!linspace", yaml_linspace)
yaml.SafeLoader.add_constructor("!logspace", yaml_logspace)
yaml.SafeLoader.add_constructor("!repeat", yaml_repeat)
yaml.SafeLoader.add_constructor("!flatten", yaml_flatten)
yaml.SafeLoader.add_constructor("!product", yaml_product)
yaml.SafeLoader.add_constructor("!product_transposed", yaml_product_transposed)
yaml.SafeLoader.add_constructor("!implicit_out", yaml_implicit_out)
yaml.SafeLoader.add_constructor("!file_range", yaml_file_range)
yaml.SafeLoader.add_constructor("!zip", yaml_zip)
yaml.SafeLoader.add_constructor("!merge", yaml_merge)
