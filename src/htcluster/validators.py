from pathlib import Path
from typing import Optional, Self

from pydantic import ConfigDict, field_validator, model_validator

from htcluster.job_submit.yaml import ImplicitOut, yaml  # TODO: does module make sense?

from .validator_base import BaseModel
from .validators_3_9_compat import JobSettings


class JobParams(BaseModel):
    model_config = ConfigDict(arbitrary_types_allowed=True)

    in_files: list[Path] | list[list[Path]] = []
    out_files: list[Path] | ImplicitOut
    # in_staging: Path
    params: Optional[dict[str, list[str | int | float | list[str | int | float]]]] = (
        None
    )

    @field_validator("out_files")
    @classmethod
    def out_files_relative(cls, v: Path | ImplicitOut) -> Path | ImplicitOut:
        if isinstance(v, Path):
            assert Path(v.name) == v, "output paths should be name only, no subdirs"
        return v

    def has_inputs(self) -> bool:
        return len(self.in_files) > 0

    def has_params(self) -> bool:
        return self.params is not None


class ClusterJob(BaseModel):
    model_config = ConfigDict(arbitrary_types_allowed=True)
    job: JobSettings
    params: JobParams
    n_jobs: int = 0

    @classmethod
    def from_yaml_file(cls: type[Self], path: Path) -> Self:
        with open(path, "r") as fp:
            return cls(**yaml.safe_load(fp))

    @classmethod
    def from_yaml_str(cls: type[Self], s: str) -> Self:
        return cls(**yaml.safe_load(s))

    @model_validator(mode="after")
    def validate_params(self) -> Self:
        match (self.params.has_inputs(), self.params.has_params()):
            case (True, True):
                n_files = len(self.params.in_files)
                self.n_jobs = n_files
                assert self.params.params is not None  # mypy
                for k, v in self.params.params.items():
                    if (n_params := len(v)) != n_files:
                        raise ValueError(
                            "number of params must match number of files, there are "
                            f"{n_files} in_files and {n_params} params for {k}"
                        )
            case (False, True):
                assert self.params.params is not None  # mypy
                first_key = list(self.params.params.keys())[0]
                first_len = len(self.params.params[first_key])
                for k, v in self.params.params.items():
                    if (n_params := len(v)) != first_len:
                        raise ValueError(
                            f"number of params must be equal for all params. {first_key} "
                            f"has {first_len} params whereas {k} has {n_params}"
                        )
                self.n_jobs = first_len
            case (True, False):
                self.n_jobs = len(self.params.in_files)
            case (False, False):
                raise ValueError("in_files and/or params must be specified in inputs")

        return self
