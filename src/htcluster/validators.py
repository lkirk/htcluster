from pathlib import Path
from typing import Optional, Self

from pydantic import ConfigDict, model_validator

from .validator_base import BaseModel
from .validators_3_9_compat import JobSettings


class ImplicitOut:
    suffix: str

    def __init__(self, suffix: str):
        self.suffix = suffix


class ProgrammaticJobParams(BaseModel):
    model_config = ConfigDict(arbitrary_types_allowed=True)

    in_files: list[Path] = []
    out_files: list[Path] | ImplicitOut
    # in_staging: Path
    params: Optional[dict[str, list[str | int | float | list[str | int | float]]]] = (
        None
    )


class ClusterJob(BaseModel):
    model_config = ConfigDict(arbitrary_types_allowed=True)
    job: JobSettings
    params: dict[str, ProgrammaticJobParams] | ProgrammaticJobParams
    n_jobs: int = 0
    grouped: bool = False

    @model_validator(mode="after")
    def validate_params(self) -> Self:
        jp = self.params
        match jp:
            case ProgrammaticJobParams():
                self._validate_prog_job_params(jp)
            case dict():
                raise Exception("grouped jobs not supported (yet)")
        return self

    def _validate_prog_job_params(self, jp: ProgrammaticJobParams):
        match (len(jp.in_files) > 0, jp.params is not None):
            case (True, True):
                n_files = len(jp.in_files)
                self.n_jobs = n_files
                assert jp.params is not None  # mypy
                for k, v in jp.params.items():
                    if (n_params := len(v)) != n_files:
                        raise ValueError(
                            "number of params must match number of files, there are "
                            f"{n_files} in_files and {n_params} params for {k}"
                        )
            case (False, True):
                assert jp.params is not None  # mypy
                first_key = list(jp.params.keys())[0]
                first_len = len(jp.params[first_key])
                for k, v in jp.params.items():
                    if (n_params := len(v)) != first_len:
                        raise ValueError(
                            f"number of params must be equal for all params. {first_key} "
                            f"has {first_len} params whereas {k} has {n_params}"
                        )
                self.n_jobs = first_len
            case (True, False):
                self.n_jobs = len(jp.in_files)
            case (False, False):
                raise ValueError("in_files and/or params must be specified in inputs")

    # @field_validator(""):
    # @classmethod
    # def path_exists(cls, v: str) -> Path:
    #     p = Path(v)
    #     assert p.exists(), f"{p} does not exist"
