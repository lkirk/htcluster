from pathlib import Path
from typing import Self, TypeAlias

from pydantic import ConfigDict, field_validator, model_validator

from htcluster.job_submit.yaml import ImplicitOut, yaml  # TODO: does module make sense?

from .validator_base import BaseModel
from .validators_3_9_compat import JobSettings

# nested types are fine, validation on the workflow side handles this
ParamType: TypeAlias = str | int | float | list | dict
ParamCollection: TypeAlias = list[ParamType | dict[str, ParamType]]


class JobParams(BaseModel):
    model_config = ConfigDict(arbitrary_types_allowed=True)

    in_files: list[Path] = []
    out_files: list[Path] | ImplicitOut
    params: dict[str, ParamCollection] | None = None
    n_jobs: int = 0
    _params_len: int = 0

    @field_validator("out_files")
    @classmethod
    def out_files_relative(cls, v: Path | ImplicitOut) -> Path | ImplicitOut:
        if isinstance(v, Path):
            assert Path(v.name) == v, "output paths should be name only, no subdirs"
        return v

    def has_inputs(self) -> bool:
        return len(self.in_files) > 0

    def has_outputs(self) -> bool:
        match self.out_files:
            case list():
                return len(self.out_files) > 0
        return False

    def has_params(self) -> bool:
        return self.params is not None

    @model_validator(mode="after")
    def validate_params(self) -> Self:
        # TODO: this should be a field validator
        if self.params is not None:
            first_key = next(iter(self.params.keys()))
            first_len = len(self.params[first_key])
            for k, v in self.params.items():
                if (n_params := len(v)) != first_len:
                    raise ValueError(
                        f"number of params must be equal for all params. {first_key} "
                        f"has {first_len} params whereas {k} has {n_params}"
                    )
            self._params_len = first_len
        if self.has_inputs() and self.has_params():
            self.n_jobs = self._assert_ins_equal_params()
        if self.has_inputs() and self.has_outputs():
            self.n_jobs = self._assert_ins_equal_outs()
        if self.has_params() and self.has_outputs():
            self.n_jobs = self._assert_outs_equal_params()
        if (not self.has_inputs()) and (not self.has_params()):
            # TODO: do we want to validate the presence of outputs in certain cases?
            raise ValueError("in_files and/or params must be specified in inputs")
        return self

    def _assert_ins_equal_outs(self):
        assert not isinstance(self.out_files, ImplicitOut)  # mypy
        n_in = len(self.in_files)
        n_out = len(self.out_files)
        if n_in != n_out:
            raise ValueError(
                "The number of specified output files must be equal to the number "
                f"of specified input files. Got {n_in} input files and {n_out} "
                "output files."
            )
        return n_in

    def _assert_ins_equal_params(self):
        n_in = len(self.in_files)
        if n_in != self._params_len:
            raise ValueError(
                "The number of input files must match the number of parameters, if "
                f"parameters are specified. There are {self._params_len} params and "
                f"{n_in} input files."
            )
        return n_in

    def _assert_outs_equal_params(self):
        assert not isinstance(self.out_files, ImplicitOut)  # mypy
        n_out = len(self.out_files)
        if n_out != self._params_len:
            raise ValueError(
                "The number of output files must match the number of parameters, if "
                f"parameters are specified. There are {self._params_len} params and "
                f"{n_out} output files."
            )
        return n_out


class ClusterJob(BaseModel):
    model_config = ConfigDict(arbitrary_types_allowed=True)
    job: JobSettings
    params: JobParams

    @classmethod
    def from_yaml_file(cls: type[Self], path: Path) -> Self:
        with open(path, "r") as fp:
            return cls(**yaml.safe_load(fp))

    @classmethod
    def from_yaml_str(cls: type[Self], s: str) -> Self:
        return cls(**yaml.safe_load(s))
