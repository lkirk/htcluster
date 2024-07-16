"""
The cluster uses python 3.9, so split these validators into their
own module to ensure that the server script is completely compatible
with python 3.9
"""

import re
from pathlib import Path
from typing import Optional, Union

from pydantic import field_validator

from .validator_base import BaseModel

SIZE_PAT = re.compile("[0-9]+(K|M|G|T)(B)?")


class JobSettings(BaseModel):
    name: str
    memory: str
    disk: str
    cpus: int
    entrypoint: str
    docker_image: str
    classads: str = ""
    in_staging: bool = False
    out_staging: bool = False

    @field_validator("disk", "memory")
    @classmethod
    def contains_units(cls, v: str) -> str:
        assert not v.startswith("-"), "value can not be negative"
        assert (
            SIZE_PAT.match(v) is not None
        ), f"value must be a valid size, matching {SIZE_PAT.pattern}"
        return v


class JobArgs(BaseModel):
    in_files: Optional[Path] = None
    out_files: Optional[Path] = None
    params: Optional[dict] = None


class RunnerPayload(BaseModel):
    job: JobSettings
    job_dir: Path
    out_dir: Path
    log_dir: Path
    params: list[JobArgs]
    in_files: list[Path] = []
    out_files: list[Path] = []
    in_files_staging: list[str] = []
    out_files_staging: list[str] = []

    def get_in_file(self, idx: int) -> Union[Path, str]:
        if self.job.in_staging:
            return self.in_files_staging[idx]
        return self.in_files[idx]

    def get_out_file(self, idx: int) -> Union[Path, str]:
        if self.job.out_staging:
            return self.out_files_staging[idx]
        return self.out_files[idx]

    def has_inputs(self):
        return (len(self.in_files) > 0) or (len(self.in_files_staging) > 0)

    def has_outputs(self):
        return (len(self.out_files) > 0) or (len(self.out_files_staging) > 0)
