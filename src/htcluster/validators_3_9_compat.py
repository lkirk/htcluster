"""
The cluster uses python 3.9, so split these validators into their
own module to ensure that the server script is completely compatible
with python 3.9
"""
import re
from pathlib import Path
from typing import Optional

from pydantic import BaseModel, field_validator

SIZE_PAT = re.compile("[0-9]+(K|M|G|T)(B)?")

class JobSettings(BaseModel):
    name: str
    memory: str
    disk: str
    cpus: int
    entrypoint: str
    docker_image: str
    classads: str = ""

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
    out_dir: Path
    params: list[JobArgs]
    in_files: list[Path]
    out_files: list[Path]
    # in_from_staging: bool

