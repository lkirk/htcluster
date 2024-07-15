import os
from pathlib import Path

import yaml
from pydantic import field_validator

from htcluster.validator_base import BaseModel

# Override with env variable
CONFIG_PATH = Path(
    os.environ.get("HTCLUSTER_CONF_PATH", "~/.config/htcluster/config.yml")
).expanduser()


class Config(BaseModel):
    ssh_remote_user: str
    ssh_remote_server: str
    github_user: str
    github_token: Path
    docker_token: Path
    zmq_bind_port: int

    @field_validator("github_token", "docker_token")
    @classmethod
    def path_exists(cls, v: Path) -> Path:
        v = v.expanduser()
        assert v.exists(), f"{v} does not exist"
        assert v.is_file(), f"{v} is not a file"
        return v


def load_config():
    if not CONFIG_PATH.exists():
        raise Exception(f"config path: {CONFIG_PATH} does not exist")
    with open(CONFIG_PATH) as fp:
        return Config(**yaml.safe_load(fp))
