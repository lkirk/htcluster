from pathlib import Path

import structlog
from paramiko import SFTPClient, SSHClient

LOG = structlog.get_logger()


def chtc_ssh_client(remote_user: str, remote_server: str) -> SSHClient:
    client = SSHClient()
    client.load_system_host_keys()
    client.connect(remote_server, username=remote_user)
    return client


def mkdir_sftp(client: SFTPClient, path: Path) -> None:
    try:
        client.stat(str(path))
        raise Exception(f"{path} exists on remote server")
    except FileNotFoundError:
        pass
    try:
        LOG.info("creating dir", dir=str(path))
        client.mkdir(str(path))
    except FileNotFoundError:
        raise Exception(f"{path.parent} does not exist on remote server")


def write_file_sftp(client: SFTPClient, dest: Path, data: str) -> None:
    LOG.info("writing file", dest=str(dest))
    with client.open(str(dest), "w") as fp:
        fp.write(data)


def copy_file_sftp(client: SFTPClient, source: Path, dest: Path) -> None:
    LOG.info("copying file", src=str(source), dest=str(dest))
    client.put(str(source), str(dest), confirm=True)
