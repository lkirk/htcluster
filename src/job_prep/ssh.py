from pathlib import Path

from paramiko import SFTPClient, SSHClient


def chtc_ssh_client() -> SSHClient:
    client = SSHClient()
    client.load_system_host_keys()
    # TODO: config
    client.connect("ap2002.chtc.wisc.edu", username="lkirk2")
    return client


def mkdir(client: SFTPClient, path: Path) -> None:
    try:
        client.stat(str(path))
        raise Exception(f"{path} exists on remote server")
    except FileNotFoundError:
        pass
    try:
        client.mkdir(str(path))
    except FileNotFoundError:
        raise Exception(f"{path.parent} does not exist on remote server")


def write_file(client: SFTPClient, dest: Path, data: str) -> None:
    with client.open(str(dest), "w") as fp:
        fp.write(data)


def copy_file(client: SFTPClient, source: Path, dest: Path) -> None:
    client.put(str(source), str(dest), confirm=True)
