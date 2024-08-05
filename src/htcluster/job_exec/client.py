import gzip
import warnings

import zmq
from cryptography.utils import CryptographyDeprecationWarning

# TODO: remove when this is resolved
with warnings.catch_warnings(action="ignore", category=CryptographyDeprecationWarning):
    import zmq.ssh

from htcluster.validators_3_9_compat import RunnerPayload


def connect_remote(
    remote_port: int, remote_ssh_user: str, remote_ssh_server: str
) -> zmq.SyncSocket:
    context = zmq.Context()
    socket = context.socket(zmq.REQ)
    zmq.ssh.tunnel_connection(
        socket,
        f"tcp://127.0.0.1:{remote_port}",
        f"{remote_ssh_user}@{remote_ssh_server}",
    )
    return socket


def connect_local(port: int) -> zmq.SyncSocket:
    context = zmq.Context()
    socket = context.socket(zmq.REQ)
    socket.connect(f"tcp://localhost:{port}")
    return socket


def send(socket: zmq.SyncSocket, runner_payload: RunnerPayload):
    # TODO: timeouts
    socket.send(gzip.compress(runner_payload.model_dump_json().encode()))
    assert socket.recv() == b"ack"
