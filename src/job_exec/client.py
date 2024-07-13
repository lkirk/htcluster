import zmq
import zmq.ssh


def connect(
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
