import argparse

import structlog
import zmq


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--port",
        "-p",
        type=int,
        default=5555,
        help="port for zmq to bind and listen on",
    )
    parser.add_argument(
        "--json-logging",
        action="store_true",
        default=False,
        help="json logging output",
    )
    return parser.parse_args()


def main():
    args = parse_args()
    context = zmq.Context()
    socket = context.socket(zmq.REP)
    socket.bind(f"tcp://*:{args.port}")

    if args.json_logging:
        structlog.configure(processors=[structlog.processors.JSONRenderer()])

    log = structlog.get_logger()
    while True:
        message = socket.recv()
        log.info("received request", message=message)
        socket.send(b"ack")


if __name__ == "__main__":
    main()
