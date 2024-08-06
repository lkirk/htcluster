import sys

import structlog


def log_config():
    structlog.configure(logger_factory=structlog.PrintLoggerFactory(sys.stderr))
    processors = structlog.get_config()["processors"]

    # replace default timestamper
    ts_idx = [
        i
        for i, v in enumerate(processors)
        if isinstance(v, structlog.processors.TimeStamper)
    ]
    if len(ts_idx) == 1:
        ts_idx = ts_idx[0]
        processors[ts_idx] = structlog.processors.TimeStamper(
            # ensure that we log in local time
            fmt="%Y-%m-%d %H:%M:%S",
            utc=False,
        )
    elif len(ts_idx) > 1:
        raise Exception(f"found more than one stamper {ts_idx}")

    structlog.configure(processors=processors)
