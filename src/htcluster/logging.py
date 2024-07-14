import sys

import structlog


def log_config():
    structlog.configure(logger_factory=structlog.PrintLoggerFactory(sys.stderr))
