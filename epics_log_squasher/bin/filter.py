"""
`epics_log_squasher filter` will filter log data from standard input.
"""

import argparse
import sys
import threading
import time
from typing import List

from ..parser import Squasher

DESCRIPTION = __doc__


def build_arg_parser(argparser=None):
    if argparser is None:
        argparser = argparse.ArgumentParser()

    argparser.description = DESCRIPTION
    argparser.formatter_class = argparse.RawTextHelpFormatter

    argparser.add_argument(
        "--period",
        type=float,
        default=10.0,
        help="Log buffering period",
    )

    return argparser


def _read_thread(lines: List[str], lock: threading.RLock):
    while True:
        line = sys.stdin.readline().strip()
        if line:
            with lock:
                lines.append(line)


def main(period: float = 10.0):
    lines = []
    lock = threading.RLock()
    read_thread = threading.Thread(target=_read_thread, daemon=True, args=(lines, lock))
    read_thread.start()

    bytes_raw = 0
    bytes_filtered = 0
    try:
        while True:
            time.sleep(period)
            if not lines:
                continue

            with lock:
                acquired = lines.copy()
                lines.clear()

            squash = Squasher()
            squash.add_lines("\n".join(acquired))
            squashed = squash.squash()

            bytes_raw += sum(len(line) for line in acquired)
            bytes_filtered += sum(len(line) for line in squashed.lines)

            for line in squashed.lines:
                print(line)
            print(f"({bytes_raw} -> {bytes_filtered} bytes)", file=sys.stderr)
    except KeyboardInterrupt:
        ...
