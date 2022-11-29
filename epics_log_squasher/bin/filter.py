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
        'period',
        type=float,
        default=10.0,
        help='Get help on this.',
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
            for line in squashed.lines:
                if len(squashed.lines) != acquired:
                    print("**", len(acquired), "down to", len(squashed.lines))
                print(line)
    except KeyboardInterrupt:
        ...
