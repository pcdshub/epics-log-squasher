from __future__ import annotations

import collections
import datetime
import glob
import io
import json
import logging
import os
import re
import sys
import threading
import time
from dataclasses import dataclass, field
from typing import Deque, Dict, List, Optional, Tuple

from .parser import Message, Squasher

logger = logging.getLogger(__name__)


class FileSizeMonitor:
    stat: os.stat_result
    position: int

    def __init__(self, filename: str):
        self.filename = filename
        self.inode = -1
        self.position = 0
        self.check()

    def reset(self):
        if self.inode == -1:
            # First open at startup; start at the end of the file
            self.position = self.stat.st_size
        else:
            # New file; start at the beginning
            self.position = 0
        self.inode = self.stat.st_ino

    def check(self):
        self.stat = os.stat(self.filename)
        if self.stat.st_ino != self.inode:
            self.reset()

    @property
    def data_available(self) -> bool:
        return self.stat.st_size > self.position


def _split_lines(data: str) -> Tuple[str, List[str]]:
    """
    Split lines and return a buffer to be used next time, if applicable.

    Returns
    -------
    buffer : str
        Remaining characters without a newline. That is, the buffer to prepend
        onto the next one.
    lines : List[str]
        List of log lines.
    """
    lines = data.splitlines(keepends=True)
    if "\n" in lines[-1]:
        buffer = ""
    else:
        buffer = lines.pop(-1)
    return buffer, [line.rstrip() for line in lines]


@dataclass
class File:
    filename: str
    monitor: FileSizeMonitor
    fp: Optional[io.TextIOBase] = None
    last_update: float = field(default_factory=time.monotonic)
    lines: Deque[Tuple[float, str]] = field(default_factory=collections.deque)
    squasher: Squasher = field(default_factory=Squasher)
    num_bytes_in: int = 0
    num_lines_in: int = 0
    buffer: str = ""
    short_name: str = ""

    def close(self):
        if self.fp is None:
            return

        self.fp.close()
        self.fp = None

    def open(self, seek_pos: Optional[int] = None):
        if self.fp is not None:
            # self.fp.close()
            raise RuntimeError()

        self.fp = open(self.filename, "rt", encoding="latin-1")
        os.set_blocking(self.fp.fileno(), False)

        if seek_pos is not None:
            self.fp.seek(seek_pos)

    def read(self):
        assert self.fp is not None
        try:
            data = self.fp.read()
        except BlockingIOError:
            return

        if len(data) == 0:
            return

        self.buffer, lines = _split_lines(self.buffer + data)
        self.num_bytes_in += len(data)
        self.num_lines_in += len(lines)

        ts = time.time()
        for line in lines:
            self.lines.append((ts, line))

        self.monitor.position = self.fp.tell()
        self.last_update = time.monotonic()

    def squash(self) -> List[Message]:
        self.squasher = Squasher()
        while self.lines:
            # Atomic popping of lines to avoid locking
            local_timestamp, line = self.lines.popleft()
            self.squasher.add_lines(line, local_timestamp=local_timestamp)
        return self.squasher.squash()

    @property
    def elapsed_since_last_update(self) -> float:
        return time.monotonic() - self.last_update

    @classmethod
    def from_filename(cls, filename: str, short_name: str = "") -> File:
        return cls(
            filename=filename,
            monitor=FileSizeMonitor(filename),
            fp=None,
            short_name=short_name,
        )


class FileReaderThread:
    _thread: Optional[threading.Thread]
    files: Dict[str, File]

    def __init__(self, close_timeout: float = 30.0):
        self.close_timeout = close_timeout
        self.files = {}
        self._stop_event = threading.Event()
        self._thread = None
        self._lock = threading.RLock()

    def start(self):
        self._thread = threading.Thread(target=self.poll_loop, daemon=True)
        self._thread.start()

    def _poll(self):
        with self._lock:
            files = list(self.files.values())

        to_remove = []

        for file in files:
            file.read()
            if file.elapsed_since_last_update > self.close_timeout:
                to_remove.append(file)

        with self._lock:
            for file in to_remove:
                logger.warning(
                    "%s has not updated within the past %.1f seconds; closing "
                    "and freeing up resources",
                    file.filename,
                    self.close_timeout,
                )
                file.close()
                self.files.pop(file.filename)

    def poll_loop(self):
        logger.info("Poll loop started")
        while not self._stop_event.is_set():
            time.sleep(0)
            self._poll()

    def stop(self):
        self._stop_event.set()

    def add_file(self, file: File):
        if file.fp is None:
            file.open(seek_pos=file.monitor.position)

        with self._lock:
            self.files[file.filename] = file


class PeriodicEvent:
    def __init__(self, period: float, ready_at_start: bool = False):
        self.period = period
        self.ready_at_start = ready_at_start

        if ready_at_start:
            self._last_time = time.monotonic() - period
        else:
            self._last_time = time.monotonic()

    @property
    def is_ready(self) -> bool:
        return (time.monotonic() - self._last_time) > self.period

    def check(self) -> bool:
        if self.is_ready:
            self._last_time = time.monotonic()
            return True
        return False


@dataclass
class GlobalMonitorStatistics:
    startup_time: float = field(default_factory=time.monotonic)
    bytes_in: int = 0
    bytes_out: int = 0
    lines_in: int = 0
    lines_out: int = 0

    def clear(self):
        self.bytes_in = 0
        self.bytes_out = 0
        self.lines_in = 0
        self.lines_out = 0

    @property
    def bytes_percent(self) -> float:
        if self.bytes_in <= 0:
            return 1.0
        return (self.bytes_out / self.bytes_in) * 100.

    @property
    def lines_percent(self) -> float:
        if self.lines_in <= 0:
            return 1.0
        return (self.lines_out / self.lines_in) * 100.

    @property
    def elapsed_time(self) -> float:
        return time.monotonic() - self.startup_time

    @property
    def elapsed_time_timedelta(self) -> datetime.timedelta:
        return datetime.timedelta(seconds=self.elapsed_time)

    def __str__(self) -> str:
        return (
            f"Running for {self.elapsed_time_timedelta}: "
            f"{self.bytes_in} bytes in "
            f"-> {self.bytes_out} bytes out "
            f"({self.bytes_percent:.2f} %). "
            f"{self.lines_in} lines in "
            f"-> {self.lines_out} lines out "
            f"({self.lines_percent:.2f} %)"
        )


class GlobalMonitor:
    files: Dict[str, File]

    def __init__(
        self,
        file_glob: str,
        name_regex: str = r"/cds/data/iocData/(?P<name>.*)/iocInfo/.*",
        start_thread: bool = True,
    ):
        self.file_glob = file_glob
        self.files = {}
        self.stats = GlobalMonitorStatistics()
        self.name_regex = re.compile(name_regex)
        self.reader = FileReaderThread()
        self._stop_event = threading.Event()
        if start_thread:
            self.reader.start()

    @property
    def monitored_files(self) -> List[str]:
        return [fn for fn, info in self.files.items() if info.fp is not None]

    def get_short_name_from_filename(self, filename: str) -> str:
        match = self.name_regex.fullmatch(filename)
        if match is None:
            return filename
        return match.groupdict()["name"]

    def update(self):
        all_files = glob.glob(self.file_glob)
        new_files = set(all_files) - set(self.files)
        # removed_files = set(self.files) - set(all_files)

        for file in sorted(new_files):
            self.files[file] = File.from_filename(
                file,
                short_name=self.get_short_name_from_filename(file)
            )

        previously_monitored = self.monitored_files
        for fn, file in self.files.items():
            file.monitor.check()
            if fn not in previously_monitored and file.monitor.data_available:
                logger.info("Log file changed: %s", file.filename)
                self.reader.add_file(file)

        if len(previously_monitored) != len(self.monitored_files):
            logger.warning(
                "Monitored files: %d of %d", len(self.monitored_files), len(self.files)
            )

        for fn in self.monitored_files:
            file = self.files[fn]
            # file.fp.seek(file.monitor.stat.st_size)
            # file.position = file.monitor.stat.st_size
            # logger.info("File %s Pos: %d of %d", fn, file.position, file.monitor.stat.st_size)

    def squash(self, out_file=sys.stdout, raw_out_file=None):
        num_out_bytes = 0
        num_lines_out = 0
        num_bytes_in = 0
        num_lines_in = 0
        for fn in self.monitored_files:
            file = self.files[fn]
            if not file.lines:
                continue

            squashed = file.squash()

            if raw_out_file is not None:
                for line in file.squasher.messages:
                    print(f"{file.short_name} {line.value}", file=raw_out_file)

            num_bytes_in += file.squasher.num_bytes
            num_lines_in += len(file.squasher.messages)
            num_lines_out += len(squashed)
            for line in squashed:
                output = json.dumps(line.asdict())
                print(output, file=out_file)
                num_out_bytes += len(output) + 1  # include the newline

        self.stats.bytes_in += num_bytes_in
        self.stats.bytes_out += num_out_bytes

        self.stats.lines_in += num_lines_in
        self.stats.lines_out += num_lines_out

    def run(
        self,
        file_check_period: float = 1.0,
        squash_period: float = 30.0,
        show_statistics_after: int = 2,
    ):
        file_check = PeriodicEvent(file_check_period, ready_at_start=True)
        do_squash = PeriodicEvent(squash_period)
        squashed_count = 0
        # raw_out_file = open("raw-output.txt", "wt")
        while not self._stop_event.is_set():
            if file_check.check():
                self.update()
            if do_squash.check():
                self.squash()
                sys.stdout.flush()
                squashed_count += 1
                # raw_out_file.flush()
                if squashed_count == show_statistics_after:
                    logger.info("Statistics: %s", str(self.stats))
                    squashed_count = 0

            time.sleep(0.1)

    def stop(self):
        self._stop_event.set()
        self.reader.stop()


if __name__ == "__main__":
    logging.basicConfig(level="DEBUG")
    monitor = GlobalMonitor(file_glob=sys.argv[1])
    try:
        monitor.run()
    except KeyboardInterrupt:
        ...
