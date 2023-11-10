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
    """
    Helper to monitor file size and inode.

    Parameters
    ----------
    filename : str
        The filename to monitor.
    """
    #: The output of ``os.stat()`` for our file
    stat: os.stat_result
    #: The user-provided position that's been read until (inclusive)
    position: int
    #: The last inode of the file.
    inode: int

    def __init__(self, filename: str):
        self.filename = filename
        self.inode = -1
        self.position = 0
        self.check()

    def reset(self) -> None:
        """
        Reset the position and inode number.

        On the first reset, sets ``position`` to the end of the file.
        On subsequent resets, this starts at the beginning of the file (for
        when the inode changes and we have a new file).
        """
        if self.inode == -1:
            # First open at startup; start at the end of the file
            self.position = self.stat.st_size
        else:
            # New file; start at the beginning
            self.position = 0
        self.inode = self.stat.st_ino

    def check(self) -> None:
        """Update our statistics of the file."""
        self.stat = os.stat(self.filename)
        if self.stat.st_ino != self.inode:
            self.reset()

    @property
    def data_available(self) -> bool:
        """
        Is there new data available to be read?

        Returns
        -------
        bool
        """
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
    """
    A monitored file.

    Attributes
    ----------
    filename : str
        The full filename.
    monitor : FileSizeMonitor
        The size monitor.
    fp : io.TextIOBase or None
        The file object, if open.
    last_update : float
        ``time.monotonic()``-based timestamp when the file was last updated.
    lines : Deque[Tuple[float, str]]
        Lines available to be squashed. Tuples of ``time.time``-based timestamp
        and message.
    squasher : Squasher
        The squasher instance for squashing ``lines``.
    num_bytes_in : int
        Bytes (or rather characters) read in.
    num_lines_in : int
        Full lines read in.
    buffer : str
        Buffer for when a partial line is read from the file.  Preprended
        to the subsequent read.
    short_name : str
        Short name or identifier for the file, to be used in logs.
    """
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

    def open(self, seek_pos: Optional[int] = None) -> None:
        """
        Open the file and seek to the byte position, relative to the file start.

        Ensures that the file is set to non-blocking for the monitor.

        Parameters
        ----------
        seek_pos : int, optional
            Seek to this position, in bytes.

        Raises
        ------
        RuntimeError
            If the file is already open
        IOError, FileNotFoundError, etc.
            If the file fails to open.
        """
        if self.fp is not None:
            raise RuntimeError("File already open")

        self.fp = open(self.filename, "rt", encoding="latin-1")
        os.set_blocking(self.fp.fileno(), False)

        if seek_pos is not None:
            self.fp.seek(seek_pos)

    def close(self) -> None:
        """Close the file."""
        if self.fp is None:
            return

        self.fp.close()
        self.fp = None

    def read(self) -> None:
        """
        Read data from the file at the current position and update the state.

        No-operation if the ``read`` would be blocking.

        Raises
        ------
        IOError
            If the read failes otherwise.
        """
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
        """
        Squash the log lines into a list of `Message`.

        Returns
        -------
        List[Message]
        """
        self.squasher = Squasher()
        while self.lines:
            # Atomic popping of lines to avoid locking
            local_timestamp, line = self.lines.popleft()
            self.squasher.add_lines(line, local_timestamp=local_timestamp)
        return self.squasher.squash()

    @property
    def elapsed_since_last_update(self) -> float:
        """
        The amount of time in seconds since the last update of the file.

        Returns
        -------
        float
        """
        return time.monotonic() - self.last_update

    @classmethod
    def from_filename(cls, filename: str, short_name: str = "") -> File:
        """
        Create a File instance, given the filename and (optional) short_name.

        Parameters
        ----------
        filename : str
            The filename.
        short_name : str, optional
            The short identifier for the file.

        Returns
        -------
        File
        """
        return cls(
            filename=filename,
            monitor=FileSizeMonitor(filename),
            fp=None,
            short_name=short_name,
        )


class FileReaderThread:
    """
    A thread for repetitively reading a list of opened files.

    Parameters
    ----------
    close_timeout : float, optional
        If no updates were found from a file in this period, close the file.
        Defaults to 30 seconds.
    poll_period : float, optional
        Time to sleep between polls.
        Defaults to 0.0 seconds, or enough time to yield to other threads.
    """
    files: Dict[str, File]
    poll_period: float
    close_timeout: float
    _thread: Optional[threading.Thread] = None

    def __init__(self, close_timeout: float = 30.0, poll_period: float = 0.0):
        self.poll_period = poll_period
        self.close_timeout = close_timeout
        self.files = {}
        self._stop_event = threading.Event()
        self._thread = None
        self._lock = threading.RLock()

    def start(self):
        """Start the polling thread."""
        if self._thread is not None:
            raise RuntimeError("Thread already started")

        self._thread = threading.Thread(target=self.poll_loop, daemon=True)
        self._thread.start()

    def _poll(self):
        """Single poll iteration."""
        with self._lock:
            files = list(self.files.values())

        to_remove = []

        for file in files:
            try:
                file.read()
            except Exception:
                logger.exception(
                    "Failed to read file: %s. Removing from list.",
                    file.filename
                )
                to_remove.append(file)
            else:
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
        """The main poll loop."""
        logger.info("Poll loop started")
        while not self._stop_event.is_set():
            time.sleep(self.poll_period)
            self._poll()

    def stop(self):
        """Stop polling."""
        self._stop_event.set()

    def add_file(self, file: File) -> None:
        """Add a new file to monitor."""
        if file.fp is None:
            file.open(seek_pos=file.monitor.position)

        with self._lock:
            self.files[file.filename] = file


class PeriodicEvent:
    """
    A helper for events which are to be performed periodically.

    Parameters
    ----------
    period : float
        Period that the event should happen, in seconds.
    ready_at_start : bool, optional
        If the event should be marked as "ready" at startup.
        Defaults to False.
    """

    def __init__(self, period: float, ready_at_start: bool = False):
        self.period = float(period)
        self.ready_at_start = ready_at_start

        if ready_at_start:
            self._last_time = time.monotonic() - period
        else:
            self._last_time = time.monotonic()

    @property
    def is_ready(self) -> bool:
        """Is the event ready to be run?"""
        return (time.monotonic() - self._last_time) > self.period

    def check(self) -> bool:
        """
        Check if ready.

        Returns
        -------
        bool
            True if ready.  The caller is expected to perform its operation,
            and a new period will start implicitly in this class.
        """
        if self.is_ready:
            self._last_time = time.monotonic()
            return True
        return False


@dataclass
class GlobalMonitorStatistics:
    #: Time at startup
    startup_time: float = field(default_factory=time.monotonic)
    #: Bytes read in
    bytes_in: int = 0
    #: Bytes written out
    bytes_out: int = 0
    #: Lines read in
    lines_in: int = 0
    #: Lines written out
    lines_out: int = 0

    def clear(self) -> None:
        """Clear the statistics."""
        self.bytes_in = 0
        self.bytes_out = 0
        self.lines_in = 0
        self.lines_out = 0

    @property
    def bytes_percent(self) -> float:
        """Percentage calculation of (bytes out / bytes in)."""
        if self.bytes_in <= 0:
            return 1.0
        return (self.bytes_out / self.bytes_in) * 100.

    @property
    def lines_percent(self) -> float:
        """Percentage calculation of (lines out / lines in)."""
        if self.lines_in <= 0:
            return 1.0
        return (self.lines_out / self.lines_in) * 100.

    @property
    def elapsed_time(self) -> float:
        """Elapsed time since startup."""
        return time.monotonic() - self.startup_time

    @property
    def elapsed_time_timedelta(self) -> datetime.timedelta:
        """Elapsed time since startup, as a timedelta."""
        return datetime.timedelta(seconds=self.elapsed_time)

    def __str__(self) -> str:
        return (
            f"Running for {self.elapsed_time_timedelta}: "
            f"{self.bytes_out} bytes out / {self.bytes_in} bytes in "
            f"= {self.bytes_percent:.2f}% "
            f"and {self.lines_out} lines out / {self.lines_in} lines in "
            f"= {self.lines_percent:.2f}%"
        )


class GlobalMonitor:
    """
    Primary monitor class for tailing and squashing log files.

    Parameters
    ----------
    file_glob : str
        Glob string to match files.
    short_name_regex : str, optional
        Regular expression to match the filename to its identifier "short
        name". Defaults to r"/cds/data/iocData/(?P<name>.*)/iocInfo/.*",
        because this was written for LCLS PCDS/ECS.
    start_thread : bool, optional
        Start the monitor thread on initialization. Defaults to True.
    """
    files: Dict[str, File]
    file_glob: str
    stats: GlobalMonitorStatistics
    short_name_regex: re.Pattern
    reader: FileReaderThread
    _stop_event: threading.Event

    def __init__(
        self,
        file_glob: str,
        short_name_regex: str = r"/cds/data/iocData/(?P<name>.*)/iocInfo/.*",
        start_thread: bool = True,
    ):
        self.file_glob = file_glob
        self.files = {}
        self.stats = GlobalMonitorStatistics()
        self.short_name_regex = re.compile(short_name_regex)
        self.reader = FileReaderThread()
        self._stop_event = threading.Event()
        if start_thread:
            self.reader.start()

    @property
    def monitored_files(self) -> List[str]:
        """
        File names that are currently being monitored.

        Returns
        -------
        List[str]
        """
        return [fn for fn, info in self.files.items() if info.fp is not None]

    def get_short_name_from_filename(self, filename: str) -> str:
        """
        Get a "short name" for the file, its user-friendly identifier.

        In the case of the default/normal EPICS IOCs, this is the IOC name.

        Parameters
        ----------
        filename : str
            The filename.

        Returns
        -------
        str
            A short name, if it matches the short_name_regex.  The full name
            otherwise.
        """
        match = self.short_name_regex.fullmatch(filename)
        if match is None:
            return filename
        return match.groupdict()["name"]

    def update(self) -> None:
        """
        Check for files that were recently added, modified, or removed.
        """
        all_files = glob.glob(self.file_glob)
        new_files = set(all_files) - set(self.files)
        removed_files = set(self.files) - set(all_files)

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
                try:
                    self.reader.add_file(file)
                except Exception as ex:
                    logger.warning("Failed to open file %r (%s: %s)", fn, type(ex).__name__, ex)
                    logger.debug("Failed to open file %r", fn, exc_info=True)

        if len(previously_monitored) != len(self.monitored_files):
            logger.warning(
                "Monitored files: %d of %d", len(self.monitored_files), len(self.files)
            )

        for file in removed_files:
            self.files[file].close()

    def squash(self, out_file=sys.stdout, raw_out_file=None) -> None:
        """
        Squash the output from all monitored files and track statistics.

        Parameters
        ----------
        out_file : file-like object
            File to write squashed, JSON-dumped Message instances to.
        raw_out_file : file-like object
            An optional file to write raw log messages to.  Lines are
            prepended with the short name.
        """
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
                line_dict = line.asdict()
                line_dict["ioc"] = file.short_name
                output = json.dumps(line_dict)
                print(output, file=out_file)
                num_out_bytes += len(output) + 1  # include the newline

            # If we were in the middle of parsing a group, there may be some
            # lines for next time
            for line in reversed(file.squasher.pending_lines):
                file.lines.insert(0, (line.timestamp.timestamp(), line.value))

        self.stats.bytes_in += num_bytes_in
        self.stats.bytes_out += num_out_bytes

        self.stats.lines_in += num_lines_in
        self.stats.lines_out += num_lines_out

    def run(
        self,
        file_check_period: float = 1.0,
        squash_period: float = 30.0,
        show_statistics_after: int = 2,
    ) -> None:
        """
        Primary monitor loop.

        Intended to be run in the main thread.

        Parameters
        ----------
        file_check_period : float, optional
            Period, in seconds, to check for new/removed files. Defaults to
            1.0 second.
        squash_period : float, optional
            Period, in seconds, to squash messages from all files. Defaults to
            30.0 seconds.
        show_statistics_after : int, optional
            Show statistics after every n-squashes. Defaults to 2 squashes.
        """
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

    def stop(self) -> None:
        """Stop the monitoring process."""
        self._stop_event.set()
        self.reader.stop()


if __name__ == "__main__":
    logging.basicConfig(level="DEBUG")
    monitor = GlobalMonitor(file_glob=sys.argv[1])
    try:
        monitor.run()
    except KeyboardInterrupt:
        ...
