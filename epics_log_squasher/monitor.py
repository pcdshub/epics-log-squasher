from __future__ import annotations

import glob
import io
import logging
import os
import sys
import threading
import time
from dataclasses import dataclass, field
from typing import Dict, Optional

logger = logging.getLogger(__name__)


class FileSizeMonitor:
    stat: stat.fstat
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


@dataclass
class File:
    filename: str
    monitor: FileSizeMonitor
    fp: Optional[io.TextIOBase] = None
    last_update: float = field(default_factory=time.monotonic)
    buffer: list[str] = field(default_factory=list)

    def close(self):
        self.fp.close()
        self.fp = None

    def open(self, seek_pos: Optional[int] = None):
        if self.fp is not None:
            # self.fp.close()
            raise RuntimeError()

        self.fp = open(self.filename, "rt")
        os.set_blocking(self.fp.fileno(), False)

        if seek_pos is not None:
            self.fp.seek(seek_pos)

    def read(self):
        try:
            data = self.fp.read()
        except BlockingIOError:
            return

        if len(data) == 0:
            return

        self.buffer.append(data)
        self.monitor.position = self.fp.tell()
        self.last_update = time.monotonic()
        logger.info(
            "%s has %d bytes buffered", self.filename, sum(len(b) for b in self.buffer)
        )

    @property
    def elapsed_since_last_update(self) -> float:
        return time.monotonic() - self.last_update

    @classmethod
    def from_filename(cls, filename: str) -> File:
        return cls(
            filename=filename,
            monitor=FileSizeMonitor(filename),
            fp=None,
        )


class FileReaderThread:
    _thread: Optional[threading.Thread]
    files: Dict[int, File]

    def __init__(self, close_timeout: float = 30.0):
        self.close_timeout = close_timeout
        self.files = {}
        self._stop_event = threading.Event()
        self._thread = None
        self._lock = threading.RLock()

    def start(self):
        self._thread = threading.Thread(target=self.poll_loop, daemon=True)
        self._thread.start()

    def poll_loop(self):
        logger.info("Poll loop started")
        while not self._stop_event.is_set():
            time.sleep(0)

            with self._lock:
                files = list(self.files.items())

            to_remove = []

            for fn, file in files:
                file.read()
                if file.elapsed_since_last_update > self.close_timeout:
                    to_remove.append(file)

            with self._lock:
                for file in to_remove:
                    logger.warning(
                        "%s has not updated within the past %.1f seconds; closing "
                        "and freeing up resources",
                        file.filename, self.close_timeout,
                    )
                    file.close()
                    self.files.pop(file.filename)

    def stop(self):
        self._stop_event.set()
        self.poller.close()

    def add_file(self, file: File):
        if file.fp is None:
            file.open(seek_pos=file.monitor.position)

        with self._lock:
            self.files[file.filename] = file


class GlobalMonitor:
    files: Dict[str, File]

    def __init__(self, file_glob: str):
        self.file_glob = file_glob
        self.files = {}
        self.reader = FileReaderThread()
        self.reader.start()

    @property
    def monitored_files(self):
        return [fn for fn, info in self.files.items() if info.fp is not None]

    @property
    def unmonitored_files(self):
        return [fn for fn, info in self.files.items() if info.fp is None]

    def update(self):
        all_files = glob.glob(self.file_glob)
        new_files = set(all_files) - set(self.files)
        # removed_files = set(self.files) - set(all_files)

        for file in sorted(new_files):
            self.files[file] = File.from_filename(file)

        previously_monitored = self.monitored_files
        for fn, file in self.files.items():
            file.monitor.check()
            if fn not in previously_monitored and file.monitor.data_available:
                logger.info("Log file changed: %s", file.filename)
                self.reader.add_file(file)

        if len(previously_monitored) != len(self.monitored_files):
            logger.warning("Monitored files: %d of %d", len(self.monitored_files), len(self.files))

        for fn in self.monitored_files:
            file = self.files[fn]
            # file.fp.seek(file.monitor.stat.st_size)
            file.position = file.monitor.stat.st_size
            # logger.info("File %s Pos: %d of %d", fn, file.position, file.monitor.stat.st_size)


if __name__ == "__main__":
    logging.basicConfig(level="DEBUG")
    monitor = GlobalMonitor(file_glob=sys.argv[1])
    while True:
        monitor.update()
        time.sleep(1)
