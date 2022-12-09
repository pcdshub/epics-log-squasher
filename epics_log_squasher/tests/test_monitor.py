import datetime
import io
import json
import tempfile
import threading
import time
import unittest.mock
from typing import Generator, Union

import pytest

from .. import monitor
from ..parser import Message


class MockLogFile:
    filename: str
    bytes_written: int
    lines_written: int
    fp: Union[io.TextIOBase, "tempfile._TemporaryFileWrapper"]

    def __init__(self):
        self.fp = tempfile.NamedTemporaryFile(mode="wt", encoding="latin-1")
        self.filename = self.fp.name
        self.bytes_written = 0
        self.lines_written = 0

    def close(self):
        self.fp.close()

    def write_line(self, line: str, include_timestamp: bool = True):
        assert "\n" not in line
        if include_timestamp:
            ts = datetime.datetime.now().strftime("%Y/%m/%d %H:%M:%S.%f")
            line = f"{ts} {line}"
        line = f"{line}\n"
        self.fp.write(line)
        self.fp.flush()
        self.bytes_written += len(line)
        self.lines_written += 1


class MonitorTest:
    global_monitor: monitor.GlobalMonitor
    mock_log_file: MockLogFile
    file: monitor.File

    def __init__(self):
        self.mock_log_file = MockLogFile()

        self.global_monitor = monitor.GlobalMonitor(
            file_glob=self.mock_log_file.filename,
            start_thread=False,
        )
        self.global_monitor.update()

        file, = list(self.global_monitor.files.values())
        file.short_name = "short_name"
        assert file.filename == self.mock_log_file.filename
        self.file = file

    def clear_statistics(self):
        self.mock_log_file.lines_written = 0
        self.mock_log_file.bytes_written = 0
        for file_ in self.global_monitor.files.values():
            file_.num_lines_in = 0
        self.global_monitor.stats.clear()

    def update(self):
        self.global_monitor.update()
        self.global_monitor.reader._poll()

    def squash(self):
        with io.StringIO() as fp:
            self.global_monitor.squash(out_file=fp)
            return fp.getvalue()

    def close(self):
        for file in self.global_monitor.files.values():
            file.close()


@pytest.fixture(scope="function")
def monitor_test() -> Generator[MonitorTest, None, None]:
    test = MonitorTest()
    try:
        yield test
    finally:
        test.close()


@pytest.fixture(scope="function")
def mock_log_file(monitor_test: MonitorTest) -> MockLogFile:
    return monitor_test.mock_log_file


def test_global_monitor(monitor_test: MonitorTest, mock_log_file: MockLogFile):
    mon = monitor_test.global_monitor
    file = monitor_test.file

    for count in [2, 5, 10]:
        # Reset our statistics
        monitor_test.clear_statistics()

        for _ in range(count):
            mock_log_file.write_line("hello")

        monitor_test.update()

        assert file.num_lines_in == count
        assert len(mon.monitored_files) == 1

        results = monitor_test.squash()

        expected = f"[{count}x] hello"
        assert expected in results

        expected_full = Message(message=expected).asdict()
        expected_full["ioc"] = "short_name"
        full_message_length = len(json.dumps(expected_full)) + 1
        assert mon.stats.bytes_in == mock_log_file.bytes_written
        assert mon.stats.bytes_out == full_message_length
        assert mon.stats.lines_in == count
        assert mon.stats.lines_out == 1


def test_global_monitor_file_overwrite(
    monitor_test: MonitorTest,
    mock_log_file: MockLogFile,
):
    mon = monitor_test.global_monitor
    file = monitor_test.file

    count = 2
    for _ in range(count):
        mock_log_file.write_line("hello")

    monitor_test.update()

    assert file.num_lines_in == count
    assert len(mon.monitored_files) == 1

    results = monitor_test.squash()

    expected = f"[{count}x] hello"
    assert expected in results

    filename = mock_log_file.filename

    file.close()
    mock_log_file.close()

    # Overwrite the old file - new file, new inode
    mock_log_file.fp = open(filename, "wt")

    for _ in range(count):
        mock_log_file.write_line("new file")

    monitor_test.clear_statistics()
    monitor_test.update()

    assert file.num_lines_in == count
    assert len(mon.monitored_files) == 1

    results = monitor_test.squash()

    expected = f"[{count}x] new file"
    assert expected in results


def test_global_monitor_file_ioerror(monitor_test: MonitorTest, mock_log_file: MockLogFile, monkeypatch):
    count = 2
    for _ in range(count):
        mock_log_file.write_line("hello")

    monitor_test.update()

    assert monitor_test.file.num_lines_in == count
    assert len(monitor_test.global_monitor.monitored_files) == 1

    read_raise = unittest.mock.Mock(side_effect=IOError("nope"))

    results = monitor_test.squash()

    expected = f"[{count}x] hello"
    assert expected in results

    monkeypatch.setattr(monitor_test.file, "read", read_raise)

    monitor_test.clear_statistics()
    monitor_test.update()

    assert monitor_test.file.num_lines_in == 0
    assert len(monitor_test.global_monitor.monitored_files) == 0

    assert monitor_test.squash() == ""
    assert read_raise.called


def test_global_monitor_file_open_error(monitor_test: MonitorTest, mock_log_file: MockLogFile, monkeypatch):
    open_permission_error = unittest.mock.Mock(side_effect=PermissionError("nope"))
    monkeypatch.setattr(monitor_test.file, "open", open_permission_error)

    monitor_test.update()
    mock_log_file.write_line("hello")
    monitor_test.update()

    assert monitor_test.file.num_lines_in == 0
    assert len(monitor_test.global_monitor.monitored_files) == 0
    assert open_permission_error.called


def test_run_stop_smoke(monitor_test: MonitorTest, monkeypatch):
    squash_event = threading.Event()
    exited_event = threading.Event()

    def mock_squash(*args, **kwargs):
        squash_event.set()

    def run_thread():
        monitor_test.global_monitor.run(
            squash_period=0.01,
            file_check_period=0.1,
            show_statistics_after=1,
        )
        exited_event.set()

    monkeypatch.setattr(monitor_test.global_monitor, "squash", mock_squash)
    threading.Thread(target=run_thread, daemon=True).start()

    # Ensure we've called squash at least once
    assert squash_event.wait(timeout=1.0)

    # Stop it and make sure that it exits
    monitor_test.global_monitor.stop()
    assert exited_event.wait(timeout=1.0)


def test_periodic_event():
    event = monitor.PeriodicEvent(period=0.075, ready_at_start=False)
    assert not event.check()
    time.sleep(0.1)
    assert event.check()


def test_periodic_event_ready_at_start():
    event = monitor.PeriodicEvent(period=0.075, ready_at_start=True)
    assert event.is_ready


def test_stats_smoke():
    stats = monitor.GlobalMonitorStatistics()
    assert stats.bytes_percent == 1.0
    assert stats.lines_percent == 1.0
    assert stats.elapsed_time > 0.0
    stats.elapsed_time_timedelta
    str(stats)

    stats.bytes_in = 1
    stats.lines_in = 1
    assert stats.bytes_percent == 0.0
    assert stats.lines_percent == 0.0
