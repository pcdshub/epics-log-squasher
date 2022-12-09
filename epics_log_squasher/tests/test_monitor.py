import datetime
import io
import json
import tempfile
from typing import Union

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
    mon: monitor.GlobalMonitor
    mock_log_file: MockLogFile
    file: monitor.File

    def __init__(self):
        self.mock_log_file = MockLogFile()

        self.mon = monitor.GlobalMonitor(
            file_glob=self.mock_log_file.filename,
            start_thread=False,
        )
        self.mon.update()

        file, = list(self.mon.files.values())
        file.short_name = "short_name"
        assert file.filename == self.mock_log_file.filename
        self.file = file

    def clear_statistics(self):
        self.mock_log_file.lines_written = 0
        self.mock_log_file.bytes_written = 0
        for file_ in self.mon.files.values():
            file_.num_lines_in = 0
        self.mon.stats.clear()

    def update(self):
        self.mon.update()
        self.mon.reader._poll()

    def squash(self):
        with io.StringIO() as fp:
            self.mon.squash(out_file=fp)
            return fp.getvalue()

    def close(self):
        for file in self.mon.files.values():
            file.close()


def test_global_monitor():
    test = MonitorTest()

    mon = test.mon
    file = test.file
    mock_log_file = test.mock_log_file

    for count in [2, 5, 10]:
        # Reset our statistics
        test.clear_statistics()

        for _ in range(count):
            mock_log_file.write_line("hello")

        test.update()

        assert file.num_lines_in == count
        assert len(mon.monitored_files) == 1

        results = test.squash()

        expected = f"[{count}x] hello"
        assert expected in results

        expected_full = Message(message=expected).asdict()
        expected_full["ioc"] = "short_name"
        full_message_length = len(json.dumps(expected_full)) + 1
        assert mon.stats.bytes_in == mock_log_file.bytes_written
        assert mon.stats.bytes_out == full_message_length
        assert mon.stats.lines_in == count
        assert mon.stats.lines_out == 1

    test.close()


def test_global_monitor_file_overwrite():
    test = MonitorTest()

    mon = test.mon
    file = test.file
    mock_log_file = test.mock_log_file

    # Reset our statistics
    test.clear_statistics()

    count = 2
    for _ in range(count):
        mock_log_file.write_line("hello")

    test.update()

    assert file.num_lines_in == count
    assert len(mon.monitored_files) == 1

    results = test.squash()

    expected = f"[{count}x] hello"
    assert expected in results

    filename = mock_log_file.filename

    file.close()
    mock_log_file.close()

    # Overwrite the old file - new file, new inode
    mock_log_file.fp = open(filename, "wt")

    for _ in range(count):
        mock_log_file.write_line("new file")

    test.clear_statistics()
    test.update()

    assert file.num_lines_in == count
    assert len(mon.monitored_files) == 1

    results = test.squash()

    expected = f"[{count}x] new file"
    assert expected in results

    test.close()
