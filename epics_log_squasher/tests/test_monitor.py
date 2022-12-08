import datetime
import io
import json
import tempfile

from .. import monitor
from ..parser import Message


class MockLogFile:
    filename: str
    bytes_written: int
    lines_written: int

    def __init__(self):
        self.tempfile = tempfile.NamedTemporaryFile(mode="wt", encoding="latin-1")
        self.filename = self.tempfile.name
        self.bytes_written = 0
        self.lines_written = 0

    def close(self):
        self.tempfile.close()

    def write_line(self, line: str, include_timestamp: bool = True):
        assert "\n" not in line
        if include_timestamp:
            ts = datetime.datetime.now().strftime("%Y/%m/%d %H:%M:%S.%f")
            line = f"{ts} {line}"
        line = f"{line}\n"
        self.tempfile.write(line)
        self.tempfile.flush()
        self.bytes_written += len(line)
        self.lines_written += 1


def test_global_monitor():
    mock_log_file = MockLogFile()

    mon = monitor.GlobalMonitor(
        file_glob=mock_log_file.filename,
        start_thread=False,
    )
    mon.update()

    file, = list(mon.files.values())
    file.short_name = "short_name"
    assert file.filename == mock_log_file.filename

    def clear_statistics():
        mock_log_file.lines_written = 0
        mock_log_file.bytes_written = 0
        for file_ in mon.files.values():
            file_.num_lines_in = 0
        mon.stats.clear()

    for count in [2, 5, 10]:
        # Reset our statistics
        clear_statistics()

        for _ in range(count):
            mock_log_file.write_line("hello")

        mon.update()
        mon.reader._poll()
        assert file.num_lines_in == count
        assert len(mon.monitored_files) == 1

        with io.StringIO() as fp:
            mon.squash(out_file=fp)
            results = fp.getvalue()

        expected = f"[{count}x] hello"
        assert expected in results

        expected_full = Message(message=expected).asdict()
        expected_full["ioc"] = "short_name"
        full_message_length = len(json.dumps(expected_full)) + 1
        assert mon.stats.bytes_in == mock_log_file.bytes_written
        assert mon.stats.bytes_out == full_message_length
        assert mon.stats.lines_in == count
        assert mon.stats.lines_out == 1
