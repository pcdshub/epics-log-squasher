import datetime
import io
import tempfile

from .. import monitor


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
    log = MockLogFile()

    mon = monitor.GlobalMonitor(file_glob=log.filename, start_thread=False)
    mon.update()

    file, = list(mon.files.values())
    file.short_name = "short_name"
    assert file.filename == log.filename

    count = 10
    for _ in range(count):
        log.write_line("hello")

    mon.update()
    mon.reader._poll()
    assert file.num_lines_in == count
    assert len(mon.monitored_files) == 1

    with io.StringIO() as fp:
        mon.squash(out_file=fp)
        results = fp.getvalue()

    expected = f"short_name [{count}x] hello\n"
    assert results == expected

    mon.update_stats()
    assert mon.stats.bytes_in == log.bytes_written
    assert mon.stats.bytes_out == len(expected)
    assert mon.stats.lines_in == count
    assert mon.stats.lines_out == 1
