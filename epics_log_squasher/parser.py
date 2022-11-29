from __future__ import annotations

import dataclasses
import datetime
import re

from dataclasses import dataclass, field
from typing import ClassVar, Dict, List, Optional, Tuple


class Regexes:
    """
    Regular expressions container for working with one or more expressions.

    Embedded in a dataclass so items can have individual names or be referenced
    more easily.
    """
    _regexes_: ClassVar[Dict[str, re.Pattern]]

    def __init_subclass__(cls):
        super().__init_subclass__()
        # This is a bit of an awkward hack, but leaving it in for now:
        cls._regexes_ = dataclasses.asdict(dataclass(cls)())

    @classmethod
    def fullmatch(cls, line: str) -> bool:
        return any(regex.fullmatch(line) for regex in cls._regexes_.values())

    @classmethod
    def sub(cls, replace_with: str, line: str) -> str:
        for regex in cls._regexes_.values():
            line = regex.sub(replace_with, line)
        return line


@dataclass
class CleanRegexes(Regexes):
    """Regular expressions for cleaning log lines."""
    # ref: https://stackoverflow.com/questions/14693701
    # We largely care about foreground changes to red for errors, but let's be
    # a bit more generic than that:
    ansi_escape_codes: re.Pattern = re.compile(r'(?:\x1B[@-_]|[\x80-\x9F])[0-?]*[ -/]*[@-~]')
   

@dataclass
class IgnoreRegexes(Regexes):
    """Regular expressions for skipping over log lines entirely."""
    empty_strings: re.Pattern = re.compile(r"\s*")


@dataclass
class GreenlitRegexes(Regexes):
    """Regular expressions for log lines that should always be recorded."""
    procserv_lines: re.Pattern = re.compile(r'^@@@ ')
   

@dataclass
class DateFormats:
    """
    datetime.datetime-compatible formats for interpreting date and timestamps.

    Embedded in a dataclass so items can have individual names or be referenced
    more easily.
    """
    standard: str = "%Y/%m/%d %H:%M:%S.%f"

    @staticmethod
    def find_timestamp(line: str) -> Tuple[Optional[datetime.datetime], str]:
        # TODO: only support basic space delimiters for now
        split = line.strip().split(" ")
        date_portion = " ".join(split[:2])
        remainder = " ".join(split[2:])

        for fmt in DATE_FORMATS.values():
            try:
                return datetime.datetime.strptime(date_portion, fmt), remainder
            except ValueError:
                ...

        return None, line


DATE_FORMATS = dataclasses.asdict(DateFormats())
IGNORE_REGEXES = dataclasses.asdict(IgnoreRegexes())


@dataclass(frozen=True)
class IndexedString:
    index: int
    timestamp: datetime.datetime
    value: str

    @classmethod
    def from_string(cls, index: int, value: str) -> IndexedString:
        timestamp, line = DateFormats.find_timestamp(value)
        if timestamp is None:
            # Insert our own timestamp if there is none
            # Even if our timestamps are off, we still respect ordering
            # by way of the "seen" index
            timestamp = datetime.datetime.now()

        return IndexedString(
            index=index,
            timestamp=timestamp,
            value=line,
        )

    def __str__(self):
        return self.value


@dataclass
class Squasher:
    by_timestamp: Dict[int, List[IndexedString]] = field(default_factory=dict)
    by_message: Dict[str, List[IndexedString]] = field(default_factory=dict)
    messages: List[IndexedString] = field(default_factory=list)
    period_sec: float = 10.0
    messages_per_sec_threshold: float = 1.0

    _index: int = 0

    def _create_indexed_string(self, value: str) -> IndexedString:
        self._index = (self._index + 1) % 1_000_000
        return IndexedString.from_string(index=self._index, value=CleanRegexes.sub("", value))

    def add_indexed_string(self, value: IndexedString):
        self.messages.append(value)

        # Bin posix timestamps by the second:
        ts = int(value.timestamp.timestamp())
        # ts = ts - (ts % self.period_sec)
        self.by_timestamp.setdefault(ts, []).append(value)
        self.by_message.setdefault(value.value, []).append(value)

    def add_lines(self, value: str):
        for line in value.splitlines():
            indexed = self._create_indexed_string(line.rstrip())
            self.add_indexed_string(indexed)
        
    def get_timespan(self) -> float:
        if len(self.by_timestamp) == 0:
            return 0.0
        return max(self.by_timestamp) - min(self.by_timestamp)

    def squash(self) -> Squashed:
        squashed = []
        for line, indexes in self.by_message.items():
            if IgnoreRegexes.fullmatch(line):
                continue
            if GreenlitRegexes.fullmatch(line):
                # Greenlit lines go in entirely
                squashed.extend(indexes)
                continue

            first = indexes[0]
            # last = indexes[-1]
            if len(indexes) == 1:
                squashed.append(first)
            else:
                count = len(indexes)
                squashed.append(
                    IndexedString(
                        value=f"[{count}x] {line}",
                        timestamp=first.timestamp,
                        index=first.index,
                    )
                )

        def by_index(value: IndexedString) -> int:
            return value.index

        return Squashed(
            lines=[item.value for item in sorted(squashed, key=by_index)],
            source_lines=len(self.messages),
        )


@dataclass
class Squashed:
    lines: List[str]
    source_lines: int
