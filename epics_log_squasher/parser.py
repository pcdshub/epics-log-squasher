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
class GroupJoiner:
    pattern: re.Pattern
    message_format: str
    extras: List[str]
    extras_join: str = ", "
    count_threshold: int = 10

    def join(self, matches: List[GroupMatch]) -> str:
        if not matches:
            return ""

        extras = [
            match.groupdict[extra]
            for match in matches
            for extra in self.extras
        ]
        if len(extras) > self.count_threshold:
            extras = extras[:self.count_threshold]
            extras.append("...")

        message = matches[0].message
        extras_joined = self.extras_join.join(extras)
        return f"{message}: {extras_joined}"


@dataclass(frozen=True)
class GroupMatch:
    name: str
    message: str
    source: IndexedString
    groupdict: Dict[str, str]


@dataclass
class GroupableRegexes:
    """Regular expressions for log lines that can be grouped."""
    groups: ClassVar[Dict[str, GroupJoiner]] = dict(
        stream_protocol_aborted=GroupJoiner(
            pattern=re.compile(r'(?P<pv>.*): Protocol aborted'),
            message_format="Protocol aborted",
            extras=["pv"],
        ),
        asyn_connect_failed=GroupJoiner(
            pattern=re.compile(r'(?P<pv>.*): pasynCommon->connect\(\) failed: (?P<reason>.*)'),
            message_format="pasynCommon->connect() failed: {reason}",
            extras=["pv"],
        ),
    )

    @classmethod
    def group_fullmatch(cls, idx: IndexedString) -> Optional[GroupMatch]:
        for group, joiner in cls.groups.items():
            match = joiner.pattern.fullmatch(idx.value)
            if match is None:
                continue

            groupdict = match.groupdict()
            joiner = cls.groups[group]
            return GroupMatch(
                name=group,
                message=joiner.message_format.format(**groupdict),
                source=idx,
                groupdict=groupdict,
            )

        return None


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


def _split_indexes_and_groups(
    messages: Union[IndexedString, GroupMatch]
) -> Tuple[List[IndexedString], List[GroupMatch]]:
    indexes = []
    groups = []
    for message in messages:
        if isinstance(message, IndexedString):
            indexes.append(message)
        else:
            groups.append(message)

    if len(groups) == 1:
        indexes.append(groups[0].source)
        groups.clear()

    return indexes, groups


@dataclass
class Squasher:
    by_timestamp: Dict[int, List[Union[IndexedString, GroupMatch]]] = field(default_factory=dict)
    by_message: Dict[str, List[Union[IndexedString, GroupMatch]]] = field(default_factory=dict)
    messages: List[IndexedString] = field(default_factory=list)
    period_sec: float = 10.0
    messages_per_sec_threshold: float = 1.0

    _index: int = 0

    def _create_indexed_string(self, value: str) -> IndexedString:
        self._index = (self._index + 1) % 1_000_000
        return IndexedString.from_string(index=self._index, value=CleanRegexes.sub("", value))

    def add_indexed_string(self, value: IndexedString):
        self.messages.append(value)
        if IgnoreRegexes.fullmatch(value.value):
            return

        # Bin posix timestamps by the second:
        ts = int(value.timestamp.timestamp())
        self.by_timestamp.setdefault(ts, []).append(value)
        # ts = ts - (ts % self.period_sec)

        match = GroupableRegexes.group_fullmatch(value)

        if match is not None:
            # Add the groupmatch, not the individual message
            self.by_message.setdefault(match.message, []).append(match)
        else:
            self.by_message.setdefault(value.value, []).append(match or value)

    def add_lines(self, value: str):
        for line in value.splitlines():
            indexed = self._create_indexed_string(line.rstrip())
            self.add_indexed_string(indexed)
        
    def get_timespan(self) -> float:
        if len(self.by_timestamp) == 0:
            return 0.0
        return (max(self.by_timestamp) - min(self.by_timestamp)) + 1

    def squash(self) -> Squashed:
        squashed = []
        for line, messages in self.by_message.items():
            indexes, groups = _split_indexes_and_groups(messages)

            if indexes:
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

            if groups:
                first = groups[0]
                joiner = GroupableRegexes.groups[first.name]
                squashed.append(
                    IndexedString(
                        value=joiner.join(groups),
                        timestamp=first.source.timestamp,
                        index=first.source.index,
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
