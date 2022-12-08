"""
EPICS IOC log file output parsing and cleaning

Pipeline is roughly:

1. Pre-process with "clean" regexes (CleanRegexes)
2. Pre-process to extract a timestamp, if embedded (DateFormats)
3. Exclude from output if listed in IgnoreRegexes, skipping remaining steps
4. Keep as-is if included in GreenlitRegexes, skipping further processing
5. If groupable as per SingleLineGroupableRegexes, reformat/regroup the message
"""
from __future__ import annotations

import dataclasses
import datetime
import enum
import functools
import re
from dataclasses import dataclass, field
from typing import (Callable, ClassVar, Dict, List, Optional, Sequence, Tuple,
                    Union, cast)


@dataclass(frozen=True)
class Message:
    message: str
    timestamp: datetime.datetime = field(default_factory=datetime.datetime.now)
    info: Tuple[Tuple[str, Union[str, Tuple[str, ...]]], ...] = field(default_factory=tuple)
    index: int = 0
    source_lines: int = 1

    @classmethod
    def from_dict(
        cls,
        message: str,
        info: Dict[str, Union[str, Sequence[str]]],
        timestamp: Optional[datetime.datetime] = None,
        index: int = 0,
        source_lines: int = 0,
    ) -> Message:
        if timestamp is None:
            timestamp = datetime.datetime.now()

        def value_to_tuple(value: Union[str, Sequence[str]]) -> Tuple[str, ...]:
            if isinstance(value, str):
                return (value, )
            return tuple(value)

        info_tuple = tuple(
            (key, value_to_tuple(value))
            for key, value in info.items()
        )
        return cls(
            message=message,
            timestamp=timestamp,
            info=info_tuple,
            index=index,
            source_lines=source_lines,
        )

    def asdict(self) -> Dict[str, Union[str, Tuple[str, ...]]]:
        res: Dict[str, Union[str, Tuple[str, ...]]] = {
            "ts": str(self.timestamp),
            "msg": self.message,
        }
        res.update(**dict((k, v) for k, v in self.info if v))
        return res

    @classmethod
    def from_indexed_string(cls, value: IndexedString) -> Message:
        return cls(message=value.value, timestamp=value.timestamp, index=value.index, info=(), source_lines=1)

    @classmethod
    def from_indexed_strings(cls, values: Sequence[IndexedString]) -> List[Message]:
        return [cls.from_indexed_string(value) for value in values]


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
        cls._regexes_ = {
            key: value
            for key, value in dataclasses.asdict(dataclass(cls)()).items()
            if isinstance(value, re.Pattern)
        }

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
    procserv_lines: re.Pattern = re.compile(r'@@@ .*')


@dataclass
class GroupJoiner:
    pattern: re.Pattern
    message_format: str
    extras: Optional[List[str]] = None
    extras_join: str = ", "
    count_threshold: int = 10

    def join(self, matches: List[GroupMatch]) -> Optional[Message]:
        if not matches:
            return None

        extras: Dict[str, List[str]] = {}
        for match in matches:
            _append_groupdict(extras, match.groupdict)

        if self.extras is not None:
            # If 'extras' are specified, only include them
            extras = {
                key: value for key, value in extras.items()
                if key in self.extras
            }

        first_match = matches[0]
        return Message(
            timestamp=first_match.source.timestamp,
            index=first_match.source.index,
            message=first_match.message,
            info=tuple((key, tuple(value)) for key, value in extras.items()),
            source_lines=len(matches),
        )


@dataclass(frozen=True)
class GroupMatch:
    name: str
    message: str
    source: IndexedString
    groupdict: Dict[str, str]


@dataclass
class SingleLineGroupableRegexes:
    """Regular expressions for log lines that can be grouped."""
    groups: ClassVar[Dict[str, GroupJoiner]] = dict(
        stream_protocol_aborted=GroupJoiner(
            pattern=re.compile(r'(?P<pv>.*): Protocol aborted'),
            message_format="Protocol aborted",
        ),
        asyn_connect_failed=GroupJoiner(
            pattern=re.compile(r'(?P<pv>.*): pasynCommon->connect\(\) failed: (?P<reason>.*)'),
            message_format="pasynCommon->connect() failed: {reason}",
            extras=["pv"],
        ),
        asyn_lock_failed=GroupJoiner(
            pattern=re.compile(r'(?P<context>.*) (?P<pv>.*) lockRequest: pasynManager->queueRequest\(\) failed: (?P<reason>.*)'),
            message_format="{context} lockRequest: pasynManager->queueRequest() failed: {reason}",
            extras=["pv"],
        ),
        snmp_querylist_timeout=GroupJoiner(
            pattern=re.compile(r'(?P<context>.*): Snmp QryList Timeout on (?P<pv>.*)'),
            message_format="{context}: Snmp QryList Timeout",
            extras=["pv"],
        ),
        snmp_error_code=GroupJoiner(
            pattern=re.compile(r'Record \[(?P<pv>.*)\] received error code \[(?P<code>.*)\]!'),
            message_format="Received error code {code}",
            extras=["pv"],
        ),
        errlog_spam=GroupJoiner(
            pattern=re.compile(r'errlog: (?P<count>\d+) messages were discarded'),
            message_format="errlog: messages were discarded",
            extras=["count"],
        ),
        active_scan_count=GroupJoiner(
            pattern=re.compile(r'(?P<pv>.*) Active scan count exceeded!'),
            message_format="Active scan count exceeded!",
            extras=["pv"],
            count_threshold=-1,  # include every PV name
        ),
    )

    @classmethod
    def group_fullmatch(cls, idx: IndexedString) -> Optional[GroupMatch]:
        for group, joiner in cls.groups.items():
            match = joiner.pattern.fullmatch(idx.value)
            if match is None:
                continue

            groupdict = match.groupdict()
            return GroupMatch(
                name=group,
                message=joiner.message_format.format(**groupdict),
                source=idx,
                groupdict=groupdict,
            )

        return None


@dataclass
class MultilineGroupJoiner:
    start_pattern: re.Pattern
    inner_patterns: List[re.Pattern]
    end_pattern: re.Pattern
    message_format: str


class MultilineMatchState(enum.Enum):
    #: Default initial state
    init = enum.auto()
    #: Saw start line
    start = enum.auto()
    #: Checking inner lines now
    inner = enum.auto()
    #: Saw end line
    end = enum.auto()
    #: Saw start and then failed to match an inner line (error exit)
    unmatched = enum.auto()


@dataclass
class MultilineGroupMatch:
    name: str
    state: MultilineMatchState = MultilineMatchState.init
    source: List[IndexedString] = field(default_factory=list)
    groupdict: Dict[str, List[str]] = field(default_factory=dict)

    def join(self) -> Message:
        group = MultiLineGroupableRegexes.groups[self.name]
        first = self.source[0]
        return Message.from_dict(
            message=group.message_format.format(**self.groupdict),
            timestamp=first.timestamp,
            info=cast(Dict[str, Sequence[str]], self.groupdict),
            source_lines=len(self.source),
        )


def _append_groupdict(existing: Dict[str, List[str]], add: Dict[str, str]) -> None:
    for key, value in add.items():
        existing.setdefault(key, []).append(value)


@dataclass
class MultiLineGroupableRegexes:
    """Regular expressions for context-sensitive log lines that can be grouped."""
    groups: ClassVar[Dict[str, MultilineGroupJoiner]] = dict(
        procserv_status_update=MultilineGroupJoiner(
            message_format="procServ status update",
            start_pattern=re.compile(r'@@@ @@@ @@@ @@@ @@@'),
            inner_patterns=[
                re.compile(r'@@@ Received a sigChild for process (?P<pid>\d+). Normal exit status = (?P<exit_code>\d+)'),
                re.compile(r'@@@ Received a sigChild for process (?P<pid>\d+). The process was killed by signal (?P<signal>\d+)'),
                re.compile(r'@@@ Current time: (?P<timestamp>.*)'),
                re.compile(r'@@@ Child process is shutting down, a new one will be restarted shortly'),
                re.compile(r'@@@ \^R or \^X restarts the child, \^Q quits the server'),
                re.compile(r'@@@ Restarting child "(?P<procserv_iocname>.*)"'),
                re.compile(r'@@@    \(as (?P<process>.*)\)'),
                re.compile(r'@@@ Toggled auto restart mode to (?P<restart_mode>.*)'),
                re.compile(r'@@@ The PID of new child ".*" is: (?P<new_pid>\d+)'),
            ],
            end_pattern=re.compile(r'@@@ @@@ @@@ @@@ @@@'),
        ),
    )

    @classmethod
    def group_fullmatch(
        cls, state: Optional[MultilineGroupMatch], idx: IndexedString
    ) -> Optional[MultilineGroupMatch]:
        if state is not None:
            joiner = cls.groups[state.name]
            for pattern in joiner.inner_patterns:
                match = pattern.fullmatch(idx.value)
                if match is not None:
                    state.source.append(idx)
                    _append_groupdict(state.groupdict, match.groupdict())
                    return state

            match = joiner.end_pattern.fullmatch(idx.value)
            if match is not None:
                state.source.append(idx)
                _append_groupdict(state.groupdict, match.groupdict())
                state.state = MultilineMatchState.end
                return None

            # We're out of the group and we didn't see a recognized line
            state.state = MultilineMatchState.unmatched
            return None

        for group, joiner in cls.groups.items():
            match = joiner.start_pattern.fullmatch(idx.value)
            if match is None:
                continue

            return MultilineGroupMatch(
                name=group,
                source=[idx],
                state=MultilineMatchState.start,
                groupdict={
                    key: [value]
                    for key, value in match.groupdict().items()
                }
            )

        return None


@dataclass
class DateFormat:
    format: str
    #: The character to split the line on, with the first part being the
    #: date and timestamp, the remainder being the log message
    split_char: str = " "
    #: This number of ``split_char`` to partition the message
    split_count: int = 2
    #: A cleaner that can be called on the result message
    cleaner: Optional[Callable[[str], str]] = None


@dataclass
class DateFormats:
    """
    datetime.datetime-compatible formats for interpreting date and timestamps.

    Embedded in a dataclass so items can have individual names or be referenced
    more easily.
    """
    _date_formats_: ClassVar[Dict[str, DateFormat]] = dict(
        standard=DateFormat(
            format="%Y/%m/%d %H:%M:%S.%f",
        ),
        # Found in ioc-xrt-m3h-switch
        short=DateFormat(
            format="%m/%d %H:%M:%S.%f",
        ),
        # ads-ioc ISO8601-ish timestamps _with_ T and a time zone we can't
        # easily work with: e.g., 2022-12-02T13:30:56-08:00"
        iso8601_1=DateFormat(
            format="%Y-%m-%dT%H:%M:%S",  # suffix: -0800
            split_char="-",
            split_count=3,
            cleaner=functools.partial(re.compile(r"^\d+\s+").sub, ""),
        ),
    )

    @classmethod
    def find_timestamp(cls, line: str) -> Tuple[Optional[datetime.datetime], str]:
        for fmt in cls._date_formats_.values():
            try:
                split = line.strip().split(fmt.split_char)
                date_portion = fmt.split_char.join(split[:fmt.split_count])
                dt = datetime.datetime.strptime(date_portion, fmt.format)
            except ValueError:
                ...
            else:
                remainder = fmt.split_char.join(split[fmt.split_count:])
                if fmt.cleaner:
                    remainder = fmt.cleaner(remainder)
                return dt, remainder

        return None, line


@dataclass(frozen=True)
class IndexedString:
    index: int
    timestamp: datetime.datetime
    value: str

    @classmethod
    def from_string(
        cls, index: int, value: str, local_timestamp: Optional[float] = None
    ) -> IndexedString:
        timestamp, line = DateFormats.find_timestamp(value)
        if timestamp is None:
            if local_timestamp is not None:
                # This could be the time it was read from the log file or time
                # information determined some other way
                timestamp = datetime.datetime.fromtimestamp(local_timestamp)
            else:
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
    messages: List[Union[IndexedString, GroupMatch]]
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
    by_message: Dict[str, List[Union[IndexedString, GroupMatch]]] = field(default_factory=dict)
    messages: List[IndexedString] = field(default_factory=list)
    multiline_matches: List[MultilineGroupMatch] = field(default_factory=list)
    multiline_match: Optional[MultilineGroupMatch] = None
    num_bytes: int = 0
    messages_per_sec_threshold: float = 1.0

    _index: int = 0

    def _create_indexed_string(
        self, value: str, local_timestamp: Optional[float] = None
    ) -> IndexedString:
        self._index = (self._index + 1) % 1_000_000
        return IndexedString.from_string(
            index=self._index,
            value=CleanRegexes.sub("", value),
            local_timestamp=local_timestamp,
        )

    def add_multiline_match(self, match: MultilineGroupMatch):
        if not match.source:
            return

        if match.state != MultilineMatchState.end:
            for line in match.source:
                self._add_indexed_string(
                    line,
                    allow_multiline=False
                )
            return

        self.multiline_matches.append(match)
        # first = match.source[0]
        # self._add_indexed_string(
        #     IndexedString(
        #         index=first.index,
        #         timestamp=first.timestamp,
        #         value=str(match.join()),  # TODO
        #     ),
        #     allow_multiline=False
        # )

    def add_indexed_string(self, value: IndexedString):
        self.messages.append(value)
        if IgnoreRegexes.fullmatch(value.value):
            return

        self._add_indexed_string(value)

    def _add_indexed_string(self, value: IndexedString, *, allow_multiline: bool = True):
        if allow_multiline:
            last_match = self.multiline_match
            self.multiline_match = MultiLineGroupableRegexes.group_fullmatch(last_match, value)
            if last_match is not None:
                if self.multiline_match is not last_match:
                    self.add_multiline_match(last_match)
                if self.multiline_match is None and last_match.state == MultilineMatchState.end:
                    # We finished a multiline group. Don't process the final line.
                    return

            if self.multiline_match is not None:
                # We're in a multiline group; don't process it further
                return

        match = SingleLineGroupableRegexes.group_fullmatch(value)

        if match is not None:
            # Add the groupmatch, not the individual message
            self.by_message.setdefault(match.message, []).append(match)
        else:
            self.by_message.setdefault(value.value, []).append(match or value)

    def add_lines(self, value: str, local_timestamp: Optional[float] = None):
        if "\n" in value:
            self.num_bytes += len(value)
        else:
            # TODO: mostly for test suite
            self.num_bytes += len(value) + 1

        for line in value.splitlines():
            indexed = self._create_indexed_string(line.rstrip(), local_timestamp=local_timestamp)
            self.add_indexed_string(indexed)

    def squash(self) -> List[Message]:
        squashed: List[Message] = []

        if self.multiline_match is not None:
            # Inside a multiline match: what to do?
            # TODO Continue next time, right?
            self.add_multiline_match(self.multiline_match)

        for match in self.multiline_matches:
            squashed.append(match.join())

        for line, messages in self.by_message.items():
            indexes, groups = _split_indexes_and_groups(messages)

            if indexes:
                if GreenlitRegexes.fullmatch(line):
                    # Greenlit lines go in entirely
                    # squashed.extend(indexes)
                    squashed.extend(Message.from_indexed_strings(indexes))
                    continue

                first = indexes[0]
                if len(indexes) == 1:
                    squashed.append(Message.from_indexed_string(first))
                else:
                    count = len(indexes)
                    squashed.append(
                        Message(
                            message=f"[{count}x] {line}",
                            timestamp=first.timestamp,
                            index=first.index,
                            source_lines=count,
                        )
                    )

            if groups:
                first = groups[0]
                joiner = SingleLineGroupableRegexes.groups[first.name]
                message = joiner.join(groups)
                if message is not None:
                    squashed.append(message)

        def by_index(value: Message) -> int:
            return value.index

        return [item for item in sorted(squashed, key=by_index)]
