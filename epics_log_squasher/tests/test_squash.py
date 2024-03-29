from __future__ import annotations

import datetime
import textwrap
from typing import Dict, List

import pytest

from .. import parser
from ..parser import Message

test_cases = [
    pytest.param(
        """\
        2022/11/09 09:32:01.014 HPL:GIGE:BASLER10:CONNECTION devAsynInt32 process error
        2022/11/09 09:32:01.115 HPL:GIGE:BASLER10:CONNECTION devAsynInt32 process error
        2022/11/09 09:32:01.215 HPL:GIGE:BASLER10:CONNECTION devAsynInt32 process error
        2022/11/09 09:32:01.315 HPL:GIGE:BASLER10:CONNECTION devAsynInt32 process error
        2022/11/09 09:32:01.414 HPL:GIGE:BASLER10:CONNECTION devAsynInt32 process error
        2022/11/09 09:32:01.515 HPL:GIGE:BASLER10:CONNECTION devAsynInt32 process error
        2022/11/09 09:32:01.615 HPL:GIGE:BASLER10:CONNECTION devAsynInt32 process error
        2022/11/09 09:32:01.715 HPL:GIGE:BASLER10:CONNECTION devAsynInt32 process error
        2022/11/09 09:32:01.815 HPL:GIGE:BASLER10:CONNECTION devAsynInt32 process error
        2022/11/09 09:32:01.914 HPL:GIGE:BASLER10:CONNECTION devAsynInt32 process error
        2022/11/09 09:32:01.994 HPL:GIGE:BASLER10:CONNECTION devAsynInt32 process error
        """,
        # "2022/11/09 09:32:xx.xxx HPL:GIGE:BASLER10:CONNECTION devAsynInt32 process error",
        [
            Message(
                message="[11x] HPL:GIGE:BASLER10:CONNECTION devAsynInt32 process error",
                source_lines=11,
            ),
        ],
        id="repetitive_with_timestamp_asyn",
    ),
    pytest.param(
        """\
        2021/12/07 00:01:01.886 prosilica:frameCallback: ERROR, frame has error code 16
        2021/12/07 00:01:02.876 prosilica:frameCallback: ERROR, frame has error code 16
        2021/12/07 00:01:03.875 prosilica:frameCallback: ERROR, frame has error code 16
        """,
        [
            Message(
                "[3x] prosilica:frameCallback: ERROR, frame has error code 16",
                source_lines=3,
            ),
        ],
        id="repetitive_with_timestamp_prosilica",
    ),
    pytest.param(
        """\
        \x1b[31;1m2022/11/03 17:35:08.587 CXI:SC1:CVV:04 CXI:SC1:CVV:04:SetGain: No reply from device within 800 ms
        \x1b[31;1m2022/11/03 17:35:08.587 CXI:SC1:CVV:05 CXI:SC1:CVV:05:SetGain: No reply from device within 800 ms
        \x1b[31;1m2022/11/03 17:35:09.587 CXI:SC1:CVV:04 CXI:SC1:CVV:04:SetGain: No reply from device within 800 ms
        \x1b[31;1m2022/11/03 17:35:09.587 CXI:SC1:CVV:05 CXI:SC1:CVV:05:SetGain: No reply from device within 800 ms
        \x1b[31;1m2022/11/03 17:35:10.587 CXI:SC1:CVV:04 CXI:SC1:CVV:04:SetGain: No reply from device within 800 ms
        \x1b[31;1m2022/11/03 17:35:10.587 CXI:SC1:CVV:05 CXI:SC1:CVV:05:SetGain: No reply from device within 800 ms
        """,
        [
            Message(
                "[3x] CXI:SC1:CVV:04 CXI:SC1:CVV:04:SetGain: No reply from device within 800 ms",
                source_lines=3,
            ),
            Message(
                "[3x] CXI:SC1:CVV:05 CXI:SC1:CVV:05:SetGain: No reply from device within 800 ms",
                source_lines=3,
            ),
        ],
        id="repetitive_with_timestamp_ansi",
    ),
    pytest.param(
        """\
        abc: Protocol aborted
        def: Protocol aborted
        ghi: Protocol aborted
        """,
        [
            Message.from_dict(
                message="Protocol aborted",
                info={"pv": ["abc", "def", "ghi"]},
                source_lines=3,
            ),
        ],
        id="protocol_abort_group",
    ),
    pytest.param(
        """\
        0: Protocol aborted
        1: Protocol aborted
        2: Protocol aborted
        3: Protocol aborted
        4: Protocol aborted
        5: Protocol aborted
        6: Protocol aborted
        7: Protocol aborted
        8: Protocol aborted
        9: Protocol aborted
        10: Protocol aborted
        """,
        [
            Message.from_dict(
                message="Protocol aborted",
                info={"pv": ("0", "1", "2", "3", "4", "5", "6", "7", "8", "9", "10")},
                source_lines=11,
            ),
        ],
        # TODO: no longer truncated; should we truncate extra info?
        id="protocol_abort_truncated_group",
    ),
    pytest.param(
        """\
        abc: pasynCommon->connect() failed: some reason 1
        012: pasynCommon->connect() failed: some reason 2
        def: pasynCommon->connect() failed: some reason 1
        345: pasynCommon->connect() failed: some reason 2
        """,
        [
            Message.from_dict(
                "pasynCommon->connect() failed: some reason 1",
                info={"pv": ["abc", "def"]},
                source_lines=2,
            ),
            Message.from_dict(
                "pasynCommon->connect() failed: some reason 2",
                info={"pv": ["012", "345"]},
                source_lines=2,
            ),
        ],
        id="asyn_connect_failed",
    ),
    pytest.param(
        """\
        2022/11/30 14:49:19.326201 scan-1 CXI:MCS2:01:m2:STATE_RBV lockRequest: pasynManager->queueRequest() failed: port TCP0 not connected
        2022/11/30 14:49:19.326233 scan-1 CXI:MCS2:01:m3:SCAN_POS lockRequest: pasynManager->queueRequest() failed: port TCP0 not connected
        2022/11/30 14:49:19.326261 scan-1 CXI:MCS2:01:m3:STATE_RBV lockRequest: pasynManager->queueRequest() failed: port TCP0 not connected
        """,
        [
            Message.from_dict(
                "scan-1 lockRequest: pasynManager->queueRequest() failed: port TCP0 not connected",
                info={"pv": ["CXI:MCS2:01:m2:STATE_RBV", "CXI:MCS2:01:m3:SCAN_POS", "CXI:MCS2:01:m3:STATE_RBV"]},
                source_lines=3,
            ),
        ],
        id="asyn_connect_failed",
    ),
    pytest.param(
        """\
        errlog: 676 messages were discarded
        errlog: 189 messages were discarded
        errlog: 511 messages were discarded
        abc
        errlog: 511 messages were discarded
        errlog: 40 messages were discarded
        """,
        [
            Message.from_dict(
                "errlog: messages were discarded",
                info={"count": ["676", "189", "511", "511", "40"]},
                source_lines=5,
            ),
            Message("abc"),
        ],
        id="errlog_spam",
    ),
    pytest.param(
        """\
        @@@ This
        @@@ This
        @@@ Should be
        @@@ Should be
        @@@ Greenlit
        @@@ Greenlit
        """,
        [
            Message("@@@ This"),
            Message("@@@ This"),
            Message("@@@ Should be"),
            Message("@@@ Should be"),
            Message("@@@ Greenlit"),
            Message("@@@ Greenlit"),
        ],
        id="greenlit_ioc",
    ),
    pytest.param(
        """\
        2022-12-02T13:48:08-0800 Info: Connected to 172.21.68.250
        2022-12-02T13:48:08-0800 Info: connection closed by remote
        2022/12/02 13:48:11.030 cyclicThread: forcing disconnect.
        2022-12-02T13:48:11-0800 Info: Connected to 172.21.68.250
        2022-12-02T13:48:11-0800 Info: connection closed by remote
        2022/12/02 13:48:12.530 cyclicThread: forcing disconnect.
        """,
        [
            Message(
                "[2x] Info: Connected to 172.21.68.250",
                source_lines=2,
            ),
            Message(
                "[2x] Info: connection closed by remote",
                source_lines=2,
            ),
            Message(
                "[2x] cyclicThread: forcing disconnect.",
                source_lines=2,
            ),
        ],
        id="repetitive_with_weird_timestamps",
    ),
]


def compare_results(results: List[Message], all_expected: List[Message]):
    assert len(results) == len(all_expected)
    for idx, (generated, expected) in enumerate(zip(results, all_expected)):
        assert generated.message == expected.message, f"Message from index {idx}"
        assert generated.info == expected.info, f"Info metadata from index {idx}"
        assert generated.source_lines == expected.source_lines, f"Source line count from index {idx}"


@pytest.mark.parametrize("lines, expected", test_cases)
def test_squash(lines: str, expected: List[Message]):
    squasher = parser.Squasher()
    squasher.add_lines(textwrap.dedent(lines.rstrip()))

    squashed = squasher.squash()
    compare_results(squashed, expected)


@pytest.mark.parametrize(
    "source_message, group, expected_message, groupdict",
    [
        pytest.param(
            "abc: Protocol aborted",
            "stream_protocol_aborted",
            "Protocol aborted",
            {
                "pv": "abc",
            },
            id="stream_protocol_aborted",
        ),
        pytest.param(
            "abc: pasynCommon->connect() failed: some reason",
            "asyn_connect_failed",
            "pasynCommon->connect() failed: some reason",
            {
                "pv": "abc",
                "reason": "some reason",
            },
            id="asyn_connect_failed",
        ),
        pytest.param(
            "ctx: Snmp QryList Timeout on pv",
            "snmp_querylist_timeout",
            "ctx: Snmp QryList Timeout",
            {
                "pv": "pv",
                "context": "ctx",
            },
            id="snmp_querylist_timeout",
        ),
        pytest.param(
            "Record [XRT:R44:PWR:20:Sensor:2:GetStatus] received error code [0x00040000]!",
            "snmp_error_code",
            "Received error code 0x00040000",
            {
                "pv": "XRT:R44:PWR:20:Sensor:2:GetStatus",
                "code": "0x00040000",
            },
            id="snmp_error_code",
        ),
        pytest.param(
            "errlog: 661 messages were discarded",
            "errlog_spam",
            "errlog: messages were discarded",
            {
                "count": "661",
            },
            id="errlog_spam",
        ),
        pytest.param(
            "pv Active scan count exceeded!",
            "active_scan_count",
            "Active scan count exceeded!",
            {
                "pv": "pv",
            },
            id="active_scan_count",
        ),
    ],
)
def test_groupable_regexes(
    source_message: str,
    group: str,
    expected_message: str,
    groupdict: Dict[str, str],
):
    idx = parser.IndexedString(
        index=0,
        timestamp=datetime.datetime.now(),
        value=source_message,
    )
    match = parser.SingleLineGroupableRegexes.group_fullmatch(idx)
    assert match is not None
    assert match.message == expected_message
    assert match.name == group
    assert match.groupdict == groupdict


@pytest.mark.parametrize(
    "source_message, expected_message",
    [
        pytest.param(
            """\
            @@@ @@@ @@@ @@@ @@@
            @@@ Received a sigChild for process 16392. Normal exit status = 127
            @@@ Current time: Fri Dec  2 16:41:19 2022
            @@@ Child process is shutting down, a new one will be restarted shortly
            @@@ ^R or ^X restarts the child, ^Q quits the server
            @@@ @@@ @@@ @@@ @@@
            """,
            Message.from_dict(
                message="procServ status update",
                info={
                    "pid": ["16392"],
                    "exit_code": ["127"],
                    "procserv_ts": ["Fri Dec  2 16:41:19 2022"],
                },
                source_lines=6,
            ),
            id="procserv_status_update",
        ),
        pytest.param(
            """\
            @@@ @@@ @@@ @@@ @@@
            @@@ Received a sigChild for process 28147. Normal exit status = 126
            @@@ Current time: Thu Dec  8 15:12:25 2022
            @@@ Child process is shutting down, a new one will be restarted shortly
            @@@ ^R or ^X restarts the child, ^Q quits the server
            @@@ Restarting child "ioc-cxi-protura"
            @@@    (as /reg/g/pcds/pyps/config/cxi/iocmanager/startProc)
            @@@ The PID of new child "ioc-cxi-protura" is: 28320
            @@@ @@@ @@@ @@@ @@@
            """,
            Message.from_dict(
                message="procServ status update",
                info={
                    "pid": ["28147"],
                    "exit_code": ["126"],
                    "procserv_ts": ["Thu Dec  8 15:12:25 2022"],
                    "procserv_iocname": ["ioc-cxi-protura"],
                    "process": ["/reg/g/pcds/pyps/config/cxi/iocmanager/startProc"],
                    "new_pid": ["28320"],
                },
                source_lines=9,
            ),
            id="procserv_status_update",
        ),
        pytest.param(
            """\
            @@@ @@@ @@@ @@@ @@@
            @@@ Received a sigChild for process 150853. The process was killed by signal 9
            @@@ Toggled auto restart mode to ONESHOT
            @@@ @@@ @@@ @@@ @@@
            """,
            Message.from_dict(
                message="procServ status update",
                info={
                    "pid": ["150853"],
                    "signal": ["9"],
                    "restart_mode": ["ONESHOT"],
                },
                source_lines=4,
            ),
            id="procserv_status_update",
        ),
        pytest.param(
            """\
            @@@ @@@ @@@ @@@ @@@
            @@@ Received a sigChild for process 27111. Normal exit status = 127
            @@@ Current time: Thu Dec  8 15:29:34 2022
            @@@ Child process is shutting down, a new one will be restarted shortly
            @@@ ^R or ^X restarts the child, ^Q quits the server
            @@@ Restarting child "ioc-cxi-rec01-evr"
            @@@    (as /reg/g/pcds/pyps/config/cxi/iocmanager/startProc)
            @@@ The PID of new child "ioc-cxi-rec01-evr" is: 27511
            @@@ @@@ @@@ @@@ @@@
            """,
            Message.from_dict(
                message="procServ status update",
                info={
                    "pid": ["27111"],
                    "exit_code": ["127"],
                    "procserv_ts": ["Thu Dec  8 15:29:34 2022"],
                    "procserv_iocname": ["ioc-cxi-rec01-evr"],
                    "process": ["/reg/g/pcds/pyps/config/cxi/iocmanager/startProc"],
                    "new_pid": ["27511"],
                },
                source_lines=9,
            ),
            id="procserv_status_update",
        ),
    ],
)
def test_multiline_groupable_regexes(
    source_message: str,
    expected_message: Message,
):
    squasher = parser.Squasher()
    squasher.add_lines(textwrap.dedent(source_message.rstrip()))
    squashed = squasher.squash()
    compare_results(squashed, [expected_message])


@pytest.mark.parametrize(
    "source_message, expected_lines",
    [
        pytest.param(
            """\
            @@@ @@@ @@@ @@@ @@@
            @@@ Received a sigChild for process 27111. Normal exit status = 127
            @@@ Current time: Thu Dec  8 15:29:34 2022
            @@@ Child process is shutting down, a new one will be restarted shortly
            """,
            0,
            id="procserv_status_update",
        ),
    ],
)
def test_multiline_groupable_regexes_pending(
    source_message: str,
    expected_lines: int,
):
    squasher = parser.Squasher()
    squasher.add_lines(textwrap.dedent(source_message.rstrip()))
    squashed = squasher.squash()
    assert len(squashed) == expected_lines
    # Total line count should be correct
    assert (len(squashed) + len(squasher.pending_lines)) == len(squasher.messages)


@pytest.mark.parametrize(
    "source_message, expected_lines",
    [
        pytest.param(
            """\
            @@@ @@@ @@@ @@@ @@@
            @@@ Received a sigChild for process 27111. Normal exit status = 127
            @@@ Current time: Thu Dec  8 15:29:34 2022
            @@@ Child process is shutting down, a new one will be restarted shortly
            interruption to group match is here
            """,
            4 + 1,  # procserv lines and interruption
            id="procserv_status_update",
        ),
    ],
)
def test_multiline_groupable_regexes_interrupted(
    source_message: str,
    expected_lines: int,
):
    squasher = parser.Squasher()
    squasher.add_lines(textwrap.dedent(source_message.rstrip()))
    squashed = squasher.squash()
    assert len(squashed) == expected_lines
