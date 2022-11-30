from typing import Dict

import pytest
import textwrap

import datetime

from .. import parser

# tail -n 10000 /cds/data/iocData/*/iocInfo/ioc.log > ~/last_log.txt

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
        parser.Squashed(
            lines=[
                # "2022/11/09 09:32:xx.xxx HPL:GIGE:BASLER10:CONNECTION devAsynInt32 process error",
                "[11x] HPL:GIGE:BASLER10:CONNECTION devAsynInt32 process error",
            ],
            source_lines=11,
        ),
        id="repetitive_with_timestamp_asyn",
    ),
    pytest.param(
        """\
        2021/12/07 00:01:01.886 prosilica:frameCallback: ERROR, frame has error code 16
        2021/12/07 00:01:02.876 prosilica:frameCallback: ERROR, frame has error code 16
        2021/12/07 00:01:03.875 prosilica:frameCallback: ERROR, frame has error code 16
        """,
        parser.Squashed(
            lines=[
                # "2022/11/09 09:32:xx.xxx HPL:GIGE:BASLER10:CONNECTION devAsynInt32 process error",
                "[3x] prosilica:frameCallback: ERROR, frame has error code 16",
            ],
            source_lines=3,
        ),
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
        parser.Squashed(
            lines=[
                "[3x] CXI:SC1:CVV:04 CXI:SC1:CVV:04:SetGain: No reply from device within 800 ms",
                "[3x] CXI:SC1:CVV:05 CXI:SC1:CVV:05:SetGain: No reply from device within 800 ms",
            ],
            source_lines=6,
        ),
        id="repetitive_with_timestamp_ansi",
    ),
    pytest.param(
        """\
        abc: Protocol aborted
        def: Protocol aborted
        ghi: Protocol aborted
        """,
        parser.Squashed(
            lines=[
                "Protocol aborted: abc, def, ghi",
            ],
            source_lines=3,
        ),
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
        parser.Squashed(
            lines=[
                "Protocol aborted: 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, ..."
            ],
            source_lines=11,
        ),
        id="protocol_abort_truncated_group",
    ),
    pytest.param(
        """\
        abc: pasynCommon->connect() failed: some reason 1
        012: pasynCommon->connect() failed: some reason 2
        def: pasynCommon->connect() failed: some reason 1
        345: pasynCommon->connect() failed: some reason 2
        """,
        parser.Squashed(
            lines=[
                "pasynCommon->connect() failed: some reason 1: abc, def",
                "pasynCommon->connect() failed: some reason 2: 012, 345",
            ],
            source_lines=4,
        ),
        id="asyn_connect_failed",
    ),
    pytest.param(
        """\
        2022/11/30 14:49:19.326201 scan-1 CXI:MCS2:01:m2:STATE_RBV lockRequest: pasynManager->queueRequest() failed: port TCP0 not connected
        2022/11/30 14:49:19.326233 scan-1 CXI:MCS2:01:m3:SCAN_POS lockRequest: pasynManager->queueRequest() failed: port TCP0 not connected
        2022/11/30 14:49:19.326261 scan-1 CXI:MCS2:01:m3:STATE_RBV lockRequest: pasynManager->queueRequest() failed: port TCP0 not connected
        """,
        parser.Squashed(
            lines=[
                "scan-1 lockRequest: pasynManager->queueRequest() failed: port TCP0 not connected: CXI:MCS2:01:m2:STATE_RBV, CXI:MCS2:01:m3:SCAN_POS, CXI:MCS2:01:m3:STATE_RBV",
            ],
            source_lines=3,
        ),
        id="asyn_connect_failed",
    ),
]


@pytest.mark.parametrize("lines, expected", test_cases)
def test_squash(lines: str, expected: parser.Squashed):
    squasher = parser.Squasher()
    squasher.add_lines(textwrap.dedent(lines.rstrip()))
    squashed = squasher.squash()
    assert squashed == expected


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
    match = parser.GroupableRegexes.group_fullmatch(idx)
    assert match is not None
    assert match.message == expected_message
    assert match.name == group
    assert match.groupdict == groupdict
