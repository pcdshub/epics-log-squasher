
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
        id="repetitive_with_timestamp_asyn"
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
        id="repetitive_with_timestamp_prosilica"
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
        id="repetitive_with_timestamp_ansi"
    ),
]


@pytest.mark.parametrize("lines, expected", test_cases)
def test_squash(lines: str, expected: parser.Squashed):
    squasher = parser.Squasher()
    squasher.add_lines(textwrap.dedent(lines.rstrip()))
    squashed = squasher.squash()
    assert squashed == expected
