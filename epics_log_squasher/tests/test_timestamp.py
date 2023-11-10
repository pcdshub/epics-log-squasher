import datetime
from typing import Optional

import pytest

from .. import parser


@pytest.mark.parametrize(
    "line, timestamp_expected, remainder_expected",
    [
        pytest.param(
            "2022/11/09 09:32:01.994 HPL:GIGE:BASLER10:CONNECTION devAsynInt32 process error",
            datetime.datetime(2022, 11, 9, 9, 32, 1, 994000),
            "HPL:GIGE:BASLER10:CONNECTION devAsynInt32 process error",
            id="from_log_file",
        ),
        pytest.param(
            "  2022/11/09 09:32:01.994 something else ",
            datetime.datetime(2022, 11, 9, 9, 32, 1, 994000),
            "something else",
            id="with_space",
        ),
        pytest.param(
            "no timestamp",
            None,
            "no timestamp",
            id="no_timestamp",
        ),
    ],
)
def test_timestamp_checker(
    line: str, timestamp_expected: Optional[datetime.datetime], remainder_expected: str
):
    timestamp, remainder = parser.DateFormats.find_timestamp(line)
    assert timestamp == timestamp_expected
    assert remainder == remainder_expected
