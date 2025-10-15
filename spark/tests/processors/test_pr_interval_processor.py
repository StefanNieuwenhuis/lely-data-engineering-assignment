from datetime import datetime, timezone
from typing import Tuple, Any

import pandas as pd
import pytest

from app.src.processors import AveragePRIntervalProcessor

SECONDS_PER_DAY = 24 * 60 * 60

class MockedGroupState:
    """Mock of pyspark.sql.streaming.GroupState"""
    def __init__(self, initial_state: Tuple[Any, int, float] | None = None):
        self._state = initial_state or (None, 0, 0.0)
        self.exists = initial_state is not None
        self.hasTimedOut = False
        self._updated = None

    @property
    def value(self) -> Tuple[Any, int, float]:
        return self._state

    @value.setter
    def value(self, new_state: Tuple[Any, int, float]):
        self._state = new_state
        self.exists = True
        self._updated = new_state

    @property
    def updated(self) -> Tuple[Any, int, float]:
        return self._updated

    def get(self) -> Tuple[Any, int, float]:
        return self._state

    def update(self, new_state: Tuple[Any, int, float]):
        self._state = new_state
        self.exists = True
        self._updated = new_state

    def __repr__(self):
        return f"TestGroupState(state={self._state}, exists={self.exists})"

class DummyGroupState:
    """Mock of pyspark.sql.streaming.GroupState"""
    def __init__(self, exists=False, value=None):
        self._exists = exists
        self._value = value or (None, 0, 0.0)
        self._updated = None
        self._hasTimedOut = False

    @property
    def value(self) -> Tuple[Any | None, int, float]:
        return self._value

    @value.setter
    def value(self, new_value) -> None:
        self._updated = True
        self._value = new_value

    def __repr__(self):
        return f"DummyGroupState(value={self._value})"


@pytest.fixture
def processor():
    """Provide a new AveragePRIntervalProcessor instance before each test"""
    return AveragePRIntervalProcessor()
@pytest.fixture
def sample_pr_data() -> pd.DataFrame:
    """Simulated micro-batch of PullRequestEvents"""
    return pd.DataFrame({
        "created_at": [
            pd.Timestamp(ts_input="2025-10-10T12:00:00Z", tzinfo=timezone.utc),
            pd.Timestamp(ts_input="2025-10-11T12:00:00Z"),
            pd.Timestamp(ts_input="2025-10-12T12:00:00Z"),
        ]
    })

@pytest.fixture
def empty_state():
    return MockedGroupState()  # no prior state

@pytest.fixture
def populated_state(sample_pr_data):
    last_ts = sample_pr_data["created_at"].iloc[-1]
    count = len(sample_pr_data)
    total_diff = (count - 1) * SECONDS_PER_DAY
    return MockedGroupState((last_ts, count, total_diff))


def test_update_state(sample_pr_data, empty_state, processor) -> None:
    """Test state initialization"""
    key = ("apache/spark",)
    result = list(processor.update_state(key, [sample_pr_data], empty_state))

    # Expect a single output DataFrame
    assert len(result) == 1
    df = result[0]
    row = df.iloc[0]

    assert row["pr_count"] == len(sample_pr_data.index)
    assert abs(row["avg_interval_seconds"] - (int(row["pr_count"]) * SECONDS_PER_DAY / 2)) < 1
    assert empty_state.exists
    assert isinstance(row["last_pull_request_ts"], pd.Timestamp)

def test_update_state_additional_batch(sample_pr_data, populated_state, processor) -> None:
    """Test adding new PR events to existing state"""
    initial_last_ts = sample_pr_data["created_at"].iloc[-1]
    initial_pr_count = len(sample_pr_data.index)
    total_diff = initial_pr_count * SECONDS_PER_DAY  # total seconds of previous intervals

    key = ("apache/spark",)
    new_batch = pd.DataFrame({
        "created_at": [pd.Timestamp("2025-10-14T12:00:00Z")]
    })

    result = list(processor.update_state(key, [new_batch], populated_state))
    df = result[0]
    row = df.iloc[0]

    assert row["pr_count"] == len(sample_pr_data.index) + 1
    assert abs(row["avg_interval_seconds"] - ((int(row["pr_count"]) * SECONDS_PER_DAY + SECONDS_PER_DAY) / 3)) < 1


def test_update_state_handles_tz_aware_and_naive_mixed(empty_state, processor) -> None:
    """Test timezone conflicts handling"""

    # Create two timestamps: one timezone-aware, the other naive
    ts_1 = pd.Timestamp(datetime(2025, 1, 1, 12, 0, 0, tzinfo=timezone.utc))
    ts_2 = pd.Timestamp(datetime(2025, 1, 2, 12, 0, 0))

    # Create DataFrame
    pdf = pd.DataFrame({
        "repo_name": ["test_repo", "test_repo"],
        "created_at": [ts_1, ts_2]
    })

    # Create iterable for DataFrames
    pdfs = [pdf]

    result = list(processor.update_state(("test_repo",), pdfs, empty_state))


    assert len(result) == 1
    df = result[0]

    assert "avg_interval_seconds" in df.columns
    assert df["avg_interval_seconds"].iloc[0] > 0
    # state should store tz-naive datetime
    assert empty_state.updated[0].tzinfo is None