from datetime import datetime, timezone
from typing import Tuple, Any

import pandas as pd
import pytest

from src.processors import AveragePRIntervalProcessor

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

def test_update_initial_state(sample_pr_data, empty_state, processor) -> None:
    """It should initialize a new state successfully"""
    key = ("apache/spark",)
    result = list(processor.update_state(key, [sample_pr_data], empty_state))

    # Expect a single output DataFrame
    assert len(result) == 1
    df = result[0]
    row = df.iloc[0]

    assert row["pr_count"] == len(sample_pr_data.index)
    assert row["avg_interval_seconds"] - SECONDS_PER_DAY == 0 #assert
    assert empty_state.exists
    assert isinstance(row["last_pull_request_ts"], pd.Timestamp)

def test_update_state_additional_batch(sample_pr_data, populated_state, processor) -> None:
    """It should append new PR events to the existing state"""

    # New PR DataFrame
    key = ("apache/spark",)
    new_batch = pd.DataFrame({
        "created_at": [pd.Timestamp("2025-10-13T12:00:00Z")]
    })

    # Update GroupState by appending the new PR df
    result = list(processor.update_state(key, [new_batch], populated_state))
    df = result[0]
    row = df.iloc[0]

    # Compute expected average
    combined_ts = list(sample_pr_data["created_at"]) + [new_batch["created_at"].iloc[0]]
    combined_ts = sorted(pd.to_datetime(combined_ts, utc=True).tz_convert(None))

    total_diff = sum(
        (combined_ts[i] - combined_ts[i - 1]).total_seconds()
        for i in range(1, len(combined_ts))
    )
    expected_avg_interval_seconds = total_diff / (len(combined_ts) - 1)

    # Assert
    pd.testing.assert_frame_equal(
        df[["repo_name", "pr_count"]].reset_index(drop=True),
        pd.DataFrame([{"repo_name": key[0], "pr_count": len(combined_ts)}])
    )

    assert pytest.approx(row["avg_interval_seconds"], rel=1e-6) == expected_avg_interval_seconds
    assert isinstance(row["last_pull_request_ts"], pd.Timestamp)

def test_update_state_handles_tz_aware_and_naive_mixed(empty_state, processor) -> None:
    """It should handle timezone naive vs. aware conflicts successfully"""

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