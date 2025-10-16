from datetime import datetime
from typing import Optional

from pydantic import BaseModel


class AvgPrIntervalModel(BaseModel):
    repo_name: str
    avg_interval_seconds: float
    last_pull_request_ts: datetime
    pr_count: int
    updated_at: datetime
