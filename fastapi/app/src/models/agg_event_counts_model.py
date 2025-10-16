from datetime import datetime
from typing import Dict

from pydantic import BaseModel


class AggregatedEventCountsModel(BaseModel):
    counts: Dict[str, int]
    start_time: datetime
    end_time: datetime
