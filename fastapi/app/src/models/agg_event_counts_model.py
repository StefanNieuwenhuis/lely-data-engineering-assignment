from datetime import datetime
from pydantic import BaseModel


class EventCountsModel(BaseModel):
    type: str
    event_count: int
