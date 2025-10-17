
import logging

from cassandra.cluster import Session
from cassandra.query import SimpleStatement
from fastapi import APIRouter, Depends, Query
from datetime import datetime, timedelta, timezone, date

from src.config import GITHUB_EVENTS_TYPES
from src.models import AggregatedEventCountsModel
from src.database import get_cassandra_session


log = logging.getLogger(__name__)



router = APIRouter(
    prefix="/v1",
    tags=["github_events"]
)

def compute_time_range(offset_minutes: int) -> tuple[datetime, datetime, date]:
    """Computes the relevant time range"""
    now = datetime.now(timezone.utc)
    start_time = now - timedelta(minutes=offset_minutes)
    today = now.date()

    return now, start_time, today

@router.get("/agg_event_counts", response_model=AggregatedEventCountsModel)
async def get_avg_pr_interval(offset_minutes: int = Query(..., description="Offset in minutes", ge=1), session: Session = Depends(get_cassandra_session))->AggregatedEventCountsModel:
    now, start_time, today = compute_time_range(offset_minutes)
    github_events_types = GITHUB_EVENTS_TYPES.split(",")

    results = {}
    # TODO: Use .env for events list
    for event_type in github_events_types:
        stmt = SimpleStatement("""
            SELECT type, count
            FROM event_counts_by_type
            WHERE type=%s
            AND bucket_day=%s
            AND time_bucket>=%s
        """)
        rows = session.execute(stmt, (event_type, today, start_time))
        results[event_type] = sum(row["count"] for row in rows)

    if not results:
        log.error(f"No events of any type are found in offset {start_time} - {now}")

    return AggregatedEventCountsModel(counts=results, start_time=start_time, end_time=now)


