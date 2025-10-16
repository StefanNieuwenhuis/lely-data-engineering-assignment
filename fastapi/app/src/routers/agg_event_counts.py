from datetime import datetime, timedelta, timezone
import logging

from cassandra.query import SimpleStatement
from fastapi import APIRouter, Depends, HTTPException, Query
from requests import Session

from src.models import EventCountsModel
from src.database import get_cassandra_session

log = logging.getLogger(__name__)

router = APIRouter(
    prefix="/v1",
    tags=["github_events"]
)

@router.get("/event_counts", response_model=EventCountsModel)
async def get_avg_pr_interval(offset: int = Query(..., ge=1), session: Session = Depends(get_cassandra_session))->EventCountsModel:
    stmt = SimpleStatement("""
            SELECT type, COUNT(*) AS event_count
            FROM event_counts_by_type
            WHERE created_at >= %s
            GROUP BY type;
        """)

    now = datetime.now(timezone.utc)
    cutoff = now - timedelta(minutes=offset)
    result = session.execute(stmt, (cutoff,))

    if not result:
        log.error(f"No events with offset '{offset} minutes' found")
        raise HTTPException(status_code=404, detail=f"No events with offset '{offset} minutes' found")

    return EventCountsModel(
        type=result["type"],
        event_count=result["event_count"]
    )
