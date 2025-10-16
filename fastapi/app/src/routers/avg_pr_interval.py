import logging

from cassandra.cluster import Session
from fastapi import APIRouter, Depends, HTTPException

from src.models.avg_pr_interval_model import AvgPrIntervalModel
from src.database import get_cassandra_session

log = logging.getLogger(__name__)

router = APIRouter(
    prefix="/v1",
    tags=["github_events"]
)

@router.get("/avg_pr_interval/{repo_name:path}", response_model=AvgPrIntervalModel)
async def get_avg_pr_interval(repo_name: str, session: Session = Depends(get_cassandra_session))->AvgPrIntervalModel:
    query = """
    SELECT repo_name, pr_count, avg_interval_seconds, last_pull_request_ts, updated_at 
    FROM avg_pr_time 
    WHERE repo_name=%s
    LIMIT 1;
    """
    result = session.execute(query, (repo_name,)).one()

    if not result:
        log.error(f"Repository with name '{repo_name}' not found")
        raise HTTPException(status_code=404, detail=f"Repository with name '{repo_name}' not found.")

    return AvgPrIntervalModel(
        repo_name=result["repo_name"],
        pr_count=result["pr_count"],
        avg_interval_seconds=result["avg_interval_seconds"],
        last_pull_request_ts=result["last_pull_request_ts"],
        updated_at=result["updated_at"],
    )
