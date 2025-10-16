from fastapi import FastAPI, APIRouter

from src.routers import avg_pr_interval

app = FastAPI(title="GitHub Events API")
router = APIRouter()

app.include_router(avg_pr_interval.router)

@app.get("/")
def read_root():
    return {"message": "Welcome to the GitHub Events API"}

@app.get("/healthcheck")
def health_check():
    return {"status": "healthy"}