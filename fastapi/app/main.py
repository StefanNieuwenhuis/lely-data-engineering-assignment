from fastapi import FastAPI

app = FastAPI(title="GitHub Events API")

@app.get("/")
def read_root():
    return {"message": "Welcome to the GitHub Events API"}

@app.get("/healthcheck")
def health_check():
    return {"status": "healthy"}