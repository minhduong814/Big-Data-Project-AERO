from fastapi import FastAPI
from .api.v1.router import router as api_router


app = FastAPI(title="Aviation Data API", version="1.0.0")
app.include_router(api_router, prefix="/api/v1")

@app.get("/")
async def root():
    return {"message": "Welcome to the Aviation Data API"}
