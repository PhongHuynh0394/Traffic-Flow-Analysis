from fastapi import FastAPI, File
import uvicorn
import os
from fastapi.responses import JSONResponse
import json
from typing import Annotated
from api import counting
from api import healthcheck
from fastapi.responses import RedirectResponse


app = FastAPI()

# Set Router
app.include_router(counting.router, prefix="/model", tags=['model'])
app.include_router(healthcheck.router)

@app.get("/")
async def root():
    return RedirectResponse(url="/docs")

@app.post("/upload-image/")
async def upload_image(file: Annotated[bytes, File()]):
    #  This endpoint accepts any uploaded image but always returns mock data
    mock_file_path = os.path.join(os.path.dirname(__file__), "mock_data.json")
    with open(mock_file_path, "r") as f:
        mock_data = json.load(f)
    return JSONResponse(content=mock_data)


if __name__ == "__main__":
    host = "0.0.0.0"
    uvicorn.run("main:app",
                host=host,
                port=8000,
                reload=True)
    