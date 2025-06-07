from fastapi import FastAPI, File
from fastapi.responses import JSONResponse
from fastapi.responses import RedirectResponse
import uvicorn

import os
import json
from typing import Annotated
from api import counting
from api import healthcheck


app = FastAPI()

# Set Router
app.include_router(counting.router, prefix="/model", tags=['model'])
app.include_router(healthcheck.router)


@app.get("/")
async def root():
    return RedirectResponse(url="/docs")


if __name__ == "__main__":
    host = "0.0.0.0"
    uvicorn.run("main:app",
                host=host,
                port=8000,
                reload=True)