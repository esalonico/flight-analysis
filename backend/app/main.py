import sys

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

sys.path.append("backend")

app = FastAPI()


app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:8080"],  # List of allowed origins
    allow_credentials=True,
    allow_methods=["*"],  # Allows all methods
    allow_headers=["*"],  # Allows all headers
)
