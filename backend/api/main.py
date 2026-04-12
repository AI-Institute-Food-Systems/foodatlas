"""Entry point for the API service."""

import os

import uvicorn

if __name__ == "__main__":
    host = os.environ.get("API_HOST", "127.0.0.1")
    # Auto-reload for local development (API_DEBUG=True); disabled in
    # production containers where API_DEBUG=False.
    reload = os.environ.get("API_DEBUG", "True").lower() == "true"
    uvicorn.run("src.app:app", host=host, port=8000, reload=reload)
