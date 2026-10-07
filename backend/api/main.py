"""Entry point for the API service."""

import os

import uvicorn

if __name__ == "__main__":
    host = os.environ.get("API_HOST", "127.0.0.1")
    reload = os.environ.get("API_DEBUG", "False").lower() == "true"
    port = int(os.environ.get("API_PORT", "8000"))
    # server_header=False: no `server: uvicorn` advertising the stack.
    uvicorn.run("src.app:app", host=host, port=port, reload=reload, server_header=False)
