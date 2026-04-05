"""FastAPI application factory."""

from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from src.config import APISettings
from src.dependencies import init_session_factory
from src.routes import chemical, disease, download, food, metadata


def create_app(settings: APISettings | None = None) -> FastAPI:
    """Create and configure the FastAPI application."""
    settings = settings or APISettings()

    @asynccontextmanager
    async def lifespan(_app: FastAPI):
        init_session_factory()
        yield

    app = FastAPI(title="FoodAtlas API", version="0.1.0", lifespan=lifespan)

    app.add_middleware(
        CORSMiddleware,
        allow_origins=settings.cors_origins.split(","),
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )

    app.include_router(food.router)
    app.include_router(chemical.router)
    app.include_router(disease.router)
    app.include_router(metadata.router)
    app.include_router(download.router)

    return app


app = create_app()
