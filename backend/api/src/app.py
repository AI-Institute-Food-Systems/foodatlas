"""FastAPI application factory."""

from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.openapi.utils import get_openapi

from src.config import APISettings
from src.dependencies import init_session_factory
from src.public_keys import get_store, init_store
from src.rate_limit import TokenBucketLimiter
from src.routes import chemical, disease, download, food, metadata, resolve
from src.routes import v1 as v1_routes

PUBLIC_API_DESCRIPTION = """
FoodAtlas exposes its food-chemical-disease knowledge graph through a
versioned public REST API under `/v1/`. Internal UI routes (e.g. `/food/`,
`/chemical/`) are not part of the public contract; use `/v1/` for any
external integration.

### Authentication
All `/v1/` requests must include `Authorization: Bearer <key>`. To request
a key, use the contact form at https://www.foodatlas.ai/contact?api-access
and provide your name, affiliation, and intended use.

### Rate limits
Each key is limited to 60 requests per minute, with a burst capacity of 10
(meaning short spikes up to 10 requests can land at once before throttling
kicks in). Over-limit requests receive `429 Too Many Requests` with a
`Retry-After` header indicating when capacity will be available again.

### Versioning & terms
Endpoints under `/v1/` follow a stable contract. Use is intended for
academic and non-commercial research. Cite FoodAtlas in any published work
that uses this data.
""".strip()


def create_app(settings: APISettings | None = None) -> FastAPI:
    """Create and configure the FastAPI application."""
    settings = settings or APISettings()
    init_store(settings)

    @asynccontextmanager
    async def lifespan(_app: FastAPI):
        init_session_factory()
        store = get_store()
        if store is not None and not settings.debug:
            await store.start()
        try:
            yield
        finally:
            if store is not None:
                await store.stop()

    app = FastAPI(
        title="FoodAtlas API",
        version="1.0.0",
        description=PUBLIC_API_DESCRIPTION,
        contact={"name": "FoodAtlas Team", "email": "aifs@ucdavis.edu"},
        license_info={"name": "Academic use only"},
        lifespan=lifespan,
    )

    app.add_middleware(
        CORSMiddleware,
        allow_origins=settings.cors_origins.split(","),
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )

    if settings.rate_limit_enabled and not settings.debug:
        app.state.rate_limiter = TokenBucketLimiter(
            sustained_per_min=settings.rate_limit_per_minute,
            burst=settings.rate_limit_burst,
        )

    @app.get("/health", tags=["health"])
    async def health() -> dict[str, str]:
        """Liveness probe for the ALB target group (no auth required)."""
        return {"status": "ok"}

    # Internal UI router (not part of the public contract)
    app.include_router(food.router)
    app.include_router(chemical.router)
    app.include_router(disease.router)
    app.include_router(metadata.router)
    app.include_router(download.router)
    app.include_router(resolve.router)

    # Public versioned API
    app.include_router(v1_routes.router)

    app.openapi = _build_openapi(app)  # type: ignore[method-assign]
    return app


def _build_openapi(app: FastAPI):
    """Inject a Bearer security scheme so the /docs Authorize button works."""

    def custom_openapi():
        if app.openapi_schema:
            return app.openapi_schema
        schema = get_openapi(
            title=app.title,
            version=app.version,
            description=app.description,
            routes=app.routes,
            contact=app.contact,
            license_info=app.license_info,
        )
        schema.setdefault("components", {}).setdefault("securitySchemes", {})[
            "bearerAuth"
        ] = {"type": "http", "scheme": "bearer"}
        for path, methods in schema.get("paths", {}).items():
            if not path.startswith("/v1/"):
                continue
            for op in methods.values():
                if isinstance(op, dict):
                    op["security"] = [{"bearerAuth": []}]
        app.openapi_schema = schema
        return schema

    return custom_openapi


app = create_app()
