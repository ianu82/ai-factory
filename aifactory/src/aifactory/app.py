"""FastAPI application: API, sign-in, inference gateway and the web app."""

from __future__ import annotations

import logging
from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import FastAPI, Request
from fastapi.responses import FileResponse, JSONResponse
from fastapi.staticfiles import StaticFiles

from . import __version__
from .api import router as api_router
from .auth import router as auth_router
from .config import get_settings
from .db import init_engine
from .gateway import router as gateway_router
from .onboarding import router as onboarding_router
from .pulls import router as pulls_router
from .pipeline.orchestrator import orchestrator

log = logging.getLogger("aifactory")
WEB_DIST = Path(__file__).resolve().parent / "web"


def create_app(*, start_orchestrator: bool | None = None) -> FastAPI:
    settings = get_settings()
    init_engine()

    @asynccontextmanager
    async def lifespan(_app: FastAPI):
        run = settings.orchestrator_enabled if start_orchestrator is None else start_orchestrator
        from .bootstrap import apply_bootstrap

        apply_bootstrap()
        if run:
            orchestrator.start()
        yield
        if run:
            await orchestrator.stop()

    app = FastAPI(title="AI Factory", version=__version__, lifespan=lifespan, docs_url="/api/docs", redoc_url=None)

    @app.middleware("http")
    async def headers(request: Request, call_next):
        response = await call_next(request)
        response.headers.setdefault("X-Content-Type-Options", "nosniff")
        response.headers.setdefault("Referrer-Policy", "same-origin")
        if not request.url.path.startswith("/api/docs"):
            response.headers.setdefault("X-Frame-Options", "DENY")
        return response

    app.include_router(api_router)
    app.include_router(auth_router)
    app.include_router(gateway_router)
    app.include_router(onboarding_router)
    app.include_router(pulls_router)

    if (WEB_DIST / "index.html").exists():
        app.mount("/assets", StaticFiles(directory=WEB_DIST / "assets"), name="assets")

        @app.get("/{path:path}", include_in_schema=False)
        async def spa(path: str):
            if path.startswith(("api/", "auth/", "gateway/")):
                return JSONResponse({"detail": "Not found"}, status_code=404)
            candidate = (WEB_DIST / path).resolve()
            if path and candidate.is_file() and WEB_DIST in candidate.parents:
                return FileResponse(candidate)
            return FileResponse(WEB_DIST / "index.html", headers={"Cache-Control": "no-cache"})
    else:
        @app.get("/", include_in_schema=False)
        async def root():
            return JSONResponse({"name": "AI Factory", "version": __version__,
                                 "detail": "Web app not built. Run `npm run build` in aifactory/web."})

    return app
