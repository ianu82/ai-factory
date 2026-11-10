"""FastAPI application: API, sign-in, inference gateway and the web app."""

from __future__ import annotations

import asyncio
import logging
from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import FastAPI, Request
from fastapi.exception_handlers import request_validation_exception_handler
from fastapi.exceptions import RequestValidationError
from fastapi.responses import FileResponse, JSONResponse
from fastapi.staticfiles import StaticFiles

from . import __version__
from .api import router as api_router
from .auth import router as auth_router
from .config import get_settings
from .db import init_engine
from .gateway import router as gateway_router
from .mind.api import router as mind_router
from .mind.runtime import EngineRuntime
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
        from . import scorers
        scorer_worker = asyncio.create_task(scorers.worker(), name="scorer-queue")
        yield
        scorer_worker.cancel()
        await asyncio.gather(scorer_worker, return_exceptions=True)
        await scorers.stop()
        if run:
            await orchestrator.stop()

    app = FastAPI(title="AI Factory", version=__version__, lifespan=lifespan, docs_url="/api/docs", redoc_url=None)

    @app.exception_handler(RequestValidationError)
    async def validation_error(request: Request, exc: RequestValidationError):
        if (request.url.path == "/auth/api-key" or
                request.url.path.startswith("/api/workspaces/") and "/credentials/" in request.url.path):
            return JSONResponse({"detail": "Invalid credential request"}, status_code=422)
        if request.url.path.startswith("/api/workspaces/") and request.url.path.endswith("/linear"):
            return JSONResponse({"detail": "Invalid Linear configuration request"}, status_code=422)
        return await request_validation_exception_handler(request, exc)

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
    # Mind: private threads and streams (T1) over the restricted engine (T2); confirmations (T3) come with api_router.
    app.include_router(mind_router, prefix="/api")
    app.state.mind_runtime = EngineRuntime()

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
