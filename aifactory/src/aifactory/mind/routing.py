"""The route class every Mind router uses. It imports nothing from the Factory, so any Mind module (the thread API,
the confirmation routes) can use it without an import cycle."""
from __future__ import annotations

from fastapi import Request
from fastapi.exceptions import RequestValidationError
from fastapi.routing import APIRoute
from starlette.responses import JSONResponse

INVALID = {"detail": "Invalid Mind request"}


class PrivateRoute(APIRoute):
    """Do not echo rejected inputs (or their field names) on validation errors.

    FastAPI's default 422 includes arbitrary input, even extra fields. Keeping this policy on the router also
    covers errors before endpoint dependencies run, without relying on application registration or an
    authenticated actor. The answer is a fixed body: nothing request-derived leaves this path.
    """

    def get_route_handler(self):
        handler = super().get_route_handler()

        async def private_handler(request: Request):
            try:
                return await handler(request)
            except RequestValidationError:
                return JSONResponse(status_code=422, content=dict(INVALID))

        return private_handler
