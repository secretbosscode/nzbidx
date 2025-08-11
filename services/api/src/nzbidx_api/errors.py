"""Helper functions for consistent JSON error responses."""

from __future__ import annotations

from starlette.responses import JSONResponse



def error_response(code: str, message: str, status_code: int) -> JSONResponse:
    return JSONResponse(
        {"error": {"code": code, "message": message}}, status_code=status_code
    )


def unauthorized(message: str = "unauthorized") -> JSONResponse:
    return error_response("unauthorized", message, 401)


def rate_limited(message: str = "rate limit exceeded") -> JSONResponse:
    return error_response("rate_limited", message, 429)


def breaker_open(message: str = "service unavailable") -> JSONResponse:
    return error_response("breaker_open", message, 503)


def nzb_unavailable(message: str = "nzb temporarily unavailable") -> JSONResponse:
    return error_response("nzb_unavailable", message, 503)


def invalid_params(message: str = "invalid parameters") -> JSONResponse:
    return error_response("invalid_params", message, 400)
