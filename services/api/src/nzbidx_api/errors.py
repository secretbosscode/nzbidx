"""Helper functions for consistent JSON error responses."""

from __future__ import annotations

from .orjson_response import ORJSONResponse


def error_response(code: str, message: str, status_code: int) -> ORJSONResponse:
    return ORJSONResponse(
        {"error": {"code": code, "message": message}}, status_code=status_code
    )


def unauthorized(message: str = "unauthorized") -> ORJSONResponse:
    return error_response("unauthorized", message, 401)


def rate_limited(message: str = "rate limit exceeded") -> ORJSONResponse:
    return error_response("rate_limited", message, 429)


def breaker_open(message: str = "service unavailable") -> ORJSONResponse:
    return error_response("breaker_open", message, 503)


def nzb_unavailable(message: str = "nzb temporarily unavailable") -> ORJSONResponse:
    return error_response("nzb_unavailable", message, 503)


def nzb_timeout(message: str = "nzb fetch timed out") -> ORJSONResponse:
    return error_response("nzb_timeout", message, 504)


def nzb_not_found(message: str = "nzb not found") -> ORJSONResponse:
    return error_response("nzb_not_found", message, 404)


def invalid_params(message: str = "invalid parameters") -> ORJSONResponse:
    return error_response("invalid_params", message, 400)
