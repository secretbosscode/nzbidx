"""Minimal subset of Starlette required for tests.

This package provides lightweight stand-ins for a handful of classes used by
our application.  It is *not* a full implementation of Starlette; it only
implements the pieces exercised by the smoke tests so that the application can
run in environments where the real dependency is unavailable.
"""
