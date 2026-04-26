"""Small utility helpers for the package skeleton."""

from __future__ import annotations


def strip_trailing_slash(value: str) -> str:
    """Normalize a path-like string by removing trailing slashes."""

    return value.rstrip("/")
