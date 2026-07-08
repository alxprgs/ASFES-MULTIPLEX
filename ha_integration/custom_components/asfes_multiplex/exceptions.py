"""Exceptions for the ASFES Multiplex integration."""

from __future__ import annotations


class AsfesMultiplexError(Exception):
    """Base exception."""


class CannotConnect(AsfesMultiplexError):
    """Cannot connect to ASFES Multiplex server."""


class InvalidAuth(AsfesMultiplexError):
    """Authentication failed (wrong credentials)."""


class InvalidTotp(AsfesMultiplexError):
    """Invalid or expired TOTP code."""


class TokenExpired(AsfesMultiplexError):
    """HA access token has expired and refresh failed."""


class AuthRequired(AsfesMultiplexError):
    """Re-authentication is required."""
