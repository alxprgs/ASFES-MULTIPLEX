from __future__ import annotations

import base64
import hashlib
import hmac
import json
import secrets
import struct
import time
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any
from urllib.parse import quote


class SecurityError(Exception):
    pass


def b64url_encode(data: bytes) -> str:
    return base64.urlsafe_b64encode(data).decode("utf-8").rstrip("=")


def b64url_decode(data: str) -> bytes:
    padding = "=" * (-len(data) % 4)
    return base64.urlsafe_b64decode(f"{data}{padding}")


def now_utc() -> datetime:
    return datetime.now(UTC)


def hash_password(password: str, pepper: str) -> str:
    salt = secrets.token_bytes(16)
    digest = hashlib.scrypt((password + pepper).encode("utf-8"), salt=salt, n=2**14, r=8, p=1)
    return f"scrypt${b64url_encode(salt)}${b64url_encode(digest)}"


def verify_password(password: str, encoded: str, pepper: str) -> bool:
    try:
        algorithm, salt_b64, digest_b64 = encoded.split("$", 2)
    except ValueError:
        return False
    if algorithm != "scrypt":
        return False
    salt = b64url_decode(salt_b64)
    expected = b64url_decode(digest_b64)
    actual = hashlib.scrypt((password + pepper).encode("utf-8"), salt=salt, n=2**14, r=8, p=1)
    return hmac.compare_digest(actual, expected)


def sha256_text(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def random_token(length: int = 48) -> str:
    return secrets.token_urlsafe(length)


def generate_totp_secret(length: int = 20) -> str:
    return base64.b32encode(secrets.token_bytes(length)).decode("ascii").rstrip("=")


def _decode_totp_secret(secret: str) -> bytes:
    normalized = secret.replace(" ", "").upper()
    padding = "=" * (-len(normalized) % 8)
    return base64.b32decode(f"{normalized}{padding}", casefold=True)


def totp_code(secret: str, *, for_time: int | None = None, period: int = 30, digits: int = 6) -> str:
    counter = int((for_time or time.time()) // period)
    digest = hmac.new(_decode_totp_secret(secret), struct.pack(">Q", counter), hashlib.sha1).digest()
    offset = digest[-1] & 0x0F
    value = struct.unpack(">I", digest[offset : offset + 4])[0] & 0x7FFFFFFF
    return str(value % (10**digits)).zfill(digits)


def verify_totp_code(secret: str, code: str, *, window: int = 1, period: int = 30, digits: int = 6) -> bool:
    normalized = "".join(ch for ch in code if ch.isdigit())
    if len(normalized) != digits:
        return False
    current_time = int(time.time())
    for offset in range(-window, window + 1):
        expected = totp_code(secret, for_time=current_time + offset * period, period=period, digits=digits)
        if hmac.compare_digest(expected, normalized):
            return True
    return False


def build_totp_uri(*, secret: str, issuer: str, account_name: str) -> str:
    label = f"{issuer}:{account_name}"
    return f"otpauth://totp/{quote(label)}?secret={quote(secret)}&issuer={quote(issuer)}"


def build_pkce_challenge(verifier: str) -> str:
    digest = hashlib.sha256(verifier.encode("utf-8")).digest()
    return b64url_encode(digest)


def verify_pkce(verifier: str, challenge: str, method: str) -> bool:
    normalized = method.upper()
    if normalized == "S256":
        return hmac.compare_digest(build_pkce_challenge(verifier), challenge)
    if normalized == "PLAIN":
        return hmac.compare_digest(verifier, challenge)
    return False


@dataclass(slots=True, frozen=True)
class TokenBundle:
    access_token: str
    refresh_token: str
    expires_in: int
    token_type: str = "Bearer"


def encode_jwt(payload: dict[str, Any], secret: str) -> str:
    header = {"alg": "HS256", "typ": "JWT"}
    encoded_header = b64url_encode(json.dumps(header, separators=(",", ":"), sort_keys=True).encode("utf-8"))
    encoded_payload = b64url_encode(json.dumps(payload, separators=(",", ":"), sort_keys=True).encode("utf-8"))
    signature = hmac.new(secret.encode("utf-8"), f"{encoded_header}.{encoded_payload}".encode("utf-8"), hashlib.sha256).digest()
    return f"{encoded_header}.{encoded_payload}.{b64url_encode(signature)}"


def decode_jwt(token: str, secret: str, *, issuer: str, audience: str, token_type: str | None = None) -> dict[str, Any]:
    try:
        encoded_header, encoded_payload, encoded_signature = token.split(".")
    except ValueError as exc:
        raise SecurityError("Malformed JWT") from exc

    expected_signature = hmac.new(
        secret.encode("utf-8"),
        f"{encoded_header}.{encoded_payload}".encode("utf-8"),
        hashlib.sha256,
    ).digest()
    if not hmac.compare_digest(b64url_encode(expected_signature), encoded_signature):
        raise SecurityError("Invalid JWT signature")

    payload = json.loads(b64url_decode(encoded_payload))
    now = int(time.time())
    if payload.get("iss") != issuer:
        raise SecurityError("Invalid issuer")
    aud = payload.get("aud")
    if isinstance(aud, list):
        if audience not in aud:
            raise SecurityError("Invalid audience")
    elif aud != audience:
        raise SecurityError("Invalid audience")
    if int(payload.get("exp", 0)) <= now:
        raise SecurityError("Token expired")
    if token_type is not None and payload.get("token_type") != token_type:
        raise SecurityError("Unexpected token type")
    return payload


def create_jwt(
    *,
    subject: str,
    secret: str,
    issuer: str,
    audience: str,
    token_type: str,
    ttl: timedelta,
    extra: dict[str, Any] | None = None,
) -> str:
    now = now_utc()
    payload = {
        "sub": subject,
        "iss": issuer,
        "aud": audience,
        "iat": int(now.timestamp()),
        "nbf": int(now.timestamp()),
        "exp": int((now + ttl).timestamp()),
        "jti": secrets.token_hex(16),
        "token_type": token_type,
    }
    if extra:
        payload.update(extra)
    return encode_jwt(payload, secret)
