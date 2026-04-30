from __future__ import annotations

from datetime import timedelta

import pytest

from server.core.qr import _reed_solomon_generator, qr_svg
from server.core.security import SecurityError, build_pkce_challenge, create_jwt, decode_jwt, encode_jwt, hash_password, totp_code, verify_password, verify_pkce


def test_password_hash_roundtrip() -> None:
    encoded = hash_password("super-secret-password", "pepper")
    assert verify_password("super-secret-password", encoded, "pepper")
    assert not verify_password("wrong-password", encoded, "pepper")


def test_pkce_verification_roundtrip() -> None:
    verifier = "a-very-long-pkce-verifier-string"
    challenge = build_pkce_challenge(verifier)
    assert verify_pkce(verifier, challenge, "S256")
    assert not verify_pkce("other", challenge, "S256")


def test_jwt_roundtrip() -> None:
    secret = "x" * 48
    token = create_jwt(
        subject="user_1",
        secret=secret,
        issuer="https://multiplex.asfes.ru/api/oauth",
        audience="multiplex-api",
        token_type="api_access",
        ttl=timedelta(minutes=5),
        extra={"username": "root"},
    )
    payload = decode_jwt(
        token,
        secret,
        issuer="https://multiplex.asfes.ru/api/oauth",
        audience="multiplex-api",
        token_type="api_access",
    )
    assert payload["sub"] == "user_1"
    assert payload["username"] == "root"


def test_jwt_rejects_unexpected_algorithm() -> None:
    token = encode_jwt(
        {
            "sub": "user_1",
            "iss": "https://multiplex.asfes.ru/api/oauth",
            "aud": "multiplex-api",
            "iat": 1,
            "nbf": 1,
            "exp": 9999999999,
            "token_type": "api_access",
        },
        "x" * 48,
    )
    header, payload, signature = token.split(".")
    import base64
    import json

    bad_header = base64.urlsafe_b64encode(json.dumps({"alg": "none", "typ": "JWT"}).encode()).decode().rstrip("=")
    with pytest.raises(SecurityError):
        decode_jwt(
            f"{bad_header}.{payload}.{signature}",
            "x" * 48,
            issuer="https://multiplex.asfes.ru/api/oauth",
            audience="multiplex-api",
            token_type="api_access",
        )


def test_totp_matches_rfc_vector() -> None:
    secret = "GEZDGNBVGY3TQOJQGEZDGNBVGY3TQOJQ"
    assert totp_code(secret, for_time=59, digits=8) == "94287082"


def test_qr_svg_is_local_svg() -> None:
    svg = qr_svg("otpauth://totp/ASFES:root?secret=ABC&issuer=ASFES")
    assert svg.startswith("<svg")
    assert "<rect" in svg


def test_qr_reed_solomon_generator_matches_qr_spec_vector() -> None:
    assert _reed_solomon_generator(7) == [127, 122, 154, 164, 11, 68, 117]
