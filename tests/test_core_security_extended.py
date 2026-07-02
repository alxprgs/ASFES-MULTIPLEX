from __future__ import annotations

from datetime import timedelta

import pytest
import jwt

from server.core.security import (
    b64url_encode,
    b64url_decode,
    verify_password,
    generate_totp_secret,
    verify_totp_code,
    build_totp_uri,
    verify_pkce,
    decode_jwt,
    create_jwt,
    SecurityError,
)


def test_b64url_encode_decode_roundtrip() -> None:
    data = b"Hello, World! \xff\x00"
    encoded = b64url_encode(data)
    assert "=" not in encoded
    decoded = b64url_decode(encoded)
    assert decoded == data


def test_verify_password_invalid_format() -> None:
    assert verify_password("pass", "invalid_format_no_dollars", "pepper") is False
    assert verify_password("pass", "scrypt$saltonly", "pepper") is False
    assert verify_password("pass", "bcrypt$salt$digest", "pepper") is False


def test_totp_generation_and_verification() -> None:
    secret = generate_totp_secret()
    assert len(secret) >= 16

    uri = build_totp_uri(secret=secret, issuer="ASFES", account_name="admin")
    assert "otpauth://totp/" in uri
    assert "secret=" in uri

    # Verify code (invalid code length)
    assert verify_totp_code(secret, "12345") is False
    assert verify_totp_code(secret, "1234567") is False


def test_verify_pkce_plain_and_invalid() -> None:
    assert verify_pkce("verifier", "verifier", "PLAIN") is True
    assert verify_pkce("verifier", "wrong_challenge", "PLAIN") is False
    assert verify_pkce("verifier", "challenge", "INVALID_METHOD") is False


def test_decode_jwt_malformed_and_invalid() -> None:
    secret = "x" * 48
    # Malformed token
    with pytest.raises(SecurityError) as exc:
        decode_jwt("not.a.jwt", secret, issuer="iss", audience="aud")
    assert "Malformed JWT" in str(exc.value)

    # Invalid header
    bad_header_token = jwt.encode({"sub": "1"}, secret, algorithm="HS256", headers={"typ": "NOT_JWT"})
    with pytest.raises(SecurityError) as exc:
        decode_jwt(bad_header_token, secret, issuer="iss", audience="aud")
    assert "Invalid JWT header" in str(exc.value)

    # Unexpected token type
    token_with_type = create_jwt(
        subject="1",
        secret=secret,
        issuer="iss",
        audience="aud",
        token_type="access",
        ttl=timedelta(minutes=5),
    )
    with pytest.raises(SecurityError) as exc:
        decode_jwt(token_with_type, secret, issuer="iss", audience="aud", token_type="refresh")
    assert "Unexpected token type" in str(exc.value)


def test_decode_jwt_missing_and_expired() -> None:
    secret = "x" * 48
    # Expired token
    expired_token = create_jwt(
        subject="1",
        secret=secret,
        issuer="iss",
        audience="aud",
        token_type="access",
        ttl=timedelta(seconds=-15),
    )
    with pytest.raises(SecurityError) as exc:
        decode_jwt(expired_token, secret, issuer="iss", audience="aud")
    assert "Invalid JWT" in str(exc.value)
