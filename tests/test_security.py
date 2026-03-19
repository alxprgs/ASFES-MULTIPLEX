from __future__ import annotations

from datetime import timedelta

from server.core.security import build_pkce_challenge, create_jwt, decode_jwt, hash_password, verify_password, verify_pkce


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
    token = create_jwt(
        subject="user_1",
        secret="secret",
        issuer="https://multiplex.asfes.ru/api/oauth",
        audience="multiplex-api",
        token_type="api_access",
        ttl=timedelta(minutes=5),
        extra={"username": "root"},
    )
    payload = decode_jwt(
        token,
        "secret",
        issuer="https://multiplex.asfes.ru/api/oauth",
        audience="multiplex-api",
        token_type="api_access",
    )
    assert payload["sub"] == "user_1"
    assert payload["username"] == "root"
