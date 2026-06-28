from __future__ import annotations

import base64
import hashlib
import os
from cryptography.hazmat.primitives.ciphers.aead import AESGCM


class ProxyEncryptor:
    def __init__(self, app_secret: str) -> None:
        # Stretch the secret key to exactly 32 bytes using SHA-256
        self.key = hashlib.sha256(app_secret.encode("utf-8")).digest()
        self.aesgcm = AESGCM(self.key)

    def encrypt(self, plaintext: str | None) -> str | None:
        if plaintext is None:
            return None
        iv = os.urandom(12)
        ciphertext = self.aesgcm.encrypt(iv, plaintext.encode("utf-8"), None)
        return base64.b64encode(iv + ciphertext).decode("utf-8")

    def decrypt(self, ciphertext_b64: str | None) -> str | None:
        if ciphertext_b64 is None:
            return None
        try:
            data = base64.b64decode(ciphertext_b64.encode("utf-8"))
            if len(data) < 12:
                raise ValueError("Invalid encrypted data length")
            iv = data[:12]
            ciphertext = data[12:]
            return self.aesgcm.decrypt(iv, ciphertext, None).decode("utf-8")
        except Exception as exc:
            raise ValueError(f"Decryption failed: {exc}") from exc
