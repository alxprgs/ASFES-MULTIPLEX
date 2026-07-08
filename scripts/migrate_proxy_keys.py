import asyncio
import hashlib
import sys
import base64
from pathlib import Path

# Add project root to Python path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from motor.motor_asyncio import AsyncIOMotorClient
from cryptography.hazmat.primitives.ciphers.aead import AESGCM

from server.core.config import get_settings
from server.core.crypto import ProxyEncryptor
from server.core.database import PROXIES


class OldProxyEncryptor:
    """Legacy encryption algorithm using simple SHA-256 (pre-HKDF)."""

    def __init__(self, app_secret: str) -> None:
        self.key = hashlib.sha256(app_secret.encode("utf-8")).digest()
        self.aesgcm = AESGCM(self.key)

    def decrypt(self, ciphertext_b64: str) -> str:
        data = base64.b64decode(ciphertext_b64.encode("utf-8"))
        if len(data) < 12:
            raise ValueError("Invalid encrypted data length")
        iv = data[:12]
        ciphertext = data[12:]
        return self.aesgcm.decrypt(iv, ciphertext, None).decode("utf-8")


async def migrate_proxy_keys():
    settings = get_settings()

    # 1. Check if the proxy_encryption_key has been set.
    proxy_key = settings.security.proxy_encryption_key.get_secret_value()
    api_secret = settings.security.api_jwt_secret.get_secret_value()

    if proxy_key == "ChangeThisProxyEncryptionKey":
        print(
            "ERROR: SECURITY__PROXY_ENCRYPTION_KEY is still set to the default value."
        )
        print(
            "Please update your .env file with a secure key before running this migration."
        )
        sys.exit(1)

    print(f"Connecting to MongoDB at {settings.mongo.uri}...")
    client = AsyncIOMotorClient(
        str(settings.mongo.uri),
        connectTimeoutMS=settings.mongo.connect_timeout_ms,
        serverSelectionTimeoutMS=settings.mongo.server_selection_timeout_ms,
        maxPoolSize=settings.mongo.max_pool_size,
    )
    db = client[settings.mongo.database]

    old_encryptor = OldProxyEncryptor(api_secret)
    new_encryptor = ProxyEncryptor(proxy_key)

    collection = db[PROXIES]
    total_proxies = await collection.count_documents({})
    print(f"Found {total_proxies} proxies in the database.")

    migrated_count = 0
    error_count = 0
    skip_count = 0

    async for proxy in collection.find({}):
        proxy_id = proxy["_id"]
        encrypted_pass = proxy.get("password_encrypted")

        if not encrypted_pass:
            skip_count += 1
            continue

        try:
            # Try to decrypt with new encryptor first (maybe it's already migrated)
            new_encryptor.decrypt(encrypted_pass)
            skip_count += 1
            continue
        except Exception:
            pass  # Needs migration

        try:
            # Decrypt with old algorithm
            plain_pass = old_encryptor.decrypt(encrypted_pass)

            # Encrypt with new HKDF-based algorithm
            new_encrypted = new_encryptor.encrypt(plain_pass)

            await collection.update_one(
                {"_id": proxy_id}, {"$set": {"password_encrypted": new_encrypted}}
            )
            migrated_count += 1
        except Exception as e:
            print(f"ERROR: Failed to migrate proxy {proxy_id}: {e}")
            error_count += 1

    print("\nMigration Summary:")
    print(f"- Total proxies: {total_proxies}")
    print(f"- Migrated: {migrated_count}")
    print(f"- Skipped (No password or already migrated): {skip_count}")
    print(f"- Errors: {error_count}")

    if error_count > 0:
        print("\nWARNING: Some proxies failed to migrate. Check logs.")
        sys.exit(1)
    else:
        print("\nMigration completed successfully.")


if __name__ == "__main__":
    asyncio.run(migrate_proxy_keys())
