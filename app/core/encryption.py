"""
Fernet-based encryption for sensitive broker tokens.

Usage:
    from app.core.encryption import encrypt_token, decrypt_token

    encrypted = encrypt_token("my_access_token")
    original  = decrypt_token(encrypted)

The ENCRYPTION_KEY in .env must be a valid Fernet key (44 URL-safe base64 chars).
Generate one with:
    python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"
"""
from __future__ import annotations
import logging
from cryptography.fernet import Fernet, InvalidToken
from app.core.config import settings

logger = logging.getLogger(__name__)

_fernet: Fernet | None = None


def _get_fernet() -> Fernet:
    """Lazily initialise Fernet cipher from ENCRYPTION_KEY setting."""
    global _fernet
    if _fernet is None:
        key = settings.ENCRYPTION_KEY
        if not key:
            raise RuntimeError(
                "ENCRYPTION_KEY is not set in .env. "
                "Generate one with: python -c \"from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())\""
            )
        _fernet = Fernet(key.encode())
    return _fernet


def encrypt_token(plaintext: str) -> str:
    """Encrypt a plaintext token string. Returns base64-encoded ciphertext."""
    return _get_fernet().encrypt(plaintext.encode()).decode()


def decrypt_token(ciphertext: str) -> str:
    """Decrypt a ciphertext string back to the original token.

    Returns empty string if decryption fails (corrupted data, wrong key, etc.).
    """
    try:
        return _get_fernet().decrypt(ciphertext.encode()).decode()
    except (InvalidToken, Exception) as e:
        logger.error("[Encryption] Failed to decrypt token: %s", e)
        return ""
