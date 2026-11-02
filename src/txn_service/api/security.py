import hashlib
import hmac


def compute_signature(secret: str, body: bytes) -> str:
    """RazorPay-style signature: hex HMAC-SHA256 of the raw request body."""
    return hmac.new(secret.encode(), body, hashlib.sha256).hexdigest()


def verify_signature(secret: str, body: bytes, signature: str | None) -> bool:
    if not signature:
        return False
    return hmac.compare_digest(compute_signature(secret, body), signature.strip())
