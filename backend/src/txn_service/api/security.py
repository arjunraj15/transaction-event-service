import hashlib
import hmac
from typing import Annotated

from fastapi import Depends, Header, HTTPException, Request, status


def compute_signature(secret: str, body: bytes) -> str:
    """RazorPay-style signature: hex HMAC-SHA256 of the raw request body."""
    return hmac.new(secret.encode(), body, hashlib.sha256).hexdigest()


def verify_signature(secret: str, body: bytes, signature: str | None) -> bool:
    if not signature:
        return False
    return hmac.compare_digest(compute_signature(secret, body), signature.strip())


ADMIN_KEY_HEADER = "X-Admin-Key"


def require_admin_key(
    request: Request, x_admin_key: Annotated[str | None, Header(alias=ADMIN_KEY_HEADER)] = None
) -> None:
    """Guards the dashboard's admin endpoints. Open only when no key is configured (local dev)."""
    configured = request.app.state.settings.admin_api_key
    if configured is None:
        return
    if not x_admin_key or not hmac.compare_digest(x_admin_key.strip(), configured.get_secret_value()):
        raise HTTPException(
            status.HTTP_401_UNAUTHORIZED,
            detail="Missing or invalid admin key",
            headers={"WWW-Authenticate": ADMIN_KEY_HEADER},
        )


AdminDep = Depends(require_admin_key)
