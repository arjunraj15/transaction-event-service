# Hash algorithms for the HMAC.
import hashlib

# HMAC signing and constant-time comparison.
import hmac

# Annotated carries the Header metadata on a parameter.
from typing import Annotated

# FastAPI pieces for dependencies, headers, errors and status codes.
from fastapi import Depends, Header, HTTPException, Request, status


# Compute the expected signature for a body.
def compute_signature(secret: str, body: bytes) -> str:
    """RazorPay-style signature: hex HMAC-SHA256 of the raw request body."""
    # HMAC-SHA256 over the raw bytes with the shared secret, as a hex string.
    return hmac.new(secret.encode(), body, hashlib.sha256).hexdigest()


# Check a received signature against the body.
def verify_signature(secret: str, body: bytes, signature: str | None) -> bool:
    # A missing or empty signature is never valid.
    if not signature:
        # Reject immediately.
        return False
    # Constant-time comparison avoids leaking the signature through timing.
    return hmac.compare_digest(compute_signature(secret, body), signature.strip())


# Name of the header carrying the admin key.
ADMIN_KEY_HEADER = "X-Admin-Key"


# Admin-key dependency; the key comes from the X-Admin-Key header (None if absent).
def require_admin_key(
    request: Request, x_admin_key: Annotated[str | None, Header(alias=ADMIN_KEY_HEADER)] = None
) -> None:
    """Guards the dashboard's admin endpoints. Open only when no key is configured (local dev)."""
    # The key configured on the server, or None if auth is disabled.
    configured = request.app.state.settings.admin_api_key
    # No key configured: allow the request.
    if configured is None:
        # Skip the check.
        return
    # Reject if the header is missing or does not match (constant-time compare).
    if not x_admin_key or not hmac.compare_digest(x_admin_key.strip(), configured.get_secret_value()):
        # Respond with 401 Unauthorized.
        raise HTTPException(
            # Use the standard status constant.
            status.HTTP_401_UNAUTHORIZED,
            # Explain the failure to the client.
            detail="Missing or invalid admin key",
            # Tell clients which header carries the credential.
            headers={"WWW-Authenticate": ADMIN_KEY_HEADER},
        )


# Reusable dependency object that routes attach via `dependencies=[AdminDep]`.
AdminDep = Depends(require_admin_key)
