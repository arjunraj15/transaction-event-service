"""ASGI entrypoint: `uvicorn txn_service.api.main:app`."""

# Import the app factory.
from txn_service.api.app import create_app

# Build the app at import time so uvicorn can find `app`.
app = create_app()
