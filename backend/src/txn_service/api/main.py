"""ASGI entrypoint: `uvicorn txn_service.api.main:app`."""

from txn_service.api.app import create_app

app = create_app()
