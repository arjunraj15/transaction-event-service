# Annotated attaches validation metadata to parameters.
from typing import Annotated

# Router, errors, query parameter helper and status codes.
from fastapi import APIRouter, HTTPException, Query, status

# Dependencies for the reader and settings.
from txn_service.api.dependencies import ReaderDep, SettingsDep

# Response models.
from txn_service.api.schemas import ServiceInfo, TransactionOut

# Dependency enforcing the admin key.
from txn_service.api.security import AdminDep

# Enum of valid transaction statuses.
from txn_service.db.models import TransactionStatus

# All routes here live under /v1.
router = APIRouter(prefix="/v1", tags=["transactions"])


# GET /v1/transactions, admin-only, returns a list.
@router.get("/transactions", response_model=list[TransactionOut], dependencies=[AdminDep])
# Handler for the listing endpoint.
async def list_transactions(
    # Injected transaction reader.
    reader: ReaderDep,
    # Page size, validated to 1..200, default 50.
    limit: Annotated[int, Query(ge=1, le=200)] = 50,
    # Rows to skip, for paging.
    offset: Annotated[int, Query(ge=0)] = 0,
    # Optional status filter (repeatable: ?status=PROCESSED&status=FAILED).
    status_filter: Annotated[list[TransactionStatus] | None, Query(alias="status")] = None,
    # Returns a list of response models.
) -> list[TransactionOut]:
    """Admin (dashboard): one page of transactions, newest first, optionally filtered by status."""
    # Query one page, passing the status values if given.
    rows = await reader.list_recent(limit, [s.value for s in status_filter] if status_filter else None, offset)
    # Convert each ORM row to the response model.
    return [TransactionOut.model_validate(row) for row in rows]


# GET /v1/transactions/stats, admin-only; declared before the {id} route so it isn't shadowed.
@router.get("/transactions/stats", response_model=dict[str, int], dependencies=[AdminDep])
async def transaction_stats(reader: ReaderDep) -> dict[str, int]:
    """Admin (dashboard): total transactions per status over the whole table."""
    return await reader.count_by_status()


# GET /v1/transactions/{id}; public, returns a one-element list.
@router.get("/transactions/{transaction_id}", response_model=list[TransactionOut])
# Handler takes the path id and the reader.
async def get_transaction(transaction_id: str, reader: ReaderDep) -> list[TransactionOut]:
    """Returns a single-element array, matching the published response contract."""
    # Look the transaction up.
    txn = await reader.get(transaction_id)
    # Nothing found...
    if txn is None:
        # ...respond 404.
        raise HTTPException(status.HTTP_404_NOT_FOUND, detail="Transaction not found")
    # Wrap the single result in a list.
    return [TransactionOut.model_validate(txn)]


# GET /v1/meta: runtime info for the dashboard.
@router.get("/meta", response_model=ServiceInfo, tags=["health"])
# Handler gets the injected settings.
async def service_info(settings: SettingsDep) -> ServiceInfo:
    """Non-secret runtime settings the dashboard needs (e.g. to draw the processing countdown)."""
    # Build the response from non-secret settings only.
    return ServiceInfo(
        # Environment name.
        environment=settings.environment,
        # Delay the dashboard uses for its countdown.
        processing_delay_seconds=settings.processing_delay_seconds,
        # Report only whether a secret is set, never the value.
        signature_required=settings.webhook_secret is not None,
        # Likewise for the admin key.
        admin_key_required=settings.admin_api_key is not None,
    )
