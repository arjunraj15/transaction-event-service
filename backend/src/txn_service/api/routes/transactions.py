from typing import Annotated

from fastapi import APIRouter, HTTPException, Query, status

from txn_service.api.dependencies import ReaderDep, SettingsDep
from txn_service.api.schemas import ServiceInfo, TransactionOut
from txn_service.api.security import AdminDep
from txn_service.db.models import TransactionStatus

router = APIRouter(prefix="/v1", tags=["transactions"])


@router.get("/transactions", response_model=list[TransactionOut], dependencies=[AdminDep])
async def list_transactions(
    reader: ReaderDep,
    limit: Annotated[int, Query(ge=1, le=200)] = 50,
    status_filter: Annotated[TransactionStatus | None, Query(alias="status")] = None,
) -> list[TransactionOut]:
    """Admin (dashboard): most recent transactions first, optionally filtered by status."""
    rows = await reader.list_recent(limit, status_filter.value if status_filter else None)
    return [TransactionOut.model_validate(row) for row in rows]


@router.get("/transactions/{transaction_id}", response_model=list[TransactionOut])
async def get_transaction(transaction_id: str, reader: ReaderDep) -> list[TransactionOut]:
    """Returns a single-element array, matching the published response contract."""
    txn = await reader.get(transaction_id)
    if txn is None:
        raise HTTPException(status.HTTP_404_NOT_FOUND, detail="Transaction not found")
    return [TransactionOut.model_validate(txn)]


@router.get("/meta", response_model=ServiceInfo, tags=["health"])
async def service_info(settings: SettingsDep) -> ServiceInfo:
    """Non-secret runtime settings the dashboard needs (e.g. to draw the processing countdown)."""
    return ServiceInfo(
        environment=settings.environment,
        processing_delay_seconds=settings.processing_delay_seconds,
        signature_required=settings.webhook_secret is not None,
        admin_key_required=settings.admin_api_key is not None,
    )
