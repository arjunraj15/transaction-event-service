from fastapi import APIRouter, HTTPException, status

from txn_service.api.dependencies import ReaderDep
from txn_service.api.schemas import TransactionOut

router = APIRouter(prefix="/v1/transactions", tags=["transactions"])


@router.get("/{transaction_id}", response_model=list[TransactionOut])
async def get_transaction(transaction_id: str, reader: ReaderDep) -> list[TransactionOut]:
    """Returns a single-element array, matching the published response contract."""
    txn = await reader.get(transaction_id)
    if txn is None:
        raise HTTPException(status.HTTP_404_NOT_FOUND, detail="Transaction not found")
    return [TransactionOut.model_validate(txn)]
