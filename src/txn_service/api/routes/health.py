from datetime import UTC, datetime

from fastapi import APIRouter, Response, status
from prometheus_client import CONTENT_TYPE_LATEST, generate_latest

from txn_service.api.dependencies import PublisherDep, ReaderDep
from txn_service.api.schemas import HealthResponse, ReadinessResponse

router = APIRouter(tags=["health"])


@router.get("/", response_model=HealthResponse)
async def liveness() -> HealthResponse:
    """Liveness: the process is up. Deliberately does not touch dependencies."""
    return HealthResponse(current_time=datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%SZ"))


@router.get("/health/ready", response_model=ReadinessResponse)
async def readiness(response: Response, publisher: PublisherDep, reader: ReaderDep) -> ReadinessResponse:
    checks = {"database": await reader.ping(), "kafka": await publisher.is_ready()}
    ready = all(checks.values())
    if not ready:
        response.status_code = status.HTTP_503_SERVICE_UNAVAILABLE
    return ReadinessResponse(status="READY" if ready else "NOT_READY", checks=checks)


@router.get("/metrics", include_in_schema=False)
async def metrics() -> Response:
    return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)
