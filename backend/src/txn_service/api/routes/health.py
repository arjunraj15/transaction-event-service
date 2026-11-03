# UTC-aware timestamps.
from datetime import UTC, datetime

# Router, response object and status codes.
from fastapi import APIRouter, Response, status

# Prometheus text exposition helpers.
from prometheus_client import CONTENT_TYPE_LATEST, generate_latest

# Dependencies for the publisher and reader.
from txn_service.api.dependencies import PublisherDep, ReaderDep

# Response models for the endpoints.
from txn_service.api.schemas import HealthResponse, ReadinessResponse

# Router for health endpoints, grouped under the 'health' tag.
router = APIRouter(tags=["health"])


# GET / is the liveness probe.
@router.get("/", response_model=HealthResponse)
# No dependencies, so it stays cheap and never fails on outages.
async def liveness() -> HealthResponse:
    """Liveness: the process is up. Deliberately does not touch dependencies."""
    # Return the current UTC time in ISO-8601 form.
    return HealthResponse(current_time=datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%SZ"))


# GET /health/ready is the readiness probe.
@router.get("/health/ready", response_model=ReadinessResponse)
# Handler takes the response (to set status) plus both dependencies.
async def readiness(response: Response, publisher: PublisherDep, reader: ReaderDep) -> ReadinessResponse:
    # Check each dependency; both must respond.
    checks = {"database": await reader.ping(), "kafka": await publisher.is_ready()}
    # Ready only if every check passed.
    ready = all(checks.values())
    # If not ready...
    if not ready:
        # ...respond 503 so load balancers stop routing here.
        response.status_code = status.HTTP_503_SERVICE_UNAVAILABLE
    # Report the verdict and the individual checks.
    return ReadinessResponse(status="READY" if ready else "NOT_READY", checks=checks)


# GET /metrics for Prometheus; hidden from the OpenAPI docs.
@router.get("/metrics", include_in_schema=False)
# Handler returns a raw response.
async def metrics() -> Response:
    # Serialize all metrics in the Prometheus text format.
    return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)
