from fastapi import FastAPI, Response

from auraflow.api.routes import router as workflow_router
from auraflow.core.metrics import get_prometheus_metrics

app = FastAPI(
    title="AuraFlow Control Plane",
    description="Distributed Fault-Tolerant DAG Workflow Engine",
    version="1.0.0",
)

app.include_router(workflow_router)


@app.get("/health", tags=["System"])
async def health_check():
    return {"status": "healthy", "service": "auraflow-orchestrator"}


@app.get("/metrics", tags=["System"])
async def metrics_endpoint():
    data, content_type = get_prometheus_metrics()
    return Response(content=data, media_type=content_type)
