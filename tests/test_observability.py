import pytest
from httpx import ASGITransport, AsyncClient

from auraflow.core.metrics import (
    TASK_DURATION_SECONDS,
    TASKS_TOTAL,
    get_prometheus_metrics,
)
from auraflow.main import app


def test_metrics_recorder_increments():
    # Increment counter
    TASKS_TOTAL.labels(task_type="sandbox_exec", status="success").inc()
    TASK_DURATION_SECONDS.labels(task_type="sandbox_exec").observe(0.12)

    raw_metrics, content_type = get_prometheus_metrics()
    metrics_text = raw_metrics.decode("utf-8")

    assert "auraflow_tasks_total" in metrics_text
    assert 'task_type="sandbox_exec"' in metrics_text
    assert "auraflow_task_duration_seconds" in metrics_text


@pytest.mark.asyncio
async def test_metrics_endpoint_http():
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://testserver") as client:
        response = await client.get("/metrics")
        assert response.status_code == 200
        assert "text/plain" in response.headers["content-type"]
        assert "auraflow_tasks_total" in response.text
