import pytest
from httpx import ASGITransport, AsyncClient
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from auraflow.core.database import Base, get_db_session
from auraflow.main import app
from auraflow.models.execution import NodeStatus, WorkflowStatus


@pytest.fixture
async def api_client():
    engine = create_async_engine("sqlite+aiosqlite:///:memory:", echo=False)
    session_factory = async_sessionmaker(engine, expire_on_commit=False, class_=AsyncSession)

    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

    async def override_get_db():
        async with session_factory() as session:
            yield session

    app.dependency_overrides[get_db_session] = override_get_db

    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://testserver") as client:
        yield client

    app.dependency_overrides.clear()
    await engine.dispose()


@pytest.mark.asyncio
async def test_submit_valid_workflow_and_query_status(api_client: AsyncClient):
    payload = {
        "workflow_id": "wf_image_pipeline",
        "description": "Download image and run inference",
        "tasks": [
            {
                "id": "fetch_img",
                "type": "io_fetch",
                "depends_on": [],
                "params": {"url": "https://example.com/asset.png"},
            },
            {
                "id": "classify_img",
                "type": "gpu_inference",
                "depends_on": ["fetch_img"],
                "params": {"model": "clip-vit"},
            },
        ],
    }

    # 1. Submit workflow
    response = await api_client.post("/api/v1/workflows", json=payload)
    assert response.status_code == 201
    data = response.json()
    run_id = data["run_id"]
    assert data["workflow_id"] == "wf_image_pipeline"
    assert data["status"] == WorkflowStatus.RUNNING.value

    # 2. Query status
    status_response = await api_client.get(f"/api/v1/workflows/{run_id}")
    assert status_response.status_code == 200
    status_data = status_response.json()

    assert len(status_data["nodes"]) == 2
    nodes_by_id = {node["node_id"]: node for node in status_data["nodes"]}

    # Root task with no dependencies must be READY immediately
    assert nodes_by_id["fetch_img"]["status"] == NodeStatus.READY.value
    # Dependent task must remain PENDING
    assert nodes_by_id["classify_img"]["status"] == NodeStatus.PENDING.value


@pytest.mark.asyncio
async def test_rejects_cyclic_workflow_submission(api_client: AsyncClient):
    cyclic_payload = {
        "workflow_id": "wf_bad",
        "tasks": [
            {"id": "a", "type": "sandbox_exec", "depends_on": ["b"]},
            {"id": "b", "type": "sandbox_exec", "depends_on": ["a"]},
        ],
    }
    response = await api_client.post("/api/v1/workflows", json=cyclic_payload)
    assert response.status_code == 422
    assert "Cycle detected" in response.json()["detail"]
