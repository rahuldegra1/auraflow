import pytest
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from auraflow.core.database import Base
from auraflow.models.execution import (
    NodeExecution,
    NodeStatus,
    WorkflowRun,
    WorkflowStatus,
)


@pytest.fixture
async def async_test_session():
    engine = create_async_engine("sqlite+aiosqlite:///:memory:", echo=False)
    async_session = async_sessionmaker(engine, expire_on_commit=False, class_=AsyncSession)

    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

    async with async_session() as session:
        yield session

    await engine.dispose()


@pytest.mark.asyncio
async def test_workflow_and_node_lifecycle(async_test_session: AsyncSession):
    # 1. Persist workflow execution record
    run = WorkflowRun(
        workflow_id="wf_data_ingest",
        status=WorkflowStatus.PENDING.value,
        definition_snapshot={"tasks": []},
    )
    async_test_session.add(run)
    await async_test_session.commit()
    await async_test_session.refresh(run)

    assert run.id is not None
    assert run.status == WorkflowStatus.PENDING.value

    # 2. Add dependent execution node in READY state
    node = NodeExecution(
        workflow_run_id=run.id,
        node_id="parse_csv_task",
        task_type="sandbox_exec",
        status=NodeStatus.READY.value,
        params={"file_name": "data.csv"},
    )
    async_test_session.add(node)
    await async_test_session.commit()
    await async_test_session.refresh(node)

    assert node.id is not None
    assert node.workflow_run_id == run.id
    assert node.status == NodeStatus.READY.value
