import pytest
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from auraflow.core.database import Base
from auraflow.core.scheduler import WorkflowScheduler
from auraflow.models.execution import (
    NodeExecution,
    NodeStatus,
    WorkflowRun,
    WorkflowStatus,
)


@pytest.fixture
async def async_test_session():
    engine = create_async_engine("sqlite+aiosqlite:///:memory:", echo=False)
    session_factory = async_sessionmaker(engine, expire_on_commit=False, class_=AsyncSession)

    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

    async with session_factory() as session:
        yield session

    await engine.dispose()


@pytest.mark.asyncio
async def test_scheduler_dependency_advancement(async_test_session: AsyncSession):
    # Definition: task_b depends on task_a
    snapshot = {
        "tasks": [
            {"id": "task_a", "type": "io_fetch", "depends_on": []},
            {"id": "task_b", "type": "sandbox_exec", "depends_on": ["task_a"]},
        ]
    }
    run = WorkflowRun(
        workflow_id="wf_linear",
        status=WorkflowStatus.PENDING.value,
        definition_snapshot=snapshot,
    )
    async_test_session.add(run)
    await async_test_session.commit()

    node_a = NodeExecution(
        workflow_run_id=run.id,
        node_id="task_a",
        task_type="io_fetch",
        status=NodeStatus.PENDING.value,
    )
    node_b = NodeExecution(
        workflow_run_id=run.id,
        node_id="task_b",
        task_type="sandbox_exec",
        status=NodeStatus.BLOCKED.value,
    )
    async_test_session.add_all([node_a, node_b])
    await async_test_session.commit()

    scheduler = WorkflowScheduler(async_test_session)

    # 1. First advancement: task_a has no deps, should become READY. task_b stays BLOCKED.
    ready = await scheduler.advance_workflow_state(run.id)
    assert ready == ["task_a"]

    # 2. Worker claims task_a and completes it
    claimed_node = await scheduler.claim_next_ready_node(worker_id="worker-01")
    assert claimed_node is not None
    assert claimed_node.node_id == "task_a"
    assert claimed_node.status == NodeStatus.RUNNING.value

    claimed_node.status = NodeStatus.COMPLETED.value
    await async_test_session.commit()

    # 3. Second advancement: task_a is COMPLETED, so task_b must become READY
    ready_second = await scheduler.advance_workflow_state(run.id)
    assert ready_second == ["task_b"]

    # 4. Another worker claims task_b
    second_claim = await scheduler.claim_next_ready_node(worker_id="worker-02")
    assert second_claim is not None
    assert second_claim.node_id == "task_b"
    assert second_claim.status == NodeStatus.RUNNING.value
