from datetime import datetime, timedelta, timezone

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
async def async_session():
    engine = create_async_engine("sqlite+aiosqlite:///:memory:", echo=False)
    session_factory = async_sessionmaker(engine, expire_on_commit=False, class_=AsyncSession)

    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

    async with session_factory() as session:
        yield session

    await engine.dispose()


@pytest.mark.asyncio
async def test_recovers_stale_worker_tasks(async_session: AsyncSession):
    run = WorkflowRun(
        workflow_id="wf_stale",
        status=WorkflowStatus.RUNNING.value,
        definition_snapshot={"tasks": []},
    )
    async_session.add(run)
    await async_session.commit()

    # Simulate a node locked 60 seconds ago by a worker that died
    past_timestamp = datetime.now(timezone.utc) - timedelta(seconds=60)
    node = NodeExecution(
        workflow_run_id=run.id,
        node_id="hung_task",
        task_type="sandbox_exec",
        status=NodeStatus.RUNNING.value,
        locked_by_worker="worker-dead",
        locked_at=past_timestamp,
        retry_count=0,
        max_retries=2,
    )
    async_session.add(node)
    await async_session.commit()

    scheduler = WorkflowScheduler(async_session)

    # Run recovery with a 30s timeout threshold
    recovered = await scheduler.recover_stale_tasks(heartbeat_timeout_seconds=30)
    assert "hung_task" in recovered

    await async_session.refresh(node)
    assert node.status == NodeStatus.READY.value
    assert node.retry_count == 1
    assert node.locked_by_worker is None


@pytest.mark.asyncio
async def test_exhausted_retries_fail_workflow(async_session: AsyncSession):
    run = WorkflowRun(
        workflow_id="wf_fail",
        status=WorkflowStatus.RUNNING.value,
        definition_snapshot={"tasks": []},
    )
    async_session.add(run)
    await async_session.commit()

    node = NodeExecution(
        workflow_run_id=run.id,
        node_id="failing_task",
        task_type="sandbox_exec",
        status=NodeStatus.RUNNING.value,
        retry_count=1,
        max_retries=1,  # Max 1 retry allowed
    )
    async_session.add(node)
    await async_session.commit()

    scheduler = WorkflowScheduler(async_session)

    # Trigger failure beyond retry threshold
    updated_node = await scheduler.handle_node_failure(
        node_id="failing_task",
        workflow_run_id=run.id,
        error_message="OOM crash simulation",
    )

    assert updated_node.status == NodeStatus.FAILED.value
    assert updated_node.retry_count == 2

    await async_session.refresh(run)
    assert run.status == WorkflowStatus.FAILED.value
