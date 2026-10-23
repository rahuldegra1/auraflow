import grpc
import pytest
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from auraflow.core.database import Base
from auraflow.models.execution import (
    NodeExecution,
    NodeStatus,
    WorkflowRun,
    WorkflowStatus,
)
from auraflow.proto import worker_pb2_grpc
from auraflow.worker.dispatcher import TaskExecutionCoordinator
from auraflow.worker.service import AuraFlowWorkerServicer


@pytest.fixture
async def async_test_session():
    engine = create_async_engine("sqlite+aiosqlite:///:memory:", echo=False)
    session_factory = async_sessionmaker(engine, expire_on_commit=False, class_=AsyncSession)

    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

    async with session_factory() as session:
        yield session

    await engine.dispose()


@pytest.fixture
async def grpc_worker_stub():
    server = grpc.aio.server()
    servicer = AuraFlowWorkerServicer(worker_id="test-dlq-worker")
    worker_pb2_grpc.add_WorkerServiceServicer_to_server(servicer, server)

    port = server.add_insecure_port("127.0.0.1:0")
    await server.start()

    channel = grpc.aio.insecure_channel(f"127.0.0.1:{port}")
    client = worker_pb2_grpc.WorkerServiceStub(channel)

    try:
        yield client
    finally:
        await channel.close()
        await server.stop(grace=0)


@pytest.mark.asyncio
async def test_successful_task_captures_logs_and_output(async_test_session: AsyncSession, grpc_worker_stub):
    run = WorkflowRun(
        workflow_id="wf_log_test",
        status=WorkflowStatus.RUNNING.value,
        definition_snapshot={"tasks": []},
    )
    async_test_session.add(run)
    await async_test_session.commit()

    node = NodeExecution(
        workflow_run_id=run.id,
        node_id="log_task",
        task_type="sandbox_exec",
        status=NodeStatus.RUNNING.value,
        params={
            "code": "print('Processing step 1')\nprint('Processing step 2')\nOUTPUT['res'] = 42",
            "params": {},
        },
        timeout_seconds=5,
    )
    async_test_session.add(node)
    await async_test_session.commit()

    coordinator = TaskExecutionCoordinator(async_test_session, grpc_worker_stub)
    completed_node = await coordinator.execute_claimed_node(node)

    assert completed_node.status == NodeStatus.COMPLETED.value
    assert "Processing step 1" in completed_node.stdout_log
    assert "Processing step 2" in completed_node.stdout_log
    assert completed_node.result_output == {"res": 42}


@pytest.mark.asyncio
async def test_poison_pill_routes_to_dead_letter_queue(async_test_session: AsyncSession, grpc_worker_stub):
    run = WorkflowRun(
        workflow_id="wf_poison_test",
        status=WorkflowStatus.RUNNING.value,
        definition_snapshot={"tasks": []},
    )
    async_test_session.add(run)
    await async_test_session.commit()

    # Node with max_retries = 0: immediate failure must route straight to DEAD_LETTER
    node = NodeExecution(
        workflow_run_id=run.id,
        node_id="bad_syntax_task",
        task_type="sandbox_exec",
        status=NodeStatus.RUNNING.value,
        params={
            "code": "raise RuntimeError('Corrupted data batch')",
            "params": {},
        },
        retry_count=0,
        max_retries=0,
        timeout_seconds=5,
    )
    async_test_session.add(node)
    await async_test_session.commit()

    coordinator = TaskExecutionCoordinator(async_test_session, grpc_worker_stub)
    dead_node = await coordinator.execute_claimed_node(node)

    assert dead_node.status == NodeStatus.DEAD_LETTER.value
    assert dead_node.retry_count == 1
    assert "non-zero code" in dead_node.error_message or "RuntimeError" in dead_node.error_message

    await async_test_session.refresh(run)
    assert run.status == WorkflowStatus.FAILED.value
