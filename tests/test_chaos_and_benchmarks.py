import asyncio

import grpc
import pytest
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine
from sqlalchemy.pool import StaticPool

from auraflow.core.database import Base
from auraflow.core.engine import WorkflowEngine
from auraflow.core.runner import WorkflowExecutionRunner
from auraflow.core.scheduler import WorkflowScheduler
from auraflow.models.execution import NodeStatus, WorkflowStatus
from auraflow.proto import worker_pb2_grpc
from auraflow.schemas.workflow import TaskNode, TaskType, WorkflowDefinition
from auraflow.worker.service import AuraFlowWorkerServicer


@pytest.fixture
async def session_factory():
    engine = create_async_engine(
        "sqlite+aiosqlite:///file:chaos_memdb?mode=memory&cache=shared&uri=true",
        connect_args={"check_same_thread": False, "timeout": 30.0},
        poolclass=StaticPool,
        echo=False,
    )
    factory = async_sessionmaker(engine, expire_on_commit=False, class_=AsyncSession)

    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

    yield factory
    await engine.dispose()


@pytest.fixture
async def ephemeral_grpc_worker():
    server = grpc.aio.server()
    servicer = AuraFlowWorkerServicer(worker_id="chaos-worker")
    await servicer.start()
    worker_pb2_grpc.add_WorkerServiceServicer_to_server(servicer, server)

    port = server.add_insecure_port("127.0.0.1:0")
    await server.start()

    channel = grpc.aio.insecure_channel(f"127.0.0.1:{port}")
    client = worker_pb2_grpc.WorkerServiceStub(channel)

    try:
        yield client, server, servicer
    finally:
        await channel.close()
        await server.stop(grace=0)
        await servicer.stop()


@pytest.mark.asyncio
async def test_concurrent_workflow_saturation(session_factory, ephemeral_grpc_worker):
    """
    Submits multiple workflows concurrently to verify atomic DAG ingestion,
    then executes them sequentially under the shared SQLite lock harness.
    """
    client, _, _ = ephemeral_grpc_worker

    # 1. Concurrent submission: verifies non-colliding writes to database
    async def submit_job(idx: int) -> str:
        async with session_factory() as session:
            engine = WorkflowEngine(session)
            definition = WorkflowDefinition(
                workflow_id=f"wf_stress_{idx}",
                tasks=[
                    TaskNode(
                        id=f"node_a_{idx}",
                        type=TaskType.SANDBOX_EXEC,
                        params={"code": "OUTPUT['res'] = 1"},
                        timeout_seconds=5,
                    ),
                    TaskNode(
                        id=f"node_b_{idx}",
                        type=TaskType.GPU_INFERENCE,
                        depends_on=[f"node_a_{idx}"],
                        params={"prompt": f"Stress test {idx}"},
                        timeout_seconds=5,
                    ),
                ],
            )
            run = await engine.submit_workflow(definition)
            return run.id

    # Concurrent submissions via gather
    run_ids = await asyncio.gather(*(submit_job(i) for i in range(3)))
    assert len(run_ids) == 3

    # 2. Execution processing
    runner = WorkflowExecutionRunner(session_factory, client)
    completed_runs = []
    for rid in run_ids:
        res = await runner.run_until_completion(rid)
        completed_runs.append(res)

    assert len(completed_runs) == 3
    for run in completed_runs:
        assert run.status == WorkflowStatus.COMPLETED.value
        assert len(run.nodes) == 2
        for n in run.nodes:
            assert n.status == NodeStatus.COMPLETED.value


@pytest.mark.asyncio
async def test_chaos_worker_drop_and_recovery(session_factory, ephemeral_grpc_worker):
    """
    Simulates a worker dying mid-flight while a task is RUNNING.
    Verifies that recover_stale_tasks resets orphaned tasks for re-execution.
    """
    client, server, servicer = ephemeral_grpc_worker

    async with session_factory() as session:
        engine = WorkflowEngine(session)
        scheduler = WorkflowScheduler(session)

        definition = WorkflowDefinition(
            workflow_id="wf_drop_test",
            tasks=[
                TaskNode(
                    id="drop_task",
                    type=TaskType.SANDBOX_EXEC,
                    params={"code": "import time; time.sleep(0.5); OUTPUT['status'] = 'ok'"},
                    timeout_seconds=5,
                    max_retries=2,
                )
            ],
        )
        _ = await engine.submit_workflow(definition)

        claimed = await scheduler.claim_next_ready_node(worker_id="dying-worker")
        assert claimed is not None
        assert claimed.status == NodeStatus.RUNNING.value

        # Terminate worker mid-run
        await server.stop(grace=0)
        await servicer.stop()

        # Orchestrator recovery sweep executes
        recovered_ids = await scheduler.recover_stale_tasks(heartbeat_timeout_seconds=0)
        assert "drop_task" in recovered_ids

        await session.refresh(claimed)
        assert claimed.status == NodeStatus.READY.value
        assert claimed.retry_count == 1
        assert claimed.locked_by_worker is None
