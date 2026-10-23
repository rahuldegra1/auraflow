import grpc
import pytest
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from auraflow.core.database import Base
from auraflow.core.engine import WorkflowEngine
from auraflow.core.runner import WorkflowExecutionRunner
from auraflow.models.execution import NodeStatus, WorkflowStatus
from auraflow.proto import worker_pb2_grpc
from auraflow.schemas.workflow import TaskNode, TaskType, WorkflowDefinition
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
async def live_worker_stub():
    server = grpc.aio.server()
    servicer = AuraFlowWorkerServicer(worker_id="e2e-worker")
    await servicer.start()
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
        await servicer.stop()


@pytest.mark.asyncio
async def test_heterogeneous_dag_execution_pipeline(
    async_test_session: AsyncSession, live_worker_stub
):
    """
    Validates end-to-end execution of a heterogeneous pipeline:
    Node A (sandbox_exec) -> produces clean_metric
    Node B (gpu_inference) -> uses clean_metric as prompt via interpolation
    """
    definition = WorkflowDefinition(
        workflow_id="e2e_heterogeneous_pipeline",
        tasks=[
            TaskNode(
                id="preprocess_task",
                type=TaskType.SANDBOX_EXEC,
                params={
                    "code": (
                        "data_in = PARAMS.get('raw', [10, 20, 30])\n"
                        "avg = sum(data_in) / len(data_in)\n"
                        "OUTPUT['clean_metric'] = f'Computed average is {avg}'\n"
                    ),
                    "params": {"raw": [10, 20, 30]},
                },
                timeout_seconds=5,
            ),
            TaskNode(
                id="inference_task",
                type=TaskType.GPU_INFERENCE,
                depends_on=["preprocess_task"],
                params={
                    "prompt": "Evaluate metric: {{preprocess_task.clean_metric}}",
                },
                timeout_seconds=5,
            ),
        ],
    )

    engine = WorkflowEngine(async_test_session)
    run = await engine.submit_workflow(definition)

    runner = WorkflowExecutionRunner(async_test_session, live_worker_stub)
    final_run = await runner.run_until_completion(run.id)

    # 1. Workflow must finish with COMPLETED
    assert final_run.status == WorkflowStatus.COMPLETED.value
    assert final_run.completed_at is not None

    # 2. Check nodes
    nodes = {n.node_id: n for n in final_run.nodes}
    assert nodes["preprocess_task"].status == NodeStatus.COMPLETED.value
    assert nodes["preprocess_task"].result_output["clean_metric"] == "Computed average is 20.0"

    assert nodes["inference_task"].status == NodeStatus.COMPLETED.value
    # Prompt must have been interpolated with Stage 1 result
    assert "Computed average is 20.0" in nodes["inference_task"].params["prompt"]
    assert "batch of 1" in nodes["inference_task"].result_output["generated_text"]
