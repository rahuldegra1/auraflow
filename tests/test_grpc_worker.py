import json

import grpc
import pytest

from auraflow.proto import worker_pb2, worker_pb2_grpc
from auraflow.worker.service import AuraFlowWorkerServicer


@pytest.fixture
async def grpc_server_and_client():
    server = grpc.aio.server()
    servicer = AuraFlowWorkerServicer(worker_id="test-worker-01")
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
async def test_worker_healthcheck(grpc_server_and_client):
    client = grpc_server_and_client
    response = await client.HealthCheck(worker_pb2.PingRequest(worker_id="probe"))
    assert response.healthy is True
    assert response.active_tasks == 0


@pytest.mark.asyncio
async def test_worker_streaming_task_execution(grpc_server_and_client):
    client = grpc_server_and_client
    assignment = worker_pb2.TaskAssignment(
        workflow_run_id="run-test-100",
        node_id="node-extract",
        task_type="sandbox_exec",
        timeout_seconds=5,
        payload_json=json.dumps(
            {
                "code": "OUTPUT['status'] = 'processed_ok'\nprint('Execution completed.')",
                "params": {"input_file": "raw.csv"},
            }
        ),
    )

    updates = []
    async for update in client.ExecuteTask(assignment):
        updates.append(update)

    # Must contain starting, running, and completed stages
    stages = [u.stage for u in updates]
    assert worker_pb2.STAGE_STARTING in stages
    assert worker_pb2.STAGE_RUNNING in stages
    assert worker_pb2.STAGE_COMPLETED in stages

    final_update = updates[-1]
    assert final_update.stage == worker_pb2.STAGE_COMPLETED
    assert final_update.exit_code == 0
    assert "processed_ok" in final_update.result_json
