import asyncio
import json

import grpc
import pytest

from auraflow.proto import worker_pb2, worker_pb2_grpc
from auraflow.worker.batcher import DynamicBatchScheduler
from auraflow.worker.service import AuraFlowWorkerServicer


@pytest.mark.asyncio
async def test_dynamic_batcher_groups_concurrent_requests():
    batcher = DynamicBatchScheduler(max_batch_size=3, batch_timeout_ms=50.0)
    await batcher.start()

    try:
        # Submit 3 concurrent tasks simultaneously
        tasks = [
            batcher.submit_prompt(f"req_{i}", f"Task prompt {i}")
            for i in range(3)
        ]
        results = await asyncio.gather(*tasks)

        assert len(results) == 3
        # Verify all 3 requests were merged into a single batch of size 3
        for res in results:
            assert "batch of 3" in res.generated_text
            assert res.latency_ms > 0
    finally:
        await batcher.stop()


@pytest.mark.asyncio
async def test_dynamic_batcher_dispatches_on_timeout():
    # Max batch is 10, but only 1 request is sent: it must dispatch when timeout fires
    batcher = DynamicBatchScheduler(max_batch_size=10, batch_timeout_ms=30.0)
    await batcher.start()

    try:
        res = await batcher.submit_prompt("req_single", "Lone prompt")
        assert "batch of 1" in res.generated_text
    finally:
        await batcher.stop()


@pytest.fixture
async def grpc_gpu_client():
    server = grpc.aio.server()
    servicer = AuraFlowWorkerServicer(worker_id="test-gpu-worker")
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
async def test_grpc_worker_executes_gpu_inference(grpc_gpu_client):
    client = grpc_gpu_client
    assignment = worker_pb2.TaskAssignment(
        workflow_run_id="run-gpu-001",
        node_id="node-llm-batch",
        task_type="gpu_inference",
        timeout_seconds=5,
        payload_json=json.dumps({"prompt": "Summarize batching"}),
    )

    updates = []
    async for update in client.ExecuteTask(assignment):
        updates.append(update)

    assert len(updates) >= 2
    stages = [u.stage for u in updates]
    assert worker_pb2.STAGE_STARTING in stages
    assert worker_pb2.STAGE_COMPLETED in stages

    final_update = updates[-1]
    assert final_update.exit_code == 0
    assert "batch of 1" in final_update.result_json
