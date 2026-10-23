import json

import pytest

from auraflow.worker.gpu_manager import GPUMemoryManager, GPUOutOfMemoryBudgetError
from auraflow.worker.gpu_runner import GPUInferenceRunner


def test_gpu_memory_snapshot_telemetry():
    manager = GPUMemoryManager()
    snapshot = manager.get_memory_snapshot()

    assert snapshot.total_bytes > 0
    assert snapshot.free_bytes >= 0
    assert 0.0 <= snapshot.utilization_pct <= 100.0
    assert snapshot.device_name is not None


def test_gpu_budget_rejection_on_excessive_request():
    manager = GPUMemoryManager()
    # Request an impossible 500GB VRAM to trigger allocation guard
    excessive_bytes = 500 * 1024 * 1024 * 1024

    with pytest.raises(GPUOutOfMemoryBudgetError) as exc_info:
        manager.reserve_budget(excessive_bytes)

    assert "exceeds available headroom" in str(exc_info.value)


@pytest.mark.asyncio
async def test_gpu_inference_runner_streaming():
    runner = GPUInferenceRunner()
    stdout_chunks = []
    exit_meta = {}

    async for stream_type, chunk in runner.execute_inference_stream(
        model_name="qwen2.5-coder-3b",
        prompt="Explain Kahn's algorithm",
        estimated_vram_mb=64,  # Small requirement passes cleanly
    ):
        if stream_type == "stdout":
            stdout_chunks.append(chunk)
        elif stream_type == "exit":
            exit_meta = json.loads(chunk)

    full_output = "".join(stdout_chunks)
    assert "Hardware target" in full_output
    assert "token: AuraFlow" in full_output
    assert exit_meta["exit_code"] == 0
    assert exit_meta["output"]["model"] == "qwen2.5-coder-3b"
    assert "AuraFlow" in exit_meta["output"]["output_text"]
