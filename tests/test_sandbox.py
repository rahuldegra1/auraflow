import json

import pytest

from auraflow.worker.sandbox import ContainerSandboxRunner


@pytest.mark.asyncio
async def test_sandbox_executes_valid_code_and_captures_output():
    runner = ContainerSandboxRunner()
    code = (
        "print('Starting processing...')\n"
        "val = PARAMS.get('multiplier', 1) * 10\n"
        "OUTPUT['computed_value'] = val\n"
        "print(f'Computed result: {val}')\n"
    )

    stdout_lines = []
    exit_meta = {}

    async for stream_type, chunk in runner.execute_code_stream(
        code_str=code,
        input_params={"multiplier": 5},
        timeout_seconds=5,
    ):
        if stream_type == "stdout":
            stdout_lines.append(chunk)
        elif stream_type == "exit":
            exit_meta = json.loads(chunk)

    full_stdout = "".join(stdout_lines)
    assert "Starting processing..." in full_stdout
    assert "Computed result: 50" in full_stdout
    assert exit_meta["exit_code"] == 0
    assert exit_meta["output"]["computed_value"] == 50


@pytest.mark.asyncio
async def test_sandbox_kills_infinite_loops_on_timeout():
    runner = ContainerSandboxRunner()
    infinite_loop_code = (
        "import time\n"
        "print('Loop beginning...')\n"
        "while True:\n"
        "    time.sleep(0.1)\n"
    )

    exit_meta = {}
    async for stream_type, chunk in runner.execute_code_stream(
        code_str=infinite_loop_code,
        input_params={},
        timeout_seconds=1,  # Short timeout
    ):
        if stream_type == "exit":
            exit_meta = json.loads(chunk)

    # Exit code 124 represents timeout kill
    assert exit_meta["exit_code"] == 124
