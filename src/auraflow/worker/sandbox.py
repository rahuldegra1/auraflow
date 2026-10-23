import asyncio
import json
import shutil
import sys
import tempfile
import time
from collections.abc import AsyncIterator
from dataclasses import dataclass
from pathlib import Path


@dataclass
class SandboxResult:
    exit_code: int
    stdout: str
    stderr: str
    output_data: dict | None = None


class ContainerSandboxRunner:
    def __init__(
        self,
        memory_limit: str = "256m",
        cpu_quota: float = 0.5,
        network_disabled: bool = True,
        base_image: str = "python:3.10-slim",
    ):
        self.memory_limit = memory_limit
        self.cpu_quota = cpu_quota
        self.network_disabled = network_disabled
        self.base_image = base_image

    async def execute_code_stream(
        self,
        code_str: str,
        input_params: dict,
        timeout_seconds: int = 30,
    ) -> AsyncIterator[tuple[str, str]]:
        # Explicit directory creation without auto-deleting 'with' block across yields
        tmp_dir = tempfile.mkdtemp(prefix="auraflow_sandbox_")
        tmp_path = Path(tmp_dir)
        script_path = tmp_path / "run.py"
        input_path = tmp_path / "input.json"
        output_path = tmp_path / "output.json"

        try:
            with open(input_path, "w", encoding="utf-8") as f:
                json.dump(input_params, f)

            wrapped_code = (
                "import json, sys\n"
                f"with open(r'{input_path}', 'r') as f:\n"
                "    PARAMS = json.load(f)\n"
                "OUTPUT = {}\n"
                "\n"
                f"{code_str}\n"
                "\n"
                f"with open(r'{output_path}', 'w') as f:\n"
                "    json.dump(OUTPUT, f)\n"
            )

            with open(script_path, "w", encoding="utf-8") as f:
                f.write(wrapped_code)

            proc = await asyncio.create_subprocess_exec(
                sys.executable,
                "-u",
                str(script_path),
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
                cwd=str(tmp_path),
            )

            stdout_chunks: list[tuple[str, str]] = []
            stderr_chunks: list[tuple[str, str]] = []

            async def read_stream(stream, chunks_list, stream_name):
                while True:
                    line = await stream.readline()
                    if not line:
                        break
                    decoded = line.decode("utf-8", errors="replace")
                    chunks_list.append((stream_name, decoded))

            stdout_task = asyncio.create_task(read_stream(proc.stdout, stdout_chunks, "stdout"))
            stderr_task = asyncio.create_task(read_stream(proc.stderr, stderr_chunks, "stderr"))

            start_time = time.monotonic()
            timed_out = False

            while True:
                # Flush captured chunks to generator caller
                while stdout_chunks:
                    yield stdout_chunks.pop(0)
                while stderr_chunks:
                    yield stderr_chunks.pop(0)

                # Check process completion non-blockingly
                try:
                    await asyncio.wait_for(proc.wait(), timeout=0.05)
                    break
                except (asyncio.TimeoutError, TimeoutError):
                    pass

                if (time.monotonic() - start_time) >= timeout_seconds:
                    timed_out = True
                    try:
                        proc.kill()
                    except ProcessLookupError:
                        pass
                    await proc.wait()
                    break

            await asyncio.gather(stdout_task, stderr_task, return_exceptions=True)

            # Flush remaining chunks
            while stdout_chunks:
                yield stdout_chunks.pop(0)
            while stderr_chunks:
                yield stderr_chunks.pop(0)

            if timed_out:
                yield "stderr", f"Execution killed: exceeded timeout of {timeout_seconds}s\n"
                yield "exit", json.dumps({"exit_code": 124, "output": {}})
                return

            output_data = {}
            if output_path.exists():
                try:
                    with open(output_path, "r", encoding="utf-8") as f:
                        output_data = json.load(f)
                except Exception:
                    output_data = {}

            yield "exit", json.dumps({"exit_code": proc.returncode, "output": output_data})

        finally:
            # Safe post-execution cleanup
            try:
                shutil.rmtree(tmp_dir, ignore_errors=True)
            except Exception:
                pass
