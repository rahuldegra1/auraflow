import asyncio
import os
import time
from dataclasses import dataclass, field

import httpx

from auraflow.worker.gpu_manager import GPUMemoryManager


@dataclass
class BatchRequest:
    request_id: str
    prompt: str
    max_tokens: int
    future: asyncio.Future = field(default_factory=asyncio.Future)
    created_at: float = field(default_factory=time.monotonic)


@dataclass
class BatchResponse:
    request_id: str
    generated_text: str
    token_count: int
    latency_ms: float


class DynamicBatchScheduler:
    def __init__(
        self,
        max_batch_size: int = 4,
        batch_timeout_ms: float = 30.0,
        memory_manager: GPUMemoryManager | None = None,
    ):
        self.max_batch_size = max_batch_size
        self.batch_timeout_ms = batch_timeout_ms
        self.memory_manager = memory_manager or GPUMemoryManager()
        self._queue: asyncio.Queue[BatchRequest] = asyncio.Queue()
        self._loop_task: asyncio.Task | None = None
        self._running = False

    async def start(self) -> None:
        if not self._running:
            self._running = True
            self._loop_task = asyncio.create_task(self._batch_processing_loop())
    async def stop(self) -> None:
        self._running = False
        if self._loop_task:
            self._loop_task.cancel()
            try:
                await self._loop_task
            except (asyncio.CancelledError, Exception):
                pass

    async def submit_prompt(
        self, request_id: str, prompt: str, max_tokens: int = 64
    ) -> BatchResponse:
        if not self._running:
            await self.start()
        req = BatchRequest(request_id=request_id, prompt=prompt, max_tokens=max_tokens)
        await self._queue.put(req)
        return await req.future

    async def _batch_processing_loop(self) -> None:
        while self._running:
            try:
                # Wait for the next item
                try:
                    first_req = await asyncio.wait_for(self._queue.get(), timeout=0.1)
                except (asyncio.TimeoutError, TimeoutError):
                    continue

                batch = [first_req]
                deadline = time.monotonic() + (self.batch_timeout_ms / 1000.0)

                while len(batch) < self.max_batch_size:
                    rem = deadline - time.monotonic()
                    if rem <= 0:
                        break
                    try:
                        next_req = await asyncio.wait_for(self._queue.get(), timeout=rem)
                        batch.append(next_req)
                    except (asyncio.TimeoutError, TimeoutError):
                        break

                await self._process_batch(batch)

            except asyncio.CancelledError:
                break
            except Exception:
                await asyncio.sleep(0.01)

    async def _process_batch(self, batch: list[BatchRequest]) -> None:
        time.monotonic()
        ollama_base_url = os.getenv("OLLAMA_BASE_URL", "http://host.docker.internal:11434")
        default_model = os.getenv("OLLAMA_MODEL", "qwen2.5:3b")

        try:
            async with httpx.AsyncClient(timeout=60.0) as client:
                for req in batch:
                    if req.future.done():
                        continue

                    req_start = time.monotonic()
                    try:
                        resp = await client.post(
                            f"{ollama_base_url}/api/generate",
                            json={
                                "model": default_model,
                                "prompt": req.prompt,
                                "stream": False,
                            },
                        )
                        if resp.status_code == 200:
                            data = resp.json()
                            generated_text = data.get("response", "").strip()
                            token_count = data.get("eval_count", len(generated_text.split()))
                        else:
                            generated_text = f"Ollama HTTP {resp.status_code}: {resp.text}"
                            token_count = len(generated_text.split())
                    except Exception as net_err:
                        generated_text = f"Processed [{req.prompt}] in batch of {len(batch)} (Fallback: Ollama unreachable: {net_err})"
                        token_count = len(generated_text.split())

                    req_latency_ms = (time.monotonic() - req_start) * 1000.0

                    req.future.set_result(
                        BatchResponse(
                            request_id=req.request_id,
                            generated_text=generated_text,
                            token_count=token_count,
                            latency_ms=max(round(req_latency_ms, 2), 0.01),
                        )
                    )
        except Exception as exc:
            for req in batch:
                if not req.future.done():
                    req.future.set_exception(exc)
