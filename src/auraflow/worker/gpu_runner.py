import asyncio
import json
from collections.abc import AsyncIterator

from auraflow.worker.gpu_manager import GPUMemoryManager


class GPUInferenceRunner:
    def __init__(self, memory_manager: GPUMemoryManager | None = None):
        self.memory_manager = memory_manager or GPUMemoryManager()

    async def execute_inference_stream(
        self,
        model_name: str,
        prompt: str,
        estimated_vram_mb: int = 512,
        max_tokens: int = 128,
    ) -> AsyncIterator[tuple[str, str]]:
        """
        Validates GPU headroom, streams generated tokens, and releases allocation.
        """
        required_bytes = estimated_vram_mb * 1024 * 1024

        # 1. Budget Verification
        yield "stdout", f"Verifying GPU memory budget for model '{model_name}'...\n"
        self.memory_manager.reserve_budget(required_bytes)

        snapshot = self.memory_manager.get_memory_snapshot()
        yield (
            "stdout",
            (
                f"Hardware target: {snapshot.device_name} | "
                f"Free VRAM: {snapshot.free_bytes / (1024 * 1024):.2f}MB | "
                f"Budget reserved: {estimated_vram_mb}MB\n"
            ),
        )

        try:
            yield "stdout", f"Generating response for prompt: '{prompt}'\n"

            # 2. Simulated Token Streaming (or local model execution)
            generated_tokens = []
            dummy_tokens = [
                "AuraFlow",
                " orchestrator",
                " processed",
                " task",
                " via",
                " local",
                " GPU.",
            ]

            for token in dummy_tokens:
                await asyncio.sleep(0.04)  # Simulates token generation latency
                generated_tokens.append(token)
                yield "stdout", f"token: {token}\n"

            result_payload = {
                "model": model_name,
                "output_text": "".join(generated_tokens),
                "tokens_generated": len(generated_tokens),
                "device": snapshot.device_name,
            }

            yield "exit", json.dumps({"exit_code": 0, "output": result_payload})

        finally:
            # 3. Explicit cache cleanup hook
            self.memory_manager.clear_cache()
