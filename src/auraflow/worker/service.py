import asyncio
import json
from collections.abc import AsyncIterator

import grpc

from auraflow.proto import worker_pb2, worker_pb2_grpc
from auraflow.worker.batcher import DynamicBatchScheduler
from auraflow.worker.gpu_manager import GPUMemoryManager
from auraflow.worker.sandbox import ContainerSandboxRunner


class AuraFlowWorkerServicer(worker_pb2_grpc.WorkerServiceServicer):
    def __init__(self, worker_id: str = "worker-default"):
        self.worker_id = worker_id
        self._active_tasks: dict[str, asyncio.Task] = {}
        self.sandbox_runner = ContainerSandboxRunner()
        self.gpu_manager = GPUMemoryManager()
        self.batch_scheduler = DynamicBatchScheduler(memory_manager=self.gpu_manager)

    async def start(self) -> None:
        await self.batch_scheduler.start()

    async def stop(self) -> None:
        await self.batch_scheduler.stop()

    async def HealthCheck(
        self, request: worker_pb2.PingRequest, context: grpc.aio.ServicerContext
    ) -> worker_pb2.PingResponse:
        snapshot = self.gpu_manager.get_memory_snapshot()
        return worker_pb2.PingResponse(
            healthy=True,
            active_tasks=len(self._active_tasks),
            available_memory_bytes=snapshot.free_bytes,
        )

    async def ExecuteTask(
        self,
        request: worker_pb2.TaskAssignment,
        context: grpc.aio.ServicerContext,
    ) -> AsyncIterator[worker_pb2.ExecutionStreamUpdate]:
        task_key = f"{request.workflow_run_id}:{request.node_id}"

        yield worker_pb2.ExecutionStreamUpdate(
            workflow_run_id=request.workflow_run_id,
            node_id=request.node_id,
            stage=worker_pb2.STAGE_STARTING,
            stdout_chunk=f"Worker {self.worker_id} initializing task {request.node_id} ({request.task_type})\n",
        )

        try:
            payload = json.loads(request.payload_json) if request.payload_json else {}

            if request.task_type == "sandbox_exec":
                code = payload.get("code", "OUTPUT['status'] = 'noop'")

                yield worker_pb2.ExecutionStreamUpdate(
                    workflow_run_id=request.workflow_run_id,
                    node_id=request.node_id,
                    stage=worker_pb2.STAGE_RUNNING,
                    stdout_chunk="Spinning up isolated sandbox runner...\n",
                )

                last_exit_code = 0
                last_result = {}

                async for stream_type, chunk in self.sandbox_runner.execute_code_stream(
                    code_str=code,
                    input_params=payload.get("params", {}),
                    timeout_seconds=request.timeout_seconds or 30,
                ):
                    if stream_type == "stdout":
                        yield worker_pb2.ExecutionStreamUpdate(
                            workflow_run_id=request.workflow_run_id,
                            node_id=request.node_id,
                            stage=worker_pb2.STAGE_RUNNING,
                            stdout_chunk=chunk,
                        )
                    elif stream_type == "stderr":
                        yield worker_pb2.ExecutionStreamUpdate(
                            workflow_run_id=request.workflow_run_id,
                            node_id=request.node_id,
                            stage=worker_pb2.STAGE_RUNNING,
                            stderr_chunk=chunk,
                        )
                    elif stream_type == "exit":
                        meta = json.loads(chunk)
                        last_exit_code = meta.get("exit_code", 0)
                        last_result = meta.get("output", {})

                if last_exit_code == 0:
                    yield worker_pb2.ExecutionStreamUpdate(
                        workflow_run_id=request.workflow_run_id,
                        node_id=request.node_id,
                        stage=worker_pb2.STAGE_COMPLETED,
                        exit_code=0,
                        result_json=json.dumps(last_result),
                    )
                else:
                    yield worker_pb2.ExecutionStreamUpdate(
                        workflow_run_id=request.workflow_run_id,
                        node_id=request.node_id,
                        stage=worker_pb2.STAGE_FAILED,
                        exit_code=last_exit_code,
                        error_message=f"Process exited with non-zero code {last_exit_code}",
                    )

            elif request.task_type == "gpu_inference":
                prompt = payload.get("prompt", "Hello AuraFlow")
                yield worker_pb2.ExecutionStreamUpdate(
                    workflow_run_id=request.workflow_run_id,
                    node_id=request.node_id,
                    stage=worker_pb2.STAGE_RUNNING,
                    stdout_chunk=f"Enqueueing prompt for dynamic GPU batching: '{prompt}'\n",
                )

                response = await self.batch_scheduler.submit_prompt(
                    request_id=task_key,
                    prompt=prompt,
                    max_tokens=payload.get("max_tokens", 64),
                )

                yield worker_pb2.ExecutionStreamUpdate(
                    workflow_run_id=request.workflow_run_id,
                    node_id=request.node_id,
                    stage=worker_pb2.STAGE_COMPLETED,
                    exit_code=0,
                    stdout_chunk=f"Generated output: {response.generated_text} (latency: {response.latency_ms}ms)\n",
                    result_json=json.dumps(
                        {
                            "generated_text": response.generated_text,
                            "token_count": response.token_count,
                            "latency_ms": response.latency_ms,
                        }
                    ),
                )

            else:
                yield worker_pb2.ExecutionStreamUpdate(
                    workflow_run_id=request.workflow_run_id,
                    node_id=request.node_id,
                    stage=worker_pb2.STAGE_COMPLETED,
                    exit_code=0,
                    result_json=json.dumps({"status": "completed", "type": request.task_type}),
                )

        except asyncio.CancelledError:
            yield worker_pb2.ExecutionStreamUpdate(
                workflow_run_id=request.workflow_run_id,
                node_id=request.node_id,
                stage=worker_pb2.STAGE_CANCELLED,
                error_message="Task cancelled by orchestrator.",
            )
            raise
        except Exception as exc:
            yield worker_pb2.ExecutionStreamUpdate(
                workflow_run_id=request.workflow_run_id,
                node_id=request.node_id,
                stage=worker_pb2.STAGE_FAILED,
                exit_code=1,
                error_message=str(exc),
            )

    async def CancelTask(
        self,
        request: worker_pb2.CancelTaskRequest,
        context: grpc.aio.ServicerContext,
    ) -> worker_pb2.CancelTaskResponse:
        task_key = f"{request.workflow_run_id}:{request.node_id}"
        if task_key in self._active_tasks:
            self._active_tasks[task_key].cancel()
            return worker_pb2.CancelTaskResponse(
                acknowledged=True, message=f"Task {task_key} cancelled."
            )
        return worker_pb2.CancelTaskResponse(
            acknowledged=False, message=f"Task {task_key} was not active."
        )
