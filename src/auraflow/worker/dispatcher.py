from datetime import datetime, timezone
import json
import time
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from auraflow.core.metrics import TASK_DURATION_SECONDS, TASKS_TOTAL
from auraflow.models.execution import NodeExecution, NodeStatus, WorkflowRun, WorkflowStatus
from auraflow.proto import worker_pb2, worker_pb2_grpc


class TaskExecutionCoordinator:
    def __init__(self, session: AsyncSession, worker_stub: worker_pb2_grpc.WorkerServiceStub):
        self.session = session
        self.worker_stub = worker_stub

    async def execute_claimed_node(
        self,
        node: NodeExecution,
    ) -> NodeExecution:
        # Guarantee valid JSON string payload
        if isinstance(node.params, str):
            payload_str = node.params
        else:
            payload_str = json.dumps(node.params if node.params is not None else {})

        assignment = worker_pb2.TaskAssignment(
            workflow_run_id=node.workflow_run_id,
            node_id=node.node_id,
            task_type=node.task_type,
            timeout_seconds=node.timeout_seconds or 30,
            payload_json=payload_str,
        )

        stdout_chunks = []
        stderr_chunks = []
        final_result = None
        error_msg = None
        exit_code = 0
        terminal_stage = None
        start_time = time.monotonic()

        try:
            async for update in self.worker_stub.ExecuteTask(assignment):
                if update.stdout_chunk:
                    stdout_chunks.append(update.stdout_chunk)
                if update.stderr_chunk:
                    stderr_chunks.append(update.stderr_chunk)

                terminal_stage = update.stage

                if update.stage == worker_pb2.STAGE_COMPLETED:
                    exit_code = update.exit_code
                    if update.result_json:
                        try:
                            final_result = json.loads(update.result_json)
                        except Exception:
                            final_result = {"raw": update.result_json}
                elif update.stage in [worker_pb2.STAGE_FAILED, worker_pb2.STAGE_CANCELLED]:
                    exit_code = update.exit_code or 1
                    error_msg = update.error_message or "Task execution failed"

        except Exception as exc:
            terminal_stage = worker_pb2.STAGE_FAILED
            exit_code = 1
            error_msg = f"Transport failure or RPC exception: {str(exc)}"

        duration = time.monotonic() - start_time
        try:
            TASK_DURATION_SECONDS.labels(task_type=node.task_type).observe(duration)
        except Exception:
            pass

        node.stdout_log = "".join(stdout_chunks)
        node.stderr_log = "".join(stderr_chunks)

        if terminal_stage == worker_pb2.STAGE_COMPLETED and exit_code == 0:
            node.status = NodeStatus.COMPLETED.value
            node.result_output = final_result
            node.completed_at = datetime.now(timezone.utc)
            try:
                TASKS_TOTAL.labels(task_type=node.task_type, status="success").inc()
            except Exception:
                pass
        else:
            node.retry_count += 1
            node.error_message = error_msg

            if node.retry_count <= node.max_retries:
                node.status = NodeStatus.READY.value
                node.locked_by_worker = None
                node.locked_at = None
                try:
                    TASKS_TOTAL.labels(task_type=node.task_type, status="retrying").inc()
                except Exception:
                    pass
            else:
                node.status = NodeStatus.DEAD_LETTER.value
                node.completed_at = datetime.now(timezone.utc)
                try:
                    TASKS_TOTAL.labels(task_type=node.task_type, status="failed").inc()
                except Exception:
                    pass

                wf_res = await self.session.execute(
                    select(WorkflowRun).where(WorkflowRun.id == node.workflow_run_id)
                )
                run = wf_res.scalar_one_or_none()
                if run:
                    run.status = WorkflowStatus.FAILED.value
                    run.completed_at = datetime.now(timezone.utc)

        await self.session.commit()
        await self.session.refresh(node)
        return node