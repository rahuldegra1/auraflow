import asyncio

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker
from sqlalchemy.orm import selectinload

from auraflow.core.engine import WorkflowEngine
from auraflow.core.scheduler import WorkflowScheduler
from auraflow.models.execution import WorkflowRun, WorkflowStatus
from auraflow.proto import worker_pb2_grpc
from auraflow.worker.dispatcher import TaskExecutionCoordinator


class WorkflowExecutionRunner:
    def __init__(
        self,
        session_or_factory: AsyncSession | async_sessionmaker[AsyncSession],
        worker_stub: worker_pb2_grpc.WorkerServiceStub,
    ):
        self.session_or_factory = session_or_factory
        self.worker_stub = worker_stub

    async def run_until_completion(
        self, workflow_run_id: str, poll_interval_s: float = 0.05
    ) -> WorkflowRun:
        if isinstance(self.session_or_factory, async_sessionmaker):
            async with self.session_or_factory() as session:
                return await self._execute_loop(session, workflow_run_id, poll_interval_s)
        else:
            return await self._execute_loop(self.session_or_factory, workflow_run_id, poll_interval_s)

    async def _execute_loop(
        self, session: AsyncSession, workflow_run_id: str, poll_interval_s: float
    ) -> WorkflowRun:
        engine = WorkflowEngine(session)
        scheduler = WorkflowScheduler(session)
        coordinator = TaskExecutionCoordinator(session, self.worker_stub)

        while True:
            # 1. Advance dependency graph
            await scheduler.advance_workflow_state(workflow_run_id)

            # 2. Claim next available ready task for this workflow
            node = await scheduler.claim_next_ready_node(
                worker_id="runner-daemon",
                workflow_run_id=workflow_run_id,
            )

            if node:
                # 3. Resolve upstream inputs
                resolved = await engine.resolve_node_parameters(node)
                node.params = resolved

                # 4. Execute via gRPC worker
                await coordinator.execute_claimed_node(node)
                continue

            # 3. Check terminal condition with eager loading
            stmt = (
                select(WorkflowRun)
                .where(WorkflowRun.id == workflow_run_id)
                .options(selectinload(WorkflowRun.nodes))
            )
            res = await session.execute(stmt)
            run = res.scalar_one_or_none()

            if not run:
                raise RuntimeError(f"Workflow run {workflow_run_id} missing during execution")

            if run.status in [WorkflowStatus.COMPLETED.value, WorkflowStatus.FAILED.value]:
                return run

            await asyncio.sleep(poll_interval_s)
