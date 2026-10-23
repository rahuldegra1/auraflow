from datetime import datetime, timedelta, timezone

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from auraflow.models.execution import (
    NodeExecution,
    NodeStatus,
    WorkflowRun,
    WorkflowStatus,
)


class WorkflowScheduler:
    def __init__(self, session: AsyncSession):
        self.session = session

    async def advance_workflow_state(self, workflow_run_id: str) -> list[str]:
        """
        Evaluates nodes in a workflow run. Any node whose parent dependencies
        are all COMPLETED is promoted to READY.
        Returns the list of node IDs transitioned to READY.
        """
        wf_stmt = select(WorkflowRun).where(WorkflowRun.id == workflow_run_id)
        wf_res = await self.session.execute(wf_stmt)
        run = wf_res.scalar_one_or_none()
        if not run or run.status in [WorkflowStatus.FAILED.value, WorkflowStatus.CANCELLED.value]:
            return []

        nodes_stmt = select(NodeExecution).where(NodeExecution.workflow_run_id == workflow_run_id)
        nodes_res = await self.session.execute(nodes_stmt)
        node_records = {node.node_id: node for node in nodes_res.scalars().all()}

        definition_tasks = run.definition_snapshot.get("tasks", [])
        dep_map = {task["id"]: task.get("depends_on", []) for task in definition_tasks}

        newly_ready_nodes = []

        for node_id, node in node_records.items():
            if node.status in [NodeStatus.PENDING.value, NodeStatus.BLOCKED.value]:
                required_deps = dep_map.get(node_id, [])
                deps_satisfied = all(
                    dep in node_records and node_records[dep].status == NodeStatus.COMPLETED.value
                    for dep in required_deps
                )

                if deps_satisfied:
                    node.status = NodeStatus.READY.value
                    newly_ready_nodes.append(node.node_id)

        # Check if the entire workflow is now complete
        all_completed = len(node_records) > 0 and all(
            n.status == NodeStatus.COMPLETED.value for n in node_records.values()
        )
        if all_completed and run.status == WorkflowStatus.RUNNING.value:
            run.status = WorkflowStatus.COMPLETED.value
            run.completed_at = datetime.now(timezone.utc)

        if newly_ready_nodes:
            if run.status == WorkflowStatus.PENDING.value:
                run.status = WorkflowStatus.RUNNING.value
                run.started_at = datetime.now(timezone.utc)

        await self.session.commit()
        return newly_ready_nodes

    async def claim_next_ready_node(
        self,
        worker_id: str,
        task_type: str | None = None,
        workflow_run_id: str | None = None,
    ) -> NodeExecution | None:
        stmt = (
            select(NodeExecution)
            .where(NodeExecution.status == NodeStatus.READY.value)
            .with_for_update(skip_locked=True)
            .order_by(NodeExecution.created_at.asc())
            .limit(1)
        )

        if workflow_run_id:
            stmt = stmt.where(NodeExecution.workflow_run_id == workflow_run_id)

        if task_type:
            stmt = stmt.where(NodeExecution.task_type == task_type)

        result = await self.session.execute(stmt)
        node = result.scalar_one_or_none()

        now = datetime.now(timezone.utc)
        if node:
            node.status = NodeStatus.RUNNING.value
            node.locked_by_worker = worker_id
            node.locked_at = now
            node.started_at = now
            await self.session.commit()
            await self.session.refresh(node)
            return node

        return None

    async def record_heartbeat(self, node_id: str, workflow_run_id: str, worker_id: str) -> bool:
        """
        Updates the locked_at timestamp to indicate the worker is actively executing.
        """
        stmt = (
            select(NodeExecution)
            .where(
                NodeExecution.node_id == node_id,
                NodeExecution.workflow_run_id == workflow_run_id,
                NodeExecution.locked_by_worker == worker_id,
                NodeExecution.status == NodeStatus.RUNNING.value,
            )
            .with_for_update()
        )
        result = await self.session.execute(stmt)
        node = result.scalar_one_or_none()
        if node:
            node.locked_at = datetime.now(timezone.utc)
            await self.session.commit()
            return True
        return False

    async def handle_node_failure(
        self, node_id: str, workflow_run_id: str, error_message: str
    ) -> NodeExecution:
        """
        Increments retry count. If retries remain, returns node to READY.
        If retries are exhausted, marks node and workflow as FAILED.
        """
        stmt = (
            select(NodeExecution)
            .where(
                NodeExecution.node_id == node_id,
                NodeExecution.workflow_run_id == workflow_run_id,
            )
            .with_for_update()
        )
        res = await self.session.execute(stmt)
        node = res.scalar_one()

        node.retry_count += 1
        node.error_message = error_message
        node.locked_by_worker = None
        node.locked_at = None

        if node.retry_count <= node.max_retries:
            node.status = NodeStatus.READY.value
        else:
            node.status = NodeStatus.FAILED.value
            node.completed_at = datetime.now(timezone.utc)
            # Mark parent workflow as FAILED
            wf_stmt = select(WorkflowRun).where(WorkflowRun.id == workflow_run_id).with_for_update()
            wf_res = await self.session.execute(wf_stmt)
            run = wf_res.scalar_one()
            run.status = WorkflowStatus.FAILED.value
            run.completed_at = datetime.now(timezone.utc)

        await self.session.commit()
        await self.session.refresh(node)
        return node

    async def recover_stale_tasks(self, heartbeat_timeout_seconds: int = 30) -> list[str]:
        """
        Scans for RUNNING tasks whose locked_at heartbeat has expired.
        Resets them to READY for re-dispatch or triggers retry evaluation.
        """
        now = datetime.now(timezone.utc)
        stale_threshold = now - timedelta(seconds=heartbeat_timeout_seconds)

        stmt = (
            select(NodeExecution)
            .where(
                NodeExecution.status == NodeStatus.RUNNING.value,
                NodeExecution.locked_at <= stale_threshold,
            )
            .with_for_update(skip_locked=True)
        )
        res = await self.session.execute(stmt)
        stale_nodes = res.scalars().all()

        recovered_node_ids = []
        for node in stale_nodes:
            recovered_node_ids.append(node.node_id)
            node.locked_by_worker = None
            node.locked_at = None
            node.retry_count += 1

            if node.retry_count <= node.max_retries:
                node.status = NodeStatus.READY.value
                node.error_message = f"Recovered from silent worker failure after {heartbeat_timeout_seconds}s timeout"
            else:
                node.status = NodeStatus.FAILED.value
                node.error_message = (
                    f"Heartbeat expired and maximum retries ({node.max_retries}) exhausted"
                )

        if recovered_node_ids:
            await self.session.commit()

        return recovered_node_ids
