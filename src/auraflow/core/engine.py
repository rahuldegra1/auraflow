import re
from typing import Any

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from auraflow.core.graph import DAGCompiler
from auraflow.core.scheduler import WorkflowScheduler
from auraflow.core.security import SecuritySanitizer
from auraflow.models.execution import (
    NodeExecution,
    NodeStatus,
    WorkflowRun,
    WorkflowStatus,
)
from auraflow.schemas.workflow import WorkflowDefinition


class WorkflowEngine:
    def __init__(self, session: AsyncSession):
        self.session = session
        self.scheduler = WorkflowScheduler(session)

    async def submit_workflow(self, definition: WorkflowDefinition) -> WorkflowRun:
        # 1. Topological cycle detection & DAG validation
        compiler = DAGCompiler(definition)
        compiler.validate_and_compile()

        # 2. Security validation: inspect code payloads in sandbox tasks
        for task in definition.tasks:
            if task.type.value == "sandbox_exec" and "code" in task.params:
                SecuritySanitizer.validate_code_safety(task.params["code"])

        # 3. Persist WorkflowRun record
        run = WorkflowRun(
            workflow_id=definition.workflow_id,
            status=WorkflowStatus.PENDING.value,
            definition_snapshot=definition.model_dump(),
        )
        self.session.add(run)
        await self.session.flush()

        # 4. Create all task executions in PENDING state
        nodes = [
            NodeExecution(
                workflow_run_id=run.id,
                node_id=task.id,
                task_type=task.type.value,
                status=NodeStatus.PENDING.value,
                params=task.params,
                timeout_seconds=task.timeout_seconds,
                max_retries=task.max_retries,
            )
            for task in definition.tasks
        ]
        self.session.add_all(nodes)
        await self.session.commit()
        await self.session.refresh(run)

        # 5. Advance initial root tasks to READY
        await self.scheduler.advance_workflow_state(run.id)
        await self.session.refresh(run)

        return run

    async def resolve_node_parameters(self, node: NodeExecution) -> dict[str, Any]:
        stmt = select(NodeExecution).where(
            NodeExecution.workflow_run_id == node.workflow_run_id,
            NodeExecution.status == NodeStatus.COMPLETED.value,
        )
        res = await self.session.execute(stmt)
        completed_nodes = {n.node_id: (n.result_output or {}) for n in res.scalars().all()}

        resolved_params: dict[str, Any] = {}
        pattern = re.compile(r"\{\{([a-zA-Z0-9_\-]+)\.([a-zA-Z0-9_\-]+)\}\}")

        for k, v in node.params.items():
            if isinstance(v, str):
                matches = pattern.findall(v)
                new_str = v
                for dep_node_id, dep_field in matches:
                    if dep_node_id in completed_nodes and dep_field in completed_nodes[dep_node_id]:
                        replacement = str(completed_nodes[dep_node_id][dep_field])
                        new_str = new_str.replace(f"{{{{{dep_node_id}.{dep_field}}}}}", replacement)
                resolved_params[k] = new_str
            else:
                resolved_params[k] = v

        return resolved_params
