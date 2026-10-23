from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from auraflow.core.database import get_db_session
from auraflow.core.engine import WorkflowEngine
from auraflow.core.exceptions import DAGValidationError
from auraflow.models.execution import WorkflowRun
from auraflow.schemas.workflow import WorkflowDefinition

router = APIRouter(prefix="/api/v1/workflows", tags=["Workflows"])


@router.post("", status_code=status.HTTP_201_CREATED)
async def submit_workflow(
    definition: WorkflowDefinition,
    db: AsyncSession = Depends(get_db_session),
):
    engine = WorkflowEngine(db)
    try:
        run = await engine.submit_workflow(definition)
        return {
            "run_id": run.id,
            "workflow_id": run.workflow_id,
            "status": run.status,
            "created_at": run.created_at,
        }
    except DAGValidationError as exc:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_CONTENT,
            detail=str(exc),
        ) from exc


@router.get("/{run_id}")
async def get_workflow_status(
    run_id: str,
    db: AsyncSession = Depends(get_db_session),
):
    stmt = (
        select(WorkflowRun)
        .where(WorkflowRun.id == run_id)
        .options(selectinload(WorkflowRun.nodes))
    )
    res = await db.execute(stmt)
    run = res.scalar_one_or_none()

    if not run:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Workflow run not found")

    return {
        "run_id": run.id,
        "workflow_id": run.workflow_id,
        "status": run.status,
        "created_at": run.created_at,
        "started_at": run.started_at,
        "completed_at": run.completed_at,
        "nodes": [
            {
                "node_id": node.node_id,
                "task_type": node.task_type,
                "status": node.status,
                "retry_count": node.retry_count,
                "locked_by_worker": node.locked_by_worker,
                "started_at": node.started_at,
                "completed_at": node.completed_at,
            }
            for node in run.nodes
        ],
    }
