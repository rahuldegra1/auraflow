from enum import Enum
from typing import Any

from pydantic import BaseModel, Field, field_validator


class TaskType(str, Enum):
    SANDBOX_EXEC = "sandbox_exec"
    GPU_INFERENCE = "gpu_inference"
    IO_FETCH = "io_fetch"
    DATA_TRANSFORM = "data_transform"


class TaskNode(BaseModel):
    id: str = Field(..., description="Unique task identifier within the DAG")
    type: TaskType = Field(..., description="Compute worker target")
    depends_on: list[str] = Field(
        default_factory=list,
        description="List of task IDs that must complete before execution",
    )
    params: dict[str, Any] = Field(default_factory=dict)
    timeout_seconds: int = Field(default=60, ge=1, le=3600)
    max_retries: int = Field(default=3, ge=0, le=10)

    @field_validator("id")
    @classmethod
    def validate_id_alphanumeric(cls, v: str) -> str:
        clean = v.replace("_", "").replace("-", "")
        if not clean.isalnum():
            raise ValueError("Task ID must be alphanumeric (hyphens and underscores allowed)")
        return v


class WorkflowDefinition(BaseModel):
    workflow_id: str
    description: str | None = None
    tasks: list[TaskNode]

    @field_validator("tasks")
    @classmethod
    def validate_unique_task_ids(cls, tasks: list[TaskNode]) -> list[TaskNode]:
        seen = set()
        for task in tasks:
            if task.id in seen:
                raise ValueError(f"Duplicate task id detected: {task.id}")
            seen.add(task.id)
        return tasks
