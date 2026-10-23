import pytest

from auraflow.core.exceptions import CircularDependencyError, DanglingDependencyError
from auraflow.core.graph import DAGCompiler
from auraflow.schemas.workflow import TaskNode, TaskType, WorkflowDefinition


def test_valid_dag_compilation_and_stages():
    """
    Graph:
        A -> B \
                 -> D
        A -> C /
    Expected execution batches: [[A], [B, C], [D]]
    """
    workflow = WorkflowDefinition(
        workflow_id="wf_001",
        tasks=[
            TaskNode(id="task_a", type=TaskType.IO_FETCH),
            TaskNode(id="task_b", type=TaskType.SANDBOX_EXEC, depends_on=["task_a"]),
            TaskNode(id="task_c", type=TaskType.GPU_INFERENCE, depends_on=["task_a"]),
            TaskNode(id="task_d", type=TaskType.DATA_TRANSFORM, depends_on=["task_b", "task_c"]),
        ],
    )
    compiler = DAGCompiler(workflow)
    stages = compiler.validate_and_compile()

    assert stages == [["task_a"], ["task_b", "task_c"], ["task_d"]]


def test_detects_circular_dependency():
    """
    Graph: A -> B -> C -> A (Cycle)
    """
    workflow = WorkflowDefinition(
        workflow_id="wf_cycle",
        tasks=[
            TaskNode(id="node_1", type=TaskType.SANDBOX_EXEC, depends_on=["node_3"]),
            TaskNode(id="node_2", type=TaskType.SANDBOX_EXEC, depends_on=["node_1"]),
            TaskNode(id="node_3", type=TaskType.SANDBOX_EXEC, depends_on=["node_2"]),
        ],
    )
    compiler = DAGCompiler(workflow)
    with pytest.raises(CircularDependencyError) as exc_info:
        compiler.validate_and_compile()
    assert "Cycle detected" in str(exc_info.value)


def test_detects_dangling_dependency():
    """
    Node depends on an ID that was never defined.
    """
    workflow = WorkflowDefinition(
        workflow_id="wf_dangling",
        tasks=[
            TaskNode(id="step_1", type=TaskType.IO_FETCH, depends_on=["missing_task"]),
        ],
    )
    with pytest.raises(DanglingDependencyError) as exc_info:
        DAGCompiler(workflow)
    assert "missing_task" in str(exc_info.value)
