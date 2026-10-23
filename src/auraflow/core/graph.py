from collections import defaultdict, deque

from auraflow.core.exceptions import CircularDependencyError, DanglingDependencyError
from auraflow.schemas.workflow import WorkflowDefinition


class DAGCompiler:
    def __init__(self, workflow: WorkflowDefinition):
        self.workflow = workflow
        self.nodes = {task.id: task for task in workflow.tasks}
        self.adj_list: dict[str, list[str]] = defaultdict(list)
        self.in_degree: dict[str, int] = {task.id: 0 for task in workflow.tasks}
        self._build_graph()

    def _build_graph(self) -> None:
        for task in self.workflow.tasks:
            for dep in task.depends_on:
                if dep not in self.nodes:
                    raise DanglingDependencyError(
                        f"Task '{task.id}' depends on non-existent task '{dep}'"
                    )
                # Directed edge: dependency -> dependent task
                self.adj_list[dep].append(task.id)
                self.in_degree[task.id] += 1

    def validate_and_compile(self) -> list[list[str]]:
        """
        Executes Kahn's algorithm.
        Returns a list of stages (batches), where each stage contains task IDs
        that can be executed concurrently.
        """
        queue = deque([node_id for node_id, deg in self.in_degree.items() if deg == 0])
        stages: list[list[str]] = []
        visited_count = 0

        current_in_degrees = self.in_degree.copy()

        while queue:
            # All tasks currently in the queue have in_degree == 0 and can run concurrently
            current_stage = list(queue)
            stages.append(current_stage)
            queue.clear()

            for node_id in current_stage:
                visited_count += 1
                for neighbor in self.adj_list[node_id]:
                    current_in_degrees[neighbor] -= 1
                    if current_in_degrees[neighbor] == 0:
                        queue.append(neighbor)

        if visited_count != len(self.nodes):
            unresolved = [node_id for node_id, deg in current_in_degrees.items() if deg > 0]
            raise CircularDependencyError(
                f"Cycle detected in workflow. Unresolved dependency nodes: {unresolved}"
            )

        return stages
