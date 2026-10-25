"""Command Line Interface for AuraFlow."""

import argparse
import asyncio
import json
import sys
import time
from typing import Any

from auraflow.core.graph import DAGCompiler
from auraflow.schemas.workflow import WorkflowDefinition

SAMPLE_PIPELINE = {
    "workflow_id": "demo_heterogeneous_pipeline",
    "tasks": [
        {
            "id": "ingest_data",
            "type": "sandbox_exec",
            "params": {
                "code": (
                    "import time\n"
                    "time.sleep(0.5)\n"
                    "OUTPUT['raw_values'] = [14, 28, 42, 56, 70]\n"
                    "OUTPUT['sensor_id'] = 'alpha-9'\n"
                ),
                "params": {},
            },
            "timeout_seconds": 10,
            "max_retries": 1,
        },
        {
            "id": "aggregate_metrics",
            "type": "sandbox_exec",
            "depends_on": ["ingest_data"],
            "params": {
                "code": (
                    "vals = PARAMS.get('raw_values', [])\n"
                    "OUTPUT['total'] = sum(vals)\n"
                    "OUTPUT['mean'] = sum(vals) / len(vals) if vals else 0\n"
                ),
                "params": {"raw_values": "{{ingest_data.raw_values}}"},
            },
            "timeout_seconds": 10,
            "max_retries": 1,
        },
        {
            "id": "generate_summary_inference",
            "type": "gpu_inference",
            "depends_on": ["aggregate_metrics"],
            "params": {
                "prompt": (
                    "Generate operational health briefing for sensor {{ingest_data.sensor_id}}: "
                    "Mean calculated at {{aggregate_metrics.mean}} across {{aggregate_metrics.total}} units."
                )
            },
            "timeout_seconds": 15,
            "max_retries": 1,
        },
    ],
}


def print_banner() -> None:
    banner = """
    ╔══════════════════════════════════════════════════════════════╗
    ║        AuraFlow Distributed DAG Orchestrator CLI             ║
    ║   Heterogeneous Compute: CPU Sandbox | GPU Batch Inference   ║
    ╚══════════════════════════════════════════════════════════════╝
    """
    print(banner)


async def execute_demo() -> None:
    print_banner()
    print("[1/3] Parsing and validating workflow definition...")
    wf = WorkflowDefinition.model_validate(SAMPLE_PIPELINE)
    print(f"      Workflow ID: '{wf.workflow_id}' with {len(wf.tasks)} tasks.")

    print("\n[2/3] Compiling DAG & resolving topological execution order...")
    compiler = DAGCompiler(wf)
    stages = compiler.validate_and_compile()

    for idx, stage in enumerate(stages, 1):
        print(f"      Stage {idx}: {stage}")

    print("\n[3/3] Simulating coordinated task execution...")
    start_time = time.perf_counter()

    outputs: dict[str, Any] = {}

    for stage_idx, stage in enumerate(stages, 1):
        print(f"\n---> Executing Stage {stage_idx} [{', '.join(stage)}]")
        for task_id in stage:
            task = next(t for t in wf.tasks if t.id == task_id)
            print(f"     * [{task_id}] TYPE={task.type} STATUS=RUNNING...")
            await asyncio.sleep(0.4)

            if task.type == "sandbox_exec":
                if task_id == "ingest_data":
                    res = {"raw_values": [14, 28, 42, 56, 70], "sensor_id": "alpha-9"}
                else:
                    raw = outputs.get("ingest_data", {}).get("raw_values", [])
                    res = {"total": sum(raw), "mean": sum(raw) / len(raw)}
            else:
                prompt_str = (
                    f"Generated briefing for {outputs['ingest_data']['sensor_id']}: "
                    f"Mean {outputs['aggregate_metrics']['mean']} over total {outputs['aggregate_metrics']['total']}."
                )
                res = {
                    "text": prompt_str,
                    "batch_size": 1,
                    "latency_ms": 24.8,
                }

            outputs[task_id] = res
            print(f"       [{task_id}] STATUS=COMPLETED Output: {res}")

    total_duration = time.perf_counter() - start_time
    print("\n" + "=" * 62)
    print(f"DAG Execution Succeeded in {total_duration:.2f}s!")
    print("Final Resolved Artifacts:")
    print(json.dumps(outputs, indent=2))
    print("=" * 62)


def main() -> None:
    parser = argparse.ArgumentParser(
        prog="auraflow",
        description="AuraFlow Distributed Heterogeneous DAG Workflow Orchestrator CLI",
    )
    parser.add_argument(
        "--example",
        action="store_true",
        help="Run an end-to-end sample heterogeneous DAG workflow simulation",
    )

    # In src/auraflow/cli.py, update main() to accept --file:
    parser.add_argument(
        "--file",
        "-f",
        type=str,
        help="Path to a JSON workflow definition file to execute",
    )

    args = parser.parse_args()

    if args.example:
        asyncio.run(execute_demo())
    else:
        parser.print_help()
        sys.exit(0)

    if args.file:
        with open(args.file, "r") as f:
            data = json.load(f)
        wf = WorkflowDefinition.model_validate(data)
        # execute wf using compiler and runner


if __name__ == "__main__":
    main()