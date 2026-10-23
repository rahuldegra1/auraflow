from prometheus_client import (
    CONTENT_TYPE_LATEST,
    Counter,
    Gauge,
    Histogram,
    generate_latest,
)

# Metrics definitions
TASKS_TOTAL = Counter(
    "auraflow_tasks_total",
    "Total number of tasks processed",
    ["task_type", "status"],
)

TASK_DURATION_SECONDS = Histogram(
    "auraflow_task_duration_seconds",
    "Duration of individual task execution",
    ["task_type"],
    buckets=(0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0),
)

ACTIVE_WORKFLOWS = Gauge(
    "auraflow_active_workflows",
    "Number of workflows currently in-flight",
)

GPU_VRAM_FREE_BYTES = Gauge(
    "auraflow_gpu_vram_free_bytes",
    "Current free VRAM in bytes on primary GPU",
    ["device_name"],
)


def get_prometheus_metrics() -> tuple[bytes, str]:
    """Generates the latest Prometheus exposition metrics."""
    return generate_latest(), CONTENT_TYPE_LATEST
