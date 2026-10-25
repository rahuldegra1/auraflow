# AuraFlow: Distributed Heterogeneous DAG Workflow Orchestrator
 
[![CI/CD Pipeline](https://github.com/rahuldegra1/auraflow/actions/workflows/ci.yml/badge.svg)](https://github.com/rahuldegra1/auraflow/actions/workflows/ci.yml)
[![Python Version](https://img.shields.io/badge/python-3.10%2B-blue.svg)](https://www.python.org/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![Code Style: Ruff](https://img.shields.io/badge/code%20style-ruff-000000.svg)](https://github.com/astral-sh/ruff)
[![Type Checked: Mypy](https://img.shields.io/badge/type%20checked-mypy-blue.svg)](https://mypy-lang.org/)
[![Test Suite](https://img.shields.io/badge/tests-29%20passed-brightgreen.svg)]()
 
**AuraFlow** is a distributed, fault-tolerant DAG (Directed Acyclic Graph) workflow orchestrator built for heterogeneous compute workloads. It coordinates dependencies across isolated sandboxed code execution (CPU) and dynamically batched inference (GPU/CUDA), with durable state persistence and distributed concurrency locking.
 
---
 
## Table of Contents
 
- [Architecture Overview](#architecture-overview)
- [Key Systems Capabilities](#key-systems-capabilities)
- [System State Machine](#system-state-machine)
- [gRPC Protocol Specification](#grpc-protocol-specification)
- [Quickstart: Local Installation](#quickstart-local-installation)
- [Environment Configuration](#environment-configuration)
- [Running with Docker Compose](#running-with-docker-compose)
- [API Reference & Workflow Submission](#api-reference--workflow-submission)
- [Verification & Stress Tests](#verification--stress-tests)
- [Benchmark: Dynamic GPU Batching vs Sequential](#benchmark-dynamic-gpu-batching-vs-sequential)
- [Key Systems Engineering Takeaways](#key-systems-engineering-takeaways)
- [Project Layout](#project-layout)
- [License](#license)
---
 
## Architecture Overview
 
```
                 +-----------------------------------+
                 |      AuraFlow Control Plane       |
                 |  (FastAPI / SQLAlchemy Async ORM) |
                 +-----------------+-----------------+
                                   |
             +---------------------+---------------------+
             |                                           |
    +--------v--------+                         +--------v--------+
    | State Database  |                         | Telemetry Hub   |
    | PostgreSQL 16   |                         | Prometheus      |
    | (SKIP LOCKED)   |                         | OpenTelemetry   |
    +--------+--------+                         +-----------------+
             |
+------------+------------+-------------------------+
|  AuraFlow Scheduler     |  Recovery Daemon        |
|  - Graph Resolvers      |  - Heartbeat Sweeper    |
|  - Lease Coordinators   |  - Dead-Letter Router   |
+------------+------------+-------------------------+
             |
             | gRPC (HTTP/2, server-streaming)
             v
+---------------------------------------------------+
|               AuraFlow Worker Nodes               |
|                                                   |
|  +--------------------+   +--------------------+  |
|  | Container Sandbox  |   | Dynamic Batcher    |  |
|  | - Subprocess I/O   |   | - Adaptive Window  |  |
|  | - Memory/CPU Caps  |   | - VRAM Manager     |  |
|  | (CPU Workloads)    |   | (CUDA Inference)   |  |
|  +--------------------+   +--------------------+  |
+---------------------------------------------------+
```
 
---
 
## Key Systems Capabilities
 
### 1. Heterogeneous Task Scheduling & Dynamic Batching
* **Adaptive Batch Windows**: GPU inference requests are aggregated into batches (up to N = 4 requests or a window of Δt = 30 ms, whichever comes first) by the `DynamicBatchScheduler`, giving roughly **2.9x lower pipeline latency** than serial processing (see [benchmark](#benchmark-dynamic-gpu-batching-vs-sequential)) while guarding against out-of-memory (OOM) failures.
* **Isolated Subprocess Sandboxing**: Code tasks run under strict resource ceilings, with non-blocking pipe draining and execution timeouts.
* **Jinja-Style Dependency Piping**: Upstream outputs are bound into downstream inputs via `{{upstream_task.output_key}}` resolution.
### 2. High-Concurrency Distributed State Machine
* **Race-Free Lease Acquisition**: Workers lease ready tasks with atomic `SELECT ... FOR UPDATE SKIP LOCKED`, avoiding lock contention and deadlocks under heavy load.
* **Autonomous Orphan Sweeping**: The orchestrator continuously scans for tasks whose worker heartbeat has expired (T > 30 s), recovering them or routing poison pills to a Dead Letter Queue (DLQ).
### 3. Hardened Security & Isolation
* **Cryptographic Payload Signing**: Task payloads are verified with HMAC-SHA256 to prevent node spoofing and unauthorized task injection.
* **AST & Pattern Sanitization**: Script bodies are scanned for shell escapes, malicious process spawns, and filesystem traversal before execution.
### 4. Telemetry & Observability
* **Native Prometheus Metrics**: Counters and histograms (`auraflow_tasks_total`, `auraflow_task_duration_seconds`, `auraflow_gpu_vram_bytes`) exposed at `/metrics`.
* **Structured JSON Logging**: Request tracing across the FastAPI control plane and gRPC worker channels.
---
 
## System State Machine
 
```
              [ PENDING ]
                   |
                   v  (topological stage resolved)
               [ READY ] <-----------------+
                   |                       |
                   v  (worker acquires     |  (failure and
                      lease via            |   retries < max;
                      SKIP LOCKED)         |   backoff retry)
              [ RUNNING ] -----------------+
                 |     |
      (success)  |     |  (failure and retries >= max)
                 v     v
        [ COMPLETED ]  [ DEAD_LETTER ]
```
 
---
 
## gRPC Protocol Specification
 
`worker.proto`:
 
```protobuf
syntax = "proto3";
 
package auraflow.worker;
 
service WorkerService {
  rpc HealthCheck (PingRequest) returns (PingResponse);
  rpc ExecuteTask (TaskAssignment) returns (stream ExecutionStreamUpdate);
}
 
message PingRequest {}
 
message PingResponse {
  bool healthy = 1;
  int32 active_tasks = 2;
  int64 available_memory_bytes = 3;
}
 
message TaskAssignment {
  string workflow_run_id = 1;
  string node_id = 2;
  string task_type = 3;   // "sandbox_exec" | "gpu_inference"
  string payload_json = 4;
}
 
enum ExecutionStage {
  STAGE_UNSPECIFIED = 0;
  STAGE_STARTING = 1;
  STAGE_RUNNING = 2;
  STAGE_COMPLETED = 3;
  STAGE_FAILED = 4;
}
 
message ExecutionStreamUpdate {
  string workflow_run_id = 1;
  string node_id = 2;
  ExecutionStage stage = 3;
  int32 exit_code = 4;
  string stdout_chunk = 5;
  string error_message = 6;
  string result_json = 7;
}
```
 
---
 
## Quickstart: Local Installation
 
### Prerequisites
* Python 3.10+
* Git
* (Optional) NVIDIA GPU with CUDA drivers for GPU batching
### Setup
 
1. **Clone the repository:**
```bash
   git clone https://github.com/rahuldegra1/auraflow.git
   cd auraflow
```
 
2. **Create and activate a virtual environment:**
```bash
   # Linux / macOS
   python3 -m venv .venv
   source .venv/bin/activate
 
   # Windows (PowerShell)
   python -m venv .venv
   .venv\Scripts\Activate.ps1
```
 
3. **Install dependencies:**
```bash
   pip install --upgrade pip
   pip install torch --index-url https://download.pytorch.org/whl/cpu
   pip install -r requirements.txt
   pip install -e .
```
 
4. **Run static analysis and tests:**
```bash
   ruff check src/ tests/
   mypy src/ tests/
   pytest -v
```
 
---
 
## Environment Configuration
 
AuraFlow reads settings from environment variables or a root `.env` file:
 
```ini
# Database & Persistence
DATABASE_URL=sqlite+aiosqlite:///./auraflow.db
# For production PostgreSQL:
# DATABASE_URL=postgresql+asyncpg://auraflow:auraflow@localhost:5432/auraflow_db
 
# Control Plane API
AURAFLOW_HOST=0.0.0.0
AURAFLOW_PORT=8000
AURAFLOW_DEBUG=false
 
# Worker Daemon & gRPC
AURAFLOW_GRPC_PORT=50051
AURAFLOW_WORKER_HEARTBEAT_INTERVAL=5
AURAFLOW_HEARTBEAT_TIMEOUT=30
 
# Security (change this in any real deployment)
AURAFLOW_SECRET_KEY=change-me-hmac-key-for-payload-signing
 
# GPU Batching
AURAFLOW_GPU_MAX_BATCH_SIZE=4
AURAFLOW_GPU_BATCH_TIMEOUT_MS=30
```
 
---
 
## Running with Docker Compose
 
Deploy the multi-service cluster (PostgreSQL, Redis, and the AuraFlow control plane):
 
```bash
docker compose up --build -d
docker compose ps
curl http://localhost:8000/health
curl http://localhost:8000/metrics
```
 
---
 
## API Reference & Workflow Submission
 
### Submit a Heterogeneous DAG Workflow
 
**Endpoint:** `POST /api/v1/workflows`
 
```bash
curl -X POST http://localhost:8000/api/v1/workflows \
  -H "Content-Type: application/json" \
  -d '{
    "workflow_id": "data_inference_pipeline",
    "tasks": [
      {
        "id": "preprocess_raw_data",
        "type": "sandbox_exec",
        "params": {
          "code": "data = PARAMS.get(\"input\", [12, 19, 34])\nOUTPUT[\"mean\"] = sum(data)/len(data)\n",
          "params": {"input": [10, 20, 30]}
        },
        "timeout_seconds": 10,
        "max_retries": 2
      },
      {
        "id": "generate_report",
        "type": "gpu_inference",
        "depends_on": ["preprocess_raw_data"],
        "params": {
          "prompt": "Summarize analysis: Computed mean was {{preprocess_raw_data.mean}}"
        },
        "timeout_seconds": 15,
        "max_retries": 1
      }
    ]
  }'
```
 
### Query Workflow Status
 
**Endpoint:** `GET /api/v1/workflows/{workflow_run_id}`
 
```bash
curl http://localhost:8000/api/v1/workflows/<RUN_ID>
```
 
Example response:
 
```json
{
  "run_id": "b3f3b9c7-95da-48c2-a42f-8705c9359e19",
  "workflow_id": "data_inference_pipeline",
  "status": "COMPLETED",
  "nodes": [
    {
      "node_id": "preprocess_raw_data",
      "status": "COMPLETED",
      "retry_count": 0,
      "result_output": { "mean": 20.0 }
    },
    {
      "node_id": "generate_report",
      "status": "COMPLETED",
      "retry_count": 0,
      "result_output": {
        "text": "Processed [Summarize analysis: Computed mean was 20.0] in batch of 1"
      }
    }
  ]
}
```
 
---
 
## Verification & Stress Tests
 
| Metric / Scenario | Result | Guarantee |
| :--- | :--- | :--- |
| Full test suite | 29 / 29 passed (8.44 s) | Passes across all 4 development phases |
| Topological sort | < 1.2 ms for 100-node DAGs | Cycle detection via Kahn's algorithm |
| GPU dynamic batching | 2.87x lower latency, 2.63x higher token throughput | 30 ms window, no VRAM thrashing |
| Worker drop | Immediate lease reclamation | Stale locks recovered to `READY` |
| Poison pill | Auto-routed to DLQ | Failure recorded without deadlocks |
 
---
 
## Benchmark: Dynamic GPU Batching vs Sequential
 
**Workload:** 16 concurrent math-summary prompts against `qwen2.5:3b` on an NVIDIA GeForce RTX 5050 Laptop GPU (8 GB VRAM, 4096-token context).
 
| Execution Mode | Queue Window (Δt) | Batch Size | Total Pipeline Latency | Avg Token Speed | GPU VRAM |
| :--- | :--- | :--- | :--- | :--- | :--- |
| **Sequential (1x1)** | N/A | 1 | 8,940 ms | ~44.8 tok/s | 2.1 GiB |
| **Dynamic Batching** | 30 ms | 4 | **3,110 ms** | **~118.2 tok/s** | 3.4 GiB |
| **Improvement** | | | **2.87x faster** | **2.63x higher** | +1.3 GiB |
 
---
 
## Key Systems Engineering Takeaways
 
### A. Distributed State Machines & Zombie Recovery
* **Lease invalidation on lifecycle events**: When a worker halts abruptly (SIGKILL, crash, container restart), its in-flight tasks stay `RUNNING` in the database. Without heartbeat expiry or boot-time lease reconciliation, polling loops stall on runs that no worker owns.
* **Defensive state queries**: Pollers must not rely on `workflow_runs.status == 'RUNNING'` alone. Each must verify that at least one task is eligible (`READY` or non-stale `PENDING`), or run an orphan-reaping sweep on boot.
### B. Async Concurrency vs. gRPC Streaming
* **Event loop decoupling**: Background batching loops (`asyncio.create_task`) need lazy initialization or an explicit service lifecycle. If requests await a future (`await req.future`) fed through an `asyncio.Queue`, the consumer loop must be running before ingestion, or futures wait forever without raising.
* **Streaming updates**: `ExecutionStreamUpdate` lets workers push incremental stdout and telemetry without buffering artifacts in memory or writing a database row per chunk. Database writes are reserved for discrete state transitions (`READY` -> `RUNNING` -> `COMPLETED`).
### C. Host-to-Container Networking
* **Loopback pitfalls**: Inside Docker, `127.0.0.1` is the container's own namespace. To reach a host service such as Ollama, bind it to `0.0.0.0` (`OLLAMA_HOST=0.0.0.0:11434`), resolve the host via `host.docker.internal`, and add the matching `extra_hosts` entry in the container spec.
---
 
## Project Layout
 
```
auraflow/
├── .github/workflows/ci.yml        # CI: tests, linting, type checks
├── docker-compose.yml              # Cluster orchestration
├── Dockerfile                      # Multi-stage production image
├── pyproject.toml                  # Packaging, pytest, ruff & mypy config
├── requirements.txt                # Pinned dependencies
├── src/auraflow/
│   ├── api/routes.py               # REST control plane endpoints
│   ├── core/
│   │   ├── database.py             # SQLAlchemy async engine & sessionmaker
│   │   ├── engine.py               # Parameter resolution & workflow lifecycle
│   │   ├── graph.py                # DAG validation & Kahn's topological sort
│   │   ├── metrics.py              # Prometheus metrics
│   │   ├── runner.py               # Concurrent execution loop
│   │   ├── scheduler.py            # SKIP LOCKED scheduling & crash recovery
│   │   └── security.py             # HMAC signing & payload sanitization
│   ├── models/execution.py         # Database models
│   ├── proto/                      # gRPC stubs
│   ├── schemas/workflow.py         # Pydantic schemas
│   └── worker/
│       ├── batcher.py              # Dynamic GPU batch scheduling
│       ├── dispatcher.py           # Task coordinator & state tracking
│       ├── gpu_manager.py          # PyTorch VRAM monitoring & caching
│       ├── sandbox.py              # Subprocess execution sandbox
│       └── service.py              # gRPC worker daemon
└── tests/                          # 29 end-to-end and chaos tests
```
 
---
 
## License
 
Licensed under the MIT License. See [LICENSE](LICENSE).
 