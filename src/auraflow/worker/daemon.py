import asyncio
import logging
import os
import signal
import sys
import grpc
from sqlalchemy import select

from auraflow.core.database import AsyncSessionLocal
from auraflow.core.runner import WorkflowExecutionRunner
from auraflow.models.execution import WorkflowRun, WorkflowStatus
from auraflow.proto import worker_pb2_grpc
from auraflow.worker.service import AuraFlowWorkerServicer

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] [%(name)s] %(message)s",
)
logger = logging.getLogger("auraflow.worker.daemon")


async def run_daemon():
    grpc_port = os.getenv("GRPC_PORT", "50051")
    poll_interval = float(os.getenv("WORKER_POLL_INTERVAL", "1.5"))

    # 1. Start gRPC Worker Service
    server = grpc.aio.server()
    servicer = AuraFlowWorkerServicer(worker_id=f"daemon-{os.getpid()}")
    await servicer.start()
    worker_pb2_grpc.add_WorkerServiceServicer_to_server(servicer, server)

    server.add_insecure_port(f"0.0.0.0:{grpc_port}")
    await server.start()
    logger.info(f"gRPC Worker Servicer started and listening on port {grpc_port}")

    channel = grpc.aio.insecure_channel(f"127.0.0.1:{grpc_port}")
    worker_stub = worker_pb2_grpc.WorkerServiceStub(channel)

    stop_event = asyncio.Event()

    def handle_shutdown():
        logger.info("Shutdown signal received. Stopping worker daemon...")
        stop_event.set()

    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, handle_shutdown)
        except NotImplementedError:
            # Windows does not support asyncio signal handlers for SIGTERM
            pass

    logger.info("Worker daemon ready. Polling for active RUNNING workflow runs...")

    try:
        while not stop_event.is_set():
            active_run_id = None

            async with AsyncSessionLocal() as session:
                stmt = (
                    select(WorkflowRun.id)
                    .where(WorkflowRun.status == WorkflowStatus.RUNNING.value)
                    .order_by(WorkflowRun.created_at.asc())
                    .limit(1)
                )
                res = await session.execute(stmt)
                active_run_id = res.scalar_one_or_none()

            if active_run_id:
                logger.info(f"Discovered active workflow run [{active_run_id}]. Starting runner...")
                async with AsyncSessionLocal() as session:
                    runner = WorkflowExecutionRunner(session, worker_stub)
                    try:
                        final_run = await runner.run_until_completion(active_run_id)
                        logger.info(
                            f"Workflow run [{active_run_id}] finished with status: {final_run.status}"
                        )
                    except Exception as e:
                        logger.error(f"Error executing run [{active_run_id}]: {e}", exc_info=True)
            else:
                try:
                    await asyncio.wait_for(stop_event.wait(), timeout=poll_interval)
                except asyncio.TimeoutError:
                    pass
    finally:
        logger.info("Cleaning up worker resources...")
        await channel.close()
        await server.stop(grace=1)
        await servicer.stop()
        logger.info("Worker daemon gracefully stopped.")


if __name__ == "__main__":
    try:
        asyncio.run(run_daemon())
    except (KeyboardInterrupt, SystemExit):
        sys.exit(0)