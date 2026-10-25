import asyncio
import sys
import grpc
from auraflow.core.database import AsyncSessionLocal
from auraflow.core.runner import WorkflowExecutionRunner
from auraflow.proto import worker_pb2_grpc
from auraflow.worker.service import AuraFlowWorkerServicer

async def main(run_id: str):
    server = grpc.aio.server()
    servicer = AuraFlowWorkerServicer(worker_id="local-worker-1")
    await servicer.start()
    worker_pb2_grpc.add_WorkerServiceServicer_to_server(servicer, server)

    port = server.add_insecure_port("127.0.0.1:50051")
    await server.start()
    print(f"[*] Worker gRPC server started on port {port}")

    channel = grpc.aio.insecure_channel(f"127.0.0.1:{port}")
    worker_stub = worker_pb2_grpc.WorkerServiceStub(channel)

    try:
        async with AsyncSessionLocal() as session:
            runner = WorkflowExecutionRunner(session, worker_stub)
            print(f"[*] Executing workflow run: {run_id} ...")
            final_run = await runner.run_until_completion(run_id)
            print(f"[+] Workflow status: {final_run.status}")

            for node in final_run.nodes:
                print(f"    - Node '{node.node_id}' ({node.task_type}): {node.status}")
                if node.result_output:
                    print(f"      Output: {node.result_output}")
    finally:
        await channel.close()
        await server.stop(grace=0)
        await servicer.stop()

if __name__ == "__main__":
    target_id = sys.argv[1] if len(sys.argv) > 1 else "5543e1b9-2b2f-428b-859e-c3f07e0c3c16"
    asyncio.run(main(target_id))