import asyncio

from temporalio.client import Client
from temporalio.worker import Worker

from activities import get_running_download_workflows
from workflows import CleanupWorkflow, DownloadWorkflow, MasterWorkflow


async def main():
    client = await Client.connect("localhost:7233", namespace="default")
    # Run the worker
    worker = Worker(
        client,
        task_queue="region",
        workflows=[DownloadWorkflow, CleanupWorkflow, MasterWorkflow],
        activities=[get_running_download_workflows],
    )
    await worker.run()


if __name__ == "__main__":
    asyncio.run(main())
