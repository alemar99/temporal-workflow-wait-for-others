import asyncio

from temporalio.common import WorkflowIDReusePolicy

from temporalio.client import Client

from workflows import MasterWorkflow


async def main():
    # Create client connected to server at the given address
    client = await Client.connect("localhost:7233")

    # Execute a workflow
    await client.start_workflow(
        MasterWorkflow.run,
        id="master-workflow",
        task_queue="region",
        id_reuse_policy=WorkflowIDReusePolicy.TERMINATE_IF_RUNNING,
    )

    print("sleeping")
    await asyncio.sleep(7)
    print("restart")

    # Execute the same workflow again:
    # - running download workflows won't be stopped
    # - cleanup workflow will be stopped and restarted
    await client.start_workflow(
        MasterWorkflow.run,
        id="master-workflow",
        task_queue="region",
        id_reuse_policy=WorkflowIDReusePolicy.TERMINATE_IF_RUNNING,
    )


if __name__ == "__main__":
    asyncio.run(main())
