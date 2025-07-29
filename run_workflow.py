import asyncio

from temporalio.common import WorkflowIDReusePolicy

from temporalio.client import Client

from workflows import MasterWorkflow


async def test_normal_execution(client: Client) -> None:
    # Start the workflow
    print("Start MasterWorkflow")
    await client.start_workflow(
        MasterWorkflow.run,
        id="master-workflow",
        task_queue="region",
        id_reuse_policy=WorkflowIDReusePolicy.TERMINATE_IF_RUNNING,
    )
    await asyncio.sleep(25)


async def test_signal_sent_before_calculating_files(client: Client) -> None:
    # Start the workflow
    await client.start_workflow(
        MasterWorkflow.run,
        id="master-workflow",
        task_queue="region",
        id_reuse_policy=WorkflowIDReusePolicy.TERMINATE_IF_RUNNING,
    )

    await asyncio.sleep(5)

    await client.start_workflow(
        MasterWorkflow.run,
        id="master-workflow",
        task_queue="region",
        id_reuse_policy=WorkflowIDReusePolicy.TERMINATE_IF_RUNNING,
    )


async def main():
    # Create client connected to server at the given address
    client = await Client.connect("localhost:7233")

    await test_normal_execution(client)

    await test_signal_sent_before_calculating_files(client)


if __name__ == "__main__":
    asyncio.run(main())
