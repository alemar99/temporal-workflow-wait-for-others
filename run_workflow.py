import asyncio

from temporalio.common import WorkflowIDReusePolicy

from temporalio.client import Client

from workflows import (
    MASTER_WORKFLOW_ID,
    DownloadFileParam,
    MasterInputParam,
    MasterWorkflow,
)


async def test_normal_execution(client: Client) -> None:
    """Normal execution.

    Console output:
    Start MasterWorkflow
    Files to download calculated
    Start download wf for file: 0000000
    Signal received for file: 0000000
    File 0000000 completed download.
    Start CleanupWorkflow
    End CleanupWorkflow
    End MasterWorkflow
    Start the workflow
    """
    print("Start MasterWorkflow")
    await client.start_workflow(
        MasterWorkflow.run,
        arg=MasterInputParam(
            startup_sleep_duration=3,
            files=[DownloadFileParam(duration=5, sha256="0" * 64)],
        ),
        id=MASTER_WORKFLOW_ID,
        task_queue="region",
        id_reuse_policy=WorkflowIDReusePolicy.TERMINATE_IF_RUNNING,
    )
    await asyncio.sleep(10)


async def test_signal_sent_before_calculating_files(client: Client) -> None:
    """A signal is sent when files to download are being calculated. This is an
    edge case where the download workflow for that file will be started again.

    Console output:
    Start MasterWorkflow
    Files to download calculated
    Start download wf for file: 0000000
    Start download wf for file: 1111111
    Start MasterWorkflow
    Signal received for file: 0000000
    File 0000000 completed download.
    Files to download calculated
    Start download wf for file: 0000000
    Download wf for 1111111 already running
    Signal received for file: 0000000
    File 0000000 completed download.
    Signal received for file: 1111111
    File 1111111 completed download.
    Start CleanupWorkflow
    End CleanupWorkflow
    End MasterWorkflow
    Start the workflow
    """
    await client.start_workflow(
        MasterWorkflow.run,
        arg=MasterInputParam(
            startup_sleep_duration=3,
            files=[
                DownloadFileParam(duration=3, sha256="0" * 64),
                DownloadFileParam(duration=10, sha256="1" * 64),
            ],
        ),
        id=MASTER_WORKFLOW_ID,
        task_queue="region",
        id_reuse_policy=WorkflowIDReusePolicy.TERMINATE_IF_RUNNING,
    )

    await asyncio.sleep(5)

    await client.start_workflow(
        MasterWorkflow.run,
        arg=MasterInputParam(
            startup_sleep_duration=3,
            files=[
                DownloadFileParam(duration=3, sha256="0" * 64),
                DownloadFileParam(duration=10, sha256="1" * 64),
            ],
        ),
        id=MASTER_WORKFLOW_ID,
        task_queue="region",
        id_reuse_policy=WorkflowIDReusePolicy.TERMINATE_IF_RUNNING,
    )


async def test_wf_should_be_cancelled(client: Client) -> None:
    """A download workflow must be canceled as the file isn't needed anymore.

    Console output:
    Start MasterWorkflow
    Files to download calculated
    Start download wf for file: 0000000
    Start download wf for file: 1111111
    Start MasterWorkflow
    Files to download calculated
    Canceling workflow with id: download-workflow-0000000
    Download wf for 1111111 already running
    Signal received for file: 1111111
    File 1111111 completed download.
    Start CleanupWorkflow
    End CleanupWorkflow
    End MasterWorkflow
    Start the workflow
    """
    await client.start_workflow(
        MasterWorkflow.run,
        arg=MasterInputParam(
            startup_sleep_duration=3,
            files=[
                DownloadFileParam(duration=10, sha256="0" * 64),
                DownloadFileParam(duration=10, sha256="1" * 64),
            ],
        ),
        id=MASTER_WORKFLOW_ID,
        task_queue="region",
        id_reuse_policy=WorkflowIDReusePolicy.TERMINATE_IF_RUNNING,
    )

    await asyncio.sleep(5)

    await client.start_workflow(
        MasterWorkflow.run,
        arg=MasterInputParam(
            startup_sleep_duration=3,
            files=[
                DownloadFileParam(duration=10, sha256="1" * 64),
            ],
        ),
        id=MASTER_WORKFLOW_ID,
        task_queue="region",
        id_reuse_policy=WorkflowIDReusePolicy.TERMINATE_IF_RUNNING,
    )


async def main():
    # Create client connected to server at the given address
    client = await Client.connect("localhost:7233")

    await test_normal_execution(client)

    await test_signal_sent_before_calculating_files(client)

    await test_wf_should_be_cancelled(client)


if __name__ == "__main__":
    asyncio.run(main())
