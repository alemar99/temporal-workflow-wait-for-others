import asyncio
from datetime import timedelta

from temporalio import workflow
from temporalio.common import WorkflowIDReusePolicy
from temporalio.exceptions import WorkflowAlreadyStartedError

from activities import get_running_download_workflows


@workflow.defn
class DownloadWorkflow:
    @workflow.run
    async def run(self, duration: int) -> None:
        await asyncio.sleep(duration)


@workflow.defn
class CleanupWorkflow:
    @workflow.run
    async def run(self) -> None:
        # simulate db work
        await asyncio.sleep(2)

        await self._wait_for_download_workflows_to_finish()

        await self._do_cleanup()

    async def _wait_for_download_workflows_to_finish(self) -> None:
        workflow.logger.info("Wait for all DownloadWorkflow instances to complete")

        while True:
            running_download_workflows = await workflow.execute_activity(
                get_running_download_workflows,
                start_to_close_timeout=timedelta(30),
            )

            if running_download_workflows.count == 0:
                workflow.logger.info("No more DownloadWorkflow running")
                break

            workflow.logger.info(
                f"Still waiting for {running_download_workflows} DownloadWorkflow to finish"
            )

            await asyncio.sleep(5)

    async def _do_cleanup(self) -> None:
        workflow.logger.info("Starting cleanup")

        await asyncio.sleep(2)

        workflow.logger.info("Cleanup finished")


@workflow.defn
class MasterWorkflow:
    @workflow.run
    async def run(self) -> None:
        try:
            await workflow.start_child_workflow(
                DownloadWorkflow.run,
                args=[5],
                id="download-workflow-1",
                parent_close_policy=workflow.ParentClosePolicy.ABANDON,
            )
            await workflow.start_child_workflow(
                DownloadWorkflow.run,
                args=[10],
                id="download-workflow-2",
                parent_close_policy=workflow.ParentClosePolicy.ABANDON,
            )
            await workflow.start_child_workflow(
                DownloadWorkflow.run,
                args=[15],
                id="download-workflow-3",
                parent_close_policy=workflow.ParentClosePolicy.ABANDON,
            )
        except WorkflowAlreadyStartedError:
            workflow.logger.info(
                "Some workflows were already running, skipping them..."
            )

        # execute_child_workflow waits for the completion of child workflows
        await workflow.execute_child_workflow(
            CleanupWorkflow.run,
            id="cleanup-workflow",
            parent_close_policy=workflow.ParentClosePolicy.TERMINATE,
            id_reuse_policy=WorkflowIDReusePolicy.TERMINATE_IF_RUNNING,
        )
