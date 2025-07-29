import asyncio

from temporalio import workflow
from temporalio.common import WorkflowIDReusePolicy
from temporalio.exceptions import WorkflowAlreadyStartedError


@workflow.defn
class DownloadWorkflow:
    @workflow.run
    async def run(self, duration: int, sha256: str) -> None:
        print(f"Start download wf for file: {sha256[:7]}")
        await asyncio.sleep(duration)
        handle = workflow.get_external_workflow_handle_for(
            MasterWorkflow.run, "master-workflow"
        )
        await handle.signal(MasterWorkflow.file_completed_download, sha256)
        print(f"File {sha256[:7]} completed download.")


@workflow.defn
class CleanupWorkflow:
    @workflow.run
    async def run(self) -> None:
        # simulate db work
        print("Start CleanupWorkflow")
        await asyncio.sleep(2)

        await self._do_cleanup()

    async def _do_cleanup(self) -> None:
        workflow.logger.info("Starting cleanup")

        await asyncio.sleep(2)

        workflow.logger.info("Cleanup finished")


@workflow.defn
class MasterWorkflow:
    def __init__(self) -> None:
        # keep the sha256 of the files that need to be downloaded
        self._files_to_download = set()

    @workflow.run
    async def run(self) -> None:
        print("Start MasterWorkflow")
        # calculate which files need to be downloaded
        await asyncio.sleep(3)
        print("Files to download calculated")
        self._files_to_download = {"1" * 64, "2" * 64, "3" * 64}

        download_workflows = []
        for sha, duration in zip(sorted(self._files_to_download), [3, 9, 20]):
            try:
                download_workflows.append(
                    await workflow.start_child_workflow(
                        DownloadWorkflow.run,
                        args=[duration, sha],
                        id=f"download-workflow-{sha[:7]}",
                        parent_close_policy=workflow.ParentClosePolicy.ABANDON,
                    )
                )
            except WorkflowAlreadyStartedError:
                print(f"Download wf for {sha[:7]} already running")
                workflow.logger.info(
                    "Some workflows were already running, skipping them..."
                )

        await asyncio.gather(*download_workflows)

        await workflow.wait_condition(lambda: self._files_to_download == set())

        # execute_child_workflow waits for the completion of child workflows
        await workflow.execute_child_workflow(
            CleanupWorkflow.run,
            id="cleanup-workflow",
            parent_close_policy=workflow.ParentClosePolicy.TERMINATE,
            id_reuse_policy=WorkflowIDReusePolicy.TERMINATE_IF_RUNNING,
        )

    @workflow.signal
    def file_completed_download(self, sha256: str) -> None:
        print(f"Signal received for file: {sha256[:7]}")
        try:
            self._files_to_download.remove(sha256)
        except KeyError:
            # KeyError can happen if the signal is sent when the MasterWorkflow has
            # been restarted but it hasn't populated the files yet.
            pass
