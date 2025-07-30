import asyncio
from dataclasses import dataclass
from datetime import timedelta

from temporalio import workflow
from temporalio.common import WorkflowIDReusePolicy
from temporalio.exceptions import WorkflowAlreadyStartedError

from activities import SHA_PREFIX_LENGTH, MyActivity, WorkflowShasParam

MASTER_WORKFLOW_ID = "master-workflow"
CLEANUP_WORKFLOW_ID = "cleanup-workflow"


@dataclass
class DownloadFileParam:
    duration: int
    sha256: str


@dataclass
class MasterInputParam:
    startup_sleep_duration: int
    files: list[DownloadFileParam]


@workflow.defn
class DownloadWorkflow:
    @workflow.run
    async def run(self, input: DownloadFileParam) -> None:
        print(f"Start download wf for file: {input.sha256[:SHA_PREFIX_LENGTH]}")
        await asyncio.sleep(input.duration)
        handle = workflow.get_external_workflow_handle_for(
            MasterWorkflow.run, MASTER_WORKFLOW_ID
        )
        await handle.signal(MasterWorkflow.file_completed_download, input.sha256)
        print(f"File {input.sha256[:SHA_PREFIX_LENGTH]} completed download.")


@workflow.defn
class CleanupWorkflow:
    @workflow.run
    async def run(self) -> None:
        # simulate db work
        print("Start CleanupWorkflow")
        await asyncio.sleep(2)

        await self._do_cleanup()
        print("End CleanupWorkflow")

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
    async def run(self, input: MasterInputParam) -> None:
        print("Start MasterWorkflow")
        # calculate which files need to be downloaded
        await asyncio.sleep(input.startup_sleep_duration)
        print("Files to download calculated")
        self._files_to_download = {f.sha256 for f in input.files}

        await workflow.execute_activity(
            MyActivity.cancel_not_matching_workflows,
            WorkflowShasParam(shas_to_download=self._files_to_download),
            start_to_close_timeout=timedelta(seconds=30),
        )

        download_workflows = []
        for file in input.files:
            try:
                download_workflows.append(
                    # FIXME
                    await workflow.start_child_workflow(
                        DownloadWorkflow.run,
                        arg=file,
                        id=f"download-workflow-{file.sha256[:SHA_PREFIX_LENGTH]}",
                        parent_close_policy=workflow.ParentClosePolicy.ABANDON,
                    )
                )
            except WorkflowAlreadyStartedError:
                print(
                    f"Download wf for {file.sha256[:SHA_PREFIX_LENGTH]} already running"
                )
                workflow.logger.info(
                    "Some workflows were already running, skipping them..."
                )

        await asyncio.gather(*download_workflows)

        await workflow.wait_condition(lambda: self._files_to_download == set())

        # execute_child_workflow waits for the completion of child workflows
        await workflow.execute_child_workflow(
            CleanupWorkflow.run,
            id=CLEANUP_WORKFLOW_ID,
            parent_close_policy=workflow.ParentClosePolicy.TERMINATE,
            id_reuse_policy=WorkflowIDReusePolicy.TERMINATE_IF_RUNNING,
        )

        print("End MasterWorkflow")

    @workflow.signal
    def file_completed_download(self, sha256: str) -> None:
        print(f"Signal received for file: {sha256[:SHA_PREFIX_LENGTH]}")
        try:
            self._files_to_download.remove(sha256)
        except KeyError:
            # KeyError can happen if the signal is sent when the MasterWorkflow has
            # been restarted but it hasn't populated the files yet.
            pass
