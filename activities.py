from dataclasses import dataclass
from temporalio.client import Client
from temporalio import activity

SHA_PREFIX_LENGTH = 7


class BaseActivity:
    def __init__(self, client: Client) -> None:
        self.temporal_client = client


@dataclass
class WorkflowShasParam:
    shas_to_download: list[str]


class MyActivity(BaseActivity):
    @activity.defn
    async def cancel_not_matching_workflows(self, param: WorkflowShasParam) -> None:
        shas_to_download = [sha[:SHA_PREFIX_LENGTH] for sha in param.shas_to_download]

        async for wf in self.temporal_client.list_workflows(
            query="WorkflowType='DownloadWorkflow' AND ExecutionStatus='Running'"
        ):
            sha = wf.id.removeprefix("download-workflow-")
            if sha not in shas_to_download:
                handle = self.temporal_client.get_workflow_handle(wf.id)
                print(f"Canceling workflow with id: {wf.id}")
                await handle.cancel()
