from dataclasses import dataclass
from temporalio import activity
from temporalio.client import Client


async def get_temporal_client() -> Client:
    return await Client.connect("localhost:7233")


@dataclass
class GetRunningWorkflows:
    count: int


@activity.defn
async def get_running_download_workflows() -> GetRunningWorkflows:
    client = await get_temporal_client()

    execution_count = await client.count_workflows(
        "WorkflowType='DownloadWorkflow' AND ExecutionStatus='Running'"
    )
    return GetRunningWorkflows(count=execution_count.count)
