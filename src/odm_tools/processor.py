import asyncio
from collections.abc import Iterable
from pathlib import Path

import structlog
from pyodm import Node, Task
from pyodm.api import TaskInfo, TaskStatus
from pyodm.exceptions import NodeConnectionError, NodeResponseError

from odm_tools.config import settings
from odm_tools.models import DataType, ProcessingOptions, ProcessingRequest, TaskTracker
from odm_tools.notifier import AsyncRabbitMQNotifier
from odm_tools.utils import find_images

log = structlog.get_logger()


class ProcessingError(Exception):
    """
    Base exception for processing errors.
    """


class ODMProcessor:
    """
    Main workflow orchestrator for ODM processing using PyODM.
    """

    def __init__(self):
        self.node = Node(
            host=settings.nodeodm.host,
            port=settings.nodeodm.port,
            token=settings.nodeodm.token,
        )
        self.active_tasks: dict[str, TaskTracker] = {}
        self.notifier = AsyncRabbitMQNotifier()
        # self.uploader = CKANUploader()

    async def process_request(
        self,
        request_path: Path,
        quality: str = "medium",
        dsm: bool = True,
        dtm: bool = False,
    ) -> None:
        options = ProcessingOptions(quality=quality, dsm=dsm, dtm=dtm)
        await self.process_request_with_options(request_path, options)

    def check_node_availability(self) -> None:
        try:
            node_info = self.node.info()
            log.info(
                "NodeODM server info",
                version=node_info.version,
                engine=node_info.engine,
                queue_count=node_info.task_queue_count,
            )
        except (NodeConnectionError, NodeResponseError) as e:
            raise ProcessingError(f"NodeODM server is not available: {e}")

    def load_request_metadata(self, request_path: Path) -> ProcessingRequest:
        try:
            request = ProcessingRequest.from_file(request_path / "request.json")
            return request
        except Exception as e:
            raise ProcessingError(f"Invalid request metadata: {e}")

    async def get_existing_tasks(self, statuses: Iterable[TaskStatus] | None = None) -> dict[str, Task]:
        existing_tasks = {}
        task_list: dict = self.node.get("task/list")  # type: ignore
        for summary in task_list:
            task = self.node.get_task(summary["uuid"])
            task_info = task.info()
            if statuses is not None:
                if task_info.status not in statuses:
                    continue
            existing_tasks[task_info.name] = task
        return existing_tasks

    async def list_tasks(self, request_path: Path | None, statuses: list[TaskStatus] | None = None) -> list[TaskInfo]:
        request = None
        tasks = await self.get_existing_tasks(statuses=statuses)
        if request_path is not None:
            request = self.load_request_metadata(request_path)
            filtered_tasks = []
            for task_name, task in tasks.items():
                task_request = task_name.split("_", 1)[0]
                if task_request != request.request_id:
                    log.debug(f"Skipping task '{task_name}'", request=request.request_id)
                    continue
                filtered_tasks.append(task)
            return filtered_tasks
        return [t.info() for t in tasks.values()]

    async def process_request_with_options(self, request_path: Path, options: ProcessingOptions) -> None:
        async with self.notifier:
            request = self.load_request_metadata(request_path)
            # Print info about the request
            log.info(
                "Processing request",
                request_id=request.request_id,
                datatype_ids=request.datatype_ids,
                options=options.model_dump(),
            )

            # Find datatype directories and validate images
            datatype_groups = []
            for datatype_id in request.datatype_ids:
                datatype_path = request_path / DataType(datatype_id).name
                if not datatype_path.exists():
                    log.warning("Datatype directory not found", datatype_id=datatype_id, path=str(datatype_path))
                    continue
                images = find_images(datatype_path)
                if not images:
                    log.warning(
                        "No images found in datatype directory", datatype_id=datatype_id, path=str(datatype_path)
                    )
                    continue
                datatype_groups.append((datatype_id, datatype_path, images))
            if not datatype_groups:
                raise ProcessingError("No valid datatype directories found")

            # Create PyODM tasks
            odm_tasks = []
            # First, retrieve existing tasks so that we do not restart an existing one
            try:
                existing_tasks = await self.get_existing_tasks()
            except (NodeConnectionError, NodeResponseError) as e:
                log.error("Failed to retrieve the list of tasks", error=str(e))
                raise ProcessingError("Failed to retrieve the list of tasks")

            for datatype_id, datatype_path, images in datatype_groups:
                task_name = f"{request.request_id}_{DataType(datatype_id).name}"
                if task_name in existing_tasks:
                    log.info(f"Task '{task_name}' already created, tracking...", task_name=task_name)
                    old_task = existing_tasks[task_name]
                    odm_tasks.append(old_task)
                    tracker = TaskTracker(
                        pyodm_task_id=old_task.uuid,
                        request_id=request.request_id,
                        datatype_id=datatype_id,
                        datatype_name=DataType(datatype_id).name,
                    )
                    self.active_tasks[old_task.uuid] = tracker
                    continue

                log.info("Creating PyODM task", datatype_id=datatype_id, image_count=len(images), task_name=task_name)
                try:
                    # Convert images to list of strings for PyODM
                    image_paths = [str(img) for img in images]
                    # Create task using PyODM
                    task = self.node.create_task(
                        files=image_paths,
                        options=options.to_pyodm_options(),
                        name=task_name,
                    )
                    # Track our task
                    task_tracker = TaskTracker(
                        pyodm_task_id=task.uuid,
                        request_id=request.request_id,
                        datatype_id=datatype_id,
                        datatype_name=DataType(datatype_id).name,
                    )

                    self.active_tasks[task.uuid] = task_tracker
                    odm_tasks.append(task)
                    log.info("PyODM task created", task_id=task.uuid, datatype_name=task_tracker.datatype_name)
                    await self.notifier.send_task_start(request_id=request.request_id, datatype_id=datatype_id)

                except (NodeConnectionError, NodeResponseError) as e:
                    log.error("Failed to create PyODM task", datatype_id=datatype_id, error=str(e))
                    raise ProcessingError(f"Failed to create task for datatype {datatype_id}: {e}")

            if not odm_tasks:
                raise ProcessingError("No tasks were created")

            # Monitor tasks until completion
            await self.monitor_tasks(odm_tasks, request)
            completed_tasks, failed_tasks = await self.process_completed_tasks(odm_tasks, request)
            log.info("Monitoring completed", completed=completed_tasks, failed=failed_tasks)

    async def monitor_tasks(self, tasks: list, request: ProcessingRequest) -> None:
        log.info("Starting task monitoring", task_count=len(tasks))
        completed_count = 0
        total_tasks = len(tasks)
        total_retries = 0

        while completed_count < total_tasks and total_retries < settings.nodeodm.poll_retries:
            for task in tasks:
                try:
                    task_info = task.info()
                    task_tracker = self.active_tasks[task.uuid]

                    # Log status changes
                    log.info(
                        "Task status update",
                        task_id=task.uuid,
                        datatype=task_tracker.datatype_name,
                        status=task_info.status.name,
                        progress=task_info.progress,
                        total_tasks=total_tasks,
                        completed_count=completed_count,
                    )
                    # Send appropriate status notification based on task status
                    if task_info.status == TaskStatus.RUNNING:
                        await self.notifier.send_task_update(
                            request_id=request.request_id,
                            datatype_id=task_tracker.datatype_id,
                            message=f"Processing in progress - {task_info.progress}% complete",
                        )
                    elif task_info.status == TaskStatus.FAILED:
                        log.error("Task failed", task_id=task.uuid, error=task_info.last_error)
                        completed_count += 1
                    elif task_info.status == TaskStatus.COMPLETED:
                        # Don't send updates for QUEUED (already sent) or COMPLETED (will be sent in process_completed_tasks)
                        log.info(
                            "Task completed successfully",
                            task_id=task.uuid,
                            processing_time=task_info.processing_time,
                        )
                        completed_count += 1
                    elif task_info.status == TaskStatus.CANCELED:
                        await self.notifier.send_task_end(
                            request_id=request.request_id,
                            datatype_id=task_tracker.datatype_id,
                            message="ODM Task cancelled",
                        )
                        completed_count += 1
                    else:
                        # send something, just to feel alive
                        await self.notifier.send_task_update(
                            request_id=request.request_id,
                            datatype_id=task_tracker.datatype_id,
                            message=f"ODM Task status: {task_info.status}",
                        )

                except (NodeConnectionError, NodeResponseError) as e:
                    log.warning("Failed to get task info", task_id=task.uuid, error=str(e))
                    total_retries += 1
                    continue

            await asyncio.sleep(settings.nodeodm.poll_interval)

    async def process_completed_tasks(self, tasks: list, request: ProcessingRequest) -> tuple[int, int]:
        log.info("Processing completed tasks")
        completed_tasks = 0
        failed_tasks = 0

        for task in tasks:
            try:
                task_info = task.info()
                task_tracker = self.active_tasks[task.uuid]

                if task_info.status == TaskStatus.COMPLETED:
                    # Create output directory
                    output_dir = request.path / "outputs" / task_tracker.datatype_name
                    output_dir.mkdir(parents=True, exist_ok=True)
                    log.info("Downloading task results", task_id=task.uuid, output_dir=str(output_dir))
                    result_path = Path(task.download_assets(str(output_dir)))
                    task_tracker.output_path = result_path
                    log.info("Task results downloaded", task_id=task.uuid, result_path=result_path)

                    # Find result files for upload
                    result_files = []
                    if result_path and result_path.exists():
                        ortho_path = result_path / "odm_orthophoto" / "odm_orthophoto.tif"
                        report_path = result_path / "odm_report" / "odm_report.pdf"
                        result_files.extend([ortho_path, report_path])

                    # Upload to CKAN if configured
                    if result_files:
                        try:
                            assert all(f.exists() for f in result_files)
                            # TODO: implement upload
                            # upload_result = self.uploader.upload_processing_results(
                            #     request, task_tracker, result_files
                            # )
                            upload_result = {"dataset_id": "none"}
                            if upload_result:
                                log.info(
                                    "Results uploaded",
                                    task_id=task.uuid,
                                    dataset_id=upload_result["dataset_id"],
                                )
                                await self.notifier.send_task_end(
                                    request_id=request.request_id,
                                    datatype_id=task_tracker.datatype_id,
                                )
                        except Exception as e:
                            log.error("CKAN upload failed", task_id=task.uuid, error=str(e))
                            await self.notifier.send_task_error(
                                request_id=request.request_id,
                                datatype_id=task_tracker.datatype_id,
                                message="Data upload failed",
                            )
                    completed_tasks += 1

                elif task_info.status == TaskStatus.FAILED:
                    await self.notifier.send_task_error(
                        request_id=request.request_id,
                        datatype_id=task_tracker.datatype_id,
                        message="ODM Task failed",
                    )
                    log.error("Task failed, cannot download results", task_id=task.uuid, error=task_info.last_error)
                    failed_tasks += 1

            except Exception as e:
                log.error("Error processing completed task", task_id=task.uuid, error=str(e))
                failed_tasks += 1
        return completed_tasks, failed_tasks

    async def clear_tasks(
        self,
        request_path: Path | None,
        statuses: Iterable[TaskStatus] | None = None,
        dry_run: bool = False,
    ) -> list[str]:
        request = None
        if request_path is not None:
            request = self.load_request_metadata(request_path)

        tasks = await self.get_existing_tasks(statuses=statuses)
        removed = []

        for task_name, task in tasks.items():
            task_request = task_name.split("_")[0]  # the request id is the first part of the task name
            if request is not None and task_request != request.request_id:
                log.info(
                    f"Skipping task '{task_name}', not associated with the request",
                    request_id=request.request_id,
                )
                continue
            if dry_run:
                log.info(f"Would remove '{task_name}'", request=request, task=task.uuid)
            if task.remove():
                removed.append(task.uuid)

        return removed

    def __enter__(self):
        """Context manager entry."""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit - cleanup resources."""
        # self.uploader.close()
