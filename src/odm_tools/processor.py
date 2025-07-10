import asyncio
from collections.abc import Iterable
from pathlib import Path

import structlog
from pyodm import Node, Task
from pyodm.api import TaskInfo, TaskStatus
from pyodm.exceptions import NodeConnectionError, NodeResponseError

from odm_tools.config import settings
from odm_tools.io import FileManager
from odm_tools.models import DataType, ProcessingOptions, ProcessingRequest, TaskTracker
from odm_tools.notifier import AsyncRabbitMQNotifier
from odm_tools.uploader import CKANUploader

log = structlog.get_logger()


class ProcessingError(Exception):
    """
    Base exception for processing errors.
    """


class ProcessingCancelled(Exception):
    """
    Exception raised when processing is cancelled.
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
        self._cancel_tasks_on_shutdown = settings.nodeodm.cancel_on_shutdown
        self._shutdown_event = asyncio.Event()
        self._running_tasks: set[asyncio.Task] = set()
        self.active_tasks: dict[str, TaskTracker] = {}
        self.notifier = AsyncRabbitMQNotifier()
        self.uploader = CKANUploader()

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

    def _load_request_metadata(self, request_path: Path) -> ProcessingRequest:
        try:
            request = ProcessingRequest.from_file(request_path / "request.json")
            return request
        except Exception as e:
            raise ProcessingError(f"Invalid request metadata: {e}")

    async def _get_task_info_async(self, task: Task) -> TaskInfo:
        return await asyncio.to_thread(task.info)

    async def _create_task_async(self, files: list[str], options: dict, name: str) -> Task:
        return await asyncio.to_thread(self.node.create_task, files, options, name)

    async def _cancel_task_async(self, task: Task) -> bool:
        """Cancel a task on the ODM server."""
        try:
            return await asyncio.to_thread(task.cancel)
        except Exception as e:
            log.error("Failed to cancel ODM task", task_id=task.uuid, error=str(e))
            return False

    def _track_task(self, task: asyncio.Task):
        """Track an asyncio task for cancellation."""
        self._running_tasks.add(task)
        task.add_done_callback(self._running_tasks.discard)

    async def _get_existing_tasks(self, statuses: Iterable[TaskStatus] | None = None) -> dict[str, Task]:
        existing_tasks = {}
        task_list: dict = self.node.get("task/list")  # type: ignore
        for summary in task_list:
            task = self.node.get_task(summary["uuid"])
            task_info = await self._get_task_info_async(task)
            if statuses is not None:
                if task_info.status not in statuses:
                    continue
            existing_tasks[task_info.name] = task
        return existing_tasks

    async def _create_tasks(self, datatype_groups: list[tuple], request, options) -> list[Task]:
        odm_tasks = []
        # First, retrieve existing tasks so that we do not restart an existing one
        try:
            existing_tasks = await self._get_existing_tasks()
        except (NodeConnectionError, NodeResponseError) as e:
            log.error("Failed to retrieve the list of tasks", error=str(e))
            raise ProcessingError("Failed to retrieve the list of tasks")

        for datatype_id, _, images in datatype_groups:
            # Check for shutdown during task creation
            if self._shutdown_event.is_set():
                log.info("Shutdown requested during task creation")
                raise ProcessingCancelled("Task creation cancelled")

            task_name = f"{request.request_id}_{DataType(datatype_id).name}"
            log.info("Processing task", datatype_id=datatype_id, image_count=len(images), task_name=task_name)

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

            try:
                # Convert image paths to list of strings for PyODM
                image_paths = [str(img) for img in images]
                task = await self._create_task_async(
                    files=image_paths,
                    options=options.to_pyodm_options(),
                    name=task_name,
                )
                task_tracker = TaskTracker(
                    pyodm_task_id=task.uuid,
                    request_id=request.request_id,
                    datatype_id=datatype_id,
                    datatype_name=DataType(datatype_id).name,
                )
                self.active_tasks[task.uuid] = task_tracker
                odm_tasks.append(task)
                log.info("PyODM task created", task_id=task.uuid, datatype_name=task_tracker.datatype_name)

                # notify creation
                await self.notifier.send_task_start(request_id=request.request_id, datatype_id=datatype_id)

            except (NodeConnectionError, NodeResponseError) as e:
                log.error("Failed to create PyODM task", datatype_id=datatype_id, error=str(e))
                raise ProcessingError(f"Failed to create task for datatype {datatype_id}: {e}")
        return odm_tasks

    async def _process_results(self, request: ProcessingRequest, task: Task, task_info: TaskInfo):
        file_manager = FileManager(request.path)
        task_tracker = self.active_tasks[task_info.uuid]
        # Create output directory
        output_dir = file_manager.get_output_directory(task_tracker.datatype_id)
        log.info("Downloading task results", task_id=task.uuid, output_dir=str(output_dir))

        # Make download cancellable
        result_path = await asyncio.to_thread(task.download_assets, str(output_dir))
        result_path = Path(result_path)
        task_tracker.output_path = result_path
        log.info("Task results downloaded", task_id=task.uuid, result_path=result_path)

        # Find result files for upload
        result_files = file_manager.find_result_files(result_path)
        if not result_files:
            log.error(
                "Missing result files",
                request_id=request.request_id,
                datatype=task_tracker.datatype_name,
            )
            raise ProcessingError("Missing result files")
        try:
            assert all(p.exists() for p in result_files.values())
            datasets = self.uploader.upload_results(
                request=request,
                datatype_id=task_tracker.datatype_id,
                results=result_files,
            )
            return datasets
        except Exception as e:
            log.error("CKAN upload failed", task_id=task.uuid, error=str(e))
            await self.notifier.send_task_error(
                request_id=request.request_id,
                datatype_id=task_tracker.datatype_id,
                message="Data upload failed",
            )

    async def list_tasks(self, request_path: Path | None, statuses: list[TaskStatus] | None = None) -> list[TaskInfo]:
        request = None
        tasks = await self._get_existing_tasks(statuses=statuses)
        if request_path is not None:
            request = self._load_request_metadata(request_path)
            filtered_tasks = []
            for task_name, task in tasks.items():
                task_request = task_name.split("_", 1)[0]
                if task_request != request.request_id:
                    log.debug(f"Skipping task '{task_name}'", request=request.request_id)
                    continue
                filtered_tasks.append(task)
            return filtered_tasks
        return [t.info() for t in tasks.values()]

    async def _cancellable_sleep(self, duration: float):
        """Sleep that can be interrupted by shutdown event."""
        try:
            # Wait for either timeout or shutdown event
            await asyncio.wait_for(self._shutdown_event.wait(), timeout=duration)
            # If we get here, shutdown was requested
            raise ProcessingCancelled("Shutdown requested during sleep")
        except TimeoutError:
            # Normal timeout, continue
            pass

    async def _cancel_odm_tasks(self, tasks: list[Task]):
        if self._cancel_tasks_on_shutdown:
            log.warning("Letting tasks run", task_count=len(tasks))
            log.warning("Update the YAML config to cancel tasks on shutdown")
            return

        log.info("Cancelling ODM tasks", task_count=len(tasks))
        for task in tasks:
            try:
                task_info = await self._get_task_info_async(task)
                # Only cancel if task is still running
                if task_info.status in [TaskStatus.QUEUED, TaskStatus.RUNNING]:
                    success = await self._cancel_task_async(task)
                    if success:
                        log.info("ODM task cancelled", task_id=task.uuid)

                        # Send cancellation notification
                        if task.uuid in self.active_tasks:
                            tracker = self.active_tasks[task.uuid]
                            await self.notifier.send_task_end(
                                request_id=tracker.request_id,
                                datatype_id=tracker.datatype_id,
                                message="Task cancelled by user",
                            )
                    else:
                        log.warning("Failed to cancel ODM task", task_id=task.uuid)
                else:
                    log.debug(
                        "Task already completed, skipping cancellation",
                        task_id=task.uuid,
                        status=task_info.status.name,
                    )
            except Exception as e:
                log.error("Error during task cancellation", task_id=task.uuid, error=str(e))

    async def process_request_with_options(self, request_path: Path, options: ProcessingOptions) -> None:
        request = self._load_request_metadata(request_path)
        log.info(
            "Processing request",
            request_id=request.request_id,
            datatype_ids=request.datatype_ids,
            options=options.model_dump(),
        )

        # Find datatype directories and validate images
        async with self.notifier:
            try:
                file_manager = FileManager(request_path=request_path)
                datatype_groups = file_manager.validate_datatype_groups(request.datatype_ids)
                odm_tasks = await self._create_tasks(datatype_groups=datatype_groups, request=request, options=options)

                if not odm_tasks:
                    raise ProcessingError("No tasks were created")

                # Monitor tasks until completion
                await self.monitor_tasks(odm_tasks, request)
                completed_tasks, failed_tasks = await self.process_completed_tasks(odm_tasks, request)
                log.info("Monitoring completed", completed=completed_tasks, failed=failed_tasks)

            except ProcessingCancelled:
                log.info("Processing cancelled, cleaning up...")
                # Cancel any tasks that were created
                if "odm_tasks" in locals():
                    await self._cancel_odm_tasks(odm_tasks)
                raise
            except Exception as e:
                log.error("Processing failed", error=str(e))
                # Cancel any tasks that were created
                if "odm_tasks" in locals():
                    await self._cancel_odm_tasks(odm_tasks)
                raise

    async def monitor_tasks(self, tasks: list, request: ProcessingRequest) -> None:
        log.info("Starting task monitoring", task_count=len(tasks))
        completed_count = 0
        total_tasks = len(tasks)
        total_retries = 0

        while completed_count < total_tasks and total_retries < settings.nodeodm.poll_retries:
            # Check for shutdown at the beginning of each loop
            if self._shutdown_event.is_set():
                log.info("Shutdown requested during monitoring")
                await self._cancel_odm_tasks(tasks)
                raise ProcessingCancelled("Monitoring cancelled by user")

            for task in tasks:
                # Check for shutdown before processing each task
                if self._shutdown_event.is_set():
                    log.info("Shutdown requested during task monitoring")
                    await self._cancel_odm_tasks(tasks)
                    raise ProcessingCancelled("Task monitoring cancelled by user")

                try:
                    task_info = await self._get_task_info_async(task)
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
                        log.info(
                            "Task completed",
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

            # Use cancellable sleep instead of regular sleep
            try:
                await self._cancellable_sleep(settings.nodeodm.poll_interval)
            except ProcessingCancelled:
                await self._cancel_odm_tasks(tasks)
                raise

    async def process_completed_tasks(self, tasks: list, request: ProcessingRequest) -> tuple[int, int]:
        log.info("Processing completed tasks")
        completed_tasks = 0
        failed_tasks = 0

        for task in tasks:
            # Check for shutdown before processing each completed task
            if self._shutdown_event.is_set():
                log.info("Shutdown requested during completed task processing")
                raise ProcessingCancelled("Completed task processing cancelled")

            try:
                task_info = await self._get_task_info_async(task)
                task_tracker = self.active_tasks[task.uuid]

                if task_info.status == TaskStatus.COMPLETED:
                    upload_results = await self._process_results(
                        request=request,
                        task=task,
                        task_info=task_info,
                    )
                    if upload_results:
                        completed_tasks += 1
                        log.info(
                            "Results uploaded",
                            task_id=task.uuid,
                            datasets=len(upload_results),
                        )
                        await self.notifier.send_task_end(
                            request_id=request.request_id,
                            datatype_id=task_tracker.datatype_id,
                        )
                    else:
                        failed_tasks += 1
                        log.error("Upload failed", task_id=task.uuid, request=request.request_id)
                        await self.notifier.send_task_error(
                            request_id=request.request_id,
                            datatype_id=task_tracker.datatype_id,
                            message="Upload failed",
                        )

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
            request = self._load_request_metadata(request_path)

        tasks = await self._get_existing_tasks(statuses=statuses)
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
            else:
                success = await asyncio.to_thread(task.remove)
                if success:
                    removed.append(task.uuid)

        return removed

    async def shutdown(self):
        """Signal shutdown and wait for cleanup."""
        log.info("Shutdown requested, cleaning up...")
        self._shutdown_event.set()

        # Cancel all running asyncio tasks
        for task in self._running_tasks:
            if not task.done():
                task.cancel()

        # Wait for tasks to finish canceling
        if self._running_tasks:
            await asyncio.gather(*self._running_tasks, return_exceptions=True)

        log.info("Shutdown complete")
