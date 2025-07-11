import asyncio
import signal
import sys
from pathlib import Path

import structlog
from pyodm.api import TaskStatus

from odm_tools.processor import ODMProcessor, ProcessingCancelledError
from odm_tools.utils import validate_request_structure

log = structlog.get_logger()


class GracefulShutdown:
    """Handles graceful shutdown on signals."""

    def __init__(self, processor: ODMProcessor):
        self.processor = processor
        self.shutdown_requested = False

    def request_shutdown(self, signum, _):
        """Signal handler for graceful shutdown."""
        if self.shutdown_requested:
            log.warning("Force shutdown requested")
            sys.exit(1)

        log.info("Graceful shutdown requested", signal=signum)
        self.shutdown_requested = True

        # Create a task to handle shutdown
        asyncio.create_task(self.processor.shutdown())


class ProcessingService:
    """Service layer for ODM processing operations."""

    def __init__(self):
        self.processor = ODMProcessor()

    async def process_request(self, request_path: Path, dry_run: bool = False) -> int:
        """
        Process a request with graceful shutdown support.

        Returns:
            0 for success, 1 for error, 2 for cancellation
        """
        try:
            # Validate request structure first
            validate_request_structure(request_path)
            log.info("Starting processing", request_path=str(request_path), dry_run=dry_run)

            if dry_run:
                log.info("Dry run completed - request structure is valid")
                return 0

            # Setup signal handlers for graceful shutdown
            shutdown_handler = GracefulShutdown(self.processor)
            signal.signal(signal.SIGINT, shutdown_handler.request_shutdown)  # Ctrl+C
            signal.signal(signal.SIGTERM, shutdown_handler.request_shutdown)  # Termination

            # Check node availability
            self.processor.check_node_availability()

            # Process the request
            await self.processor.process_request(request_path)

            log.info("Processing completed successfully")
            return 0

        except ProcessingCancelledError:
            log.info("Processing was cancelled by user")
            return 2
        except KeyboardInterrupt:
            log.info("Processing interrupted by user")
            return 2
        except Exception as e:
            log.error("Processing failed", error=str(e))
            return 1

    async def cleanup_tasks(
        self, request_path: Path | None = None, statuses: list[TaskStatus] | None = None, dry_run: bool = False
    ) -> tuple[int, list[str]]:
        """
        Clean up tasks with graceful shutdown support.

        Returns:
            Tuple of (exit_code, removed_task_ids)
        """
        try:
            log.info("Starting cleanup", request_path=str(request_path), dry_run=dry_run, statuses=statuses)

            # Setup signal handlers for graceful shutdown
            shutdown_handler = GracefulShutdown(self.processor)
            signal.signal(signal.SIGINT, shutdown_handler.request_shutdown)
            signal.signal(signal.SIGTERM, shutdown_handler.request_shutdown)

            self.processor.check_node_availability()

            removed_tasks = await self.processor.clear_tasks(
                request_path,
                statuses=statuses,
                dry_run=dry_run,
            )

            log.info("Tasks removed", removed_tasks=removed_tasks)
            log.info("Cleanup completed successfully")
            return 0, removed_tasks

        except ProcessingCancelledError:
            log.info("Cleanup was cancelled by user")
            return 2, []
        except KeyboardInterrupt:
            log.info("Cleanup interrupted by user")
            return 2, []
        except Exception as e:
            log.error("Cleanup failed", error=str(e))
            return 1, []

    async def list_tasks(
        self, request_path: Path | None = None, statuses: list[TaskStatus] | None = None
    ) -> tuple[int, list]:
        """
        List tasks with graceful shutdown support.

        Returns:
            Tuple of (exit_code, task_infos)
        """
        try:
            log.info("Listing tasks", request_path=str(request_path), statuses=statuses)

            # Setup signal handlers for graceful shutdown
            shutdown_handler = GracefulShutdown(self.processor)
            signal.signal(signal.SIGINT, shutdown_handler.request_shutdown)
            signal.signal(signal.SIGTERM, shutdown_handler.request_shutdown)

            self.processor.check_node_availability()

            task_infos = await self.processor.list_tasks(request_path=request_path, statuses=statuses)

            return 0, task_infos

        except ProcessingCancelledError:
            log.info("Task listing was cancelled by user")
            return 2, []
        except KeyboardInterrupt:
            log.info("Task listing interrupted by user")
            return 2, []
        except Exception as e:
            log.error("List failed", error=str(e))
            return 1, []


# Convenience functions for backward compatibility
async def process_request_with_shutdown(request_path: Path, dry_run: bool = False) -> int:
    """Convenience function for processing requests."""
    service = ProcessingService()
    return await service.process_request(request_path, dry_run)


async def cleanup_tasks_with_shutdown(
    request_path: Path | None = None, statuses: list[TaskStatus] | None = None, dry_run: bool = False
) -> tuple[int, list[str]]:
    """Convenience function for cleanup tasks."""
    service = ProcessingService()
    return await service.cleanup_tasks(request_path, statuses, dry_run)


async def list_tasks_with_shutdown(
    request_path: Path | None = None, statuses: list[TaskStatus] | None = None
) -> tuple[int, list]:
    """Convenience function for listing tasks."""
    service = ProcessingService()
    return await service.list_tasks(request_path, statuses)
