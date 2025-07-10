import asyncio
import sys
from pathlib import Path
from typing import Literal

import structlog
from argdantic import ArgField, ArgParser
from pyodm.api import TaskStatus

from odm_tools.service import ProcessingService
from odm_tools.utils import setup_logging

cli = ArgParser(name="odm-tool", description="ODM Tools - Drone imagery orthorectification CLI")


@cli.command()
def process(
    request_path: Path = ArgField("-r", description="Path to the request to process"),
    dry_run: bool = ArgField("-d", default=False, description="Execute a first check without processing anything"),
    log_level: Literal["debug", "info", "warning"] = ArgField(
        "-l", default="info", description="Log level for stdout"
    ),
) -> None:
    setup_logging(log_level=log_level)

    try:
        service = ProcessingService()
        exit_code = asyncio.run(service.process_request(request_path, dry_run))
        sys.exit(exit_code)
    except KeyboardInterrupt:
        # Handle Ctrl+C during startup/teardown
        structlog.get_logger().info("Interrupted during startup")
        sys.exit(2)


@cli.command()
def cleanup(
    request_path: Path | None = ArgField("-r", default=None, description="Path to the request to process"),
    task_status: list[Literal["queued", "running", "completed", "failed"]] = ArgField(
        "-s", default=None, description="Cleanup tasks only when they appear in the given status"
    ),
    dry_run: bool = ArgField("-d", default=False, description="Execute a first check without processing anything"),
    log_level: Literal["debug", "info", "warning"] = ArgField(
        "-l", default="info", description="Log level for stdout"
    ),
) -> None:
    setup_logging(log_level=log_level)

    # Convert string statuses to TaskStatus enums
    statuses = [TaskStatus[s.upper()] for s in task_status] if task_status else None

    try:
        service = ProcessingService()
        exit_code, _ = asyncio.run(service.cleanup_tasks(request_path, statuses, dry_run))
        sys.exit(exit_code)
    except KeyboardInterrupt:
        # Handle Ctrl+C during startup/teardown
        structlog.get_logger().info("Interrupted during startup")
        sys.exit(2)


@cli.command()
def list(
    request_path: Path | None = ArgField("-r", default=None, description="Path to the request to process"),
    task_status: list[Literal["queued", "running", "completed", "failed"]] = ArgField(
        "-s", default=None, description="Cleanup tasks only when they appear in the given status"
    ),
    log_level: Literal["debug", "info", "warning"] = ArgField(
        "-l", default="info", description="Log level for stdout"
    ),
):
    setup_logging(log_level=log_level)
    log = structlog.get_logger()

    # Convert string statuses to TaskStatus enums
    statuses = [TaskStatus[s.upper()] for s in task_status] if task_status else None

    try:
        service = ProcessingService()
        exit_code, task_infos = asyncio.run(service.list_tasks(request_path, statuses))

        # Print task information
        for info in task_infos:
            log.info(
                f"Task {info.uuid}",
                name=info.name,
                status=info.status.name,
                created_at=info.date_created,
            )

        sys.exit(exit_code)
    except KeyboardInterrupt:
        # Handle Ctrl+C during startup/teardown
        structlog.get_logger().info("Interrupted during startup")
        sys.exit(2)


def main():
    cli()


if __name__ == "__main__":
    main()
