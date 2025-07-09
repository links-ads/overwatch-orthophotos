import asyncio
import sys
from pathlib import Path
from typing import Literal

import structlog
from argdantic import ArgField, ArgParser
from pyodm.api import TaskStatus

from odm_tools.processor import ODMProcessor
from odm_tools.utils import setup_logging, validate_request_structure

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
    log = structlog.get_logger()

    validate_request_structure(request_path)
    log.info("Starting processing", request_path=str(request_path), dry_run=dry_run)

    if dry_run:
        log.info("Dry run completed - request structure is valid")
        return

    workflow = ODMProcessor()
    try:
        workflow.check_node_availability()
        asyncio.run(workflow.process_request(request_path))
        log.info("Processing completed successfully")
    except Exception as e:
        log.error("Processing failed", error=str(e))
        sys.exit(1)


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
    log = structlog.get_logger()
    log.info("Starting cleanup", request_path=str(request_path), dry_run=dry_run, statuses=task_status)

    workflow = ODMProcessor()
    statuses = [TaskStatus[s.upper()] for s in task_status] if task_status else None
    try:
        workflow.check_node_availability()
        removed_tasks = asyncio.run(
            workflow.clear_tasks(
                request_path,
                statuses=statuses,
                dry_run=dry_run,
            )
        )
        log.info("Tasks removed", removed_tasks=removed_tasks)
        log.info("Cleanup completed successfully")
    except Exception as e:
        log.error("Cleanup failed", error=str(e))
        sys.exit(1)


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
    log.info("Listing tasks", request_path=str(request_path), statuses=task_status)

    workflow = ODMProcessor()
    statuses = [TaskStatus[s.upper()] for s in task_status] if task_status else None
    try:
        workflow.check_node_availability()
        task_infos = asyncio.run(workflow.list_tasks(request_path=request_path, statuses=statuses))
        for info in task_infos:
            log.info(
                f"Task {info.uuid}",
                name=info.name,
                status=info.status.name,
                created_at=info.date_created,
            )

    except Exception as e:
        log.error("List failed", error=str(e))
        sys.exit(1)


def main():
    cli()
