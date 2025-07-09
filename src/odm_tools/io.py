from pathlib import Path

import structlog

from odm_tools.models import DataType
from odm_tools.utils import find_images

log = structlog.get_logger()


class FileManager:
    """
    Handles file operations, to abstract over the processing code.
    It is bound to the single request.
    """

    def __init__(self, request_path: Path) -> None:
        self.request_path = request_path

    def find_datatype_images(self, datatype_id: int) -> list[Path]:
        """
        Find images for a specific datatype.

        Args:
            datatype_id: ID of the specific datatype (depends on the mapping form)

        Returns:
            list of images beloning to this request and the given datatype
        """
        datatype_path = self.request_path / DataType(datatype_id).name
        if not datatype_path.exists():
            log.warning("Missing datatype", datatype_id=datatype_id)
            return []
        return find_images(datatype_path)

    def get_output_directory(self, datatype_id: int) -> Path:
        """
        Get the output directory for the given datatype.

        Args:
            datatype_id (int): datatype to create a director for.

        Returns:
            Path where to save outputs for the given datatype.
        """
        output_dir = self.request_path / "outputs" / DataType(datatype_id).name
        output_dir.mkdir(parents=True, exist_ok=True)
        return output_dir

    def find_result_files(self, result_path: Path) -> list[Path]:
        result_files = []
        if result_path and result_path.exists():
            ortho_path = result_path / "odm_orthophoto" / "odm_orthophoto.tif"
            report_path = result_path / "odm_report" / "odm_report.pdf"

            if ortho_path.exists():
                result_files.append(ortho_path)
            if report_path.exists():
                result_files.append(report_path)

        return result_files

    def validate_datatype_groups(self, datatype_ids: list[int]) -> list[tuple[str, Path, list[Path]]]:
        """Validate and return valid datatype groups."""
        datatype_groups = []

        for datatype_id in datatype_ids:
            images = self.find_datatype_images(datatype_id)
            if images:
                datatype_path = self.request_path / DataType(datatype_id).name
                datatype_groups.append((datatype_id, datatype_path, images))
            else:
                log.warning(
                    "No images found for datatype",
                    datatype_id=datatype_id,
                    path=str(self.request_path / DataType(datatype_id).name),
                )

        return datatype_groups
