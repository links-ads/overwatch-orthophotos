from pathlib import Path

import structlog

from odm_tools.models import DataType, ProcessingRequest
from odm_tools.utils import find_images

log = structlog.get_logger()


class FileManager:
    """
    Handles file operations, to abstract over the processing code.
    It is bound to the single request.
    """

    def __init__(self, root_dir: Path) -> None:
        self.root_dir = root_dir

    def find_datatype_images(self, datatype_id: int) -> list[Path]:
        """
        Find images for a specific datatype, based on suffix.

        Args:
            datatype_id: ID of the specific datatype (depends on the mapping form)

        Returns:
            list of images beloning to this request and the given datatype
        """
        return find_images(
            root_path=self.root_dir,
            suffix=f"_{str(DataType(datatype_id).name).upper()}",
            extension=".jpg",
        )

    def get_output_directory(self) -> Path:
        """
        Get the output directory for the given datatype.

        Args:
            datatype_id (int): datatype to create a director for.

        Returns:
            Path where to save outputs for the given datatype.
        """
        output_dir = self.root_dir / "outputs"
        output_dir.mkdir(parents=True, exist_ok=True)
        return output_dir

    def find_result_files(self, result_path: Path, request: ProcessingRequest) -> dict[str, Path]:
        result_files = {}
        if result_path and result_path.exists():
            ortho_path = result_path / "odm_orthophoto" / "odm_orthophoto.tif"
            # TODO add thermal and optionally 3D or report
            # report_path = result_path / "odm_report" / "odm_report.pdf"

            if ortho_path.exists():
                ortho_name = f"{request.start.strftime('%Y%m%d')}_drone_ortho_{DataType['rgb'].value}"
                result_files[ortho_name] = ortho_path

        return result_files

    def validate_datatype_groups(self, datatype_ids: list[int]) -> list[Path]:
        """Validate and return valid datatype groups."""
        all_images = []

        for datatype_id in datatype_ids:
            images = self.find_datatype_images(datatype_id)
            if images:
                log.debug("Found %d images for datatype %d", len(images), datatype_id)
                all_images.extend(images)
            else:
                log.warning(
                    "No images found for datatype",
                    datatype_id=datatype_id,
                    path=str(self.root_dir),
                )

        return all_images
