import shutil
from pathlib import Path

import rasterio
import structlog

from odm_tools.models import DataType, ProcessingRequest
from odm_tools.utils import find_images

log = structlog.get_logger()

OUTPUT_FILE_PATTERN = "{date}_drone-map_{datatype_id}_{request_id}.tif"


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

    def find_result_files(self, result_path: Path, request: ProcessingRequest) -> dict[DataType, Path]:
        result_files = {}
        if result_path and result_path.exists():
            ortho_path = result_path / "odm_orthophoto" / "odm_orthophoto.tif"
            # report_path = result_path / "odm_report" / "odm_report.pdf"

            if ortho_path.exists():
                # split if thermal is present
                if DataType.thermal.value in request.datatype_ids:
                    tiffs: dict[str, Path] = self.split_multiband_geotiff(
                        input_file=ortho_path,
                        output_dir=ortho_path.parent,
                        request=request,
                    )
                    # select only the ones that have been chosen by the user
                    # (thermal requires RGB to parse every time)
                    for dt_id in request.datatype_ids:
                        dt_enum = DataType(dt_id)
                        result_files[dt_enum] = tiffs[dt_enum.name]
                # otherwise no thermal extraction needed
                else:
                    new_ortho_path = ortho_path.parent / OUTPUT_FILE_PATTERN.format(
                        date=request.start.strftime("%Y%m%d"),
                        datatype_id=DataType.rgb.value,
                        request_id=request.request_id,
                    )
                    shutil.move(ortho_path, new_ortho_path)
                    result_files[DataType.rgb] = new_ortho_path

        return result_files

    def gather_images_by_datatype(self, datatype_ids: list[int]) -> list[Path]:
        """Validate and return valid datatype groups."""
        all_images = []

        for datatype_id in datatype_ids:
            if images := self.find_datatype_images(datatype_id):
                log.debug("Found %d images for datatype %d", len(images), datatype_id)
                all_images.extend(images)
            else:
                log.warning(
                    "No images found for datatype",
                    datatype_id=datatype_id,
                    path=str(self.root_dir),
                )
        # additionally add RGB images if thermal only has been requested
        if len(datatype_ids) == 1 and datatype_ids[0] == DataType.thermal.value:
            log.debug("Thermal-only, adding RGB images")
            all_images.extend(self.find_datatype_images(DataType.rgb.value))

        return all_images

    def split_multiband_geotiff(self, input_file: Path, output_dir: Path, request: ProcessingRequest):
        """
        Split a multiband GeoTIFF into RGB (bands 1,2,3) and single band (band 4).

        Args:
            input_file: Path to input multiband GeoTIFF
            output_dir: Directory for output files
            request: request obj to ccustomize paths

        Returns:
            tuple[Path, Path]: Paths to (rgb_output, band4_output)
        """
        output_dir.mkdir(exist_ok=True, parents=True)

        with rasterio.open(input_file) as src:
            # Validate band count
            if src.count < 4:
                raise ValueError(f"Expected at least 4 bands, got {src.count}")
            # set nodata value
            nodata = src.nodata if src.nodata is not None else 0
            # Output paths
            rgb_output = output_dir / OUTPUT_FILE_PATTERN.format(
                date=request.start.strftime("%Y%m%d"),
                datatype_id=DataType.rgb.value,
                request_id=request.request_id,
            )
            trm_output = output_dir / OUTPUT_FILE_PATTERN.format(
                date=request.start.strftime("%Y%m%d"),
                datatype_id=DataType.thermal.value,
                request_id=request.request_id,
            )

            # Create RGB GeoTIFF (bands 1, 2, 3)
            rgb_meta = src.meta.copy()
            rgb_meta.update(count=3, nodata=nodata)

            with rasterio.open(rgb_output, "w", **rgb_meta) as rgb_dst:
                for i in range(1, 4):
                    rgb_dst.write(src.read(i), i)

            log.info("Created RGB GeoTIFF", path=rgb_output)
            # Create single band GeoTIFF (band 4)
            band4_meta = src.meta.copy()
            band4_meta.update(count=1)

            with rasterio.open(trm_output, "w", **band4_meta) as band4_dst:
                band4_dst.write(src.read(4), 1)
            log.info("Created LWIR GeoTIFF", path=trm_output)
            return {"rgb": rgb_output, "thermal": trm_output}
