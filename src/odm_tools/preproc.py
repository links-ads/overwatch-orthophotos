"""
Image preprocessing pipeline for ODM processing.
Handles validation, spatial filtering, and framerate subsampling.
"""

import shutil
from pathlib import Path

import exif
import structlog
from shapely.geometry import Point, shape

from odm_tools.models import ProcessingRequest

log = structlog.get_logger()


class PreprocessingManager:
    """Orchestrates the preprocessing pipeline."""

    def __init__(self, request_path: Path, request: ProcessingRequest, framerate_factor: int = 1):
        self.request_path = request_path
        self.request = request
        self.framerate_factor = framerate_factor
        self.processed_path = request_path / "processed"

        # Extract geometry from request
        self.geometry = shape(request.feature.__geo_interface__)

    def is_preprocessing_needed(self, force: bool = False) -> bool:
        """Check if preprocessing should run."""
        if force:
            log.debug("Preprocessing forced: deleting any previous data")
            self.processed_path.unlink(missing_ok=True)
            return True

        # Check if processed/ exists and is complete
        if not self.processed_path.exists():
            return True

        # Validate completeness (check for marker file)
        marker = self.processed_path / ".preprocessing_complete"
        return not marker.exists()

    def preprocess(self) -> Path:
        """Run the full preprocessing pipeline."""
        log.info("Starting preprocessing", request_id=self.request.request_id)

        # Create processed directory structure
        self.processed_path.mkdir(exist_ok=True)

        # Process each datatype
        for datatype_id in self.request.datatype_ids:
            datatype_name = self._get_datatype_name(datatype_id)
            source_dir = self.request_path / datatype_name
            dest_dir = self.processed_path / datatype_name

            if not source_dir.exists():
                log.warning("Datatype directory not found, skipping", datatype=datatype_name)
                continue

            log.info("Processing datatype", datatype=datatype_name)
            processed_count = self._process_datatype(source_dir, dest_dir)

            log.info("Datatype processing complete", datatype=datatype_name, images_processed=processed_count)

        # Write completion marker
        (self.processed_path / ".preprocessing_complete").touch()

        log.info("Preprocessing complete", output_path=str(self.processed_path))
        return self.processed_path

    def _process_datatype(self, source_dir: Path, dest_dir: Path) -> int:
        """Process a single datatype directory."""
        dest_dir.mkdir(parents=True, exist_ok=True)

        # Find all images
        images = self._find_images(source_dir)
        log.info("Found images", count=len(images), source=str(source_dir))

        # Step 1: Validate and filter by geometry
        valid_images = []
        for img_path in images:
            try:
                coords = self._extract_gps_coords(img_path)
                if coords and self._is_within_geometry(coords):
                    valid_images.append(img_path)
            except Exception as e:
                log.warning("Skipping image", image=img_path.name, reason=str(e))

        log.info("Images after geo filtering", count=len(valid_images))

        if not valid_images:
            log.warning(f"No valid images found for {source_dir.name} after filtering")
            return 0

        # Step 2: Framerate filtering
        selected_images = valid_images[:: self.framerate_factor]
        log.info("Images after framerate filtering", count=len(selected_images), factor=self.framerate_factor)

        # Step 3: Copy to destination
        for img_path in selected_images:
            dest_path = dest_dir / img_path.name
            shutil.copy2(img_path, dest_path)

        return len(selected_images)

    def _extract_gps_coords(self, image_path: Path) -> tuple[float, float] | None:
        """Extract GPS coordinates from image EXIF."""
        try:
            with open(image_path, "rb") as f:
                img = exif.Image(f)

            if not img.has_exif or not hasattr(img, "gps_latitude"):
                return None

            lat = self._convert_to_degrees(img.gps_latitude, img.gps_latitude_ref)
            lon = self._convert_to_degrees(img.gps_longitude, img.gps_longitude_ref)

            return (lat, lon)
        except Exception as e:
            log.debug("Failed to extract GPS", image=image_path.name, error=str(e))
            return None

    def _convert_to_degrees(self, value: tuple, ref: str) -> float:
        """Convert GPS coordinates to decimal degrees."""
        d, m, s = value
        degrees = d + m / 60 + s / 3600
        if ref in ["S", "W"]:
            degrees = -degrees
        return degrees

    def _is_within_geometry(self, coords: tuple[float, float]) -> bool:
        """Check if coordinates are within request geometry."""
        point = Point(coords[1], coords[0])  # shapely uses (lon, lat)
        return self.geometry.contains(point)

    def _find_images(self, directory: Path) -> list[Path]:
        """Find all image files in directory."""
        image_extensions = {".jpg", ".jpeg", ".png", ".tiff", ".tif"}
        images = [f for f in directory.iterdir() if f.is_file() and f.suffix.lower() in image_extensions]
        return sorted(images)

    def _get_datatype_name(self, datatype_id: int) -> str:
        """Convert datatype ID to folder name."""
        from odm_tools.models import DataType

        for dt in DataType:
            if dt.value == datatype_id:
                return dt.name
        raise ValueError(f"Unknown datatype ID: {datatype_id}")
