import re
import shutil
from pathlib import Path

import exif
import pyexiv2
import structlog
from shapely.geometry import Point, shape

from odm_tools.config import EXIF_TAGS_GPS, EXIF_TAGS_TRM
from odm_tools.models import DataType, ProcessingRequest

log = structlog.get_logger()


class PreprocessingManager:
    """Orchestrates the preprocessing pipeline."""

    def __init__(self, request_path: Path, request: ProcessingRequest, framerate_factor: int = 1):
        self.request_path = request_path
        self.request = request
        self.framerate_factor = framerate_factor
        self.processed_path = request_path / "processed"
        self.geometry = shape(request.feature.__geo_interface__)

    def is_preprocessing_needed(self, force: bool = False) -> bool:
        """Check if preprocessing should run."""
        if force:
            log.debug("Preprocessing forced: deleting any previous data")
            self.processed_path.unlink(missing_ok=True)
            return True
        # check if processed/ exists and is complete
        if not self.processed_path.exists():
            return True
        # validate completeness (check for marker file)
        marker = self.processed_path / ".preprocessing_complete"
        return not marker.exists()

    def copy_exif_tags(self, rgb_path: Path, trm_path: Path):
        """Copy GPS from RGB to thermal and set band name."""
        # Read GPS from RGB
        pyexiv2.registerNs("http://pix4d.com/camera/1.0/", "Camera")
        additional_info = {}
        with pyexiv2.Image(str(rgb_path)) as rgb_img:
            rgb_exif = rgb_img.read_exif()
            if comment := rgb_exif.get("Exif.Photo.UserComment"):
                log.debug("Found gimbal info, writing as XMP")
                additional_info.update(self._extract_gimbal_info(comment))
                rgb_img.modify_xmp(additional_info)

        # Write to thermal
        with pyexiv2.Image(str(trm_path)) as trm_img:
            # Copy GPS tags
            gps_tags = {tag: rgb_exif[tag] for tag in EXIF_TAGS_GPS if tag in rgb_exif}
            if gps_tags:
                trm_img.modify_exif(gps_tags)
            # Add band name (XMP)
            additional_info.update(EXIF_TAGS_TRM)
            trm_img.modify_xmp(additional_info)
        log.debug("Copied EXIF/XMP tags", rgb=rgb_path.name, thermal=trm_path.name)

    def preprocess(self) -> Path:
        log.info("Starting preprocessing", request_id=self.request.request_id)
        # create processed directory structure
        self.processed_path.mkdir(exist_ok=True)
        # find RGB and thermal, match them in tuples
        rgb_images = self._find_images(self.request.path / "rgb")
        trm_images = self._find_images(self.request.path / "thermal")
        # infinite checks
        if not rgb_images:
            raise ValueError("No RGB images found, cannot proceed")
        if not trm_images and DataType.thermal.value in self.request.datatype_ids:
            raise ValueError("User requested thermal imagery, but could not find any, aborting.")
        if len(rgb_images) != len(trm_images):
            raise ValueError(f"Length mismatch: {len(rgb_images)} (RGB), {len(trm_images)} (Thermal)")

        # step 1: validate and filter by geometry
        valid_tuples: list[tuple[int, Path, Path]] = []
        log.info("Filtering by area...")
        for i, (rgb, trm) in enumerate(zip(rgb_images, trm_images)):
            try:
                assert rgb.name == trm.name, f"Name mismatch {rgb} <-> {trm}"
                coords = self._extract_gps_coords(rgb)
                if coords and self._is_within_geometry(coords):
                    valid_tuples.append((i, rgb, trm))
            except Exception as e:
                log.warning("Skipping image", image=rgb.name, reson=str(e))

        # step 2: framerate filtering and renaming
        valid_tuples = valid_tuples[:: self.framerate_factor]
        log.info("Images after framerate filtering", count=len(valid_tuples), factor=self.framerate_factor)
        log.info("Renaming images...")
        for i, rgb, trm in valid_tuples:
            new_rgb_path = self.processed_path / f"{i:04d}_RGB{rgb.suffix}"
            new_trm_path = self.processed_path / f"{i:04d}_THERMAL{trm.suffix}"
            shutil.copy2(rgb, new_rgb_path)
            shutil.copy2(trm, new_trm_path)
            self.copy_exif_tags(new_rgb_path, new_trm_path)

        log.info("Processing complete", images_processed=len(valid_tuples))
        (self.processed_path / ".preprocessing_complete").touch()
        return self.processed_path

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

    def _extract_gimbal_info(self, exif_comments: str) -> dict[str, str]:
        # user comment in this broken format:
        # charset=InvalidCharsetId i:GimbalRoll:-90.0,drone-dji:GimbalPitch:0.0,drone-dji:GimbalYaw:-90.0
        pattern = r"GimbalRoll:(-?\d+\.?\d*).*?GimbalPitch:(-?\d+\.?\d*).*?GimbalYaw:(-?\d+\.?\d*)"
        match = re.search(pattern, exif_comments)
        if match is None:
            log.warning("Could not find gimbal info in %s", exif_comments)
            return {}
        roll, pitch, yaw = match.groups()
        return {
            "Xmp.Camera.Roll": roll,
            "Xmp.Camera.Pitch": pitch,
            "Xmp.Camera.Yaw": yaw,
        }

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
