#!/usr/bin/env python3
"""
Drone Image Preprocessing Script

This script processes drone imagery from the raw data directory, filtering and subsampling
images to prepare them for ODM orthorectification processing.

Key features:
- Finds intersection of thermal and vis images per bag
- Trims 25% from beginning and end (keeps middle 50%)
- Subsamples by keeping every Nth image
- Can target a specific total image count
- Maintains directory structure in output
"""

import argparse
import json
import logging
import shutil
import sys
from datetime import datetime
from pathlib import Path

logger = logging.getLogger(__name__)


def get_frame_number(filename: Path):
    """Extract bag number and frame number from filename"""
    # Pattern: bag_N_M.extension
    frame = int(filename.stem.rsplit("_", 1)[-1])
    return frame


def get_image_files(directory: Path) -> list[Path]:
    """
    Get all image files from directory, sorted by name.

    Args:
        directory: Directory to scan

    Returns:
        Sorted list of image file paths
    """
    if not directory.exists():
        logger.warning(f"Directory does not exist: {directory}")
        return []
    image_extensions = {".jpg", ".jpeg", ".png", ".tiff", ".tif"}
    image_files = []
    for ext in image_extensions:
        image_files.extend(directory.glob(f"*{ext}"))
        image_files.extend(directory.glob(f"*{ext.upper()}"))
    return sorted(image_files, key=get_frame_number)


def find_image_intersection(thermal_dir: Path, vis_dir: Path) -> tuple[list[Path], list[Path]]:
    """
    Find intersection of thermal and vis images based on base names.

    Args:
        thermal_dir: Directory containing thermal images
        vis_dir: Directory containing vis images

    Returns:
        Tuple of (matched_thermal_files, matched_vis_files)
    """
    thermal_files = get_image_files(thermal_dir)
    vis_files = get_image_files(vis_dir)
    logger.info(f"Found {len(thermal_files)} thermal images, {len(vis_files)} vis images")
    # Create mapping of base names to file paths
    thermal_map = {f.stem: f for f in thermal_files}
    vis_map = {f.stem: f for f in vis_files}
    # Find intersection
    common_basenames = set(thermal_map.keys()).intersection(set(vis_map.keys()))
    logger.info(f"Found {len(common_basenames)} matching image pairs")

    # Sort by basename to maintain consistent ordering
    sorted_basenames = sorted(common_basenames)
    matched_thermal = [thermal_map[basename] for basename in sorted_basenames]
    matched_vis = [vis_map[basename] for basename in sorted_basenames]
    return matched_thermal, matched_vis


def trim_and_subsample(image_list: list[Path], subsample_n: int, trim_start: int, trim_end: int) -> list[Path]:
    """
    Trim 25% from beginning and end, then subsample by keeping every Nth image.

    Args:
        image_list: List of image paths (sorted)
        subsample_n: Keep every Nth image (1 = keep all, 2 = keep every other, etc.)

    Returns:
        Filtered list of image paths
    """
    assert trim_start >= 0 and trim_start <= len(image_list), f"Invalid trim start: {trim_start}"
    assert trim_end >= 0 and trim_end <= len(image_list), f"Invalid trim end: {trim_end}"
    if not image_list:
        return []

    total_count = len(image_list)
    # Calculate trim indices (remove 25% from each end, keep middle 50%)
    start_idx = trim_start
    end_idx = len(image_list) - trim_end
    trimmed_list = image_list[start_idx:end_idx]
    logger.info(
        f"After trimming: {len(trimmed_list)} images (removed {start_idx} from start, {total_count - end_idx} from end)"
    )
    # Subsample by keeping every Nth image
    if subsample_n <= 1:
        subsampled_list = trimmed_list
    else:
        subsampled_list = trimmed_list[::subsample_n]
    logger.info(f"After subsampling (every {subsample_n}): {len(subsampled_list)} images")
    return subsampled_list


def calculate_subsample_n(total_images: int, target_count: int) -> int:
    """
    Calculate the subsample factor N to achieve approximately target_count images
    after trimming 50% (keeping middle 50%).

    Args:
        total_images: Total number of matched images
        target_count: Desired number of images to keep

    Returns:
        Subsample factor N
    """
    # After trimming, we have 50% of images
    images_after_trim = int(total_images * 0.5)
    if target_count >= images_after_trim:
        return 1  # Keep all trimmed images
    # Calculate N such that images_after_trim / N ≈ target_count
    n = max(1, int(images_after_trim / target_count))
    logger.info(f"Calculated subsample factor: {n} (will result in ~{images_after_trim // n} images)")
    return n


def copy_images(source_images: list[Path], dest_dir: Path) -> None:
    """
    Copy selected images to destination directory.

    Args:
        source_images: List of source image paths
        dest_dir: Destination directory
    """
    dest_dir.mkdir(parents=True, exist_ok=True)
    copied_count = 0
    for source_path in source_images:
        dest_path = dest_dir / source_path.name
        try:
            shutil.copy2(source_path, dest_path)
            copied_count += 1
        except Exception as e:
            logger.error(f"Failed to copy {source_path}: {e}")
    logger.info(f"Successfully copied {copied_count} images to {dest_dir}")


def process_bag(
    bag_name: str,
    raw_data_dir: Path,
    processed_data_dir: Path,
    subsample_n: int | None = None,
    target_count: int | None = None,
    rgb_dir: str = "vis",
    swir_dir: str = "thermal",
    matching_only: bool = True,
    trim_start: int = 0,
    trim_end: int = 0,
) -> dict[str, int]:
    """
    Process a single image bag (request ID).

    Args:
        bag_name: Name of the bag directory (request ID)
        raw_data_dir: Path to raw data directory
        processed_data_dir: Path to processed data directory
        subsample_n: Subsample factor (if None, will be calculated from target_count)
        target_count: Target number of images per datatype (used if subsample_n is None)

    Returns:
        Dictionary with processing statistics
    """
    print(f"MATCHING ONLY: {matching_only}")
    logger.info(f"Processing bag: {bag_name}")

    bag_dir = raw_data_dir / bag_name
    thermal_dir = bag_dir / swir_dir
    vis_dir = bag_dir / rgb_dir

    # Find intersection of thermal and vis images if matching required
    if matching_only:
        thermal_files, vis_files = find_image_intersection(thermal_dir, vis_dir)
        if not thermal_files:
            logger.warning(f"No matching images found in {bag_name}")
            return {swir_dir: 0, rgb_dir: 0}
    else:
        thermal_files = get_image_files(thermal_dir)
        vis_files = get_image_files(vis_dir)

    # Calculate subsample factor if not provided
    subsample_n_vis = subsample_n_thermal = subsample_n or 1
    if subsample_n is None and target_count is not None:
        subsample_n_vis = calculate_subsample_n(len(vis_files), target_count)
        subsample_n_thermal = calculate_subsample_n(len(thermal_files), target_count)

    # Process thermal images
    selected_thermal = trim_and_subsample(
        image_list=thermal_files,
        subsample_n=subsample_n_thermal,
        trim_start=trim_start,
        trim_end=trim_end,
    )
    thermal_dest_dir = processed_data_dir / bag_name / "thermal"
    copy_images(selected_thermal, thermal_dest_dir)

    # Process vis images
    selected_vis = trim_and_subsample(
        image_list=vis_files,
        subsample_n=subsample_n_vis,
        trim_start=trim_start,
        trim_end=trim_end,
    )
    vis_dest_dir = processed_data_dir / bag_name / "rgb"
    copy_images(selected_vis, vis_dest_dir)

    return {swir_dir: len(selected_thermal), rgb_dir: len(selected_vis)}


def create_request_json(
    bag_name: str,
    processed_data_dir: Path,
    situation_id: str = "none",
    datatype_ids: list | None = None,
    bbox_coords: list | None = None,
) -> None:
    """
    Create a request.json file for the processed bag.

    Args:
        bag_name: Name of the bag (request ID)
        processed_data_dir: Path to processed data directory
        situation_id: Situation identifier (default: "none")
        datatype_ids: List of datatype IDs (default: [22002, 22003])
        bbox_coords: Bounding box coordinates as [min_lon, min_lat, max_lon, max_lat]
    """
    if datatype_ids is None:
        datatype_ids = [22002, 22003]

    # Default coordinates if none provided
    if bbox_coords is None:
        # [min_lon, min_lat, max_lon, max_lat]
        bbox_coords = [-8.472698427917976, 40.405542218603046, -7.472698427917976, 40.558658193522064]

    min_lon, min_lat, max_lon, max_lat = bbox_coords

    # Create polygon from bounding box
    polygon_coords = [
        [min_lon, max_lat],  # top-left
        [min_lon, min_lat],  # bottom-left
        [max_lon, min_lat],  # bottom-right
        [max_lon, max_lat],  # top-right
        [min_lon, max_lat],  # close polygon
    ]

    current_time = datetime.now().isoformat() + "Z"

    request_data = {
        "id": bag_name,
        "requestId": bag_name,
        "situationId": situation_id,
        "timestamp": current_time,
        "start": current_time,
        "end": current_time,
        "datatypeIds": datatype_ids,
        "feature": {
            "type": "Feature",
            "properties": {},
            "geometry": {
                "coordinates": [polygon_coords],
                "type": "Polygon",
            },
        },
    }
    request_json_path = processed_data_dir / bag_name / "request.json"
    request_json_path.parent.mkdir(parents=True, exist_ok=True)

    with open(request_json_path, "w") as f:
        json.dump(request_data, f, indent=2)

    logger.info(f"Created request.json for {bag_name}")


def main():
    parser = argparse.ArgumentParser(description="Process and filter drone imagery data")
    parser.add_argument(
        "--raw-dir",
        type=Path,
        default=Path("data/raw"),
        help="Path to raw data directory (default: data/raw)",
    )
    parser.add_argument(
        "--processed-dir",
        type=Path,
        default=Path("data/processed"),
        help="Path to processed data directory (default: data/processed)",
    )
    parser.add_argument("--trim-start", type=int, default=100, help="Amount of frames to trim from the start")
    parser.add_argument(
        "--trim-end",
        type=int,
        default=100,
        help="Amount of frames to trim from the end",
    )
    parser.add_argument(
        "--subsample-n",
        type=int,
        help="Keep every Nth image (overrides --target-count)",
    )
    parser.add_argument(
        "--target-count",
        type=int,
        default=200,
        help="Target number of images per datatype (default: 200)",
    )
    parser.add_argument("--bags", nargs="+", help="Specific bag names to process (default: all bags)")
    parser.add_argument(
        "--create-request-json",
        action="store_true",
        help="Create request.json files for processed bags",
    )
    parser.add_argument(
        "--rgb-name",
        type=str,
        default="vis",
        help="Name of the folder containing RGB images",
    )
    parser.add_argument(
        "--thermal-name",
        default="thermal",
        help="Name of the folder containing thermal images",
    )
    parser.add_argument(
        "--matching-only",
        action="store_true",
        help="Keep only matching pairs",
    )

    args = parser.parse_args()

    # Validate directories
    if not args.raw_dir.exists():
        logger.error(f"Raw data directory does not exist: {args.raw_dir}")
        sys.exit(1)

    # Find all bags if not specified
    if args.bags:
        bag_names = args.bags
    else:
        bag_names = [d.name for d in args.raw_dir.iterdir() if d.is_dir()]

    if not bag_names:
        logger.error("No image bags found to process")
        sys.exit(1)

    logger.info(f"Found {len(bag_names)} bags to process: {bag_names}")
    total_stats = {args.thermal_name: 0, args.rgb_name: 0}

    for bag_name in bag_names:
        try:
            stats = process_bag(
                bag_name=bag_name,
                raw_data_dir=args.raw_dir,
                processed_data_dir=args.processed_dir,
                subsample_n=args.subsample_n,
                target_count=args.target_count,
                rgb_dir=args.rgb_name,
                swir_dir=args.thermal_name,
                matching_only=args.matching_only,
                trim_start=args.trim_start,
                trim_end=args.trim_end,
            )

            total_stats[args.thermal_name] += stats[args.thermal_name]
            total_stats[args.rgb_name] += stats[args.rgb_name]

            if args.create_request_json:
                create_request_json(bag_name, args.processed_dir)

        except Exception as e:
            logger.error(f"Failed to process bag {bag_name}: {e}")
            continue

    # Summary
    logger.info("=" * 50)
    logger.info("PROCESSING SUMMARY")
    logger.info("=" * 50)
    logger.info(f"Processed {len(bag_names)} bags")
    logger.info(f"Total thermal images: {total_stats[args.thermal_name]}")
    logger.info(f"Total vis images: {total_stats[args.rgb_name]}")
    logger.info(f"Output directory: {args.processed_dir}")


if __name__ == "__main__":
    # Configure logging
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s",
    )
    main()
