#!/usr/bin/env python3
"""Sync thermal and RGB images by number, keeping only matches."""

import argparse
import re
import shutil
from pathlib import Path


def extract_number_from_thermal(filename):
    """Extract number from thermal filename (e.g., left0042.jpg -> 42)."""
    match = re.match(r"left(\d+)\.jpg", filename)
    return int(match.group(1)) if match else None


def extract_number_from_rgb(filename):
    """Extract number from RGB filename (e.g., bag_3_42.jpg -> 42)."""
    match = re.match(r"bag_3_(\d+)\.jpg", filename)
    return int(match.group(1)) if match else None


def main():
    parser = argparse.ArgumentParser(description="Sync thermal and RGB images - keep only matching numbers")
    parser.add_argument("source", type=Path, help="Source directory (thermal_raw)")
    parser.add_argument("destination", type=Path, help="Destination directory")
    parser.add_argument("--dry-run", action="store_true", help="Show stats only")
    args = parser.parse_args()

    # Validate source
    if not args.source.is_dir():
        print(f"Error: {args.source} is not a valid directory")
        return

    # Find RGB directory
    rgb_dir = args.source.parent / "rgb"
    if not rgb_dir.is_dir():
        print(f"Error: RGB directory not found at {rgb_dir}")
        return

    # Get thermal numbers
    thermal_files = {}
    for file in args.source.glob("left*.jpg"):
        number = extract_number_from_thermal(file.name)
        if number:
            thermal_files[number] = file

    # Get RGB numbers
    rgb_files = {}
    for file in rgb_dir.glob("bag_3_*.jpg"):
        number = extract_number_from_rgb(file.name)
        if number:
            rgb_files[number] = file

    # Find matches
    thermal_numbers = set(thermal_files.keys())
    rgb_numbers = set(rgb_files.keys())
    matched_numbers = thermal_numbers & rgb_numbers

    thermal_to_delete = thermal_numbers - matched_numbers
    rgb_to_delete = rgb_numbers - matched_numbers

    # Print stats
    print(f"Thermal images: {len(thermal_files)}")
    print(f"RGB images: {len(rgb_files)}")
    print(f"Matched: {len(matched_numbers)}")
    print(f"\nThermal to skip: {len(thermal_to_delete)}")
    print(f"RGB to DELETE: {len(rgb_to_delete)}")

    if args.dry_run:
        print("\n[Dry run - no files copied or deleted]")
        return

    # Confirm deletion
    if rgb_to_delete:
        response = input(f"\n⚠️  DELETE {len(rgb_to_delete)} RGB files? (yes/no): ")
        if response.lower() != "yes":
            print("Aborted.")
            return

    # Copy matched thermal files
    args.destination.mkdir(parents=True, exist_ok=True)
    for number in matched_numbers:
        new_name = f"bag_3_{number}.jpg"
        shutil.copy2(thermal_files[number], args.destination / new_name)

    print(f"\nCopied {len(matched_numbers)} thermal files to: {args.destination}")

    # Delete unmatched RGB files
    for number in rgb_to_delete:
        rgb_files[number].unlink()

    print(f"Deleted {len(rgb_to_delete)} unmatched RGB files from: {rgb_dir}")
    print(f"\n✓ Both folders now have {len(matched_numbers)} matching images")


if __name__ == "__main__":
    main()
