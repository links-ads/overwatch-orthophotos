#!/usr/bin/env python3

import argparse
import sys
from pathlib import Path

from PIL import Image
from PIL.ExifTags import TAGS


def read_exif(image_path):
    """Read and return EXIF data from an image file."""
    try:
        with Image.open(image_path) as img:
            exif_data = img.getexif()
            if not exif_data:
                return None

            # Convert EXIF data to readable format
            exif_dict = {}
            for tag_id, value in exif_data.items():
                tag = TAGS.get(tag_id, tag_id)
                exif_dict[tag] = value

            return exif_dict
    except Exception as e:
        print(f"Error reading image: {e}", file=sys.stderr)
        return None


def print_exif(exif_dict):
    """Print EXIF data in a nicely formatted way."""
    if not exif_dict:
        print("No EXIF data found in the image.")
        return

    print("EXIF Data:")
    print("-" * 40)

    for tag, value in exif_dict.items():
        # Handle special cases for better formatting
        if isinstance(value, bytes):
            try:
                value = value.decode("utf-8")
            except UnicodeDecodeError:
                value = f"<binary data: {len(value)} bytes>"

        print(f"{tag:20}: {value}")


def main():
    parser = argparse.ArgumentParser(description="Read and display EXIF data from an image file")
    parser.add_argument("image", type=Path, help="Path to the image file")

    args = parser.parse_args()

    # Check if file exists
    if not args.image.exists():
        print(f"Error: File '{args.image}' does not exist.", file=sys.stderr)
        sys.exit(1)

    # Check if it's a file
    if not args.image.is_file():
        print(f"Error: '{args.image}' is not a file.", file=sys.stderr)
        sys.exit(1)

    # Read and display EXIF data
    exif_data = read_exif(args.image)
    print_exif(exif_data)


if __name__ == "__main__":
    main()
