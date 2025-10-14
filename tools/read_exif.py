import argparse
import sys
from pathlib import Path

import exifread


def read_metadata(image_path):
    """Read and return EXIF data from an image file."""
    with open(image_path, "rb") as file_handle:
        # Return Exif tags
        tags = exifread.process_file(file_handle)
        return tags


def print_metadata(metadata: dict):
    """Print EXIF data in a nicely formatted way."""
    if not metadata:
        print("No EXIF data found in the image.")
        return

    print("EXIF Data:")
    print("-" * 40)

    for tag, value in metadata.items():
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
    data = read_metadata(args.image)
    print_metadata(data)


if __name__ == "__main__":
    main()
