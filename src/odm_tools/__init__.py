import argparse
import json
import logging
import os
from pathlib import Path

from odm_tools.processor import ODMProcessor


def main():
    parser = argparse.ArgumentParser(description="ODM Processing Pipeline")
    parser.add_argument("--payload", required=True, help="JSON payload file with requestId")
    parser.add_argument("--images", required=True, help="Directory containing input images")
    parser.add_argument("--output", default="./output", help="Output directory for results")
    parser.add_argument("--include-dtm", action="store_true", help="Include DTM in processing")
    parser.add_argument("--nodeodm-host", default="nodeodm", help="NodeODM host")
    parser.add_argument("--nodeodm-port", type=int, default=3000, help="NodeODM port")
    # Setup logging
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s",
    )
    logger = logging.getLogger("odm-tools")
    args = parser.parse_args()

    # Load payload
    try:
        with open(args.payload) as f:
            payload = json.load(f)

        if "requestId" not in payload:
            logger.error("requestId not found in payload")
            return 1

        request_id = payload["requestId"]
        logger.info(f"Processing request: {request_id}")

    except Exception as e:
        logger.error(f"Failed to load payload: {e}")
        return 1

    # Initialize processor
    processor = ODMProcessor(args.nodeodm_host, args.nodeodm_port)

    # Connect to NodeODM
    if not processor.connect_to_node():
        return 1

    # Validate images
    try:
        image_dir = Path(args.images)
        image_files = processor.validate_images(image_dir)
    except Exception as e:
        logger.error(f"Image validation failed: {e}")
        return 1

    # Create processing task
    if not processor.create_processing_task(image_files, args.include_dtm):
        return 1

    # Wait for completion
    if not processor.wait_for_completion():
        return 1

    # Download results
    output_dir = Path(args.output) / request_id
    results = processor.download_results(output_dir)

    if not results:
        logger.error("Failed to download results")
        return 1

    # Upload to CKAN
    ckan_url = os.getenv("CKAN_BASE_URL")
    client_id = os.getenv("OAUTH2_CLIENT_ID")
    client_secret = os.getenv("OAUTH2_CLIENT_SECRET")

    if all([ckan_url, client_id, client_secret]):
        uploader = CKANUploader(ckan_url, client_id, client_secret)

        if uploader.authenticate():
            if uploader.upload_dataset(request_id, results):
                logger.info("Successfully uploaded to CKAN")
            else:
                logger.error("Failed to upload to CKAN")
                return 1
        else:
            logger.error("CKAN authentication failed")
            return 1
    else:
        logger.warning("CKAN credentials not provided, skipping upload")

    logger.info("Processing pipeline completed successfully!")
    return 0


if __name__ == "__main__":
    exit(main())
