import uuid
from datetime import UTC, datetime
from pathlib import Path
from urllib.error import HTTPError

import requests
import structlog
from pydantic_geojson import MultiPolygonModel, PolygonModel

from odm_tools.auth import KeyCloakAuthOAuth
from odm_tools.config import settings
from odm_tools.models import (
    MetadataExternalAttribute,
    MetadataINSPIRE,
    PackageParams,
    ResourceCreateMetadata,
    ResourceUpdateMetadata,
)

log = structlog.get_logger()


class UploadException(Exception):
    """
    Base uploader exception class.
    """


class CKANUploader:
    def __init__(self) -> None:
        """
        Initializes the CKANUploader.
        """
        cfg = settings.ckan
        self.cfg = cfg
        self.package_url = f"{cfg.url}/api/action/package_create"
        self.resource_create_url = f"{cfg.url}/api/action/resource_create"
        self.resource_update_url = f"{cfg.url}/api/action/resource_update"
        self.package_search_url = f"{cfg.url}/api/action/package_search"
        self.package_show_url = f"{cfg.url}/api/action/package_show"
        self.package_delete_url = f"{cfg.url}/api/action/package_delete"
        self.package_patch_url = f"{cfg.url}/api/action/package_patch"
        self.authenticator = KeyCloakAuthOAuth()

    def authorize(self) -> dict:
        return self.authenticator.get_authorization_header()

    def retrieve_metadata(self, package_id: str, metadata_keys: list | None = None):
        """
        Retrieves metadata for a given package ID.

        Args:
            package_id (str): The ID of the package to retrieve metadata for.
            metadata_keys (list, optional): A list of specific metadata keys to retrieve.
                If not provided, all metadata keys will be returned.

        Returns:
            list or dict: If `metadata_keys` is provided, a list of the corresponding
                metadata values will be returned. Otherwise, a dictionary of all
                metadata key-value pairs will be returned.

        Raises:
            TaskException: If the metadata retrieval fails, an exception with a
                descriptive error message will be raised.
        """
        try:
            response = requests.get(f"{self.package_show_url}?id={package_id}", headers=self.authorize())
            response.raise_for_status()

            metadata = response.json()["result"]
            if metadata_keys is not None:
                return [metadata[k] for k in metadata_keys if k in metadata]

            return metadata
        except HTTPError as e:
            log.exception("HTTP error occurred", error=str(e))
            raise UploadException(e)

    def get_package_id(self, request_code: str):
        """
        Retrieves the package ID associated with the given request code.
        Args:
            request_code (str): The request code used to search for the package.
        Returns:
            str: The ID of the package if found, None otherwise.
        Raises:
            HTTPError: If an HTTP error occurs during the request.
            Exception: If any other error occurs during the request.
        """

        try:
            assert request_code is not None
            params = {
                "include_private": "true",
                "q": f'*:*"{request_code}"*',
            }
            response = requests.get(self.package_search_url, params=params, headers=self.authorize())
            response.raise_for_status()
        except HTTPError as e:
            log.exception("HTTP error occurred", error=str(e))
            raise UploadException(e)
        except Exception as e:
            log.exception("Unexpected error occurred", error=str(e))
            raise UploadException(e)

        if len(response.json()["result"]["results"]) > 0:
            return response.json()["result"]["results"][0]["id"]

    def get_resource_url(self, package_id: str, resource_names: list):
        """
        Get the resource URL based on the package ID and resource name.

        Args:
            package_id (str): The ID of the package.
            resource_name (list): list of names to test

        Returns:
            str: The resource URL if found, None otherwise.
        """
        try:
            response = requests.get(f"{self.package_show_url}?id={package_id}", headers=self.authorize())
            response.raise_for_status()

            if response.status_code != 200:
                UploadException(f"Failed to retrieve resource URL for package {package_id}")

            resource_list = response.json()["result"]["resources"]
            log.info(f"Found {len(resource_list)} resources for package {package_id}")
            log.info(f"Resource list: {resource_list}")
            if len(resource_list) == 0:
                log.info(f"No resources found for package {package_id}")
                return None
            resource_urls = []
            for res_name in resource_names:
                resource_url_list = [res["url"] for res in resource_list if res["name"].startswith(res_name)]
                resource_urls.extend(resource_url_list)

            if len(resource_urls) == 0:
                log.warning(f"No resources found for package {package_id} that started with {resource_names}")
                return None
            return resource_urls[0]

        except HTTPError as e:
            log.exception("HTTP error occurred", error=str(e))
            raise UploadException(e)
        except Exception as e:
            log.exception("Unexpected error occurred", error=str(e))
            raise UploadException(e)

    def create_resource_name(
        self,
        datatype_id: int | str,
        request_code: str,
        date: str,
        title: str,
        underscore: bool = False,
    ) -> str:
        """create the well-formatted string for the resource name

        Args:
            datatype_id (int|str): dataype id
            user_id (str): user id
            date (str): start date in the maprequest (isoformat)
            title (str): title of the request
            country_code (str): country code string

        Returns:
            str: name
        """
        if date:
            date = datetime.fromisoformat(date).date().isoformat()
        infos = [date, title, str(datatype_id), request_code]
        name = " ".join(filter(None, infos))
        if underscore:
            table = str.maketrans({" ": "_", "-": "", ":": "", ",": ""})
            name = name.translate(table)
        return name

    def create_metadata(
        self,
        title: str,
        owner: str,
        resolution: int,
        metadata_params: PackageParams,
        request_geometry: PolygonModel | MultiPolygonModel,
        start_date: str,
        end_date: str,
        acquisition_dates: dict | None = None,
        request_code: str = "",
        request_metadata: dict | None = None,
        destinatary_organization: str = "",
    ) -> MetadataINSPIRE:
        """
        Create a MetadataINSPIRE object from the parameters.

        Args:
            title: title of the resource
            owner: owner of the resource
            resolution: resolution of the resource
            metadata_params: PackageParams object with additional metadata
            request_geometry: geometry of the request
            start_date: start date of the request (isoformat)
            end_date: end date of the request (isoformat)
            acquisition_dates: dictionary of acquisition dates for the resource
            request_code: code of the request
            request_metadata: additional metadata for the resource
            destinatary_organization: destinatary organization for the resource

        Returns:
            MetadataINSPIRE: the created metadata object
        """
        try:
            metadata = MetadataINSPIRE(
                owner_org=owner,
                responsable_organization_name=self.cfg.organization_name,
                responsable_organization_email=self.cfg.organization_email,
                point_of_contact_name=self.cfg.organization_name,
                point_of_contact_email=self.cfg.organization_email,
            )

            custom_metadata = {}
            if request_metadata is not None:
                metadata = metadata.model_dump(by_alias=True)
                for k, v in request_metadata.items():
                    if k in metadata:
                        metadata[k] = v
                    custom_metadata[k] = v
                metadata = MetadataINSPIRE(**metadata)

            metadata.title = title
            metadata.name = str(uuid.uuid4())
            metadata.notes = metadata_params.notes
            metadata.keyword = metadata_params.keyword
            metadata.classification_category = metadata_params.topic_category
            metadata.spatial = request_geometry
            current_date = datetime.now(tz=UTC)
            metadata.tref_date = current_date.isoformat().split("+")[0]
            metadata.tref_date_creation = current_date.isoformat().split("+")[0]
            metadata.tref_date_publication = current_date.isoformat().split("+")[0]
            metadata.tref_date_revision = current_date.isoformat().split("+")[0]
            metadata.data_temporal_extent_begin_date = start_date
            metadata.data_temporal_extent_end_date = end_date
            metadata.quality_and_validity_spatial_resolution_latitude = str(resolution)
            metadata.quality_and_validity_spatial_resolution_longitude = str(resolution)
            metadata.request_code = request_code
            metadata.destinatary_organization = destinatary_organization
            if acquisition_dates is not None:
                external_attributes = MetadataExternalAttribute(
                    acquisition_dates=acquisition_dates,
                    **custom_metadata,
                )
                metadata.external_attributes = external_attributes
            log.info(f"Metadata: {metadata.model_dump(by_alias=True)}")
            return metadata
        except Exception as e:
            log.exception("Failed to create metadata", error=str(e))
            raise UploadException(e)

    def upload_metadata(self, metadata: MetadataINSPIRE) -> None:
        """Upload the Metadata INSPIRE

        Args:
            metadata (MetadataINSPIRE): input metadata

        Raises:
            te: Task Exception

        Returns:
            None: None
        """
        log.info("Uploading metadata to datalake...")
        try:
            log.info(f"Loading metadata {metadata.model_dump(by_alias=True)}")
            response = requests.post(
                self.package_url,
                json=metadata.model_dump(by_alias=True),
                headers=self.authorize(),
            )

            log.info(f"Response: {response.json()}")
            response.raise_for_status()
            return response.json()["result"]["id"]
        except HTTPError as e:
            log.exception("An HTTP error occurred", error=e)
            raise UploadException(e)
        except Exception as e:
            log.exception("An unexpected error occurred", error=e)
            raise UploadException(e)

    def upload_resource(
        self,
        package_id: str,
        resource_path: str,
        datatype: int | str,
        time_start: str,
        time_end: str,
        title: str,
    ) -> str:
        """Upload resource to datalake

        Args:
            package_id (str): package id
            resource_path (str): path of the file to upload
            datatype (int|str): datatype id
            time_start (datetime): start
            time_end (datetime): end
            title (str): title

        Raises:
            Exception: general exceptions

        """
        try:
            extension = Path(resource_path).suffix.lstrip(".")
            resource_metadata = ResourceCreateMetadata(
                package_id=package_id,
                datatype_resource=datatype,
                file_date_start=time_start,
                file_date_end=time_end,
                format=extension,
                name=title,
            )
            with open(f"{resource_path}", "rb") as file:
                log.info(f"Uploading resource {resource_metadata.model_dump(by_alias=True)} to datalake...")
                response = requests.post(
                    self.resource_create_url,
                    data=resource_metadata.model_dump(by_alias=True),
                    headers=self.authorize(),
                    files=[("upload", file)],
                )
                response.raise_for_status()
                return response.json()["result"]["url"]
        except Exception as e:
            log.exception("An unexpected error occurred", error=str(e))
            raise UploadException(e)

    def update_resource(
        self,
        package_id: str,
        resource_id: str,
        resource_path: str,
        datatype: int | str,
        time_start: str,
        time_end: str,
        title: str,
    ) -> None:
        """Update existing resource in datalake

        Args:
            package_id (str): package id
            resource_id (str): id of the resource to update
            resource_path (str): path of the file to upload
            datatype (int|str): datatype id
            time_start (datetime): start
            time_end (datetime): end
            title (str): title

        Raises:
            Exception: general exceptions

        Returns:
            None: None
        """
        try:
            extension = Path(resource_path).suffix.lstrip(".")
            resource_metadata = ResourceUpdateMetadata(
                id=resource_id,
                package_id=package_id,
                datatype_resource=datatype,
                file_date_start=time_start,
                file_date_end=time_end,
                format=extension,
                name=title,
            )
            with open(f"{resource_path}", "rb") as file:
                log.info(f"Updating resource {resource_metadata.model_dump(by_alias=True)} in datalake...")
                response = requests.post(
                    self.resource_update_url,  # Change this to your update URL
                    data=resource_metadata.model_dump(by_alias=True),
                    headers=self.authorize(),
                    files=[("upload", file)],
                )
                response.raise_for_status()
                return response.json()["result"]["url"]
        except Exception as e:
            log.exception("An unexpected error occurred", error=str(e))
            raise UploadException(e)
