import uuid
from datetime import UTC, datetime
from pathlib import Path

import requests
import structlog
from geojson_pydantic import MultiPolygon, Polygon

from odm_tools.auth import KeyCloakAuthenticator
from odm_tools.config import settings
from odm_tools.models import (
    MetadataINSPIRE,
    ProcessingRequest,
    ResourceCreateRequest,
)

log = structlog.get_logger()


class UploadError(Exception):
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
        self.authenticator = KeyCloakAuthenticator()

    def authorize(self) -> dict:
        return self.authenticator.get_authorization_header()

    def _retrieve_metadata(self, package_id: str, metadata_keys: list | None = None):
        try:
            response = requests.get(self.package_show_url, params={"id": package_id}, headers=self.authorize())
            response.raise_for_status()
            metadata = response.json()["result"]
            if metadata_keys is not None:
                return [metadata[k] for k in metadata_keys if k in metadata]
            return metadata
        except requests.HTTPError as e:
            log.exception("HTTP error occurred", error=str(e))
            raise UploadError(e)

    def _get_package_id(self, request_code: str):
        try:
            assert request_code is not None
            params = {
                "include_private": "true",
                "q": f'*:*"{request_code}"*',
            }
            response = requests.get(self.package_search_url, params=params, headers=self.authorize())
            response.raise_for_status()
        except requests.HTTPError as e:
            log.exception("HTTP error occurred", error=str(e))
            raise UploadError(e)
        except Exception as e:
            log.exception("Unexpected error occurred", error=str(e))
            raise UploadError(e)

        if len(response.json()["result"]["results"]) > 0:
            return response.json()["result"]["results"][0]["id"]

    def _get_resource_url(self, package_id: str, resource_names: list):
        try:
            response = requests.get(self.package_show_url, params={"id": package_id}, headers=self.authorize())
            response.raise_for_status()

            if response.status_code != 200:
                UploadError(f"Failed to retrieve resource URL for package {package_id}")

            resource_list = response.json()["result"]["resources"]
            log.info(f"Found {len(resource_list)} resources for package {package_id}")
            log.debug(f"Resource list: {resource_list}")
            if not resource_list:
                log.warning(f"No resources found for package {package_id}")
                return None

            resource_urls = []
            for res_name in resource_names:
                resource_url_list = [res["url"] for res in resource_list if res["name"].startswith(res_name)]
                resource_urls.extend(resource_url_list)

            if len(resource_urls) == 0:
                log.warning(f"No resources found for package {package_id} that started with {resource_names}")
                return None
            return resource_urls[0]

        except requests.HTTPError as e:
            log.exception("HTTP error occurred", error=str(e))
            raise UploadError(e)
        except Exception as e:
            log.exception("Unexpected error occurred", error=str(e))
            raise UploadError(e)

    def _create_resource_name(
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
        date = datetime.fromisoformat(date).date().isoformat()
        infos = [date, title, str(datatype_id), request_code]
        name = " ".join(filter(None, infos))
        if underscore:
            table = str.maketrans({" ": "_", "-": "", ":": "", ",": ""})
            name = name.translate(table)
        return name

    def _create_metadata(
        self,
        request_code: str,
        package_title: str,
        package_owner: str,
        package_keywords: list[str],
        package_topic: str,
        image_resolution: int,
        request_geometry: Polygon | MultiPolygon,
        start_date: datetime,
        end_date: datetime,
        additional_data: dict | None = None,
    ) -> MetadataINSPIRE:
        try:
            metadata = MetadataINSPIRE(
                owner_org=package_owner,
                responsable_organization_name=self.cfg.organization_name,
                responsable_organization_email=self.cfg.organization_email,
                point_of_contact_name=self.cfg.organization_name,
                point_of_contact_email=self.cfg.organization_email,
            )  # type: ignore

            metadata.title = package_title
            metadata.name = str(uuid.uuid4())
            metadata.notes = f"Drone acquisitions for request {request_code}"
            metadata.keyword = ", ".join(package_keywords)
            metadata.classification_category = package_topic
            metadata.spatial = request_geometry
            current_date = datetime.now(tz=UTC)
            metadata.tref_date = current_date.isoformat().split("+")[0]
            metadata.tref_date_creation = current_date.isoformat().split("+")[0]
            metadata.tref_date_publication = current_date.isoformat().split("+")[0]
            metadata.tref_date_revision = current_date.isoformat().split("+")[0]
            metadata.data_temporal_extent_begin_date = start_date.isoformat()
            metadata.data_temporal_extent_end_date = end_date.isoformat()
            metadata.quality_and_validity_spatial_resolution_latitude = str(image_resolution)
            metadata.quality_and_validity_spatial_resolution_longitude = str(image_resolution)
            metadata.quality_and_validity_spatial_resolution_measureunit = "cm"
            metadata.request_code = request_code
            metadata.destinatary_organization = ""
            if additional_data:
                metadata.external_attributes = additional_data
            log.debug("Metadata prepared", metadata=metadata.model_dump(by_alias=True))
            return metadata
        except Exception as e:
            log.exception("Failed to create metadata", error=str(e))
            raise UploadError(e)

    def _upload_metadata(self, metadata: MetadataINSPIRE) -> str:
        log.info("Uploading metadata to datalake...")
        try:
            response = requests.post(
                self.package_url,
                json=metadata.model_dump(by_alias=True),
                headers=self.authorize(),
            )
            response.raise_for_status()
            log.debug("Metadata uploaded", response=response.json())
            return response.json()["result"]["id"]
        except requests.HTTPError as e:
            log.exception("An HTTP error occurred", error=response.json())  # type: ignore
            raise UploadError(e)
        except Exception as e:
            log.exception("An unexpected error occurred", error=e)
            raise UploadError(e)

    def _upload_resource(
        self,
        package_id: str,
        resource_path: Path,
        resource_name: str,
        datatype_id: int,
        time_start: datetime,
        time_end: datetime,
    ) -> str:
        try:
            extension = resource_path.suffix.lstrip(".")
            resource_metadata = ResourceCreateRequest(
                package_id=package_id,
                datatype_resource=datatype_id,
                file_date_start=time_start,
                file_date_end=time_end,
                format=extension,
                name=resource_name,
            )
            with open(resource_path, "rb") as file:
                data = resource_metadata.model_dump(mode="json", by_alias=True)
                log.info("Uploading resource", resource=resource_path.name)
                log.debug("Metadata", data=data)
                response = requests.post(
                    self.resource_create_url,
                    data=data,
                    headers=self.authorize(),
                    files=[("upload", file)],
                )
                response.raise_for_status()
                return response.json()["result"]["url"]
        except requests.HTTPError as e:
            log.exception("An HTTP error occurred", error=response.json())  # type: ignore
            raise UploadError(e)
        except Exception as e:
            log.exception("An unexpected error occurred", error=str(e))
            raise UploadError(e)

    def upload_results(
        self,
        request: ProcessingRequest,
        datatypes: dict[int, str],
        results: dict[str, Path],
        package_title: str | None = None,
    ) -> list[dict[str, str]]:
        if not results:
            raise UploadError("Request failed: no data to upload to datalake")

        # Creating package title and resource name if not provided
        if not package_title:
            log.info("Generating package title...")
            package_title = f"Drone mission - situation: {request.situation_id}, request: {request.request_id}"

        log.info(f"Using package with name {package_title}")
        datasets = []

        for resource_name, resource_path in results.items():
            # Creating new package metadata
            dataset_metadata = self._create_metadata(
                package_title=package_title,
                package_owner=self.cfg.owner_org,
                package_keywords=self.cfg.data.keywords,
                package_topic=self.cfg.data.topic,
                image_resolution=self.cfg.data.resolution,
                request_geometry=request.feature.geometry,  # type: ignore
                start_date=request.start,
                end_date=request.end,
                request_code=request.request_id,
                additional_data=dict(
                    situation_id=request.situation_id,
                    request_code=request.request_id,
                ),
            )
            # uploading package
            log.info(f"Uploading metadata for package {package_title}...")
            package_id = self._upload_metadata(dataset_metadata)

            for datatype_id, datatype_name in datatypes.items():
                log.info(f"Updating resource to package {package_id}...")
                url = [
                    self._upload_resource(
                        package_id=package_id,
                        resource_path=resource_path,
                        resource_name=resource_path.name,
                        datatype_id=datatype_id,
                        time_start=request.start,
                        time_end=request.end,
                    )
                ]
                log.info("Resource uploaded", url=url)
                datasets.append(dict(dataset_id=package_id, url=url))
        return datasets
