from typing import Any

from pydantic import BaseModel
from pydantic_settings import BaseSettings, PydanticBaseSettingsSource, SettingsConfigDict, YamlConfigSettingsSource


class OAuthSettings(BaseModel):
    url: str
    username: str
    password: str
    client_id: str
    client_secret: str
    api_key: str
    grant_type: str = "password"
    scope: str = "openid"


class DatasetSettings(BaseModel):
    resolution: int = 10  # cm/pixel
    topic: str = "imageryBaseMapsEarthCover"
    keywords: list[str] = ["Delineation Map", "Wildfire"]


class CKANSettings(BaseModel):
    url: str
    owner_org: str
    organization_email: str
    organization_name: str
    auth: OAuthSettings
    data: DatasetSettings = DatasetSettings()


class RabbitMQSettings(BaseModel):
    host: str
    port: int
    vhost: str = "/"
    username: str
    password: str
    ssl: bool = True
    exchange: str = "amq.topic"
    routing_key_prefix: str = "request.status"
    retry_count: int = 3

    @property
    def url(self) -> str:
        return f"amqps://{self.username}:{self.password}@{self.host}:{self.port}{self.vhost}"


class ODMProcessingOptions(BaseModel):
    dsm: bool = False
    dtm: bool = False
    # resolution in cm/pixel
    resolution: int | None = 10
    texture_size: int = 2048
    # skip dense reconstruction
    fast_orthophoto: bool = False
    # reduce matching (good or 100s of images)
    matcher_neighbors: int = 8
    # balance between speed and quality
    feature_quality: str = "medium"
    point_cloud_quality: str = "medium"
    ignore_gsd: bool = False
    ski_3d_model: bool = True
    skip_post_processing: bool = False
    radiometric_calibration: str | None = None

    def to_pyodm_options(self) -> dict[str, Any]:
        options = {}
        options["orthophoto-resolution"] = self.resolution
        options["fast-orthophoto"] = self.fast_orthophoto
        options["skip-3dmodel"] = self.ski_3d_model
        options["matcher-neighbors"] = self.matcher_neighbors
        options["feature-quality"] = self.feature_quality
        options["pc-quality"] = self.point_cloud_quality
        options["skip-post-processing"] = self.skip_post_processing
        options["ignore-gsd"] = self.ignore_gsd
        options["radiometric-calibration"] = self.radiometric_calibration
        return options


class NodeODMSettings(BaseModel):
    host: str
    port: int
    token: str
    max_concurrent_tasks: int = 2
    poll_interval: int = 30
    poll_retries: int = 5
    cancel_on_shutdown: bool = False
    options: ODMProcessingOptions = ODMProcessingOptions()

    @property
    def url(self) -> str:
        """Get the full NodeODM URL."""
        return f"http://{self.host}:{self.port}"


class Settings(BaseSettings):
    """Application settings loaded from environment variables."""

    model_config = SettingsConfigDict(
        yaml_file="config.yml",
        yaml_file_encoding="utf-8",
    )

    nodeodm: NodeODMSettings
    ckan: CKANSettings
    rmq: RabbitMQSettings

    @classmethod
    def settings_customise_sources(
        cls,
        settings_cls: type[BaseSettings],
        init_settings: PydanticBaseSettingsSource,
        env_settings: PydanticBaseSettingsSource,
        dotenv_settings: PydanticBaseSettingsSource,
        file_secret_settings: PydanticBaseSettingsSource,
    ) -> tuple[PydanticBaseSettingsSource, ...]:
        return (YamlConfigSettingsSource(settings_cls),)


# singleton instance
settings = Settings()  # type: ignore
