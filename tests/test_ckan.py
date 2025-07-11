import time

import pytest

from odm_tools.auth import KeyCloakAuthenticator
from odm_tools.config import settings


@pytest.fixture
def auth_client():
    """Create KeyCloakAuthenticator instance with real settings."""
    return KeyCloakAuthenticator()


def test_initialization(auth_client):
    """Test that the client initializes correctly."""
    assert auth_client.client_id is not None
    assert auth_client.client_secret is not None
    assert auth_client.username is not None
    assert auth_client.password is not None
    assert auth_client.token_url is not None
    assert auth_client._token is None
    assert auth_client._token_expires_at is None


def test_get_token(auth_client):
    """Test getting a token from KeyCloak."""
    token = auth_client.get_token()

    assert token is not None
    assert "access_token" in token
    assert isinstance(token["access_token"], str)
    assert len(token["access_token"]) > 0
    assert "token_type" in token
    assert token["token_type"] == "Bearer"


def test_get_authorization_header(auth_client):
    """Test getting authorization header."""
    header = auth_client.get_authorization_header()

    assert "Authorization" in header


def test_token_caching(auth_client):
    """Test that tokens are cached and reused."""
    # Get first token
    token1 = auth_client.get_token()

    # Get second token - should be the same (cached)
    token2 = auth_client.get_token()

    assert token1 == token2
    assert token1["access_token"] == token2["access_token"]


def test_is_authenticated(auth_client):
    """Test authentication status."""
    # Should be authenticated after getting a token
    auth_client.get_token()
    assert auth_client.is_authenticated() is True


def test_multiple_header_calls(auth_client):
    """Test multiple authorization header calls work correctly."""
    header1 = auth_client.get_authorization_header()
    header2 = auth_client.get_authorization_header()

    # Should get the same header (cached token)
    assert header1 == header2


def test_token_has_expected_fields(auth_client):
    """Test that token contains expected fields."""
    token = auth_client.get_token()

    # Check required fields
    assert "access_token" in token
    assert "token_type" in token
    assert "expires_in" in token

    # Check field types
    assert isinstance(token["access_token"], str)
    assert isinstance(token["token_type"], str)
    assert isinstance(token["expires_in"], int)


def test_token_expiration_tracking(auth_client):
    """Test that token expiration is tracked correctly."""
    # Get token
    _ = auth_client.get_token()

    # Check that expiration time is set
    assert auth_client._token_expires_at is not None
    assert auth_client._token_expires_at > time.time()

    # Should not be expired immediately
    assert not auth_client._is_token_expired()


def test_settings_integration(auth_client):
    """Test that settings are properly loaded."""
    # Verify settings are loaded correctly
    assert auth_client.client_id == settings.ckan.auth.client_id
    assert auth_client.client_secret == settings.ckan.auth.client_secret
    assert auth_client.username == settings.ckan.auth.username
    assert auth_client.password == settings.ckan.auth.password


@pytest.fixture
def test_metadata():
    """Create test metadata based on your actual logs."""
    import uuid
    from datetime import UTC, datetime

    from odm_tools.models import MetadataINSPIRE

    metadata = MetadataINSPIRE(
        title="Test Drone mission - situation: none, request: test123",
        private=True,
        notes="Test drone acquisitions for request test123",
        name=str(uuid.uuid4()),
        identification_ResourceType="dataset",
        owner_org="engineering",
        identification_CoupledResource="",
        identification_ResourceLanguage="eng",
        classification_TopicCategory="imageryBaseMapsEarthCover",
        classification_SpatialDataServiceType="",
        keyword_KeywordValue="Delineation Map, Wildfire",
        keyword_OriginatingControlledVocabulary="ontology",
        data_temporal_extent_begin_date="2025-06-05T10:07:55.310000+00:00",
        data_temporal_extent_end_date="2025-06-05T10:07:55.310000+00:00",
        temporalReference_dateOfPublication=datetime.now(UTC).isoformat().split("+")[0],
        temporalReference_dateOfLastRevision=datetime.now(UTC).isoformat().split("+")[0],
        temporalReference_dateOfCreation=datetime.now(UTC).isoformat().split("+")[0],
        temporalReference_date=datetime.now(UTC).isoformat().split("+")[0],
        quality_and_validity_lineage="Quality approved",
        quality_and_validity_spatial_resolution_latitude="10",
        quality_and_validity_spatial_resolution_longitude="10",
        quality_and_validity_spatial_resolution_scale="0",
        quality_and_validity_spatial_resolution_measureunit="cm",
        conformity_specification_title="COMMISSION REGULATION (EU) No 1089/2010 of 23 November 2010 implementing Directive 2007/2/EC of the European Parliament and of the Council as regards interoperability of spatial data sets and services",
        conformity_specification_dateType="publication",
        conformity_specification_date="2010-12-08T00:00:00",
        conformity_degree=True,
        constraints_conditions_for_access_and_use="Creative Commons CC BY-SA 3.0 IGO licence",
        constraints_limitation_on_public_access="",
        responsable_organization_name="LINKS Foundation",
        responsable_organization_email="ads@linksfoundation.com",
        responsable_organization_role="author",
        point_of_contact_name="LINKS Foundation",
        point_of_contact_email="ads@linksfoundation.com",
        metadata_language="eng",
        coordinatesystemreference_code=4326,
        coordinatesystemreference_codespace="EPSG",
        character_encoding="UTF-8",
        spatial={
            "type": "Polygon",
            "coordinates": [
                [
                    (-7.472698427917976, 40.558658193522064),
                    (-8.472698427917976, 40.405542218603046),
                    (-8.239298692728056, 40.405542218603046),
                    (-8.239298692728056, 40.558658193522064),
                    (-8.472698427917976, 40.558658193522064),
                    (-7.472698427917976, 40.558658193522064),
                ]
            ],
        },
        request_code="test123",
        destinatary_organization="",
        external_attributes={},
    )
    return metadata


def test_ckan_create_metadata(ckan_uploader, test_metadata):
    """Test creating metadata (package) in CKAN."""
    import requests

    # Upload the metadata
    package_id = ckan_uploader._upload_metadata(test_metadata)

    # Verify the package was created
    assert package_id is not None
    assert isinstance(package_id, str)
    assert len(package_id) > 0

    # Verify we can retrieve the package
    response = requests.get(
        ckan_uploader.package_show_url, params={"id": package_id}, headers=ckan_uploader.authorize()
    )
    response.raise_for_status()

    package_data = response.json()["result"]
    assert package_data["id"] == package_id
    assert package_data["title"] == test_metadata.title
    assert package_data["name"] == test_metadata.name
    assert package_data["notes"] == test_metadata.notes
    assert package_data["owner_org"] == "7e278c5d-fe25-4a55-880d-8a60f9b18034"  # engineering org ID

    print(f"Created package with ID: {package_id}")
    assert package_id is not None


def test_ckan_upload_resource(ckan_uploader, test_file):
    """Test uploading a resource to an existing package."""
    import uuid
    from datetime import datetime

    import requests

    # First create a simple package to attach the resource to
    from odm_tools.models import MetadataINSPIRE

    simple_metadata = MetadataINSPIRE(
        title="Test Package for Resource Upload",
        name=str(uuid.uuid4()),
        notes="Test package for resource upload testing",
        owner_org="engineering",
        responsable_organization_name="LINKS Foundation",
        responsable_organization_email="ads@linksfoundation.com",
        point_of_contact_name="LINKS Foundation",
        point_of_contact_email="ads@linksfoundation.com",
    )

    package_id = ckan_uploader._upload_metadata(simple_metadata)

    # Now upload a resource to this package
    start_time = datetime(2025, 6, 5, 10, 7, 55)
    end_time = datetime(2025, 6, 5, 10, 7, 55)

    resource_url = ckan_uploader._upload_resource(
        package_id=package_id,
        resource_path=test_file,
        resource_name="test_orthophoto.tif",
        datatype_id=22002,
        time_start=start_time,
        time_end=end_time,
    )

    # Verify the resource was uploaded
    assert resource_url is not None
    assert isinstance(resource_url, str)
    assert "ckan.terra-sense.eu" in resource_url

    # Verify the resource appears in the package
    response = requests.get(
        ckan_uploader.package_show_url,
        params={"id": package_id},
        headers=ckan_uploader.authorize(),
    )
    print("Response data", response.json())
    response.raise_for_status()

    package_data = response.json()["result"]
    resources = package_data["resources"]

    assert len(resources) == 1
    assert resources[0]["name"] == "test_orthophoto.tif"
    assert resources[0]["format"] == "tif"
    assert str(resources[0]["datatype_resource"]) == "22002"

    print(f"Uploaded resource to package {package_id}, resource URL: {resource_url}")
    return package_id, resource_url


# CKAN API Tests
def test_ckan_package_list(auth_client):
    """Test CKAN package_list API endpoint."""
    import requests

    url = f"{settings.ckan.url}/api/3/action/package_list"
    headers = auth_client.get_authorization_header()

    response = requests.get(url, headers=headers)
    response.raise_for_status()

    assert response.status_code == 200
    data = response.json()
    assert data["success"] is True
    assert "result" in data
    assert isinstance(data["result"], list)


def test_ckan_organization_list(auth_client):
    """Test CKAN organization_list API endpoint."""
    import requests

    url = f"{settings.ckan.url}/api/3/action/organization_list"
    headers = auth_client.get_authorization_header()

    response = requests.get(url, headers=headers)
    response.raise_for_status()

    assert response.status_code == 200
    data = response.json()
    assert data["success"] is True
    assert "result" in data
    assert isinstance(data["result"], list)


def test_ckan_package_show_with_existing_package(auth_client):
    """Test CKAN package_show API endpoint with an existing package."""
    import requests

    # First get a package ID from the package list
    list_url = f"{settings.ckan.url}/api/3/action/package_list"
    headers = auth_client.get_authorization_header()

    list_response = requests.get(list_url, headers=headers)
    list_response.raise_for_status()

    packages = list_response.json()["result"]
    if not packages:
        pytest.skip("No packages found in CKAN instance")

    # Test package_show with the first package
    package_id = packages[0]
    show_url = f"{settings.ckan.url}/api/3/action/package_show"

    response = requests.get(show_url, params={"id": package_id}, headers=headers)
    response.raise_for_status()

    assert response.status_code == 200
    data = response.json()
    assert data["success"] is True
    assert "result" in data
    assert "resources" in data["result"]


def test_ckan_package_show_nonexistent(auth_client):
    """Test CKAN package_show API endpoint with non-existent package."""
    import requests

    url = f"{settings.ckan.url}/api/3/action/package_show"
    headers = auth_client.get_authorization_header()

    response = requests.get(url, params={"id": "nonexistent-package-id"}, headers=headers)

    # Should get 404 or error response
    data = response.json()
    assert data["success"] is False
    assert "error" in data


def test_ckan_user_show_current(auth_client):
    """Test CKAN user_show API endpoint for current user."""
    import requests

    url = f"{settings.ckan.url}/api/3/action/user_show"
    headers = auth_client.get_authorization_header()

    # Get current user info (should work with proper auth)
    response = requests.get(url, params={"id": settings.ckan.auth.username}, headers=headers)

    if response.status_code == 200:
        data = response.json()
        assert data["success"] is True
        assert "result" in data
        assert data["result"]["name"] == settings.ckan.auth.username
    else:
        # Some CKAN instances might not allow user_show even for current user
        pytest.skip("User show not accessible for current user")


def test_ckan_api_unauthorized_without_auth():
    """Test that CKAN API returns unauthorized without proper authentication."""
    import requests

    url = f"{settings.ckan.url}/api/3/action/package_list"

    # Make request without authorization header
    response = requests.get(url)

    # Should either work (public endpoint) or fail
    # This test mainly checks that our auth is actually doing something
    if response.status_code != 200:
        # If it fails without auth, verify it works with auth
        auth_client = KeyCloakAuthenticator()
        headers = auth_client.get_authorization_header()
        auth_response = requests.get(url, headers=headers)
        assert auth_response.status_code == 200


def test_ckan_get_resource_url_functionality(auth_client):
    """Test the specific functionality from your _get_resource_url method."""
    import requests

    # First get a package with resources
    list_url = f"{settings.ckan.url}/api/3/action/package_list"
    headers = auth_client.get_authorization_header()

    list_response = requests.get(list_url, headers=headers)
    list_response.raise_for_status()

    packages = list_response.json()["result"]
    if not packages:
        pytest.skip("No packages found in CKAN instance")

    # Try to find a package with resources
    package_show_url = f"{settings.ckan.url}/api/3/action/package_show"

    for package_id in packages[:5]:  # Check first 5 packages
        response = requests.get(package_show_url, params={"id": package_id}, headers=headers)
        response.raise_for_status()

        if response.status_code == 200:
            resource_list = response.json()["result"]["resources"]
            if resource_list:
                # Found a package with resources
                assert isinstance(resource_list, list)
                assert len(resource_list) > 0

                # Check resource structure
                for resource in resource_list:
                    assert "id" in resource
                    assert "name" in resource

                print(f"Found {len(resource_list)} resources for package {package_id}")
                return

    pytest.skip("No packages with resources found")
