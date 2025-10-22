"""
Tests for odm_tools.models module.
Focus: Testing geojson_pydantic compatibility and ProcessingRequest validation.
"""

import json
from datetime import datetime
from pathlib import Path

import pytest

from odm_tools.models import (
    DataType,
    MetadataINSPIRE,
    ODMTask,
    ProcessingRequest,
    TaskStatus,
)


class TestProcessingRequest:
    """Test ProcessingRequest model and geojson_pydantic compatibility."""

    def test_valid_request_creation(self, valid_request_data):
        """Test creating ProcessingRequest with valid data."""
        request = ProcessingRequest(**valid_request_data, file_path=Path("/test/path"))

        assert request.request_id == "test-request-123"
        assert request.situation_id == "test-situation-456"
        assert request.datatype_ids == [22002, 22001]
        assert request.feature.type == "Feature"
        assert request.feature.geometry.type == "Polygon"

    def test_geojson_pydantic_feature_compatibility(self, valid_request_data):
        """Test that geojson_pydantic Feature works correctly."""
        request = ProcessingRequest(**valid_request_data, file_path=Path("/test/path"))

        # Verify Feature structure
        assert hasattr(request.feature, "geometry")
        assert hasattr(request.feature, "properties")
        assert request.feature.geometry.coordinates is not None

    def test_multipolygon_geometry(self, valid_multipolygon_data):
        """Test ProcessingRequest with MultiPolygon geometry."""
        request = ProcessingRequest(**valid_multipolygon_data, file_path=Path("/test/path"))

        assert request.feature.geometry.type == "MultiPolygon"
        assert len(request.feature.geometry.coordinates) == 2

    def test_alias_handling(self, valid_request_data):
        """Test that camelCase aliases are properly converted to snake_case."""
        request = ProcessingRequest(**valid_request_data, file_path=Path("/test/path"))

        # Should accept requestId and convert to request_id
        assert request.request_id == "test-request-123"
        # Should accept situationId and convert to situation_id
        assert request.situation_id == "test-situation-456"
        # Should accept datatypeIds and convert to datatype_ids
        assert request.datatype_ids == [22002, 22001]

    def test_extra_fields_ignored(self, valid_request_data):
        """Test that extra fields are ignored due to ConfigDict(extra='ignore')."""
        data_with_extra = valid_request_data.copy()
        data_with_extra["extraField"] = "should be ignored"

        request = ProcessingRequest(**data_with_extra, file_path=Path("/test/path"))

        assert not hasattr(request, "extraField")
        assert not hasattr(request, "extra_field")

    def test_from_file_method(self, request_file, valid_request_data):
        """Test loading ProcessingRequest from JSON file."""
        # Create temporary request.json
        with open(request_file, "w") as f:
            json.dump(valid_request_data, f)

        # Load from file
        request = ProcessingRequest.from_file(request_file)

        assert request.request_id == "test-request-123"
        assert request.file_path == request_file

    def test_datetime_parsing(self, valid_request_data):
        """Test that datetime fields are properly parsed."""
        request = ProcessingRequest(**valid_request_data, file_path=Path("/test/path"))

        assert isinstance(request.start, datetime)
        assert isinstance(request.end, datetime)

    def test_missing_required_fields(self):
        """Test that missing required fields raise validation error."""
        incomplete_data = {
            "requestId": "test-123",
            # Missing other required fields
        }

        with pytest.raises(Exception):  # Pydantic will raise ValidationError
            ProcessingRequest(**incomplete_data, file_path=Path("/test/path"))

    def test_invalid_geometry_type(self, valid_request_data):
        """Test that invalid geometry type raises validation error."""
        invalid_data = valid_request_data.copy()
        invalid_data["feature"]["geometry"]["type"] = "None"
        invalid_data["feature"]["geometry"]["coordinates"] = [0, 0]

        # This should either work (if Point is accepted) or raise an error
        # Adjust based on your actual requirements
        with pytest.raises(Exception):
            ProcessingRequest(**invalid_data, file_path=Path("/test/path"))


class TestDataType:
    """Test DataType enum."""

    def test_datatype_values(self):
        """Test that DataType enum has correct values."""
        assert DataType.rgb.value == 22002
        assert DataType.thermal.value == 22001

    def test_datatype_names(self):
        """Test that DataType enum has correct names."""
        assert DataType.rgb.name == "rgb"
        assert DataType.thermal.name == "thermal"


class TestTaskStatus:
    """Test TaskStatus enum."""

    def test_status_values(self):
        """Test that TaskStatus enum has correct string values."""
        assert TaskStatus.queued.value == "QUEUED"
        assert TaskStatus.running.value == "RUNNING"
        assert TaskStatus.failed.value == "FAILED"
        assert TaskStatus.completed.value == "COMPLETED"
        assert TaskStatus.canceled.value == "CANCELED"


class TestODMTask:
    """Test ODMTask model."""

    def test_odm_task_creation(self):
        """Test creating ODMTask with required fields."""
        task = ODMTask(
            task_id="task-123",
            request_id="request-456",
            datatype_id=22002,
            status=TaskStatus.queued,
        )

        assert task.task_id == "task-123"
        assert task.request_id == "request-456"
        assert task.datatype_id == 22002
        assert task.status == TaskStatus.queued
        assert task.progress == 0  # Default value
        assert isinstance(task.created_at, datetime)

    def test_odm_task_with_optional_fields(self):
        """Test ODMTask with optional fields populated."""
        task = ODMTask(
            task_id="task-123",
            request_id="request-456",
            datatype_id=22002,
            status=TaskStatus.completed,
            progress=100,
            processing_time=300,
            output_path=Path("/output/path"),
            error_message=None,
        )

        assert task.progress == 100
        assert task.processing_time == 300
        assert task.output_path == Path("/output/path")


class TestMetadataINSPIRE:
    """Test MetadataINSPIRE model for CKAN uploads."""

    def test_metadata_inspire_defaults(self):
        """Test MetadataINSPIRE creation with default values."""
        metadata = MetadataINSPIRE()

        # Check some default values
        assert metadata.private is True
        assert metadata.ident_res_language == "eng"
        assert metadata.classification_category == "imageryBaseMapsEarthCover"
        assert metadata.coordinatesystemreference_code == 4326

    def test_metadata_inspire_with_custom_values(self):
        """Test MetadataINSPIRE with custom values."""
        metadata = MetadataINSPIRE(
            title="Test Dataset",
            notes="Test description",
            owner_org="test-org",
            request_code="REQ-123",
        )

        assert metadata.title == "Test Dataset"
        assert metadata.notes == "Test description"
        assert metadata.owner_org == "test-org"
        assert metadata.request_code == "REQ-123"

    def test_metadata_inspire_alias_handling(self):
        """Test that INSPIRE metadata aliases work correctly."""
        metadata = MetadataINSPIRE(
            identification_ResourceType="service",
            classification_TopicCategory="environment",
        )

        assert metadata.ident_resource_type == "service"
        assert metadata.classification_category == "environment"
