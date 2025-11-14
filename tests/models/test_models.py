"""Tests for GPUInstance and related models."""

from time import time
from typing import Any

import pytest
from pydantic import ValidationError

from gpuport_collectors.models import AvailabilityStatus, GPUInstance


class TestAvailabilityStatus:
    """Tests for AvailabilityStatus enum."""

    def test_all_values(self):
        """Test all availability status values."""
        assert AvailabilityStatus.HIGH.value == "High"
        assert AvailabilityStatus.MEDIUM.value == "Medium"
        assert AvailabilityStatus.LOW.value == "Low"
        assert AvailabilityStatus.NOT_AVAILABLE.value == "Not Available"

    def test_enum_from_string(self):
        """Test creating enum from string value."""
        assert AvailabilityStatus("High") == AvailabilityStatus.HIGH
        assert AvailabilityStatus("Medium") == AvailabilityStatus.MEDIUM
        assert AvailabilityStatus("Low") == AvailabilityStatus.LOW
        assert AvailabilityStatus("Not Available") == AvailabilityStatus.NOT_AVAILABLE


class TestGPUInstance:
    """Tests for GPUInstance model."""

    def test_minimal_valid_instance(self):
        """Test creating a GPU instance with minimal required fields."""
        instance = GPUInstance(
            provider="RunPod",
            instance_type="RTX 4090",
            v_cpus=8.0,
            memory_gib=32.0,
            accelerator_name="RTX 4090",
            accelerator_count=1.0,
            region="US",
            price=0.79,
            availability=AvailabilityStatus.HIGH,
        )
        assert instance.provider == "RunPod"
        assert instance.instance_type == "RTX 4090"
        assert instance.v_cpus == 8.0
        assert instance.memory_gib == 32.0
        assert instance.accelerator_name == "RTX 4090"
        assert instance.accelerator_count == 1.0
        assert instance.region == "US"
        assert instance.price == 0.79
        assert instance.availability == AvailabilityStatus.HIGH
        assert instance.arch is None
        assert instance.accelerator_mem_gib is None
        assert instance.gpu_info is None
        assert instance.availability_zone is None
        assert instance.spot_price is None
        assert instance.quantity is None
        assert instance.raw_data == {}
        assert isinstance(instance.collected_at, int)
        assert instance.collected_at > 0

    def test_full_instance(self):
        """Test creating a GPU instance with all fields."""
        collected_time = int(time())
        instance = GPUInstance(
            provider="Lambda Labs",
            instance_type="gpu_1x_h100_80gb",
            v_cpus=16.0,
            memory_gib=64.0,
            arch="x86_64",
            accelerator_name="H100 80GB",
            accelerator_count=1.0,
            accelerator_mem_gib=80.0,
            gpu_info="80GB HBM3",
            region="EU",
            availability_zone="eu-west-1",
            price=2.49,
            spot_price=1.25,
            availability=AvailabilityStatus.MEDIUM,
            quantity=5,
            collected_at=collected_time,
            raw_data={"foo": "bar", "nested": {"key": "value"}},
        )
        assert instance.provider == "Lambda Labs"
        assert instance.instance_type == "gpu_1x_h100_80gb"
        assert instance.v_cpus == 16.0
        assert instance.memory_gib == 64.0
        assert instance.arch == "x86_64"
        assert instance.accelerator_name == "H100 80GB"
        assert instance.accelerator_count == 1.0
        assert instance.accelerator_mem_gib == 80.0
        assert instance.gpu_info == "80GB HBM3"
        assert instance.region == "EU"
        assert instance.availability_zone == "eu-west-1"
        assert instance.price == 2.49
        assert instance.spot_price == 1.25
        assert instance.availability == AvailabilityStatus.MEDIUM
        assert instance.quantity == 5
        assert instance.collected_at == collected_time
        assert instance.raw_data == {"foo": "bar", "nested": {"key": "value"}}

    def test_provider_validation_empty(self):
        """Test that Provider cannot be empty."""
        with pytest.raises(ValidationError, match="at least 1 character"):
            GPUInstance(
                provider="",
                instance_type="RTX 4090",
                v_cpus=8.0,
                memory_gib=32.0,
                accelerator_name="RTX 4090",
                accelerator_count=1.0,
                region="US",
                price=0.79,
                availability=AvailabilityStatus.HIGH,
            )

    def test_provider_validation_whitespace(self):
        """Test that Provider cannot be whitespace-only."""
        with pytest.raises(ValidationError, match="cannot be empty or whitespace-only"):
            GPUInstance(
                provider="   ",
                instance_type="RTX 4090",
                v_cpus=8.0,
                memory_gib=32.0,
                accelerator_name="RTX 4090",
                accelerator_count=1.0,
                region="US",
                price=0.79,
                availability=AvailabilityStatus.HIGH,
            )

    def test_provider_strips_whitespace(self):
        """Test that Provider whitespace is stripped."""
        instance = GPUInstance(
            provider="  RunPod  ",
            instance_type="RTX 4090",
            v_cpus=8.0,
            memory_gib=32.0,
            accelerator_name="RTX 4090",
            accelerator_count=1.0,
            region="US",
            price=0.79,
            availability=AvailabilityStatus.HIGH,
        )
        assert instance.provider == "RunPod"

    def test_accelerator_name_validation_empty(self):
        """Test that AcceleratorName cannot be empty."""
        with pytest.raises(ValidationError, match="at least 1 character"):
            GPUInstance(
                provider="RunPod",
                instance_type="RTX 4090",
                v_cpus=8.0,
                memory_gib=32.0,
                accelerator_name="",
                accelerator_count=1.0,
                region="US",
                price=0.79,
                availability=AvailabilityStatus.HIGH,
            )

    def test_accelerator_name_strips_whitespace(self):
        """Test that AcceleratorName whitespace is stripped."""
        instance = GPUInstance(
            provider="RunPod",
            instance_type="RTX 4090",
            v_cpus=8.0,
            memory_gib=32.0,
            accelerator_name="  RTX 4090  ",
            accelerator_count=1.0,
            region="US",
            price=0.79,
            availability=AvailabilityStatus.HIGH,
        )
        assert instance.accelerator_name == "RTX 4090"

    def test_region_validation_empty(self):
        """Test that Region cannot be empty."""
        with pytest.raises(ValidationError, match="at least 1 character"):
            GPUInstance(
                provider="RunPod",
                instance_type="RTX 4090",
                v_cpus=8.0,
                memory_gib=32.0,
                accelerator_name="RTX 4090",
                accelerator_count=1.0,
                region="",
                price=0.79,
                availability=AvailabilityStatus.HIGH,
            )

    def test_region_strips_whitespace(self):
        """Test that Region whitespace is stripped."""
        instance = GPUInstance(
            provider="RunPod",
            instance_type="RTX 4090",
            v_cpus=8.0,
            memory_gib=32.0,
            accelerator_name="RTX 4090",
            accelerator_count=1.0,
            region="  US  ",
            price=0.79,
            availability=AvailabilityStatus.HIGH,
        )
        assert instance.region == "US"

    def test_price_validation_positive(self):
        """Test that Price must be positive."""
        with pytest.raises(ValidationError, match="greater than 0"):
            GPUInstance(
                provider="RunPod",
                instance_type="RTX 4090",
                v_cpus=8.0,
                memory_gib=32.0,
                accelerator_name="RTX 4090",
                accelerator_count=1.0,
                region="US",
                price=0,
                availability=AvailabilityStatus.HIGH,
            )

        with pytest.raises(ValidationError, match="greater than 0"):
            GPUInstance(
                provider="RunPod",
                instance_type="RTX 4090",
                v_cpus=8.0,
                memory_gib=32.0,
                accelerator_name="RTX 4090",
                accelerator_count=1.0,
                region="US",
                price=-1,
                availability=AvailabilityStatus.HIGH,
            )

    def test_price_validation_reasonable_max(self):
        """Test that Price cannot exceed reasonable maximum."""
        with pytest.raises(ValidationError, match="exceeds reasonable maximum"):
            GPUInstance(
                provider="RunPod",
                instance_type="RTX 4090",
                v_cpus=8.0,
                memory_gib=32.0,
                accelerator_name="RTX 4090",
                accelerator_count=1.0,
                region="US",
                price=1001,
                availability=AvailabilityStatus.HIGH,
            )

    def test_price_at_max_boundary(self):
        """Test that Price of exactly 1000 is allowed."""
        instance = GPUInstance(
            provider="RunPod",
            instance_type="RTX 4090",
            v_cpus=8.0,
            memory_gib=32.0,
            accelerator_name="RTX 4090",
            accelerator_count=1.0,
            region="US",
            price=1000,
            availability=AvailabilityStatus.HIGH,
        )
        assert instance.price == 1000

    def test_spot_price_validation_positive(self):
        """Test that SpotPrice must be positive when provided."""
        with pytest.raises(ValidationError, match="greater than 0"):
            GPUInstance(
                provider="RunPod",
                instance_type="RTX 4090",
                v_cpus=8.0,
                memory_gib=32.0,
                accelerator_name="RTX 4090",
                accelerator_count=1.0,
                region="US",
                price=0.79,
                spot_price=0,
                availability=AvailabilityStatus.HIGH,
            )

    def test_spot_price_validation_reasonable_max(self):
        """Test that SpotPrice cannot exceed reasonable maximum."""
        with pytest.raises(ValidationError, match="exceeds reasonable maximum"):
            GPUInstance(
                provider="RunPod",
                instance_type="RTX 4090",
                v_cpus=8.0,
                memory_gib=32.0,
                accelerator_name="RTX 4090",
                accelerator_count=1.0,
                region="US",
                price=0.79,
                spot_price=1001,
                availability=AvailabilityStatus.HIGH,
            )

    def test_quantity_validation_non_negative(self):
        """Test that Quantity must be non-negative."""
        with pytest.raises(ValidationError, match="greater than or equal to 0"):
            GPUInstance(
                provider="RunPod",
                instance_type="RTX 4090",
                v_cpus=8.0,
                memory_gib=32.0,
                accelerator_name="RTX 4090",
                accelerator_count=1.0,
                region="US",
                price=0.79,
                availability=AvailabilityStatus.HIGH,
                quantity=-1,
            )

    def test_quantity_zero_allowed(self):
        """Test that Quantity of 0 is allowed."""
        instance = GPUInstance(
            provider="RunPod",
            instance_type="RTX 4090",
            v_cpus=8.0,
            memory_gib=32.0,
            accelerator_name="RTX 4090",
            accelerator_count=1.0,
            region="US",
            price=0.79,
            availability=AvailabilityStatus.HIGH,
            quantity=0,
        )
        assert instance.quantity == 0

    def test_vcpus_validation_positive(self):
        """Test that vCPUs must be positive."""
        with pytest.raises(ValidationError, match="greater than 0"):
            GPUInstance(
                provider="RunPod",
                instance_type="RTX 4090",
                v_cpus=0,
                memory_gib=32.0,
                accelerator_name="RTX 4090",
                accelerator_count=1.0,
                region="US",
                price=0.79,
                availability=AvailabilityStatus.HIGH,
            )

    def test_vcpus_fractional_allowed(self):
        """Test that fractional vCPUs are allowed."""
        instance = GPUInstance(
            provider="RunPod",
            instance_type="RTX 4090",
            v_cpus=0.5,
            memory_gib=32.0,
            accelerator_name="RTX 4090",
            accelerator_count=1.0,
            region="US",
            price=0.79,
            availability=AvailabilityStatus.HIGH,
        )
        assert instance.v_cpus == 0.5

    def test_memory_validation_positive(self):
        """Test that MemoryGiB must be positive."""
        with pytest.raises(ValidationError, match="greater than 0"):
            GPUInstance(
                provider="RunPod",
                instance_type="RTX 4090",
                v_cpus=8.0,
                memory_gib=0,
                accelerator_name="RTX 4090",
                accelerator_count=1.0,
                region="US",
                price=0.79,
                availability=AvailabilityStatus.HIGH,
            )

    def test_accelerator_count_validation_positive(self):
        """Test that AcceleratorCount must be positive."""
        with pytest.raises(ValidationError, match="greater than 0"):
            GPUInstance(
                provider="RunPod",
                instance_type="RTX 4090",
                v_cpus=8.0,
                memory_gib=32.0,
                accelerator_name="RTX 4090",
                accelerator_count=0,
                region="US",
                price=0.79,
                availability=AvailabilityStatus.HIGH,
            )

    def test_accelerator_count_fractional_allowed(self):
        """Test that fractional AcceleratorCount is allowed."""
        instance = GPUInstance(
            provider="RunPod",
            instance_type="RTX 4090",
            v_cpus=8.0,
            memory_gib=32.0,
            accelerator_name="RTX 4090",
            accelerator_count=0.5,
            region="US",
            price=0.79,
            availability=AvailabilityStatus.HIGH,
        )
        assert instance.accelerator_count == 0.5

    def test_accelerator_mem_gib_validation_positive(self):
        """Test that accelerator_mem_gib must be positive when provided."""
        with pytest.raises(ValidationError, match="greater than 0"):
            GPUInstance(
                provider="RunPod",
                instance_type="RTX 4090",
                v_cpus=8.0,
                memory_gib=32.0,
                accelerator_name="RTX 4090",
                accelerator_count=1.0,
                region="US",
                price=0.79,
                availability=AvailabilityStatus.HIGH,
                accelerator_mem_gib=0,
            )

        with pytest.raises(ValidationError, match="greater than 0"):
            GPUInstance(
                provider="RunPod",
                instance_type="RTX 4090",
                v_cpus=8.0,
                memory_gib=32.0,
                accelerator_name="RTX 4090",
                accelerator_count=1.0,
                region="US",
                price=0.79,
                availability=AvailabilityStatus.HIGH,
                accelerator_mem_gib=-1,
            )

    def test_accelerator_mem_gib_none_allowed(self):
        """Test that accelerator_mem_gib can be None."""
        instance = GPUInstance(
            provider="RunPod",
            instance_type="RTX 4090",
            v_cpus=8.0,
            memory_gib=32.0,
            accelerator_name="RTX 4090",
            accelerator_count=1.0,
            region="US",
            price=0.79,
            availability=AvailabilityStatus.HIGH,
            accelerator_mem_gib=None,
        )
        assert instance.accelerator_mem_gib is None

    def test_accelerator_mem_gib_typical_values(self):
        """Test that typical accelerator_mem_gib values work correctly."""
        instance = GPUInstance(
            provider="RunPod",
            instance_type="RTX 4090",
            v_cpus=8.0,
            memory_gib=32.0,
            accelerator_name="RTX 4090",
            accelerator_count=1.0,
            region="US",
            price=0.79,
            availability=AvailabilityStatus.HIGH,
            accelerator_mem_gib=24.0,
        )
        assert instance.accelerator_mem_gib == 24.0

        # Test H100 80GB
        instance2 = GPUInstance(
            provider="Lambda Labs",
            instance_type="H100",
            v_cpus=16.0,
            memory_gib=64.0,
            accelerator_name="H100 80GB",
            accelerator_count=1.0,
            region="US",
            price=2.49,
            availability=AvailabilityStatus.HIGH,
            accelerator_mem_gib=80.0,
        )
        assert instance2.accelerator_mem_gib == 80.0

    def test_collected_at_default_is_valid(self):
        """Test that default CollectedAt is a valid Unix timestamp."""
        instance = GPUInstance(
            provider="RunPod",
            instance_type="RTX 4090",
            v_cpus=8.0,
            memory_gib=32.0,
            accelerator_name="RTX 4090",
            accelerator_count=1.0,
            region="US",
            price=0.79,
            availability=AvailabilityStatus.HIGH,
        )
        assert isinstance(instance.collected_at, int)
        assert instance.collected_at > 1600000000  # After 2020

    def test_collected_at_validation_range(self):
        """Test that CollectedAt must be within reasonable range."""
        # Far past timestamp (more than 1 year ago)
        with pytest.raises(ValidationError, match="outside reasonable range"):
            GPUInstance(
                provider="RunPod",
                instance_type="RTX 4090",
                v_cpus=8.0,
                memory_gib=32.0,
                accelerator_name="RTX 4090",
                accelerator_count=1.0,
                region="US",
                price=0.79,
                availability=AvailabilityStatus.HIGH,
                collected_at=1000000000,  # Year 2001
            )

        # Far future timestamp (more than 1 day)
        with pytest.raises(ValidationError, match="outside reasonable range"):
            GPUInstance(
                provider="RunPod",
                instance_type="RTX 4090",
                v_cpus=8.0,
                memory_gib=32.0,
                accelerator_name="RTX 4090",
                accelerator_count=1.0,
                region="US",
                price=0.79,
                availability=AvailabilityStatus.HIGH,
                collected_at=int(time()) + (2 * 24 * 60 * 60),  # 2 days in future
            )

    def test_optional_string_empty_becomes_none(self):
        """Test that empty optional strings become None."""
        instance = GPUInstance(
            provider="RunPod",
            instance_type="RTX 4090",
            v_cpus=8.0,
            memory_gib=32.0,
            accelerator_name="RTX 4090",
            accelerator_count=1.0,
            region="US",
            price=0.79,
            availability=AvailabilityStatus.HIGH,
            arch="  ",  # Whitespace-only should become None
            gpu_info="",  # Empty should become None
        )
        assert instance.arch is None
        assert instance.gpu_info is None

    def test_model_dump(self):
        """Test that model can be dumped to dict."""
        collected_time = int(time())
        instance = GPUInstance(
            provider="RunPod",
            instance_type="RTX 4090",
            v_cpus=8.0,
            memory_gib=32.0,
            accelerator_name="RTX 4090",
            accelerator_count=1.0,
            region="US",
            price=0.79,
            availability=AvailabilityStatus.HIGH,
            collected_at=collected_time,
        )
        data = instance.model_dump()
        assert data["provider"] == "RunPod"
        assert data["instance_type"] == "RTX 4090"
        assert data["v_cpus"] == 8.0
        assert data["memory_gib"] == 32.0
        assert data["accelerator_name"] == "RTX 4090"
        assert data["accelerator_count"] == 1.0
        assert data["region"] == "US"
        assert data["price"] == 0.79
        # model_dump() serializes enum to its string value
        assert data["availability"] == AvailabilityStatus.HIGH.value
        assert data["availability"] == "High"
        assert data["collected_at"] == collected_time

    def test_model_dump_json(self):
        """Test that model can be serialized to JSON."""
        instance = GPUInstance(
            provider="RunPod",
            instance_type="RTX 4090",
            v_cpus=8.0,
            memory_gib=32.0,
            accelerator_name="RTX 4090",
            accelerator_count=1.0,
            region="US",
            price=0.79,
            availability=AvailabilityStatus.HIGH,
        )
        json_str = instance.model_dump_json()
        assert '"provider":"RunPod"' in json_str
        assert '"instance_type":"RTX 4090"' in json_str
        assert '"v_cpus":8.0' in json_str
        assert '"price":0.79' in json_str

    def test_model_from_dict(self):
        """Test creating model from dict."""
        data: dict[str, Any] = {
            "provider": "RunPod",
            "instance_type": "RTX 4090",
            "v_cpus": 8.0,
            "memory_gib": 32.0,
            "accelerator_name": "RTX 4090",
            "accelerator_count": 1.0,
            "region": "US",
            "price": 0.79,
            "availability": "High",
        }
        instance = GPUInstance(**data)
        assert instance.provider == "RunPod"
        assert instance.instance_type == "RTX 4090"
        assert instance.availability == AvailabilityStatus.HIGH

    def test_availability_enum_validation(self):
        """Test that invalid availability value is rejected."""
        with pytest.raises(ValidationError, match="Input should be"):
            GPUInstance(
                provider="RunPod",
                instance_type="RTX 4090",
                v_cpus=8.0,
                memory_gib=32.0,
                accelerator_name="RTX 4090",
                accelerator_count=1.0,
                region="US",
                price=0.79,
                availability="InvalidStatus",  # type: ignore[arg-type]
            )
