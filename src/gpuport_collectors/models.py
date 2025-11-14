"""Data models for GPUPort collectors.

This module defines the core data structures used across all provider collectors,
ensuring consistent schema for fair cross-provider comparison.

Schema inspired by SkyPilot's catalog format for compatibility.
"""

from enum import Enum
from time import time
from typing import Any

from pydantic import BaseModel, Field, field_validator


class AvailabilityStatus(str, Enum):
    """Standardized availability status enum.

    Values map provider-specific availability to consistent states for
    cross-provider comparison.
    """

    HIGH = "High"
    MEDIUM = "Medium"
    LOW = "Low"
    NOT_AVAILABLE = "Not Available"


class GPUInstance(BaseModel):
    """Normalized GPU instance data model.

    This model represents a GPU instance from any provider, with all data
    normalized to a consistent schema for fair comparison. Schema is compatible
    with SkyPilot's catalog format.

    Attributes:
        provider: Cloud provider name (e.g., "RunPod", "Lambda Labs", "Vast.ai")
        instance_type: The type/name of the instance
        v_cpus: The number of virtual CPUs (float for fractional vCPUs)
        memory_gib: The amount of memory in GiB
        arch: The processor architecture (e.g., "x86_64", "arm64")
        accelerator_name: The name of accelerators/GPUs (e.g., "RTX 4090", "H100")
        accelerator_count: The number of accelerators/GPUs
        accelerator_mem_gib: Accelerator memory in GiB (e.g., 24 for RTX 4090)
        gpu_info: Human-readable GPU information (e.g., device memory)
        region: The region of the resource
        availability_zone: The availability zone (empty if not supported)
        price: The on-demand price in USD per hour
        spot_price: The spot price in USD per hour (None if not available)
        availability: Current availability status
        quantity: Number of available instances (None if not supported)
        collected_at: Unix timestamp when data was collected
        raw_data: Original raw data from provider for debugging/auditing
    """

    provider: str = Field(
        ...,
        min_length=1,
        description="Cloud provider name",
    )
    instance_type: str = Field(
        ...,
        min_length=1,
        description="The type of instance",
    )
    v_cpus: float | None = Field(
        default=None,
        gt=0,
        description="The number of virtual CPUs (None if not provided by provider)",
    )
    memory_gib: float | None = Field(
        default=None,
        gt=0,
        description="The amount of memory in GiB (None if not provided by provider)",
    )
    arch: str | None = Field(
        default=None,
        description="The processor architecture of instance type",
    )
    accelerator_name: str = Field(
        ...,
        min_length=1,
        description="The name of accelerators (GPU/TPU)",
    )
    accelerator_count: float = Field(
        ...,
        gt=0,
        description="The number of accelerators (GPU/TPU)",
    )
    accelerator_mem_gib: float | None = Field(
        default=None,
        gt=0,
        description="Accelerator memory in GiB",
    )
    gpu_info: str | None = Field(
        default=None,
        description="Human readable information of the GPU (e.g., device memory)",
    )
    region: str = Field(
        ...,
        min_length=1,
        description="The region of the resource",
    )
    availability_zone: str | None = Field(
        default=None,
        description="The availability zone of the resource",
    )
    price: float = Field(
        ...,
        ge=0,
        description="The price in USD per hour (0 if not available)",
    )
    spot_price: float | None = Field(
        default=None,
        ge=0,
        description="The spot price in USD per hour (0 if not available)",
    )
    availability: AvailabilityStatus = Field(
        ...,
        description="Current availability status",
    )
    quantity: int | None = Field(
        default=None,
        ge=0,
        description="Number of available instances",
    )
    collected_at: int = Field(
        default_factory=lambda: int(time()),
        description="Unix timestamp when data was collected",
    )
    raw_data: dict[str, Any] = Field(
        default_factory=dict,
        description="Original raw data from provider",
    )

    @field_validator("provider", "instance_type", "accelerator_name", "region")
    @classmethod
    def validate_non_empty_string(cls, v: str) -> str:
        """Validate that string fields are not empty or whitespace-only."""
        if not v or not v.strip():
            msg = "Field cannot be empty or whitespace-only"
            raise ValueError(msg)
        return v.strip()

    @field_validator("arch", "gpu_info", "availability_zone")
    @classmethod
    def validate_optional_string(cls, v: str | None) -> str | None:
        """Validate optional string fields and strip whitespace."""
        if v is not None:
            v = v.strip()
            if not v:
                return None
        return v

    @field_validator("price", "spot_price")
    @classmethod
    def validate_reasonable_price(cls, v: float | None) -> float | None:
        """Validate that price is within reasonable bounds."""
        if v is not None and v > 1000:  # $1000/hour seems like a reasonable upper bound
            msg = "Price exceeds reasonable maximum ($1000/hour)"
            raise ValueError(msg)
        return v

    @field_validator("collected_at")
    @classmethod
    def validate_timestamp(cls, v: int) -> int:
        """Validate that timestamp is reasonable (not in far future or past)."""
        current_time = int(time())
        # Allow timestamps within 1 year in the past and 1 day in the future
        one_year_ago = current_time - (365 * 24 * 60 * 60)
        one_day_future = current_time + (24 * 60 * 60)
        if v < one_year_ago or v > one_day_future:
            msg = "Timestamp is outside reasonable range"
            raise ValueError(msg)
        return v

    model_config = {
        "json_schema_extra": {
            "examples": [
                {
                    "provider": "RunPod",
                    "instance_type": "RTX 4090",
                    "v_cpus": 8.0,
                    "memory_gib": 32.0,
                    "arch": "x86_64",
                    "accelerator_name": "RTX 4090",
                    "accelerator_count": 1.0,
                    "accelerator_mem_gib": 24.0,
                    "gpu_info": "24GB GDDR6X",
                    "region": "US",
                    "availability_zone": "us-east-1",
                    "price": 0.79,
                    "spot_price": 0.39,
                    "availability": "High",
                    "quantity": 10,
                    "collected_at": 1699876800,
                    "raw_data": {},
                }
            ]
        }
    }


__all__ = ["AvailabilityStatus", "GPUInstance"]
