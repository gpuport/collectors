"""Cudo Compute GPU collector using REST API."""

import os
import time
from typing import Any

import aiohttp

from gpuport_collectors.base import BaseCollector, with_retry
from gpuport_collectors.config import CollectorConfig
from gpuport_collectors.models import AvailabilityStatus, GPUInstance


class CudoCollector(BaseCollector):
    """Collector for Cudo Compute GPU availability data.

    Uses Cudo Compute's REST API to fetch machine types and pricing:
    1. GET /machines-types to get all machine configurations
    2. Parse GPU info from gpuModelId and gpus count
    3. Extract pricing from prices array (on-demand pricing)

    Authentication: Bearer token via Authorization header
    API Docs: https://www.cudocompute.com/docs/api
    """

    API_BASE_URL = "https://rest.compute.cudo.org/v1"

    def __init__(self, config: CollectorConfig) -> None:
        """Initialize Cudo Compute collector.

        Args:
            config: Collector configuration

        Raises:
            ValueError: If CUDO_API_KEY environment variable is not set
        """
        super().__init__(config)
        self.api_key = os.environ.get("CUDO_API_KEY")
        if not self.api_key:
            raise ValueError("CUDO_API_KEY environment variable must be set")

    @property
    def provider_name(self) -> str:
        """Return provider name."""
        return "Cudo Compute"

    async def _execute_api_call(
        self,
        endpoint: str,
        method: str = "GET",
        params: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        """Execute a REST API call to Cudo Compute API.

        Args:
            endpoint: API endpoint path (e.g., "/machines-types")
            method: HTTP method (GET, POST, etc.)
            params: Optional query parameters

        Returns:
            JSON response data

        Raises:
            aiohttp.ClientError: On HTTP errors
        """
        url = f"{self.API_BASE_URL}{endpoint}"
        headers = {"Authorization": f"Bearer {self.api_key}"}

        async with (
            aiohttp.ClientSession() as session,
            session.request(
                method,
                url,
                headers=headers,
                params=params,
                timeout=aiohttp.ClientTimeout(total=self.config.timeout),
            ) as response,
        ):
            response.raise_for_status()
            result: dict[str, Any] = await response.json()
            return result

    def _parse_gpu_name(self, gpu_model_id: str) -> str:
        """Parse GPU name from Cudo GPU model ID.

        Args:
            gpu_model_id: GPU model ID (e.g., "nvidia-h100", "nvidia-l40s")

        Returns:
            Human-readable GPU name

        Examples:
            "nvidia-h100" -> "NVIDIA H100"
            "nvidia-h100-nvl-pcie" -> "NVIDIA H100 NVL PCIe"
            "nvidia-l40s" -> "NVIDIA L40S"
        """
        # Remove 'nvidia-' prefix and format
        if gpu_model_id.startswith("nvidia-"):
            gpu_part = gpu_model_id.replace("nvidia-", "")
        else:
            gpu_part = gpu_model_id

        # Convert to uppercase and add NVIDIA prefix
        parts = gpu_part.split("-")
        formatted_parts = []

        for part in parts:
            formatted_parts.append(part.upper())

        gpu_name = " ".join(formatted_parts)
        return f"NVIDIA {gpu_name}"

    def _extract_gpu_memory(self, gpu_model_id: str) -> int | None:
        """Extract GPU memory in GiB from GPU model ID.

        Args:
            gpu_model_id: GPU model ID (e.g., "nvidia-h100")

        Returns:
            GPU memory in GiB, or None if not determinable from ID

        Note: Cudo API doesn't expose GPU memory directly in the response.
        We use known GPU model specifications.
        """
        # Map known GPU models to memory sizes
        gpu_memory_map = {
            "nvidia-h100": 80,
            "nvidia-h100-nvl-pcie": 94,
            "nvidia-h200": 141,
            "nvidia-a100": 80,
            "nvidia-a100-40gb": 40,
            "nvidia-a40": 48,
            "nvidia-l40s": 48,
            "nvidia-l40": 48,
            "nvidia-a6000": 48,
        }

        memory = gpu_memory_map.get(gpu_model_id)
        if memory is None:
            self._logger.debug("Unknown GPU model for memory lookup", gpu_model_id=gpu_model_id)
        return memory

    def _get_on_demand_price(self, prices: list[dict[str, Any]]) -> float:
        """Extract on-demand price from prices array.

        Args:
            prices: Array of price objects with different commitment terms

        Returns:
            On-demand price per hour in USD
        """
        # Find price with no commitment term (on-demand)
        for price_obj in prices:
            if price_obj.get("commitmentTerm") == "COMMITMENT_TERM_NONE":
                price_hr = price_obj.get("priceHr", {})
                if isinstance(price_hr, dict):
                    return float(price_hr.get("value", 0.0))
                return float(price_hr) if price_hr else 0.0

        # Fallback to first price if no on-demand found
        if prices:
            price_hr = prices[0].get("priceHr", {})
            if isinstance(price_hr, dict):
                return float(price_hr.get("value", 0.0))
            return float(price_hr) if price_hr else 0.0

        return 0.0

    def _create_gpu_instance(
        self,
        machine_type: dict[str, Any],
        collected_at: int,
    ) -> GPUInstance:
        """Create a GPUInstance from Cudo machine type data.

        Args:
            machine_type: Machine type data from API
            collected_at: Unix timestamp when data was collected

        Returns:
            GPUInstance object
        """
        # Parse GPU info
        gpu_model_id = machine_type["gpuModelId"]
        gpu_name = self._parse_gpu_name(gpu_model_id)
        gpu_count = machine_type["gpus"]
        gpu_memory = self._extract_gpu_memory(gpu_model_id)

        # Extract pricing
        price = self._get_on_demand_price(machine_type.get("prices", []))

        # Cudo API doesn't expose availability status or quantity
        # Assume HIGH availability since machine types in API are available for deployment
        availability = AvailabilityStatus.HIGH

        return GPUInstance(
            # Identification
            provider="Cudo Compute",
            instance_type=machine_type["id"],
            accelerator_name=gpu_name,
            region=machine_type["dataCenterId"],
            availability_zone="",  # Cudo doesn't have separate availability zones
            # Pricing (Cudo doesn't have spot pricing in this API)
            price=price,
            spot_price=None,
            # Availability
            availability=availability,
            quantity=None,  # Not exposed in API
            # Hardware specs
            v_cpus=machine_type.get("cpuCores"),
            memory_gib=machine_type.get("memoryGib"),
            accelerator_count=gpu_count,
            accelerator_mem_gib=gpu_memory,
            # Metadata
            collected_at=collected_at,
            raw_data=machine_type,
        )

    @with_retry
    async def fetch_instances(self) -> list[GPUInstance]:
        """Fetch GPU instances from Cudo Compute API.

        Strategy:
        1. GET /machines-types to get all machine type configurations
        2. Parse GPU info from gpuModelId and gpus fields
        3. Create GPUInstance for each machine type

        Returns:
            List of GPUInstance objects (one per machine type)
        """
        self._logger.debug(
            "Fetching machine types from Cudo Compute",
            provider_name=self.provider_name,
        )

        # Fetch all machine types
        response = await self._execute_api_call("/machines-types")
        collected_at = int(time.time())

        machine_types = response.get("machineTypes", [])

        self._logger.debug(
            "Fetched machine types",
            provider_name=self.provider_name,
            machine_type_count=len(machine_types),
        )

        # Create GPUInstance for each machine type
        instances: list[GPUInstance] = []
        unique_data_centers: set[str] = set()

        for machine_type in machine_types:
            instance = self._create_gpu_instance(machine_type, collected_at)
            instances.append(instance)
            unique_data_centers.add(machine_type["dataCenterId"])

        # Log summary statistics
        available_instances = [
            i for i in instances if i.availability != AvailabilityStatus.NOT_AVAILABLE
        ]

        self._logger.info(
            "Fetched GPU availability data",
            provider_name=self.provider_name,
            machine_type_count=len(machine_types),
            total_instances=len(instances),
            available_instances=len(available_instances),
            unique_data_centers=len(unique_data_centers),
        )

        return self._apply_availability_filter(instances)
