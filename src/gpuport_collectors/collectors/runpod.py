"""RunPod GPU collector using GraphQL API."""

import os
import time
from typing import Any

import aiohttp

from gpuport_collectors.base import BaseCollector, with_retry
from gpuport_collectors.config import CollectorConfig
from gpuport_collectors.models import AvailabilityStatus, GPUInstance


class RunPodCollector(BaseCollector):
    """Collector for RunPod GPU availability data.

    Uses RunPod's GraphQL API to fetch GPU types and their availability
    across all datacenters with a two-call stateless strategy.
    """

    GRAPHQL_ENDPOINT = "https://api.runpod.io/graphql"

    def __init__(self, config: CollectorConfig) -> None:
        """Initialize RunPod collector.

        Args:
            config: Collector configuration

        Raises:
            ValueError: If RUNPOD_API_KEY environment variable is not set
        """
        super().__init__(config)
        self.api_key = os.environ.get("RUNPOD_API_KEY")
        if not self.api_key:
            raise ValueError("RUNPOD_API_KEY environment variable must be set")

    @property
    def provider_name(self) -> str:
        """Return provider name."""
        return "RunPod"

    async def _execute_graphql(
        self, query: str, variables: dict[str, Any] | None = None
    ) -> dict[str, Any]:
        """Execute a GraphQL query against RunPod API.

        Args:
            query: GraphQL query string
            variables: Optional query variables

        Returns:
            GraphQL response data

        Raises:
            aiohttp.ClientError: On HTTP errors
            ValueError: On GraphQL errors
        """
        url = f"{self.GRAPHQL_ENDPOINT}?api_key={self.api_key}"

        async with (
            aiohttp.ClientSession() as session,
            session.post(
                url,
                json={"query": query, "variables": variables or {}},
                timeout=aiohttp.ClientTimeout(total=self.config.timeout),
            ) as response,
        ):
            response.raise_for_status()
            result = await response.json()

            if "errors" in result:
                raise ValueError(f"GraphQL errors: {result['errors']}")

            data: dict[str, Any] = result.get("data", {})
            return data

    async def _get_datacenters(self) -> list[str]:
        """Discover all RunPod datacenters.

        Returns:
            List of datacenter IDs (e.g., ["EU-RO-1", "US-TX-1", ...])
        """
        self._logger.info(
            "Discovering RunPod datacenters",
            provider_name=self.provider_name,
        )

        query = """
        query {
          dataCenters {
            id
            name
          }
        }
        """

        data = await self._execute_graphql(query)
        datacenters_data = data.get("dataCenters", [])

        # Extract datacenter IDs
        datacenter_list = sorted([dc["id"] for dc in datacenters_data if "id" in dc])

        self._logger.info(
            "Discovered datacenters",
            provider_name=self.provider_name,
            datacenter_count=len(datacenter_list),
        )

        return datacenter_list

    def _build_gpu_query(self, datacenters: list[str]) -> str:
        """Build GraphQL query with per-region pricing aliases.

        Args:
            datacenters: List of datacenter IDs

        Returns:
            GraphQL query string with aliases for each datacenter
        """
        # Build aliases for each datacenter
        datacenter_aliases = []
        for dc in datacenters:
            # Convert datacenter ID to valid GraphQL alias (lowercase, replace - with _)
            alias = dc.lower().replace("-", "_")
            datacenter_aliases.append(
                f"""
                {alias}: lowestPrice(input: {{ dataCenterId: "{dc}", gpuCount: 1 }}) {{
                  stockStatus
                  uninterruptablePrice
                  minimumBidPrice
                  availableGpuCounts
                }}
                """
            )

        newline = "\n"
        return f"""
        query {{
          gpuTypes {{
            id
            displayName
            memoryInGb
            cudaCores
            {newline.join(datacenter_aliases)}
          }}
        }}
        """

    def _map_stock_status(self, status: str | None) -> AvailabilityStatus:
        """Map RunPod stock status to AvailabilityStatus enum.

        Args:
            status: RunPod stock status string ("High", "Medium", "Low", or None)

        Returns:
            Corresponding AvailabilityStatus enum value
        """
        if not status:
            return AvailabilityStatus.NOT_AVAILABLE

        status_map = {
            "High": AvailabilityStatus.HIGH,
            "Medium": AvailabilityStatus.MEDIUM,
            "Low": AvailabilityStatus.LOW,
        }

        return status_map.get(status, AvailabilityStatus.NOT_AVAILABLE)

    def _parse_gpu_data(self, gpu: dict[str, Any], datacenters: list[str]) -> list[GPUInstance]:
        """Parse GPU data and create GPUInstance for each region.

        Args:
            gpu: GPU type data from GraphQL response
            datacenters: List of all datacenter IDs

        Returns:
            List of GPUInstance objects (one per region)
        """
        instances = []
        collected_at = int(time.time())

        for dc in datacenters:
            # Convert datacenter ID to alias format
            alias = dc.lower().replace("-", "_")
            pricing = gpu.get(alias)

            # Get pricing data (0.0 if unavailable or None)
            # Handle both missing pricing dict and None values within the dict
            price = pricing.get("uninterruptablePrice") if pricing else None
            price = price if price is not None else 0.0

            spot_price = pricing.get("minimumBidPrice") if pricing else None
            spot_price = spot_price if spot_price is not None else 0.0

            stock_status = pricing.get("stockStatus") if pricing else None

            # availableGpuCounts can be a list or None
            quantity_raw = pricing.get("availableGpuCounts") if pricing else None
            if isinstance(quantity_raw, list):
                quantity = sum(quantity_raw) if quantity_raw else 0
            elif quantity_raw is not None:
                quantity = int(quantity_raw)
            else:
                quantity = 0

            instance = GPUInstance(
                # Identification
                provider="RunPod",
                instance_type=gpu["id"],
                accelerator_name=gpu["displayName"],
                region=dc,
                # Pricing
                price=price,
                spot_price=spot_price,
                # Availability
                availability=self._map_stock_status(stock_status),
                quantity=quantity,
                # Hardware specs (RunPod doesn't expose CPU/memory per GPU)
                v_cpus=None,
                memory_gib=None,
                accelerator_count=1,
                accelerator_mem_gib=gpu["memoryInGb"] if gpu.get("memoryInGb", 0) > 0 else None,
                # Metadata
                collected_at=collected_at,
                raw_data={
                    "gpu_type": {
                        "id": gpu["id"],
                        "displayName": gpu["displayName"],
                        "memoryInGb": gpu["memoryInGb"],
                        "cudaCores": gpu.get("cudaCores"),
                    },
                    "pricing": pricing or {},
                    "datacenter": dc,
                },
            )

            instances.append(instance)

        return instances

    @with_retry
    async def fetch_instances(self) -> list[GPUInstance]:
        """Fetch GPU instances from RunPod API.

        Uses a two-call stateless strategy:
        1. Discover all datacenters
        2. Fetch all GPU types with per-region pricing

        Returns:
            List of GPUInstance objects (includes both available and unavailable)
        """
        # Step 1: Discover datacenters (fresh query each run)
        datacenters = await self._get_datacenters()

        if not datacenters:
            self._logger.warning(
                "No datacenters discovered",
                provider_name=self.provider_name,
            )
            return []

        # Step 2: Build and execute query with per-region pricing
        self._logger.info(
            "Fetching GPU availability data",
            provider_name=self.provider_name,
            datacenter_count=len(datacenters),
        )

        query = self._build_gpu_query(datacenters)
        data = await self._execute_graphql(query)
        gpu_types = data.get("gpuTypes", [])

        # Step 3: Parse response into GPUInstance per (gpu, region)
        instances: list[GPUInstance] = []
        for gpu in gpu_types:
            instances.extend(self._parse_gpu_data(gpu, datacenters))

        # Log summary statistics
        available_instances = [
            i for i in instances if i.availability != AvailabilityStatus.NOT_AVAILABLE
        ]

        self._logger.info(
            "Fetched GPU availability data",
            provider_name=self.provider_name,
            gpu_type_count=len(gpu_types),
            datacenter_count=len(datacenters),
            total_instances=len(instances),
            available_instances=len(available_instances),
        )

        return instances
