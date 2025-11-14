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

    Uses RunPod's GraphQL API with an optimized single-call strategy:
    1. Fetch GPU types with their available datacenters (nodeGroupDatacenters)
    2. Build query with only GPU-datacenter combinations that exist

    This avoids querying 1,599 combinations (41 GPUs x 39 datacenters) when
    typically only a handful have current availability.
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

    async def _get_gpu_datacenter_mapping(self) -> dict[str, dict[str, Any]]:
        """Get GPU types with their available datacenters.

        Returns:
            Dict mapping GPU ID to GPU data including nodeGroupDatacenters
            Example: {
                "NVIDIA A100 80GB PCIe": {
                    "displayName": "A100 80GB",
                    "memoryInGb": 80,
                    "cudaCores": 6912,
                    "datacenters": ["EU-RO-1", "US-TX-1"]
                }
            }
        """
        self._logger.info(
            "Discovering GPU types and their datacenters",
            provider_name=self.provider_name,
        )

        query = """
        query {
          gpuTypes {
            id
            displayName
            memoryInGb
            cudaCores
            nodeGroupDatacenters {
              id
              name
            }
          }
        }
        """

        data = await self._execute_graphql(query)
        gpu_types = data.get("gpuTypes", [])

        # Build mapping of GPU ID to GPU data with datacenter list
        gpu_mapping = {}
        total_combinations = 0

        for gpu in gpu_types:
            gpu_id = gpu["id"]
            datacenters = [
                dc["id"]
                for dc in gpu.get("nodeGroupDatacenters", [])
                if isinstance(dc, dict) and "id" in dc
            ]

            total_combinations += len(datacenters)

            gpu_mapping[gpu_id] = {
                "displayName": gpu.get("displayName", ""),
                "memoryInGb": gpu.get("memoryInGb", 0),
                "cudaCores": gpu.get("cudaCores", 0),
                "datacenters": sorted(datacenters),
            }

        self._logger.info(
            "Discovered GPU types and datacenters",
            provider_name=self.provider_name,
            gpu_type_count=len(gpu_types),
            total_combinations=total_combinations,
        )

        return gpu_mapping

    def _get_all_datacenters_from_mapping(
        self, gpu_mapping: dict[str, dict[str, Any]]
    ) -> list[str]:
        """Get all unique datacenters from GPU mapping.

        Args:
            gpu_mapping: Dict mapping GPU ID to available datacenters

        Returns:
            Sorted list of all unique datacenters that have any GPU available
        """
        all_dcs = set()
        for gpu_data in gpu_mapping.values():
            all_dcs.update(gpu_data["datacenters"])
        return sorted(all_dcs)

    def _build_pricing_query(self, datacenters: list[str]) -> str:
        """Build GraphQL query for pricing across datacenters.

        Args:
            datacenters: List of datacenter IDs to query

        Returns:
            GraphQL query string
        """
        # Build datacenter aliases
        datacenter_aliases = []
        for dc in datacenters:
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

    def _parse_gpu_data(
        self,
        gpu: dict[str, Any],
        datacenters: list[str],  # noqa: ARG002
        gpu_mapping: dict[str, dict[str, Any]],
    ) -> list[GPUInstance]:
        """Parse GPU data and create GPUInstance for each available region.

        Only creates instances for GPU-datacenter combinations that exist in
        the gpu_mapping (from nodeGroupDatacenters). This avoids creating 1,599
        instances when only a few combinations are actually available.

        Args:
            gpu: GPU type data from GraphQL response with pricing
            datacenters: List of datacenters queried (for fallback/validation)
            gpu_mapping: Mapping of GPU IDs to their available datacenters

        Returns:
            List of GPUInstance objects (one per available region only)
        """
        instances = []
        collected_at = int(time.time())
        gpu_id = gpu["id"]

        # Get available datacenters for this GPU from the mapping
        available_datacenters = gpu_mapping.get(gpu_id, {}).get("datacenters", [])

        # Only create instances for datacenters where this GPU is actually available
        for dc in available_datacenters:
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
        """Fetch GPU instances from RunPod API with optimized querying.

        Uses an efficient two-call strategy:
        1. Get GPU types WITH their nodeGroupDatacenters (available regions)
        2. Query pricing ONLY for GPU-datacenter combinations that exist

        This avoids querying 1,599 combinations (41 GPUs x 39 datacenters) when
        typically only a handful have current availability.

        Returns:
            List of GPUInstance objects (only for available combinations)
        """
        # Step 1: Get GPU types with their available datacenters
        gpu_mapping = await self._get_gpu_datacenter_mapping()

        if not gpu_mapping:
            self._logger.warning(
                "No GPU types discovered",
                provider_name=self.provider_name,
            )
            return []

        # Step 2: Get unique datacenters that have ANY GPU available
        datacenters = self._get_all_datacenters_from_mapping(gpu_mapping)

        if not datacenters:
            self._logger.warning(
                "No datacenters with available GPUs",
                provider_name=self.provider_name,
            )
            return []

        # Step 3: Build and execute optimized pricing query
        self._logger.info(
            "Fetching GPU pricing data",
            provider_name=self.provider_name,
            datacenter_count=len(datacenters),
        )

        query = self._build_pricing_query(datacenters)
        data = await self._execute_graphql(query)
        gpu_types = data.get("gpuTypes", [])

        # Step 4: Parse response - only create instances for available combos
        instances: list[GPUInstance] = []
        for gpu in gpu_types:
            instances.extend(self._parse_gpu_data(gpu, datacenters, gpu_mapping))

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
