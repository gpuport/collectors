"""GPU data collectors for various cloud providers.

This module provides a registry for discovering and instantiating collector classes.
"""

from gpuport_collectors.base import BaseCollector
from gpuport_collectors.collectors.cudo import CudoCollector
from gpuport_collectors.collectors.lambda_labs import LambdaLabsCollector
from gpuport_collectors.collectors.novita import NovitaCollector
from gpuport_collectors.collectors.runpod import RunPodCollector

# Collector registry - add new providers here
COLLECTORS: dict[str, type[BaseCollector]] = {
    "cudo": CudoCollector,
    "lambdalabs": LambdaLabsCollector,
    "novita": NovitaCollector,
    "runpod": RunPodCollector,
}


def get_collector_class(provider: str) -> type[BaseCollector]:
    """Get a collector class by provider name.

    Args:
        provider: Provider name (e.g., "runpod", "lambdalabs", "vastai")

    Returns:
        Collector class for the specified provider

    Raises:
        ValueError: If provider is not registered

    Example:
        >>> collector_class = get_collector_class("runpod")
        >>> collector = collector_class(config=my_config)
        >>> instances = await collector.fetch_instances()
    """
    provider_lower = provider.lower()
    if provider_lower not in COLLECTORS:
        available = ", ".join(sorted(COLLECTORS.keys()))
        raise ValueError(f"Unknown provider: '{provider}'. Available providers: {available}")
    return COLLECTORS[provider_lower]


def list_providers() -> list[str]:
    """List all registered provider names.

    Returns:
        Sorted list of provider names

    Example:
        >>> providers = list_providers()
        >>> print(providers)
        ['runpod']
    """
    return sorted(COLLECTORS.keys())


__all__ = [
    "COLLECTORS",
    "CudoCollector",
    "LambdaLabsCollector",
    "NovitaCollector",
    "RunPodCollector",
    "get_collector_class",
    "list_providers",
]
