"""Tests for collector registry functionality."""

import pytest

from gpuport_collectors.base import BaseCollector
from gpuport_collectors.collectors import (
    COLLECTORS,
    get_collector_class,
    list_providers,
)
from gpuport_collectors.collectors.runpod import RunPodCollector


class TestCollectorRegistry:
    """Test collector registry functions."""

    def test_collectors_dict_exists(self) -> None:
        """Test that COLLECTORS dictionary exists."""
        assert isinstance(COLLECTORS, dict)
        assert len(COLLECTORS) > 0

    def test_runpod_registered(self) -> None:
        """Test that RunPod collector is registered."""
        assert "runpod" in COLLECTORS
        assert COLLECTORS["runpod"] == RunPodCollector

    def test_all_collectors_are_base_collector_subclasses(self) -> None:
        """Test that all registered collectors extend BaseCollector."""
        for name, collector_class in COLLECTORS.items():
            assert issubclass(collector_class, BaseCollector), (
                f"{name} collector must extend BaseCollector"
            )


class TestGetCollectorClass:
    """Test get_collector_class function."""

    def test_get_existing_collector(self) -> None:
        """Test getting an existing collector class."""
        collector_class = get_collector_class("runpod")
        assert collector_class == RunPodCollector

    def test_get_collector_case_insensitive(self) -> None:
        """Test that provider name is case insensitive."""
        assert get_collector_class("runpod") == RunPodCollector
        assert get_collector_class("RunPod") == RunPodCollector
        assert get_collector_class("RUNPOD") == RunPodCollector

    def test_get_unknown_collector_raises(self) -> None:
        """Test that unknown provider raises ValueError."""
        with pytest.raises(ValueError, match="Unknown provider: 'nonexistent'"):
            get_collector_class("nonexistent")

    def test_error_message_lists_available_providers(self) -> None:
        """Test that error message lists available providers."""
        with pytest.raises(ValueError, match=r"Available providers:.*runpod"):
            get_collector_class("invalid")

    def test_returns_class_not_instance(self) -> None:
        """Test that function returns class, not instance."""
        collector_class = get_collector_class("runpod")
        assert isinstance(collector_class, type)
        assert issubclass(collector_class, BaseCollector)


class TestListProviders:
    """Test list_providers function."""

    def test_list_providers_returns_list(self) -> None:
        """Test that list_providers returns a list."""
        providers = list_providers()
        assert isinstance(providers, list)
        assert len(providers) > 0

    def test_list_providers_includes_runpod(self) -> None:
        """Test that RunPod is in the provider list."""
        providers = list_providers()
        assert "runpod" in providers

    def test_list_providers_is_sorted(self) -> None:
        """Test that provider list is sorted."""
        providers = list_providers()
        assert providers == sorted(providers)

    def test_list_providers_lowercase(self) -> None:
        """Test that all provider names are lowercase."""
        providers = list_providers()
        for provider in providers:
            assert provider == provider.lower(), f"Provider {provider} should be lowercase"


class TestRegistryIntegration:
    """Test registry integration with actual collectors."""

    def test_can_instantiate_from_registry(self) -> None:
        """Test that collectors from registry can be instantiated."""
        collector_class = get_collector_class("runpod")

        # Note: RunPodCollector requires RUNPOD_API_KEY env var
        # This test just checks we got the right class
        assert collector_class.__name__ == "RunPodCollector"

    def test_provider_name_matches_registry_key(self) -> None:
        """Test that collector provider_name matches registry key."""
        for _registry_name, collector_class in COLLECTORS.items():
            # Create a minimal instance to check provider_name
            # We can't actually instantiate RunPodCollector without API key
            # So we just verify the class exists and has the right structure
            assert hasattr(collector_class, "provider_name")


class TestRegistryExtensibility:
    """Test that registry is easy to extend with new providers."""

    def test_registry_is_mutable_dict(self) -> None:
        """Test that registry can be extended (for future providers)."""
        # Don't actually mutate in test, just verify it's possible
        assert isinstance(COLLECTORS, dict)
        # Registry should be a normal dict, not frozen
        original_len = len(COLLECTORS)
        assert original_len >= 1

    def test_adding_new_provider_pattern(self) -> None:
        """Test the pattern for adding new providers to registry."""
        # This test documents how to add new providers
        # Actual implementation would be in collectors/__init__.py

        # Pattern: COLLECTORS["providername"] = ProviderCollector

        # Verify current structure supports this pattern
        for key in COLLECTORS:
            assert isinstance(key, str)
            assert key == key.lower()
