# New Provider Implementation Guide

## Quick Start Template

```python
"""[Provider] GPU collector using [REST API/GraphQL/SDK]."""

import os
import time
from typing import Any

import aiohttp  # or: from [provider_sdk] import AsyncClient

from gpuport_collectors.base import BaseCollector, with_retry
from gpuport_collectors.config import CollectorConfig
from gpuport_collectors.models import AvailabilityStatus, GPUInstance


class [Provider]Collector(BaseCollector):
    """Collector for [Provider] GPU availability."""

    API_BASE_URL = "https://api.provider.com/v1"

    def __init__(self, config: CollectorConfig) -> None:
        super().__init__(config)
        self.api_key = os.environ.get("[PROVIDER]_API_KEY")
        if not self.api_key:
            raise ValueError("[PROVIDER]_API_KEY environment variable must be set")

    @property
    def provider_name(self) -> str:
        return "[Provider]"

    @with_retry
    async def fetch_instances(self) -> list[GPUInstance]:
        """Fetch GPU instances from API."""
        self._logger.debug("Fetching instances", provider_name=self.provider_name)

        # 1. Call API
        response = await self._execute_api_call("/endpoint")
        collected_at = int(time.time())

        # 2. Parse to GPUInstance objects
        instances = [
            self._create_gpu_instance(item, collected_at)
            for item in response["data"]
        ]

        self._logger.info(
            "Fetched GPU data",
            provider_name=self.provider_name,
            total_instances=len(instances),
        )
        return instances
```

## Critical Implementation Notes

### Price Units
**Always verify and document API price units** - providers use different formats:
```python
# Document the conversion clearly with example
price = float(api_price) / 100.0  # Example: 6700 cents = $67.00/hour
```

Validate against real API data to ensure correct conversion.

### Timeouts
Always set explicit request timeouts to avoid hung collectors:
```python
timeout = aiohttp.ClientTimeout(total=self.config.timeout)
```

### Timestamp Validation
Use `int(time.time())` for `collected_at` - **NOT** hardcoded values:
```python
collected_at = int(time.time())  # ✓ Passes validation
# NOT: collected_at = 1234567890  # ✗ Fails: outside reasonable range
```

## Testing Requirements

### Reference Implementation
**See `tests/unit/collectors/test_runpod.py` as the complete reference implementation.**

### 1. Create Fixture from Real API Response
```bash
# Capture real API response
curl -H "Authorization: Bearer $API_KEY" https://api.provider.com/v1/gpus > \
  tests/fixtures/mock_responses/[provider].json
```

### 2. Required Test Coverage (Coverage Areas)
Cover each area below; organize into test classes as you prefer.

#### 2.1 Basic Configuration Tests
- [ ] Collector instantiates with valid configuration
- [ ] Collector requires necessary API keys/credentials (via environment variables)
- [ ] Collector raises appropriate errors when credentials are missing
- [ ] `provider_name` property returns correct value

Example:
```python
def test_init_missing_api_key(self):
    """Test that missing API key raises error."""
    with pytest.raises(ValueError, match="RUNPOD_API_KEY"):
        RunPodCollector(config=CollectorConfig())
```

#### 2.2 API Communication Tests
- [ ] Requests are properly formatted for the provider
- [ ] Authentication headers/params are present
- [ ] Response data is parsed correctly
- [ ] API errors are handled gracefully
- [ ] Rate limiting is respected (if applicable)

Example:
```python
@pytest.mark.asyncio
async def test_execute_api_call_success(self, provider_collector, mock_aiohttp):
    """Test successful API execution."""
    # Mock API response
    # Call method
    # Verify request format and response handling
```

#### 2.3 Data Discovery Tests (if applicable)
- [ ] Fetching all regions/datacenters
- [ ] Fetching all GPU/instance types
- [ ] Proper data structure validation
- [ ] Handling of empty results

#### 2.4 Pricing and Availability Tests
- [ ] Price data fetching and parsing
- [ ] Availability mapping to `AvailabilityStatus`
- [ ] Handling of missing or null pricing data
- [ ] Spot/on-demand price handling (if applicable)

#### 2.5 Instance Data Parsing Tests
- [ ] Raw API data transforms to `GPUInstance`
- [ ] Required fields are populated
- [ ] Optional fields handle `None` appropriately
- [ ] Multiple instances created when appropriate (e.g., per-region)

Example:
```python
def test_parse_gpu_data_with_availability(self, provider_collector):
    """Test parsing GPU data creates instances for available regions."""
    gpu_data = {...}
    datacenters = ["US-NY-1", "EU-SE-1"]

    instances = provider_collector._parse_gpu_data(gpu_data, datacenters)

    assert len(instances) > 0
    assert all(isinstance(i, GPUInstance) for i in instances)
```

#### 2.6 Error Handling Tests
- [ ] Network errors (timeouts, connection failures)
- [ ] API errors (invalid responses, error codes)
- [ ] Malformed data handling
- [ ] Empty result handling
- [ ] Retry behavior (if using `@with_retry`)

#### 2.7 Integration Tests
- [ ] Full end-to-end `fetch_instances()` workflow
- [ ] Real API call structure (mocked responses)
- [ ] Output validation against `GPUInstance` schema
- [ ] Data quality checks (no obviously wrong data)

### 3. Recommended Test Class Layout
Create `tests/unit/collectors/test_[provider].py` with classes like these (combine if small):

```python
class Test[Provider]CollectorInit:
    """Test collector initialization."""
    # test_init_with_api_key, test_init_without_api_key, test_provider_name

class Test[Provider]APIExecution:
    """Test API request/response handling."""
    # test_execute_* (auth, formatting, error handling)

class Test[Provider]DataDiscovery:
    """Test regions/types discovery."""
    # test_get_all_datacenters, test_get_all_instance_types

class Test[Provider]PricingAvailability:
    """Test pricing and availability mapping."""
    # test_map_availability_*, test_parse_price_*

class Test[Provider]InstanceParsing:
    """Test GPUInstance creation."""
    # test_create_gpu_instance_* (use current_timestamp fixture)

class Test[Provider]FetchInstances:
    """Test end-to-end fetching."""
    # test_fetch_instances_*, test_fetch_instances_empty_response

class Test[Provider]FixtureData:
    """Test with real fixture data."""
    # test_parse_fixture_data, test_fixture_schema_valid

class Test[Provider]ErrorHandling:
    """Test error scenarios."""
    # test_timeout, test_authentication_error, test_malformed_response

class Test[Provider]Integration:
    """Integration tests for the provider."""
    # test_fetch_instances_integration
```

### 4. Mock SDK/API Correctly
For SDK-based collectors (like Novita):
```python
# Patch at the import location where it's USED
with patch("gpuport_collectors.collectors.[provider].AsyncClient") as mock:
    mock_instance = AsyncMock()
    mock_instance.method = AsyncMock(return_value=[...])
    mock.return_value.__aenter__.return_value = mock_instance
    mock.return_value.__aexit__.return_value = AsyncMock()
```

For REST API collectors (like Cudo, Lambda):
```python
# Mock the _execute_api_call method directly
collector._execute_api_call = AsyncMock(return_value={"data": [...]})
```

### Fixtures
Create provider-specific fixtures in `conftest.py` or in your test file:

```python
@pytest.fixture
def provider_collector():
    """Create a provider collector instance with mocked credentials."""
    os.environ["PROVIDER_API_KEY"] = "test-key"
    collector = ProviderCollector(config=CollectorConfig())
    yield collector
```

### Test Data
Create realistic mock data that represents actual API responses:

```python
MOCK_REGIONS = [
    {"id": "us-east-1", "name": "US East"},
    {"id": "eu-west-1", "name": "EU West"},
]

MOCK_GPU_TYPES = [
    {
        "id": "a100-80gb",
        "name": "NVIDIA A100 80GB",
        "memory_gb": 80,
        "price": 2.50,
    }
]
```

### Schema Validation
All tests should implicitly validate schema compliance by:
1. Creating `GPUInstance` objects (Pydantic validates on construction)
2. Checking required fields are present
3. Verifying types match model expectations

### Test Execution Checklist
Before submitting your provider implementation:

- [ ] All coverage areas above are implemented
- [ ] Tests cover both success and failure paths
- [ ] Mock data represents realistic API responses
- [ ] All tests pass locally: `make test`
- [ ] Code quality checks pass: `make check`
- [ ] Test coverage is >80% for the new collector module

### Running Tests
```bash
# Run only your provider tests
uv run pytest tests/unit/collectors/test_<provider>.py -v

# Run with coverage
uv run pytest tests/unit/collectors/test_<provider>.py --cov=gpuport_collectors.collectors.<provider>

# Run all collector tests
uv run pytest tests/unit/collectors/ -v
```

### Additional Resources
- **BaseCollector documentation**: See `src/gpuport_collectors/base.py` for interface requirements
- **GPUInstance model**: See `src/gpuport_collectors/models.py` for schema details
- **RunPod implementation**: See `src/gpuport_collectors/collectors/runpod.py` for reference patterns
- **Registry integration**: Add your collector to `COLLECTORS` in `src/gpuport_collectors/collectors/__init__.py`

## Registration Checklist

### 1. Implementation
- [ ] Create `src/gpuport_collectors/collectors/[provider].py`
- [ ] Document price units clearly
- [ ] Use `int(time.time())` for timestamps
- [ ] Validate output with real API call

### 2. Registration
- [ ] Import in `src/gpuport_collectors/collectors/__init__.py`
- [ ] Add to `COLLECTORS` dict
- [ ] Add to `__all__`

### 3. Code Quality
- [ ] `uv run ruff check src/gpuport_collectors/collectors/[provider].py`
- [ ] `uv run ruff format src/gpuport_collectors/collectors/[provider].py`
- [ ] `uv run mypy src/gpuport_collectors/collectors/[provider].py`

### 4. Testing
- [ ] Capture real API response → `tests/fixtures/mock_responses/[provider].json`
- [ ] Create `tests/unit/collectors/test_[provider].py` (7 test classes)
- [ ] Add `current_timestamp` fixture for timestamp tests
- [ ] Mock SDK/API at correct import location
- [ ] Target 80%+ coverage: `uv run pytest tests/unit/collectors/test_[provider].py --cov`

### 5. Verification
- [ ] All tests pass: `uv run pytest tests/unit/collectors/test_[provider].py -v`
- [ ] Mocks prevent real API calls
- [ ] Price conversion validated against real data
- [ ] No type errors: `uv run mypy`
