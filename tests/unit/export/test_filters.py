"""Tests for export pipeline filter engine."""

from typing import Any, Literal

import pytest

from gpuport_collectors.export.config import FilterConfig
from gpuport_collectors.export.filters import (
    FilterError,
    apply_filter,
    apply_filters,
    filter_instances,
    get_field_value,
)
from gpuport_collectors.models import AvailabilityStatus, GPUInstance


@pytest.fixture
def sample_instance() -> GPUInstance:
    """Create a sample GPUInstance for testing."""
    return GPUInstance(
        provider="TestProvider",
        instance_type="test.large",
        accelerator_name="TestGPU",
        accelerator_count=4,
        accelerator_mem_gib=16.0,
        region="us-east-1",
        availability=AvailabilityStatus.HIGH,
        quantity=10,
        price=2.5,
        spot_price=1.5,
        v_cpus=16,
        memory_gib=64.0,
    )


def _make_instance_with_overrides(**overrides: Any) -> GPUInstance:
    defaults: dict[str, Any] = {
        "provider": "Test",
        "instance_type": "test",
        "accelerator_name": "GPU",
        "accelerator_count": 1,
        "region": "us-east-1",
        "availability": AvailabilityStatus.HIGH,
        "price": 1.0,
    }
    instance_kwargs: dict[str, Any] = {**defaults, **overrides}
    return GPUInstance(**instance_kwargs)


class TestGetFieldValue:
    """Tests for get_field_value function."""

    def test_get_existing_field(self, sample_instance: GPUInstance) -> None:
        """Test getting an existing field value."""
        assert get_field_value(sample_instance, "provider") == "TestProvider"
        assert get_field_value(sample_instance, "price") == 2.5

    def test_get_nonexistent_field_raises(self, sample_instance: GPUInstance) -> None:
        """Test that getting a non-existent field raises FilterError."""
        with pytest.raises(FilterError, match="does not exist"):
            get_field_value(sample_instance, "nonexistent_field")


class TestEqualityOperators:
    """Tests for equality filter operators."""

    def test_eq_operator_match(self, sample_instance: GPUInstance) -> None:
        """Test eq operator with matching value."""
        filter_config = FilterConfig(field="provider", operator="eq", value="TestProvider")
        assert apply_filter(sample_instance, filter_config) is True

    def test_eq_operator_no_match(self, sample_instance: GPUInstance) -> None:
        """Test eq operator with non-matching value."""
        filter_config = FilterConfig(field="provider", operator="eq", value="OtherProvider")
        assert apply_filter(sample_instance, filter_config) is False

    def test_ne_operator_match(self, sample_instance: GPUInstance) -> None:
        """Test ne operator with different value."""
        filter_config = FilterConfig(field="provider", operator="ne", value="OtherProvider")
        assert apply_filter(sample_instance, filter_config) is True

    def test_ne_operator_no_match(self, sample_instance: GPUInstance) -> None:
        """Test ne operator with same value."""
        filter_config = FilterConfig(field="provider", operator="ne", value="TestProvider")
        assert apply_filter(sample_instance, filter_config) is False


class TestComparisonOperators:
    """Tests for comparison filter operators."""

    def test_lt_operator_true(self, sample_instance: GPUInstance) -> None:
        """Test lt operator when value is less than."""
        filter_config = FilterConfig(field="price", operator="lt", value=3.0)
        assert apply_filter(sample_instance, filter_config) is True

    def test_lt_operator_false(self, sample_instance: GPUInstance) -> None:
        """Test lt operator when value is not less than."""
        filter_config = FilterConfig(field="price", operator="lt", value=2.0)
        assert apply_filter(sample_instance, filter_config) is False

    def test_lte_operator_equal(self, sample_instance: GPUInstance) -> None:
        """Test lte operator with equal value."""
        filter_config = FilterConfig(field="price", operator="lte", value=2.5)
        assert apply_filter(sample_instance, filter_config) is True

    def test_lte_operator_less(self, sample_instance: GPUInstance) -> None:
        """Test lte operator with less value."""
        filter_config = FilterConfig(field="price", operator="lte", value=3.0)
        assert apply_filter(sample_instance, filter_config) is True

    def test_gt_operator_true(self, sample_instance: GPUInstance) -> None:
        """Test gt operator when value is greater than."""
        filter_config = FilterConfig(field="price", operator="gt", value=2.0)
        assert apply_filter(sample_instance, filter_config) is True

    def test_gt_operator_false(self, sample_instance: GPUInstance) -> None:
        """Test gt operator when value is not greater than."""
        filter_config = FilterConfig(field="price", operator="gt", value=3.0)
        assert apply_filter(sample_instance, filter_config) is False

    def test_gte_operator_equal(self, sample_instance: GPUInstance) -> None:
        """Test gte operator with equal value."""
        filter_config = FilterConfig(field="price", operator="gte", value=2.5)
        assert apply_filter(sample_instance, filter_config) is True

    def test_gte_operator_greater(self, sample_instance: GPUInstance) -> None:
        """Test gte operator with greater value."""
        filter_config = FilterConfig(field="price", operator="gte", value=2.0)
        assert apply_filter(sample_instance, filter_config) is True

    def test_comparison_with_null_returns_false(self) -> None:
        """Test comparison operators return False for null values."""
        instance = GPUInstance(
            provider="Test",
            instance_type="test",
            accelerator_name="GPU",
            accelerator_count=1,
            region="us-east-1",
            availability=AvailabilityStatus.HIGH,
            price=1.0,
            accelerator_mem_gib=None,  # Null field
        )
        filter_config = FilterConfig(field="accelerator_mem_gib", operator="lt", value=10.0)
        assert apply_filter(instance, filter_config) is False

    @pytest.mark.parametrize("operator", ["lte", "gt", "gte"])
    def test_comparison_with_null_returns_false_for_operator(
        self,
        operator: Literal["lte", "gt", "gte"],
    ) -> None:
        """Test comparison operators return False for null values."""
        instance = GPUInstance(
            provider="Test",
            instance_type="test",
            accelerator_name="GPU",
            accelerator_count=1,
            region="us-east-1",
            availability=AvailabilityStatus.HIGH,
            price=1.0,
            accelerator_mem_gib=None,
        )
        filter_config = FilterConfig(field="accelerator_mem_gib", operator=operator, value=10.0)
        assert apply_filter(instance, filter_config) is False


class TestBetweenOperator:
    """Tests for between filter operator."""

    def test_between_operator_within_range(self, sample_instance: GPUInstance) -> None:
        """Test between operator when value is within range."""
        filter_config = FilterConfig(field="price", operator="between", min=2.0, max=3.0)
        assert apply_filter(sample_instance, filter_config) is True

    def test_between_operator_at_min_boundary(self, sample_instance: GPUInstance) -> None:
        """Test between operator at minimum boundary."""
        filter_config = FilterConfig(field="price", operator="between", min=2.5, max=3.0)
        assert apply_filter(sample_instance, filter_config) is True

    def test_between_operator_at_max_boundary(self, sample_instance: GPUInstance) -> None:
        """Test between operator at maximum boundary."""
        filter_config = FilterConfig(field="price", operator="between", min=2.0, max=2.5)
        assert apply_filter(sample_instance, filter_config) is True

    def test_between_operator_outside_range(self, sample_instance: GPUInstance) -> None:
        """Test between operator when value is outside range."""
        filter_config = FilterConfig(field="price", operator="between", min=3.0, max=4.0)
        assert apply_filter(sample_instance, filter_config) is False

    def test_between_operator_with_null_value_returns_false(self) -> None:
        """Test between operator returns False for null values."""
        instance = GPUInstance(
            provider="Test",
            instance_type="test",
            accelerator_name="GPU",
            accelerator_count=1,
            region="us-east-1",
            availability=AvailabilityStatus.HIGH,
            price=1.0,
            accelerator_mem_gib=None,
        )
        filter_config = FilterConfig(field="accelerator_mem_gib", operator="between", min=1, max=2)
        assert apply_filter(instance, filter_config) is False


class TestSetMembershipOperators:
    """Tests for set membership filter operators."""

    def test_in_operator_match(self, sample_instance: GPUInstance) -> None:
        """Test in operator when value is in list."""
        filter_config = FilterConfig(
            field="availability",
            operator="in",
            values=[AvailabilityStatus.HIGH, AvailabilityStatus.MEDIUM],
        )
        assert apply_filter(sample_instance, filter_config) is True

    def test_in_operator_no_match(self, sample_instance: GPUInstance) -> None:
        """Test in operator when value is not in list."""
        filter_config = FilterConfig(
            field="availability",
            operator="in",
            values=[AvailabilityStatus.LOW, AvailabilityStatus.NOT_AVAILABLE],
        )
        assert apply_filter(sample_instance, filter_config) is False

    def test_not_in_operator_match(self, sample_instance: GPUInstance) -> None:
        """Test not_in operator when value is not in list."""
        filter_config = FilterConfig(
            field="availability",
            operator="not_in",
            values=[AvailabilityStatus.LOW, AvailabilityStatus.NOT_AVAILABLE],
        )
        assert apply_filter(sample_instance, filter_config) is True

    def test_not_in_operator_no_match(self, sample_instance: GPUInstance) -> None:
        """Test not_in operator when value is in list."""
        filter_config = FilterConfig(
            field="availability",
            operator="not_in",
            values=[AvailabilityStatus.HIGH, AvailabilityStatus.MEDIUM],
        )
        assert apply_filter(sample_instance, filter_config) is False


class TestStringPatternOperators:
    """Tests for string pattern matching filter operators."""

    def test_regex_operator_match(self, sample_instance: GPUInstance) -> None:
        """Test regex operator with matching pattern."""
        filter_config = FilterConfig(field="region", operator="regex", value=r"^us-.*")
        assert apply_filter(sample_instance, filter_config) is True

    def test_regex_operator_no_match(self, sample_instance: GPUInstance) -> None:
        """Test regex operator with non-matching pattern."""
        filter_config = FilterConfig(field="region", operator="regex", value=r"^eu-.*")
        assert apply_filter(sample_instance, filter_config) is False

    def test_contains_operator_match(self, sample_instance: GPUInstance) -> None:
        """Test contains operator when substring is present."""
        filter_config = FilterConfig(field="instance_type", operator="contains", value="large")
        assert apply_filter(sample_instance, filter_config) is True

    def test_contains_operator_no_match(self, sample_instance: GPUInstance) -> None:
        """Test contains operator when substring is not present."""
        filter_config = FilterConfig(field="instance_type", operator="contains", value="small")
        assert apply_filter(sample_instance, filter_config) is False

    def test_starts_with_operator_match(self, sample_instance: GPUInstance) -> None:
        """Test starts_with operator when prefix matches."""
        filter_config = FilterConfig(field="instance_type", operator="starts_with", value="test")
        assert apply_filter(sample_instance, filter_config) is True

    def test_starts_with_operator_no_match(self, sample_instance: GPUInstance) -> None:
        """Test starts_with operator when prefix doesn't match."""
        filter_config = FilterConfig(field="instance_type", operator="starts_with", value="prod")
        assert apply_filter(sample_instance, filter_config) is False

    def test_string_operator_on_non_string_raises(self, sample_instance: GPUInstance) -> None:
        """Test that string operators on non-string fields raise FilterError."""
        filter_config = FilterConfig(field="price", operator="regex", value=".*")
        with pytest.raises(FilterError, match="requires string field"):
            apply_filter(sample_instance, filter_config)

    def test_contains_operator_with_null_value_returns_false(self) -> None:
        """Test contains operator returns False for null field values."""
        instance = _make_instance_with_overrides(arch=None)
        filter_config = FilterConfig(field="arch", operator="contains", value="x86")
        assert apply_filter(instance, filter_config) is False

    def test_starts_with_operator_with_null_value_returns_false(self) -> None:
        """Test starts_with operator returns False for null field values."""
        instance = _make_instance_with_overrides(arch=None)
        filter_config = FilterConfig(field="arch", operator="starts_with", value="x86")
        assert apply_filter(instance, filter_config) is False

    def test_regex_operator_with_null_value_returns_false(self) -> None:
        """Test regex operator returns False for null field values."""
        instance = _make_instance_with_overrides(arch=None)
        filter_config = FilterConfig(field="arch", operator="regex", value=".*")
        assert apply_filter(instance, filter_config) is False

    def test_contains_operator_on_non_string_raises(self, sample_instance: GPUInstance) -> None:
        """Test contains operator on non-string field raises FilterError."""
        filter_config = FilterConfig(field="price", operator="contains", value="2")
        with pytest.raises(FilterError, match="requires string field"):
            apply_filter(sample_instance, filter_config)

    def test_starts_with_operator_on_non_string_raises(self, sample_instance: GPUInstance) -> None:
        """Test starts_with operator on non-string field raises FilterError."""
        filter_config = FilterConfig(field="price", operator="starts_with", value="2")
        with pytest.raises(FilterError, match="requires string field"):
            apply_filter(sample_instance, filter_config)


class TestNullCheckOperators:
    """Tests for null check filter operators."""

    def test_is_null_operator_true(self) -> None:
        """Test is_null operator when field is null."""
        instance = GPUInstance(
            provider="Test",
            instance_type="test",
            accelerator_name="GPU",
            accelerator_count=1,
            region="us-east-1",
            availability=AvailabilityStatus.HIGH,
            price=1.0,
            spot_price=None,  # Null field
        )
        filter_config = FilterConfig(field="spot_price", operator="is_null")
        assert apply_filter(instance, filter_config) is True

    def test_is_null_operator_false(self, sample_instance: GPUInstance) -> None:
        """Test is_null operator when field is not null."""
        filter_config = FilterConfig(field="spot_price", operator="is_null")
        assert apply_filter(sample_instance, filter_config) is False

    def test_is_not_null_operator_true(self, sample_instance: GPUInstance) -> None:
        """Test is_not_null operator when field is not null."""
        filter_config = FilterConfig(field="spot_price", operator="is_not_null")
        assert apply_filter(sample_instance, filter_config) is True

    def test_is_not_null_operator_false(self) -> None:
        """Test is_not_null operator when field is null."""
        instance = GPUInstance(
            provider="Test",
            instance_type="test",
            accelerator_name="GPU",
            accelerator_count=1,
            region="us-east-1",
            availability=AvailabilityStatus.HIGH,
            price=1.0,
            spot_price=None,  # Null field
        )
        filter_config = FilterConfig(field="spot_price", operator="is_not_null")
        assert apply_filter(instance, filter_config) is False


class TestApplyFilters:
    """Tests for apply_filters function (AND logic)."""

    def test_apply_filters_all_pass(self, sample_instance: GPUInstance) -> None:
        """Test apply_filters when all filters pass."""
        filters = [
            FilterConfig(field="provider", operator="eq", value="TestProvider"),
            FilterConfig(field="price", operator="lt", value=3.0),
            FilterConfig(field="availability", operator="in", values=[AvailabilityStatus.HIGH]),
        ]
        assert apply_filters(sample_instance, filters) is True

    def test_apply_filters_one_fails(self, sample_instance: GPUInstance) -> None:
        """Test apply_filters when one filter fails."""
        filters = [
            FilterConfig(field="provider", operator="eq", value="TestProvider"),
            FilterConfig(field="price", operator="lt", value=2.0),  # This fails
            FilterConfig(field="availability", operator="in", values=[AvailabilityStatus.HIGH]),
        ]
        assert apply_filters(sample_instance, filters) is False

    def test_apply_filters_empty_list(self, sample_instance: GPUInstance) -> None:
        """Test apply_filters with empty filter list."""
        assert apply_filters(sample_instance, []) is True


class TestFilterInstances:
    """Tests for filter_instances function."""

    def test_filter_instances_basic(self) -> None:
        """Test filtering a list of instances."""
        instances = [
            GPUInstance(
                provider="Provider1",
                instance_type="type1",
                accelerator_name="GPU1",
                accelerator_count=1,
                region="us-east-1",
                availability=AvailabilityStatus.HIGH,
                price=1.0,
            ),
            GPUInstance(
                provider="Provider2",
                instance_type="type2",
                accelerator_name="GPU2",
                accelerator_count=1,
                region="us-west-1",
                availability=AvailabilityStatus.LOW,
                price=3.0,
            ),
            GPUInstance(
                provider="Provider1",
                instance_type="type3",
                accelerator_name="GPU3",
                accelerator_count=1,
                region="eu-west-1",
                availability=AvailabilityStatus.HIGH,
                price=2.0,
            ),
        ]

        filters = [
            FilterConfig(field="provider", operator="eq", value="Provider1"),
            FilterConfig(field="price", operator="lt", value=2.5),
        ]

        filtered = filter_instances(instances, filters)
        assert len(filtered) == 2
        assert all(inst.provider == "Provider1" for inst in filtered)
        assert all(inst.price < 2.5 for inst in filtered)

    def test_filter_instances_no_matches(self) -> None:
        """Test filtering when no instances match."""
        instances = [
            GPUInstance(
                provider="Provider1",
                instance_type="type1",
                accelerator_name="GPU1",
                accelerator_count=1,
                region="us-east-1",
                availability=AvailabilityStatus.HIGH,
                price=5.0,
            )
        ]

        filters = [FilterConfig(field="price", operator="lt", value=1.0)]

        filtered = filter_instances(instances, filters)
        assert len(filtered) == 0

    def test_filter_instances_empty_filters(self) -> None:
        """Test filtering with no filters returns all instances."""
        instances = [
            GPUInstance(
                provider="Provider1",
                instance_type="type1",
                accelerator_name="GPU1",
                accelerator_count=1,
                region="us-east-1",
                availability=AvailabilityStatus.HIGH,
                price=1.0,
            ),
            GPUInstance(
                provider="Provider2",
                instance_type="type2",
                accelerator_name="GPU2",
                accelerator_count=1,
                region="us-west-1",
                availability=AvailabilityStatus.LOW,
                price=2.0,
            ),
        ]

        filtered = filter_instances(instances, [])
        assert len(filtered) == 2
        assert filtered == instances


class TestFilterErrors:
    """Tests for filter error conditions."""

    def test_missing_value_parameter(self, sample_instance: GPUInstance) -> None:
        """Test that missing required value parameter raises ValidationError."""
        from pydantic import ValidationError

        with pytest.raises(ValidationError, match="Operator 'eq' requires 'value' field"):
            FilterConfig(field="price", operator="eq", value=None)

    def test_missing_values_parameter(self, sample_instance: GPUInstance) -> None:
        """Test that missing required values parameter raises ValidationError."""
        from pydantic import ValidationError

        with pytest.raises(ValidationError, match="Operator 'in' requires 'values' field"):
            FilterConfig(field="availability", operator="in", values=None)

    def test_missing_min_max_parameters(self, sample_instance: GPUInstance) -> None:
        """Test that missing min/max parameters raises ValidationError."""
        from pydantic import ValidationError

        with pytest.raises(
            ValidationError, match="Operator 'between' requires both 'min' and 'max' fields"
        ):
            FilterConfig(field="price", operator="between", min=None, max=None)

    def test_unknown_operator(self, sample_instance: GPUInstance) -> None:
        """Test that unknown operator raises FilterError."""
        # This would require bypassing Pydantic validation, so we test the error path
        # by creating a FilterConfig with a valid operator then modifying it
        filter_config = FilterConfig(field="price", operator="eq", value=1.0)
        filter_config.operator = "invalid_operator"  # type: ignore[assignment]
        with pytest.raises(FilterError, match="Unknown operator"):
            apply_filter(sample_instance, filter_config)

    def test_missing_value_parameter_eq(self, sample_instance: GPUInstance) -> None:
        """Test missing value raises FilterError in apply_filter."""
        filter_config = FilterConfig.model_construct(field="price", operator="eq", value=None)
        with pytest.raises(FilterError, match="eq operator requires 'value' parameter"):
            apply_filter(sample_instance, filter_config)

    def test_missing_value_parameter_ne(self, sample_instance: GPUInstance) -> None:
        """Test missing value raises FilterError in apply_filter."""
        filter_config = FilterConfig.model_construct(field="price", operator="ne", value=None)
        with pytest.raises(FilterError, match="ne operator requires 'value' parameter"):
            apply_filter(sample_instance, filter_config)

    def test_missing_value_parameter_lt(self, sample_instance: GPUInstance) -> None:
        """Test missing value raises FilterError in apply_filter."""
        filter_config = FilterConfig.model_construct(field="price", operator="lt", value=None)
        with pytest.raises(FilterError, match="lt operator requires 'value' parameter"):
            apply_filter(sample_instance, filter_config)

    def test_missing_value_parameter_lte(self, sample_instance: GPUInstance) -> None:
        """Test missing value raises FilterError in apply_filter."""
        filter_config = FilterConfig.model_construct(field="price", operator="lte", value=None)
        with pytest.raises(FilterError, match="lte operator requires 'value' parameter"):
            apply_filter(sample_instance, filter_config)

    def test_missing_value_parameter_gt(self, sample_instance: GPUInstance) -> None:
        """Test missing value raises FilterError in apply_filter."""
        filter_config = FilterConfig.model_construct(field="price", operator="gt", value=None)
        with pytest.raises(FilterError, match="gt operator requires 'value' parameter"):
            apply_filter(sample_instance, filter_config)

    def test_missing_value_parameter_gte(self, sample_instance: GPUInstance) -> None:
        """Test missing value raises FilterError in apply_filter."""
        filter_config = FilterConfig.model_construct(field="price", operator="gte", value=None)
        with pytest.raises(FilterError, match="gte operator requires 'value' parameter"):
            apply_filter(sample_instance, filter_config)

    def test_missing_values_parameter_in(self, sample_instance: GPUInstance) -> None:
        """Test missing values raises FilterError in apply_filter."""
        filter_config = FilterConfig.model_construct(
            field="availability", operator="in", values=None
        )
        with pytest.raises(FilterError, match="in operator requires 'values' parameter"):
            apply_filter(sample_instance, filter_config)

    def test_missing_values_parameter_not_in(self, sample_instance: GPUInstance) -> None:
        """Test missing values raises FilterError in apply_filter."""
        filter_config = FilterConfig.model_construct(
            field="availability", operator="not_in", values=None
        )
        with pytest.raises(FilterError, match="not_in operator requires 'values' parameter"):
            apply_filter(sample_instance, filter_config)

    def test_missing_min_max_between(self, sample_instance: GPUInstance) -> None:
        """Test missing min/max raises FilterError in apply_filter."""
        filter_config = FilterConfig.model_construct(
            field="price", operator="between", min=None, max=None
        )
        with pytest.raises(FilterError, match="between operator requires 'min' and 'max'"):
            apply_filter(sample_instance, filter_config)

    def test_missing_value_parameter_regex(self, sample_instance: GPUInstance) -> None:
        """Test missing value raises FilterError for regex."""
        filter_config = FilterConfig.model_construct(field="region", operator="regex", value=None)
        with pytest.raises(FilterError, match="regex operator requires 'value' parameter"):
            apply_filter(sample_instance, filter_config)

    def test_missing_value_parameter_contains(self, sample_instance: GPUInstance) -> None:
        """Test missing value raises FilterError for contains."""
        filter_config = FilterConfig.model_construct(
            field="instance_type", operator="contains", value=None
        )
        with pytest.raises(FilterError, match="contains operator requires 'value' parameter"):
            apply_filter(sample_instance, filter_config)

    def test_missing_value_parameter_starts_with(self, sample_instance: GPUInstance) -> None:
        """Test missing value raises FilterError for starts_with."""
        filter_config = FilterConfig.model_construct(
            field="instance_type", operator="starts_with", value=None
        )
        with pytest.raises(FilterError, match="starts_with operator requires 'value' parameter"):
            apply_filter(sample_instance, filter_config)

    def test_comparison_type_error_raises(self, sample_instance: GPUInstance) -> None:
        """Test comparison type errors are wrapped as FilterError."""
        filter_config = FilterConfig.model_construct(field="provider", operator="lt", value=1)
        with pytest.raises(FilterError, match="Filter evaluation failed"):
            apply_filter(sample_instance, filter_config)
