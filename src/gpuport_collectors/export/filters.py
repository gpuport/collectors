"""Filter engine for GPU instance data.

Provides filtering capabilities for export pipelines, supporting various
comparison operators and field-based filtering.
"""

import re
from typing import Any

from gpuport_collectors.export.config import FilterConfig
from gpuport_collectors.models import GPUInstance


class FilterError(Exception):
    """Raised when filter evaluation fails."""


def get_field_value(instance: GPUInstance, field: str) -> Any:
    """Get field value from GPUInstance.

    Args:
        instance: GPUInstance to extract field from
        field: Field name to extract

    Returns:
        Field value

    Raises:
        FilterError: If field doesn't exist
    """
    if not hasattr(instance, field):
        raise FilterError(f"Field '{field}' does not exist on GPUInstance")
    return getattr(instance, field)


def apply_filter(instance: GPUInstance, filter_config: FilterConfig) -> bool:
    """Apply a single filter to a GPUInstance.

    Args:
        instance: GPUInstance to filter
        filter_config: Filter configuration

    Returns:
        True if instance passes filter, False otherwise

    Raises:
        FilterError: If filter configuration is invalid or evaluation fails
    """
    field_value = get_field_value(instance, filter_config.field)
    operator = filter_config.operator

    try:
        # Equality operators
        if operator == "eq":
            if filter_config.value is None:
                raise FilterError("eq operator requires 'value' parameter")
            return bool(field_value == filter_config.value)

        if operator == "ne":
            if filter_config.value is None:
                raise FilterError("ne operator requires 'value' parameter")
            return bool(field_value != filter_config.value)

        # Comparison operators (numeric)
        if operator == "lt":
            if filter_config.value is None:
                raise FilterError("lt operator requires 'value' parameter")
            if field_value is None:
                return False
            return bool(field_value < filter_config.value)

        if operator == "lte":
            if filter_config.value is None:
                raise FilterError("lte operator requires 'value' parameter")
            if field_value is None:
                return False
            return bool(field_value <= filter_config.value)

        if operator == "gt":
            if filter_config.value is None:
                raise FilterError("gt operator requires 'value' parameter")
            if field_value is None:
                return False
            return bool(field_value > filter_config.value)

        if operator == "gte":
            if filter_config.value is None:
                raise FilterError("gte operator requires 'value' parameter")
            if field_value is None:
                return False
            return bool(field_value >= filter_config.value)

        # Range operator
        if operator == "between":
            if filter_config.min is None or filter_config.max is None:
                raise FilterError("between operator requires 'min' and 'max' parameters")
            if field_value is None:
                return False
            return bool(filter_config.min <= field_value <= filter_config.max)

        # Set membership operators
        if operator == "in":
            if filter_config.values is None:
                raise FilterError("in operator requires 'values' parameter")
            return field_value in filter_config.values

        if operator == "not_in":
            if filter_config.values is None:
                raise FilterError("not_in operator requires 'values' parameter")
            return field_value not in filter_config.values

        # String pattern matching
        if operator == "regex":
            if filter_config.value is None:
                raise FilterError("regex operator requires 'value' parameter")
            if field_value is None:
                return False
            if not isinstance(field_value, str):
                raise FilterError(f"regex operator requires string field, got {type(field_value)}")
            pattern = re.compile(str(filter_config.value))
            return pattern.search(field_value) is not None

        if operator == "contains":
            if filter_config.value is None:
                raise FilterError("contains operator requires 'value' parameter")
            if field_value is None:
                return False
            if not isinstance(field_value, str):
                raise FilterError(
                    f"contains operator requires string field, got {type(field_value)}"
                )
            return str(filter_config.value) in field_value

        if operator == "starts_with":
            if filter_config.value is None:
                raise FilterError("starts_with operator requires 'value' parameter")
            if field_value is None:
                return False
            if not isinstance(field_value, str):
                raise FilterError(
                    f"starts_with operator requires string field, got {type(field_value)}"
                )
            return field_value.startswith(str(filter_config.value))

        # Null checks
        if operator == "is_null":
            return field_value is None

        if operator == "is_not_null":
            return field_value is not None

        raise FilterError(f"Unknown operator: {operator}")

    except (TypeError, ValueError) as e:
        raise FilterError(f"Filter evaluation failed for {filter_config.field}: {e}") from e


def apply_filters(instance: GPUInstance, filters: list[FilterConfig]) -> bool:
    """Apply all filters to a GPUInstance (AND logic).

    Args:
        instance: GPUInstance to filter
        filters: List of filter configurations

    Returns:
        True if instance passes all filters, False otherwise

    Raises:
        FilterError: If any filter configuration is invalid or evaluation fails
    """
    return all(apply_filter(instance, filter_config) for filter_config in filters)


def filter_instances(
    instances: list[GPUInstance], filters: list[FilterConfig]
) -> list[GPUInstance]:
    """Filter a list of GPUInstances.

    Args:
        instances: List of GPUInstances to filter
        filters: List of filter configurations

    Returns:
        Filtered list of GPUInstances

    Raises:
        FilterError: If any filter configuration is invalid or evaluation fails
    """
    if not filters:
        return instances

    filtered = []
    for instance in instances:
        if apply_filters(instance, filters):
            filtered.append(instance)

    return filtered
