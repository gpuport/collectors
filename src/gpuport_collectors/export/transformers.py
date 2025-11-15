"""Data transformers for export pipeline.

Provides transformers to convert GPUInstance data to various output formats
(JSON, CSV) with configurable options.
"""

import csv
import json
from collections import defaultdict
from datetime import UTC, datetime
from io import StringIO
from typing import Any

from gpuport_collectors.export.config import (
    CSVTransformerConfig,
    JSONTransformerConfig,
    MetricsTransformerConfig,
)
from gpuport_collectors.models import GPUInstance


class TransformerError(Exception):
    """Raised when transformation fails."""


def transform_to_json(instances: list[GPUInstance], config: JSONTransformerConfig) -> str:
    """Transform GPUInstance list to JSON string.

    Args:
        instances: List of GPUInstances to transform
        config: JSON transformer configuration

    Returns:
        JSON string representation

    Raises:
        TransformerError: If transformation fails
    """
    try:
        # Convert instances to dictionaries
        data: list[dict[str, Any]] = [
            instance.model_dump(mode="json", exclude_none=(config.null_handling == "omit"))
            for instance in instances
        ]

        # Apply field selection and mapping if specified
        if config.fields:
            data = [
                {
                    output_name: item[source_field]
                    for source_field, output_name in config.fields.items()
                    if source_field in item
                }
                for item in data
            ]

        # Handle raw_data inclusion
        if not config.include_raw_data:
            data = [{k: v for k, v in item.items() if k != "raw_data"} for item in data]

        # Handle null values
        if config.null_handling == "empty":
            data = [{k: ("" if v is None else v) for k, v in item.items()} for item in data]

        # Serialize to JSON
        return json.dumps(
            data,
            indent=2 if config.pretty_print else None,
            ensure_ascii=not config.pretty_print,
        )

    except (TypeError, ValueError, KeyError) as e:
        raise TransformerError(f"JSON transformation failed: {e}") from e


def transform_to_csv(instances: list[GPUInstance], config: CSVTransformerConfig) -> str:
    """Transform GPUInstance list to CSV string.

    Args:
        instances: List of GPUInstances to transform
        config: CSV transformer configuration

    Returns:
        CSV string representation

    Raises:
        TransformerError: If transformation fails
    """
    try:
        if not instances:
            return ""

        # Convert instances to dictionaries
        data: list[dict[str, Any]] = [instance.model_dump(mode="json") for instance in instances]

        # Get output aliases from config
        output_aliases = list(config.fields.values())

        # Create CSV output
        output = StringIO()
        writer = csv.DictWriter(
            output,
            fieldnames=output_aliases,
            extrasaction="ignore",
            delimiter=config.delimiter,
            quotechar=config.quote_char,
            escapechar=config.escape_char,
            lineterminator=config.line_terminator,
        )

        # Write header if enabled
        if config.include_headers:
            writer.writeheader()

        # Write data rows
        for row in data:
            # Map source fields to output aliases and handle nulls
            mapped_row = {}
            for source_field, output_alias in config.fields.items():
                value = row.get(source_field)
                mapped_row[output_alias] = config.null_value if value is None else value
            writer.writerow(mapped_row)

        return output.getvalue()

    except (TypeError, ValueError, csv.Error, KeyError) as e:
        raise TransformerError(f"CSV transformation failed: {e}") from e


def transform_to_metrics(instances: list[GPUInstance], config: MetricsTransformerConfig) -> str:
    """Transform GPUInstance list to metrics JSON.

    Args:
        instances: List of GPUInstances to transform
        config: Metrics transformer configuration

    Returns:
        JSON string with aggregated metrics

    Raises:
        TransformerError: If transformation fails
    """
    try:
        result: dict[str, Any] = {}

        # Add timestamp if requested
        if config.include_timestamp:
            result["timestamp"] = datetime.now(UTC).isoformat()

        # Add collection info if requested
        if config.include_collection_info:
            result["collection_info"] = {
                "total_instances": len(instances),
                "collected_at": datetime.now(UTC).isoformat(),
            }

        # Compute each metric
        metrics_data: dict[str, Any] = {}
        for metric_config in config.metrics:
            if metric_config.group_by:
                # Group-by aggregation
                metrics_data[metric_config.name] = _compute_grouped_metric(instances, metric_config)
            else:
                # Single aggregation
                metrics_data[metric_config.name] = _compute_metric(instances, metric_config)

        result["metrics"] = metrics_data

        return json.dumps(result, indent=2)

    except (TypeError, ValueError, KeyError) as e:
        raise TransformerError(f"Metrics transformation failed: {e}") from e


def _compute_metric(instances: list[GPUInstance], metric_config: Any) -> Any:
    """Compute a single metric across all instances.

    Args:
        instances: List of GPUInstances
        metric_config: Metric configuration

    Returns:
        Computed metric value
    """
    if metric_config.type == "count":
        return len(instances)

    if not metric_config.field:
        raise TransformerError(f"Metric '{metric_config.name}' requires 'field' parameter")

    # Extract field values
    values = [
        getattr(instance, metric_config.field)
        for instance in instances
        if hasattr(instance, metric_config.field)
        and getattr(instance, metric_config.field) is not None
    ]

    if not values:
        return None

    if metric_config.type == "avg":
        return sum(values) / len(values)
    if metric_config.type == "min":
        return min(values)
    if metric_config.type == "max":
        return max(values)
    if metric_config.type == "sum":
        return sum(values)
    if metric_config.type == "unique":
        return len(set(values))

    raise TransformerError(f"Unknown metric type: {metric_config.type}")


def _compute_grouped_metric(instances: list[GPUInstance], metric_config: Any) -> dict[str, Any]:
    """Compute a metric grouped by a field.

    Args:
        instances: List of GPUInstances
        metric_config: Metric configuration with group_by field

    Returns:
        Dict mapping group values to metric values. Null/None values in the
        group_by field are converted to the string "null" and included as a
        separate group.

    Raises:
        TransformerError: If group_by parameter is missing or field doesn't exist

    Note:
        Null/None values in the group_by field are treated as a valid group
        and represented with the key "null" in the output. This differs from
        numeric field handling where None values are filtered out of aggregations.

    Example:
        If grouping by "spot_price" and some instances have spot_price=None,
        those instances will be grouped under the "null" key.
    """
    if not metric_config.group_by:
        raise TransformerError(f"Metric '{metric_config.name}' requires 'group_by' parameter")

    # Validate that group_by field exists (check first instance if list not empty)
    if instances and not hasattr(instances[0], metric_config.group_by):
        raise TransformerError(f"Field '{metric_config.group_by}' does not exist on GPUInstance")

    # Group instances by the group_by field
    groups: dict[str, list[GPUInstance]] = defaultdict(list)
    for instance in instances:
        group_value = getattr(instance, metric_config.group_by)
        # Convert to string for JSON serialization
        # None values become "null" group (documented behavior)
        group_key = str(group_value) if group_value is not None else "null"
        groups[group_key].append(instance)

    # Compute metric for each group
    result = {}
    for group_key, group_instances in groups.items():
        result[group_key] = _compute_metric(group_instances, metric_config)

    return result
