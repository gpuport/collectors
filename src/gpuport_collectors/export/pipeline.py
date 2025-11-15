"""Export pipeline executor.

Orchestrates the filter → transform → output flow for exporting GPU instance data.
"""

import time
from typing import Any

from gpuport_collectors.export.config import (
    CSVTransformerConfig,
    ExportConfig,
    HTTPSOutputConfig,
    JSONTransformerConfig,
    LocalOutputConfig,
    MetricsTransformerConfig,
    PipelineConfig,
    S3OutputConfig,
)
from gpuport_collectors.export.filters import filter_instances
from gpuport_collectors.export.outputs import write_to_https, write_to_local, write_to_s3
from gpuport_collectors.export.transformers import (
    transform_to_csv,
    transform_to_json,
    transform_to_metrics,
)
from gpuport_collectors.models import GPUInstance
from gpuport_collectors.observability import get_observability_manager


class PipelineError(Exception):
    """Raised when pipeline execution fails."""


class PipelineResult:
    """Result of pipeline execution."""

    def __init__(
        self,
        pipeline_name: str,
        enabled: bool,
        input_count: int,
        filtered_count: int,
        output_count: int,
        outputs: list[dict[str, Any]],
        error: str | None = None,
        duration_seconds: float | None = None,
        filter_duration: float | None = None,
        transform_duration: float | None = None,
        output_duration: float | None = None,
    ):
        self.pipeline_name = pipeline_name
        self.enabled = enabled
        self.input_count = input_count
        self.filtered_count = filtered_count
        self.output_count = output_count
        self.outputs = outputs
        self.error = error
        self.duration_seconds = duration_seconds
        self.filter_duration = filter_duration
        self.transform_duration = transform_duration
        self.output_duration = output_duration

    @property
    def success(self) -> bool:
        """Check if pipeline execution was successful."""
        return self.error is None

    @property
    def successful_outputs(self) -> int:
        """Count of successful outputs."""
        return sum(1 for output in self.outputs if output.get("success", False))

    @property
    def failed_outputs(self) -> int:
        """Count of failed outputs."""
        return sum(1 for output in self.outputs if not output.get("success", False))

    def to_dict(self) -> dict[str, Any]:
        """Convert result to dictionary."""
        result = {
            "pipeline_name": self.pipeline_name,
            "enabled": self.enabled,
            "success": self.success,
            "input_count": self.input_count,
            "filtered_count": self.filtered_count,
            "output_count": self.output_count,
            "successful_outputs": self.successful_outputs,
            "failed_outputs": self.failed_outputs,
            "outputs": self.outputs,
            "error": self.error,
        }

        # Add timing metrics if available
        if self.duration_seconds is not None:
            result["duration_seconds"] = self.duration_seconds
            result["filter_duration"] = self.filter_duration
            result["transform_duration"] = self.transform_duration
            result["output_duration"] = self.output_duration

        return result


def execute_pipeline(instances: list[GPUInstance], config: PipelineConfig) -> PipelineResult:
    """Execute a single export pipeline.

    Args:
        instances: List of GPU instances to process
        config: Pipeline configuration

    Returns:
        PipelineResult with execution details

    Note:
        If pipeline is disabled, returns result with enabled=False and no processing.
        Errors during execution are caught and returned in the result.
    """
    # Get logger
    obs = get_observability_manager()
    logger = obs.get_logger("export.pipeline")

    # Start timing
    start_time = time.time()

    # Log pipeline start
    logger.debug(
        f"Starting pipeline '{config.name}'",
        pipeline=config.name,
        input_count=len(instances),
        enabled=config.enabled,
    )

    # Check if pipeline is enabled
    if not config.enabled:
        logger.debug(f"Pipeline '{config.name}' is disabled, skipping")
        return PipelineResult(
            pipeline_name=config.name,
            enabled=False,
            input_count=len(instances),
            filtered_count=0,
            output_count=0,
            outputs=[],
        )

    try:
        # Step 1: Filter instances
        filter_start = time.time()
        filtered = instances
        if config.filters:
            filtered = filter_instances(instances, config.filters)
            logger.debug(
                f"Filtered instances for pipeline '{config.name}'",
                pipeline=config.name,
                input_count=len(instances),
                filtered_count=len(filtered),
                filter_count=len(config.filters),
            )
        filter_duration = time.time() - filter_start

        # Step 2: Transform data
        transform_start = time.time()
        transformer = config.transformer
        if isinstance(transformer, JSONTransformerConfig):
            data = transform_to_json(filtered, transformer)
            transform_type = "json"
        elif isinstance(transformer, CSVTransformerConfig):
            data = transform_to_csv(filtered, transformer)
            transform_type = "csv"
        elif isinstance(transformer, MetricsTransformerConfig):
            data = transform_to_metrics(filtered, transformer)
            transform_type = "metrics"
        else:
            raise PipelineError(f"Unknown transformer type: {type(transformer)}")

        transform_duration = time.time() - transform_start
        logger.debug(
            f"Transformed data for pipeline '{config.name}'",
            pipeline=config.name,
            transform_type=transform_type,
            instance_count=len(filtered),
        )

        # Step 3: Write to outputs
        output_start = time.time()
        output_results = []
        for output_config in config.outputs:
            try:
                if isinstance(output_config, LocalOutputConfig):
                    path = write_to_local(data, output_config)
                    output_results.append(
                        {
                            "type": "local",
                            "name": output_config.name,
                            "path": str(path),
                            "success": True,
                        }
                    )
                    logger.debug(
                        f"Wrote to local output for pipeline '{config.name}'",
                        pipeline=config.name,
                        output_type="local",
                        output_name=output_config.name,
                        path=str(path),
                    )
                elif isinstance(output_config, S3OutputConfig):
                    key = write_to_s3(data, output_config)
                    output_results.append(
                        {
                            "type": "s3",
                            "name": output_config.name,
                            "bucket": output_config.bucket,
                            "key": key,
                            "success": True,
                        }
                    )
                    logger.debug(
                        f"Wrote to S3 output for pipeline '{config.name}'",
                        pipeline=config.name,
                        output_type="s3",
                        output_name=output_config.name,
                        bucket=output_config.bucket,
                        key=key,
                    )
                elif isinstance(output_config, HTTPSOutputConfig):
                    result = write_to_https(data, output_config)
                    output_results.append(
                        {
                            "type": "https",
                            "name": output_config.name,
                            "url": output_config.url,
                            "success": True,
                            **result,
                        }
                    )
                    logger.debug(
                        f"Wrote to HTTPS output for pipeline '{config.name}'",
                        pipeline=config.name,
                        output_type="https",
                        output_name=output_config.name,
                        url=output_config.url,
                        **result,
                    )
                else:
                    raise PipelineError(f"Unknown output type: {type(output_config)}")

            except Exception as e:
                # Individual output failure - track but continue
                output_results.append(
                    {
                        "type": getattr(output_config, "type", "unknown"),
                        "name": getattr(output_config, "name", None),
                        "success": False,
                        "error": str(e),
                    }
                )
                logger.error(
                    f"Failed to write to output for pipeline '{config.name}'",
                    pipeline=config.name,
                    output_type=getattr(output_config, "type", "unknown"),
                    output_name=getattr(output_config, "name", None),
                    error=e,
                )

        output_duration = time.time() - output_start
        duration_seconds = time.time() - start_time

        # Log pipeline completion
        successful = sum(1 for o in output_results if o.get("success", False))
        failed = sum(1 for o in output_results if not o.get("success", True))
        logger.info(
            f"Completed pipeline '{config.name}'",
            pipeline=config.name,
            duration_seconds=round(duration_seconds, 3),
            filter_duration=round(filter_duration, 3),
            transform_duration=round(transform_duration, 3),
            output_duration=round(output_duration, 3),
            successful_outputs=successful,
            failed_outputs=failed,
        )

        return PipelineResult(
            pipeline_name=config.name,
            enabled=True,
            input_count=len(instances),
            filtered_count=len(filtered),
            output_count=len(output_results),
            outputs=output_results,
            duration_seconds=duration_seconds,
            filter_duration=filter_duration,
            transform_duration=transform_duration,
            output_duration=output_duration,
        )

    except Exception as e:
        # Pipeline-level failure
        duration_seconds = time.time() - start_time
        logger.error(
            f"Pipeline '{config.name}' failed",
            pipeline=config.name,
            error=e,
            duration_seconds=round(duration_seconds, 3),
        )
        return PipelineResult(
            pipeline_name=config.name,
            enabled=True,
            input_count=len(instances),
            filtered_count=0,
            output_count=0,
            outputs=[],
            error=str(e),
            duration_seconds=duration_seconds,
        )


def execute_pipelines(instances: list[GPUInstance], config: ExportConfig) -> list[PipelineResult]:
    """Execute all export pipelines.

    Args:
        instances: List of GPU instances to process
        config: Export configuration with multiple pipelines

    Returns:
        List of PipelineResult for each configured pipeline
    """
    results = []
    for pipeline_config in config.pipelines:
        result = execute_pipeline(instances, pipeline_config)
        results.append(result)

    return results
