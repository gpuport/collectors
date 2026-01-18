"""Command-line interface for GPU data collection and export."""

import asyncio
import logging
import sys
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import click
from dotenv import load_dotenv

from gpuport_collectors.collectors import get_collector_class, list_providers
from gpuport_collectors.export.loader import load_export_config, validate_config
from gpuport_collectors.export.pipeline import execute_pipelines
from gpuport_collectors.observability import get_observability_manager

# Load .env file if it exists (before anything else)
load_dotenv(override=False)

# Get structured logger
obs = get_observability_manager()
logger = obs.get_logger("cli")


@dataclass
class CollectionMetrics:
    """Tracks metrics during GPU instance collection."""

    instances: int = 0
    datacenters: set[str] = field(default_factory=set)
    api_calls: int = 0
    failed_api_calls: int = 0
    start_time: float = field(default_factory=time.time)

    @property
    def duration(self) -> float:
        """Return collection duration in seconds."""
        return time.time() - self.start_time


def print_summary(message: str, quiet: bool = False) -> None:
    """Print a summary message directly to stdout (not as JSON log).

    Used for human-readable summary reports that should not be formatted as JSON.

    Args:
        message: Message to print
        quiet: If True, suppress output
    """
    if not quiet:
        print(message)  # noqa: T201


def print_collection_summary(
    instances: list[Any], metrics: CollectionMetrics | None = None, quiet: bool = False
) -> None:
    """Print collection summary with metrics.

    Args:
        instances: Collected GPU instances
        metrics: Optional collection metrics
        quiet: If True, suppress output
    """
    if quiet:
        return

    # Extract datacenter/region info from instances
    # Some providers use "datacenter", others use "region"
    datacenters = set()
    for instance in instances:
        if hasattr(instance, "datacenter"):
            datacenters.add(instance.datacenter)
        elif hasattr(instance, "region"):
            datacenters.add(instance.region)

    print_summary("\n" + "=" * 60, quiet)
    print_summary("COLLECTION SUMMARY", quiet)
    print_summary("=" * 60, quiet)
    print_summary(f"  Instances: {len(instances)}", quiet)
    print_summary(f"  Datacenters: {len(datacenters)}", quiet)

    if metrics:
        print_summary(f"  API calls: {metrics.api_calls}", quiet)
        print_summary(f"  Failed API calls: {metrics.failed_api_calls}", quiet)
        print_summary(f"  Duration: {metrics.duration:.2f}s", quiet)

    print_summary("=" * 60 + "\n", quiet)


@click.group()
@click.option(
    "--quiet",
    "-q",
    is_flag=True,
    help="Suppress non-error output (quiet mode)",
)
@click.pass_context
def cli(ctx: click.Context, quiet: bool) -> None:
    """GPU data collection and export CLI."""
    # Store quiet flag in context for subcommands
    ctx.ensure_object(dict)
    ctx.obj["quiet"] = quiet

    # Configure quiet mode logging
    if quiet:
        # In quiet mode, only show ERROR level and above
        logger.logger.setLevel(logging.ERROR)


@cli.group()
def run() -> None:
    """Run data collection from GPU providers."""


@run.command()
@click.option(
    "--export-config",
    "-e",
    type=click.Path(exists=True),
    help="Path to export pipeline configuration YAML file",
)
@click.option(
    "--api-key",
    "-k",
    envvar="RUNPOD_API_KEY",
    help="RunPod API key (or set RUNPOD_API_KEY env var)",
)
@click.option(
    "--verbose",
    "-v",
    is_flag=True,
    help="Enable verbose logging",
)
@click.pass_context
def runpod(
    ctx: click.Context,
    export_config: str | None,
    api_key: str | None,
    verbose: bool,
) -> None:
    """Collect GPU data from RunPod.

    This command:
    1. Collects GPU instance data from RunPod API
    2. Optionally exports data using configured pipelines

    Example:
        gpuport-collectors run runpod --api-key YOUR_KEY
        gpuport-collectors run runpod --export-config examples/export-basic.yaml
        gpuport-collectors run runpod --export-config export.yaml --api-key YOUR_KEY
    """
    # Get quiet flag from context
    quiet = ctx.obj.get("quiet", False)

    # Configure logging level
    if verbose:
        logger.logger.setLevel(logging.DEBUG)
        logger.debug("Verbose logging enabled")
    elif quiet:
        logger.logger.setLevel(logging.ERROR)

    # Validate API key
    if not api_key:
        error_msg = "RunPod API key required. Set RUNPOD_API_KEY or use --api-key"
        logger.error(error_msg)
        click.echo(f"Error: {error_msg}", err=True)
        sys.exit(1)

    # Set API key in environment for RunPodCollector
    import os

    os.environ["RUNPOD_API_KEY"] = api_key

    # Load collector configuration
    from gpuport_collectors.config import CollectorConfig

    collector_config = CollectorConfig()

    # Create collector instance using registry
    collector_class = get_collector_class("runpod")
    collector = collector_class(config=collector_config)

    # Track collection metrics
    metrics = CollectionMetrics()

    # Show progress indicator
    if not quiet:
        print_summary("Collecting GPU instances from RunPod...", quiet)

    # Collect GPU instances
    logger.debug("Collecting GPU instances from RunPod...")
    instances = asyncio.run(collector.fetch_instances())
    logger.info(f"Collected {len(instances)} GPU instance(s)")

    # Update metrics
    metrics.instances = len(instances)
    # Note: API call metrics would need to be tracked in the collector
    # For now, we'll just show what we have

    if not instances:
        logger.warning("No instances collected")

    # Print collection summary
    print_collection_summary(instances, metrics, quiet)

    # If export config provided, run export pipelines
    if export_config:
        logger.debug(f"Loading export configuration from {export_config}")
        export_cfg = load_export_config(export_config)
        logger.debug(f"Loaded configuration with {len(export_cfg.pipelines)} pipeline(s)")

        # Validate configuration and show warnings
        warnings = validate_config(export_cfg)
        if warnings:
            logger.warning("Configuration validation warnings:")
            for warning in warnings:
                logger.warning(f"  • {warning}")

        # Execute export pipelines
        logger.debug("Executing export pipelines...")
        results = execute_pipelines(instances, export_cfg)

        # Report results
        print_summary("\n" + "=" * 60, quiet)
        print_summary("PIPELINE EXECUTION SUMMARY", quiet)
        print_summary("=" * 60, quiet)

        total_successful = 0
        total_failed = 0

        for result in results:
            if not result.enabled:
                print_summary(f"\n{result.pipeline_name}: DISABLED", quiet)
                continue

            status = "✓ SUCCESS" if result.success else "✗ FAILED"
            print_summary(f"\n{result.pipeline_name}: {status}", quiet)
            print_summary(f"  Input: {result.input_count} instances", quiet)
            print_summary(f"  Filtered: {result.filtered_count} instances", quiet)
            print_summary(
                f"  Outputs: {result.successful_outputs} successful, {result.failed_outputs} failed",
                quiet,
            )

            if result.duration_seconds is not None:
                print_summary(f"  Duration: {result.duration_seconds:.3f}s", quiet)

            if result.error:
                print_summary(f"  Error: {result.error}", quiet)
                total_failed += 1
            else:
                total_successful += 1

            # Show output details
            if result.outputs:
                print_summary("  Output details:", quiet)
                for output in result.outputs:
                    output_status = "✓" if output.get("success") else "✗"
                    output_name = output.get("name") or output.get("type", "unknown")
                    print_summary(f"    {output_status} {output_name}", quiet)
                    if output.get("path"):
                        print_summary(f"      Path: {output['path']}", quiet)
                    if not output.get("success"):
                        print_summary(f"      Error: {output.get('error')}", quiet)

        print_summary("\n" + "=" * 60, quiet)
        print_summary(
            f"TOTAL: {total_successful} successful, {total_failed} failed out of {len([r for r in results if r.enabled])} enabled pipelines",
            quiet,
        )
        print_summary("=" * 60 + "\n", quiet)

        # Exit with appropriate code
        if total_failed > 0:
            sys.exit(1)
    else:
        # Just display collected data
        logger.info("\nCollected GPU instances:")
        for instance in instances[:10]:  # Show first 10
            logger.info(
                f"  • {instance.provider} - {instance.instance_type} - "
                f"{instance.accelerator_name} x{instance.accelerator_count} - "
                f"${instance.price:.2f}/hr - {instance.availability.value}"
            )
        if len(instances) > 10:
            logger.info(f"  ... and {len(instances) - 10} more")


@run.command()
@click.option(
    "--export-config",
    "-e",
    type=click.Path(exists=True),
    help="Path to export pipeline configuration YAML file",
)
@click.option(
    "--api-key",
    "-k",
    envvar="LAMBDA_API_KEY",
    help="Lambda Labs API key (or set LAMBDA_API_KEY env var)",
)
@click.option(
    "--verbose",
    "-v",
    is_flag=True,
    help="Enable verbose logging",
)
@click.pass_context
def lambdalabs(
    ctx: click.Context,
    export_config: str | None,
    api_key: str | None,
    verbose: bool,
) -> None:
    """Collect GPU data from Lambda Labs.

    This command:
    1. Collects GPU instance data from Lambda Labs API
    2. Optionally exports data using configured pipelines

    Example:
        gpuport-collectors run lambdalabs --api-key YOUR_KEY
        gpuport-collectors run lambdalabs --export-config examples/export-basic.yaml
        gpuport-collectors run lambdalabs --export-config export.yaml --api-key YOUR_KEY
    """
    # Get quiet flag from context
    quiet = ctx.obj.get("quiet", False)

    # Configure logging level
    if verbose:
        logger.logger.setLevel(logging.DEBUG)
        logger.debug("Verbose logging enabled")
    elif quiet:
        logger.logger.setLevel(logging.ERROR)

    # Validate API key
    if not api_key:
        error_msg = "Lambda Labs API key required. Set LAMBDA_API_KEY or use --api-key"
        logger.error(error_msg)
        click.echo(f"Error: {error_msg}", err=True)
        sys.exit(1)

    # Set API key in environment for LambdaLabsCollector
    import os

    os.environ["LAMBDA_API_KEY"] = api_key

    # Load collector configuration
    from gpuport_collectors.config import CollectorConfig

    collector_config = CollectorConfig()

    # Create collector instance using registry
    collector_class = get_collector_class("lambdalabs")
    collector = collector_class(config=collector_config)

    # Track collection metrics
    metrics = CollectionMetrics()

    # Show progress indicator
    if not quiet:
        print_summary("Collecting GPU instances from Lambda Labs...", quiet)

    # Collect GPU instances
    logger.debug("Collecting GPU instances from Lambda Labs...")
    instances = asyncio.run(collector.fetch_instances())
    logger.info(f"Collected {len(instances)} GPU instance(s)")

    # Update metrics
    metrics.instances = len(instances)

    if not instances:
        logger.warning("No instances collected")

    # Print collection summary
    print_collection_summary(instances, metrics, quiet)

    # If export config provided, run export pipelines
    if export_config:
        logger.debug(f"Loading export configuration from {export_config}")
        export_cfg = load_export_config(export_config)
        logger.debug(f"Loaded configuration with {len(export_cfg.pipelines)} pipeline(s)")

        # Validate configuration and show warnings
        warnings = validate_config(export_cfg)
        if warnings:
            logger.warning("Configuration validation warnings:")
            for warning in warnings:
                logger.warning(f"  • {warning}")

        # Execute export pipelines
        logger.debug("Executing export pipelines...")
        results = execute_pipelines(instances, export_cfg)

        # Report results
        print_summary("\n" + "=" * 60, quiet)
        print_summary("PIPELINE EXECUTION SUMMARY", quiet)
        print_summary("=" * 60, quiet)

        total_successful = 0
        total_failed = 0

        for result in results:
            if not result.enabled:
                print_summary(f"\n{result.pipeline_name}: DISABLED", quiet)
                continue

            status = "✓ SUCCESS" if result.success else "✗ FAILED"
            print_summary(f"\n{result.pipeline_name}: {status}", quiet)
            print_summary(f"  Input: {result.input_count} instances", quiet)
            print_summary(f"  Filtered: {result.filtered_count} instances", quiet)
            print_summary(
                f"  Outputs: {result.successful_outputs} successful, {result.failed_outputs} failed",
                quiet,
            )

            if result.duration_seconds is not None:
                print_summary(f"  Duration: {result.duration_seconds:.3f}s", quiet)

            if result.error:
                print_summary(f"  Error: {result.error}", quiet)
                total_failed += 1
            else:
                total_successful += 1

            # Show output details
            if result.outputs:
                print_summary("  Output details:", quiet)
                for output in result.outputs:
                    output_status = "✓" if output.get("success") else "✗"
                    output_name = output.get("name") or output.get("type", "unknown")
                    print_summary(f"    {output_status} {output_name}", quiet)
                    if output.get("path"):
                        print_summary(f"      Path: {output['path']}", quiet)
                    if not output.get("success"):
                        print_summary(f"      Error: {output.get('error')}", quiet)

        print_summary("\n" + "=" * 60, quiet)
        print_summary(
            f"TOTAL: {total_successful} successful, {total_failed} failed out of {len([r for r in results if r.enabled])} enabled pipelines",
            quiet,
        )
        print_summary("=" * 60 + "\n", quiet)

        # Exit with appropriate code
        if total_failed > 0:
            sys.exit(1)
    else:
        # Just display collected data
        logger.info("\nCollected GPU instances:")
        for instance in instances[:10]:  # Show first 10
            logger.info(
                f"  • {instance.provider} - {instance.instance_type} - "
                f"{instance.accelerator_name} x{instance.accelerator_count} - "
                f"${instance.price:.2f}/hr - {instance.availability.value}"
            )
        if len(instances) > 10:
            logger.info(f"  ... and {len(instances) - 10} more")


@cli.command()
@click.option(
    "--config",
    "-c",
    type=click.Path(exists=True),
    required=True,
    help="Path to export configuration YAML file",
)
@click.option(
    "--provider",
    "-p",
    type=click.Choice(list_providers(), case_sensitive=False),
    default="runpod",
    help="GPU provider to collect from (default: runpod)",
)
@click.option(
    "--api-key",
    "-k",
    envvar="RUNPOD_API_KEY",
    help="Provider API key (or set RUNPOD_API_KEY env var)",
)
@click.option(
    "--validate-only",
    is_flag=True,
    help="Only validate configuration without executing pipelines",
)
@click.option(
    "--verbose",
    "-v",
    is_flag=True,
    help="Enable verbose logging",
)
@click.pass_context
def export(
    ctx: click.Context,
    config: str,
    provider: str,
    api_key: str | None,
    validate_only: bool,
    verbose: bool,
) -> None:
    """Collect GPU data and execute export pipelines (legacy command).

    This command:
    1. Collects GPU instance data from the specified provider
    2. Loads the export configuration
    3. Executes all enabled export pipelines
    4. Outputs results and metrics

    Example:
        gpuport-collectors export --config export.yaml --api-key YOUR_KEY
    """
    # Get quiet flag from context
    quiet = ctx.obj.get("quiet", False)

    # Configure logging level
    if verbose:
        logger.logger.setLevel(logging.DEBUG)
        logger.debug("Verbose logging enabled")
    elif quiet:
        logger.logger.setLevel(logging.ERROR)

    logger.debug(f"Loading export configuration from {config}")

    try:
        # Load and validate configuration
        export_config = load_export_config(config)
        logger.debug(f"Loaded configuration with {len(export_config.pipelines)} pipeline(s)")

        # Validate configuration and show warnings
        warnings = validate_config(export_config)
        if warnings:
            logger.warning("Configuration validation warnings:")
            click.echo("Configuration validation warnings:", err=True)
            for warning in warnings:
                logger.warning(f"  • {warning}")
                click.echo(f"  • {warning}", err=True)

        if validate_only:
            logger.info("Configuration validation complete (--validate-only mode)")
            logger.info("✓ Configuration is valid")
            click.echo("Configuration validation complete (--validate-only mode)")
            click.echo("✓ Configuration is valid")
            sys.exit(0)

        # Create collector instance using registry
        try:
            collector_class = get_collector_class(provider)
        except ValueError as e:
            logger.error(str(e))
            sys.exit(1)

        if provider == "runpod":
            if not api_key:
                logger.error("RunPod API key required. Set RUNPOD_API_KEY or use --api-key")
                sys.exit(1)
            # Set API key in environment for RunPodCollector
            import os

            os.environ["RUNPOD_API_KEY"] = api_key

        from gpuport_collectors.config import CollectorConfig

        collector = collector_class(config=CollectorConfig())

        # Collect GPU instances
        logger.debug(f"Collecting GPU instances from {provider}...")
        instances = asyncio.run(collector.fetch_instances())
        logger.info(f"Collected {len(instances)} GPU instance(s)")

        if not instances:
            logger.warning("No instances collected - pipelines will process empty data")

        # Execute export pipelines
        logger.debug("Executing export pipelines...")
        results = execute_pipelines(instances, export_config)

        # Report results
        print_summary("\n" + "=" * 60, quiet)
        print_summary("PIPELINE EXECUTION SUMMARY", quiet)
        print_summary("=" * 60, quiet)

        total_successful = 0
        total_failed = 0

        for result in results:
            if not result.enabled:
                print_summary(f"\n{result.pipeline_name}: DISABLED", quiet)
                continue

            status = "✓ SUCCESS" if result.success else "✗ FAILED"
            print_summary(f"\n{result.pipeline_name}: {status}", quiet)
            print_summary(f"  Input: {result.input_count} instances", quiet)
            print_summary(f"  Filtered: {result.filtered_count} instances", quiet)
            print_summary(
                f"  Outputs: {result.successful_outputs} successful, {result.failed_outputs} failed",
                quiet,
            )

            if result.duration_seconds is not None:
                print_summary(f"  Duration: {result.duration_seconds:.3f}s", quiet)
                if result.filter_duration is not None:
                    print_summary(f"    Filter: {result.filter_duration:.3f}s", quiet)
                    print_summary(f"    Transform: {result.transform_duration:.3f}s", quiet)
                    print_summary(f"    Output: {result.output_duration:.3f}s", quiet)

            if result.error:
                print_summary(f"  Error: {result.error}", quiet)
                total_failed += 1
            else:
                total_successful += 1

            # Show output details
            if result.outputs:
                print_summary("  Output details:", quiet)
                for output in result.outputs:
                    output_status = "✓" if output.get("success") else "✗"
                    output_name = output.get("name") or output.get("type", "unknown")
                    print_summary(f"    {output_status} {output_name}", quiet)
                    if output.get("path"):
                        print_summary(f"      Path: {output['path']}", quiet)
                    elif output.get("key"):
                        print_summary(f"      S3: {output.get('bucket')}/{output['key']}", quiet)
                    elif output.get("url"):
                        print_summary(f"      URL: {output['url']}", quiet)
                        if output.get("total_requests"):
                            print_summary(
                                f"      Requests: {output['successful_requests']}/{output['total_requests']}",
                                quiet,
                            )
                    if not output.get("success"):
                        print_summary(f"      Error: {output.get('error')}", quiet)

        print_summary("\n" + "=" * 60, quiet)
        print_summary(
            f"TOTAL: {total_successful} successful, {total_failed} failed out of {len([r for r in results if r.enabled])} enabled pipelines",
            quiet,
        )
        print_summary("=" * 60 + "\n", quiet)

        # Exit with appropriate code
        if total_failed > 0:
            sys.exit(1)

    except Exception as e:
        # Always show error message, but only include stack trace when verbose
        if verbose:
            logger.error("Export failed", error=e)
        else:
            logger.error(f"Export failed: {e}")
        sys.exit(1)


@cli.command()
@click.option(
    "--config",
    "-c",
    type=click.Path(exists=True),
    required=True,
    help="Path to export configuration YAML file",
)
def validate(config: str) -> None:
    """Validate an export configuration file.

    Checks the configuration for:
    - Valid YAML syntax
    - Required fields
    - Type correctness
    - Common mistakes (e.g., missing filters, no outputs)

    Example:
        gpuport-collectors validate --config export.yaml
    """
    # Convert string path to Path object for internal use
    config_path = Path(config)
    logger.info(f"Validating configuration: {config_path}")

    try:
        # Load configuration
        export_config = load_export_config(config)
        logger.info("✓ Configuration loaded successfully")
        logger.info(f"  Pipelines: {len(export_config.pipelines)}")

        # Validate and show warnings
        warnings = validate_config(export_config)

        if warnings:
            logger.warning("\nValidation warnings:")
            for warning in warnings:
                logger.warning(f"  • {warning}")
        else:
            logger.info("✓ No validation warnings")

        # Show pipeline summary
        print_summary("\nPipeline summary:")
        for pipeline in export_config.pipelines:
            enabled_status = "✓ enabled" if pipeline.enabled else "✗ disabled"
            print_summary(f"  {pipeline.name}: {enabled_status}")
            print_summary(f"    Filters: {len(pipeline.filters) if pipeline.filters else 0}")
            # Get transformer format (all transformers have a format field)
            transformer_format = getattr(pipeline.transformer, "format", "unknown")
            print_summary(f"    Transformer: {transformer_format}")
            print_summary(f"    Outputs: {len(pipeline.outputs)}")
            for output in pipeline.outputs:
                print_summary(f"      - {output.type}: {output.name or 'unnamed'}")

        print_summary("\n✓ Configuration is valid")

    except Exception as e:
        logger.error(f"Validation failed: {e}")
        sys.exit(1)


if __name__ == "__main__":
    cli()
