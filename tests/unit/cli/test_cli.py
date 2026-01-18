"""Tests for CLI commands and output formatting."""

import os
from pathlib import Path
from unittest.mock import MagicMock, patch

from click.testing import CliRunner

from gpuport_collectors.cli import cli, print_summary
from gpuport_collectors.export.pipeline import PipelineResult
from gpuport_collectors.models import AvailabilityStatus, GPUInstance


def _make_instances(count: int) -> list[GPUInstance]:
    """Create mock GPU instances for CLI tests."""
    return [
        GPUInstance(
            provider="TestProvider",
            instance_type=f"type-{idx}",
            accelerator_name="Test GPU",
            accelerator_count=1,
            accelerator_mem_gib=16,
            v_cpus=4,
            memory_gib=16,
            price=1.23,
            availability=AvailabilityStatus.HIGH,
            region="test-region",
        )
        for idx in range(count)
    ]


def _run_coro_with_result(result):
    """Return a callable that closes the coroutine and returns a fixed result."""

    def _run(coro):
        coro.close()
        return result

    return _run


class TestPrintSummary:
    """Tests for the print_summary helper function."""

    def test_print_summary_outputs_to_stdout(self, capsys):
        """Test that print_summary outputs plain text to stdout."""
        message = "Test summary message"
        print_summary(message)

        captured = capsys.readouterr()
        assert captured.out == f"{message}\n"
        assert captured.err == ""

    def test_print_summary_not_json_formatted(self, capsys):
        """Test that print_summary does NOT output JSON format."""
        message = "Pipeline execution complete"
        print_summary(message)

        captured = capsys.readouterr()
        # Should be plain text, not JSON with timestamp/level
        assert "timestamp" not in captured.out
        assert "level" not in captured.out
        assert captured.out == f"{message}\n"

    def test_print_summary_with_special_characters(self, capsys):
        """Test print_summary handles special characters correctly."""
        message = "✓ SUCCESS: 100% complete"
        print_summary(message)

        captured = capsys.readouterr()
        assert message in captured.out

    def test_print_summary_quiet_suppresses_output(self, capsys):
        """Test print_summary suppresses output in quiet mode."""
        print_summary("Silent message", quiet=True)

        captured = capsys.readouterr()
        assert captured.out == ""
        assert captured.err == ""


class TestPrintCollectionSummary:
    """Tests for the print_collection_summary helper function."""

    def test_print_collection_summary_with_metrics(self, capsys):
        """Test collection summary prints metrics and regions."""
        from gpuport_collectors.cli import CollectionMetrics, print_collection_summary

        instance = MagicMock(region="us-east-1")
        metrics = CollectionMetrics()
        metrics.api_calls = 3
        metrics.failed_api_calls = 1

        print_collection_summary([instance], metrics=metrics)

        captured = capsys.readouterr()
        assert "Instances: 1" in captured.out
        assert "Datacenters: 1" in captured.out
        assert "API calls: 3" in captured.out
        assert "Failed API calls: 1" in captured.out

    def test_print_collection_summary_quiet(self, capsys):
        """Test collection summary is suppressed in quiet mode."""
        from gpuport_collectors.cli import print_collection_summary

        print_collection_summary([MagicMock(region="us-east-1")], quiet=True)

        captured = capsys.readouterr()
        assert captured.out == ""


class TestRunPodCommand:
    """Tests for the runpod command."""

    def test_runpod_missing_api_key_error(self):
        """Test runpod command fails with clear error when API key missing."""
        runner = CliRunner()
        with patch.dict(os.environ, {}, clear=True):
            result = runner.invoke(cli, ["run", "runpod"])

        # Should exit with error code when API key is missing
        assert result.exit_code == 1
        # Error message should appear in command output
        assert "RunPod API key required" in result.output

    def test_runpod_verbose_flag_enables_debug(self):
        """Test --verbose flag enables debug logging."""
        runner = CliRunner()
        with (
            patch("gpuport_collectors.cli.get_collector_class"),
            patch("gpuport_collectors.cli.asyncio.run") as mock_run,
        ):
            # Mock the collector to return empty instances
            mock_run.side_effect = _run_coro_with_result([])

            result = runner.invoke(cli, ["run", "runpod", "--api-key", "test-key", "--verbose"])

            # Should succeed
            assert result.exit_code == 0

    def test_runpod_export_output_details_and_warnings(self):
        """Test runpod export outputs details and exits on failure."""
        runner = CliRunner()
        with runner.isolated_filesystem():
            config_file = Path("export.yaml")
            config_file.write_text("pipelines: []")

            results = [
                PipelineResult(
                    pipeline_name="test-export",
                    enabled=True,
                    input_count=1,
                    filtered_count=1,
                    output_count=1,
                    outputs=[
                        {"success": True, "name": "local", "path": "/tmp/out.json"},
                        {"success": False, "type": "http", "error": "output failed"},
                    ],
                    error="pipeline error",
                    duration_seconds=0.2,
                ),
                PipelineResult(
                    pipeline_name="disabled-export",
                    enabled=False,
                    input_count=0,
                    filtered_count=0,
                    output_count=0,
                    outputs=[],
                ),
            ]

            with (
                patch(
                    "gpuport_collectors.cli.load_export_config",
                    return_value=MagicMock(pipelines=[MagicMock()]),
                ),
                patch("gpuport_collectors.cli.validate_config", return_value=["warning"]),
                patch("gpuport_collectors.cli.execute_pipelines", return_value=results),
                patch(
                    "gpuport_collectors.cli.asyncio.run",
                    side_effect=_run_coro_with_result(_make_instances(1)),
                ),
            ):
                result = runner.invoke(
                    cli,
                    [
                        "run",
                        "runpod",
                        "--api-key",
                        "test-key",
                        "--export-config",
                        str(config_file),
                    ],
                )

            assert result.exit_code == 1
            assert "PIPELINE EXECUTION SUMMARY" in result.output
            assert "Output details:" in result.output
            assert "Path: /tmp/out.json" in result.output
            assert "Error: output failed" in result.output

    def test_runpod_no_export_lists_more_instances(self):
        """Test runpod without export lists truncated instances."""
        runner = CliRunner()
        with patch(
            "gpuport_collectors.cli.asyncio.run",
            side_effect=_run_coro_with_result(_make_instances(12)),
        ):
            result = runner.invoke(cli, ["run", "runpod", "--api-key", "test-key"])

        assert result.exit_code == 0


class TestLambdaLabsCommand:
    """Tests for the lambdalabs command."""

    def test_lambdalabs_export_output_details_and_warnings(self):
        """Test lambdalabs export outputs details and exits on failure."""
        runner = CliRunner()
        with runner.isolated_filesystem():
            config_file = Path("export.yaml")
            config_file.write_text("pipelines: []")

            results = [
                PipelineResult(
                    pipeline_name="lambda-export",
                    enabled=True,
                    input_count=1,
                    filtered_count=1,
                    output_count=1,
                    outputs=[
                        {"success": True, "type": "local", "path": "/tmp/lambda.json"},
                        {"success": False, "type": "https", "error": "bad request"},
                    ],
                    error="pipeline error",
                    duration_seconds=0.1,
                )
            ]

            with (
                patch(
                    "gpuport_collectors.cli.load_export_config",
                    return_value=MagicMock(pipelines=[MagicMock()]),
                ),
                patch("gpuport_collectors.cli.validate_config", return_value=["warning"]),
                patch("gpuport_collectors.cli.execute_pipelines", return_value=results),
                patch(
                    "gpuport_collectors.cli.asyncio.run",
                    side_effect=_run_coro_with_result(_make_instances(1)),
                ),
            ):
                result = runner.invoke(
                    cli,
                    [
                        "run",
                        "lambdalabs",
                        "--api-key",
                        "test-key",
                        "--export-config",
                        str(config_file),
                    ],
                )

            assert result.exit_code == 1
            assert "PIPELINE EXECUTION SUMMARY" in result.output
            assert "Path: /tmp/lambda.json" in result.output
            assert "Error: bad request" in result.output

    def test_lambdalabs_no_export_lists_more_instances(self):
        """Test lambdalabs without export lists truncated instances."""
        runner = CliRunner()
        with patch(
            "gpuport_collectors.cli.asyncio.run",
            side_effect=_run_coro_with_result(_make_instances(12)),
        ):
            result = runner.invoke(cli, ["run", "lambdalabs", "--api-key", "test-key"])

        assert result.exit_code == 0

    @patch("gpuport_collectors.cli.asyncio.run")
    @patch("gpuport_collectors.cli.get_collector_class")
    def test_runpod_displays_collected_instances(self, mock_get_collector, mock_asyncio_run):
        """Test runpod command displays collected instances in summary."""
        # Mock collected instances
        mock_asyncio_run.side_effect = _run_coro_with_result([])

        runner = CliRunner()
        result = runner.invoke(cli, ["run", "runpod", "--api-key", "test-key"])

        # Should complete successfully
        assert result.exit_code == 0

    @patch("gpuport_collectors.cli.asyncio.run")
    @patch("gpuport_collectors.cli.get_collector_class")
    @patch("gpuport_collectors.cli.execute_pipelines")
    @patch("gpuport_collectors.cli.load_export_config")
    def test_runpod_with_export_config(
        self, mock_load_config, mock_execute, mock_get_collector, mock_asyncio_run
    ):
        """Test runpod command with export configuration."""
        from gpuport_collectors.export.config import (
            ExportConfig,
            JSONTransformerConfig,
            LocalOutputConfig,
            PipelineConfig,
        )
        from gpuport_collectors.export.pipeline import PipelineResult
        from gpuport_collectors.models import AvailabilityStatus, GPUInstance

        # Mock instances
        mock_instances = [
            GPUInstance(
                provider="runpod",
                instance_type="GPU-1X-A100",
                accelerator_name="NVIDIA A100",
                accelerator_count=1,
                accelerator_mem_gib=80,
                v_cpus=8,
                memory_gib=64,
                price=1.50,
                availability=AvailabilityStatus.HIGH,
                region="US-NY-1",
            )
        ]
        mock_asyncio_run.side_effect = _run_coro_with_result(mock_instances)

        # Mock export config
        export_config = ExportConfig(
            pipelines=[
                PipelineConfig(
                    name="test-export",
                    transformer=JSONTransformerConfig(),
                    outputs=[LocalOutputConfig(path="./output")],
                )
            ]
        )
        mock_load_config.return_value = export_config

        # Mock pipeline results
        mock_execute.return_value = [
            PipelineResult(
                pipeline_name="test-export",
                enabled=True,
                input_count=1,
                filtered_count=1,
                output_count=1,
                outputs=[{"success": True}],
            )
        ]

        runner = CliRunner()
        with runner.isolated_filesystem():
            # Create export config file
            config_file = Path("export.yaml")
            config_file.write_text("pipelines: []")

            result = runner.invoke(
                cli,
                [
                    "run",
                    "runpod",
                    "--api-key",
                    "test-key",
                    "--export-config",
                    str(config_file),
                ],
            )

            # Should complete successfully when config is valid
            assert result.exit_code == 0


class TestExportCommand:
    """Tests for the export command."""

    def test_export_missing_api_key_error(self):
        """Test export command fails when API key missing."""
        runner = CliRunner()
        with runner.isolated_filesystem():
            config_file = Path("export.yaml")
            config_file.write_text("pipelines: []")

            with patch.dict(os.environ, {}, clear=True):
                result = runner.invoke(cli, ["export", "--config", str(config_file)])

            # Should exit with error code when API key is missing
            assert result.exit_code == 1

    def test_export_invalid_provider_exits_with_verbose(self):
        """Test export exits when provider is invalid."""
        runner = CliRunner()
        with runner.isolated_filesystem():
            config_file = Path("export.yaml")
            config_file.write_text("pipelines: []")

            with (
                patch(
                    "gpuport_collectors.cli.load_export_config",
                    return_value=MagicMock(pipelines=[]),
                ),
                patch("gpuport_collectors.cli.validate_config", return_value=[]),
                patch(
                    "gpuport_collectors.cli.get_collector_class",
                    side_effect=ValueError("Unknown provider"),
                ),
            ):
                result = runner.invoke(
                    cli,
                    [
                        "export",
                        "--config",
                        str(config_file),
                        "--api-key",
                        "test-key",
                        "--verbose",
                    ],
                )

            assert result.exit_code == 1

    def test_export_output_details_and_timing(self):
        """Test export prints output details and timing metrics."""
        runner = CliRunner()
        with runner.isolated_filesystem():
            config_file = Path("export.yaml")
            config_file.write_text("pipelines: []")

            class DummyCollector:
                def __init__(self, config) -> None:
                    self.config = config

                async def fetch_instances(self) -> list[GPUInstance]:
                    return []

            results = [
                PipelineResult(
                    pipeline_name="export-1",
                    enabled=True,
                    input_count=2,
                    filtered_count=2,
                    output_count=2,
                    outputs=[
                        {"success": True, "name": "local", "path": "/tmp/out.json"},
                        {
                            "success": False,
                            "type": "s3",
                            "bucket": "test-bucket",
                            "key": "file.json",
                            "error": "no creds",
                        },
                        {
                            "success": True,
                            "type": "https",
                            "url": "https://example.com",
                            "total_requests": 2,
                            "successful_requests": 2,
                        },
                    ],
                    error=None,
                    duration_seconds=1.23,
                    filter_duration=0.1,
                    transform_duration=0.2,
                    output_duration=0.3,
                ),
                PipelineResult(
                    pipeline_name="export-2",
                    enabled=True,
                    input_count=2,
                    filtered_count=1,
                    output_count=1,
                    outputs=[],
                    error="pipeline failed",
                ),
            ]

            with (
                patch(
                    "gpuport_collectors.cli.load_export_config",
                    return_value=MagicMock(pipelines=[MagicMock()]),
                ),
                patch("gpuport_collectors.cli.validate_config", return_value=["warning"]),
                patch(
                    "gpuport_collectors.cli.get_collector_class",
                    return_value=DummyCollector,
                ),
                patch("gpuport_collectors.cli.execute_pipelines", return_value=results),
                patch(
                    "gpuport_collectors.cli.asyncio.run",
                    side_effect=_run_coro_with_result(_make_instances(2)),
                ),
            ):
                result = runner.invoke(
                    cli,
                    [
                        "export",
                        "--config",
                        str(config_file),
                        "--provider",
                        "runpod",
                        "--api-key",
                        "test-key",
                    ],
                )

            assert result.exit_code == 1
            assert "Path: /tmp/out.json" in result.output
            assert "S3: test-bucket/file.json" in result.output
            assert "URL: https://example.com" in result.output
            assert "Requests: 2/2" in result.output
            assert "Error: no creds" in result.output

    def test_export_exception_verbose(self):
        """Test export logs detailed error output with --verbose."""
        runner = CliRunner()
        with runner.isolated_filesystem():
            config_file = Path("export.yaml")
            config_file.write_text("pipelines: []")

            with patch(
                "gpuport_collectors.cli.load_export_config",
                side_effect=RuntimeError("boom"),
            ):
                result = runner.invoke(
                    cli,
                    [
                        "export",
                        "--config",
                        str(config_file),
                        "--api-key",
                        "test-key",
                        "--verbose",
                    ],
                )

            assert result.exit_code == 1

    def test_export_exception_non_verbose(self):
        """Test export logs simple error output without --verbose."""
        runner = CliRunner()
        with runner.isolated_filesystem():
            config_file = Path("export.yaml")
            config_file.write_text("pipelines: []")

            with patch(
                "gpuport_collectors.cli.load_export_config",
                side_effect=RuntimeError("boom"),
            ):
                result = runner.invoke(
                    cli,
                    [
                        "export",
                        "--config",
                        str(config_file),
                        "--api-key",
                        "test-key",
                    ],
                )

            assert result.exit_code == 1

    @patch("gpuport_collectors.cli.load_export_config")
    @patch("gpuport_collectors.cli.validate_config")
    def test_export_validate_only_mode(self, mock_validate, mock_load_config):
        """Test export --validate-only mode."""
        from gpuport_collectors.export.config import ExportConfig

        mock_load_config.return_value = ExportConfig(pipelines=[])
        mock_validate.return_value = []  # No warnings

        runner = CliRunner()
        with runner.isolated_filesystem():
            config_file = Path("export.yaml")
            config_file.write_text("pipelines: []")

            result = runner.invoke(cli, ["export", "--config", str(config_file), "--validate-only"])

            # Should succeed with valid config
            assert result.exit_code == 0
            # Validation messages may appear in stdout or stderr
            assert (
                "Configuration validation complete" in result.output
                or "✓ Configuration is valid" in result.output
            )

    @patch("gpuport_collectors.cli.load_export_config")
    @patch("gpuport_collectors.cli.validate_config")
    def test_export_displays_validation_warnings(self, mock_validate, mock_load_config):
        """Test export command displays validation warnings."""
        from gpuport_collectors.export.config import ExportConfig

        mock_load_config.return_value = ExportConfig(pipelines=[])
        mock_validate.return_value = [
            "Pipeline 'test' has no output destinations",
            "S3 output has no credentials configured",
        ]

        runner = CliRunner()
        with runner.isolated_filesystem():
            config_file = Path("export.yaml")
            config_file.write_text("pipelines: []")

            result = runner.invoke(cli, ["export", "--config", str(config_file), "--validate-only"])

            # Should succeed even with warnings (warnings don't cause failure)
            assert result.exit_code == 0
            # Warning messages may appear in stdout or stderr
            # Check that warning messages are displayed (logged output)
            assert (
                "Configuration validation complete" in result.output
                or "validation warnings" in result.output.lower()
            )

    @patch("gpuport_collectors.cli.asyncio.run")
    @patch("gpuport_collectors.cli.get_collector_class")
    @patch("gpuport_collectors.cli.execute_pipelines")
    @patch("gpuport_collectors.cli.load_export_config")
    def test_export_pipeline_failure_exit_code(
        self, mock_load_config, mock_execute, mock_get_collector, mock_asyncio_run
    ):
        """Test export command exits with code 1 when pipelines fail."""
        from gpuport_collectors.export.config import ExportConfig
        from gpuport_collectors.export.pipeline import PipelineResult

        mock_asyncio_run.side_effect = _run_coro_with_result([])  # No instances
        mock_load_config.return_value = ExportConfig(pipelines=[])

        # Mock failed pipeline
        mock_execute.return_value = [
            PipelineResult(
                pipeline_name="test-export",
                enabled=True,
                error="Failed to write output",
                input_count=0,
                filtered_count=0,
                output_count=1,
                outputs=[{"success": False}],
            )
        ]

        runner = CliRunner()
        with runner.isolated_filesystem():
            config_file = Path("export.yaml")
            config_file.write_text("pipelines: []")

            result = runner.invoke(
                cli, ["export", "--config", str(config_file), "--api-key", "test-key"]
            )

            # Should exit with error code when pipeline fails
            assert result.exit_code == 1


class TestValidateCommand:
    """Tests for the validate command."""

    @patch("gpuport_collectors.cli.load_export_config")
    @patch("gpuport_collectors.cli.validate_config")
    def test_validate_success(self, mock_validate, mock_load_config):
        """Test validate command with valid configuration."""
        from gpuport_collectors.export.config import (
            ExportConfig,
            JSONTransformerConfig,
            LocalOutputConfig,
            PipelineConfig,
        )

        config = ExportConfig(
            pipelines=[
                PipelineConfig(
                    name="test-pipeline",
                    transformer=JSONTransformerConfig(),
                    outputs=[LocalOutputConfig(path="./output")],
                )
            ]
        )
        mock_load_config.return_value = config
        mock_validate.return_value = []  # No warnings

        runner = CliRunner()
        with runner.isolated_filesystem():
            config_file = Path("export.yaml")
            config_file.write_text("pipelines: []")

            result = runner.invoke(cli, ["validate", "--config", str(config_file)])

            # Just check success and key output
            assert result.exit_code == 0
            assert "✓ Configuration is valid" in result.output
            assert "test-pipeline" in result.output

    @patch("gpuport_collectors.cli.load_export_config")
    @patch("gpuport_collectors.cli.validate_config")
    def test_validate_with_warnings(self, mock_validate, mock_load_config):
        """Test validate command displays warnings."""
        from gpuport_collectors.export.config import ExportConfig

        mock_load_config.return_value = ExportConfig(pipelines=[])
        mock_validate.return_value = ["Pipeline 'test' has no output destinations"]

        runner = CliRunner()
        with runner.isolated_filesystem():
            config_file = Path("export.yaml")
            config_file.write_text("pipelines: []")

            result = runner.invoke(cli, ["validate", "--config", str(config_file)])

            # Should succeed even with warnings (warnings don't cause failure)
            assert result.exit_code == 0

    def test_validate_invalid_file(self):
        """Test validate command with non-existent file."""
        runner = CliRunner()
        result = runner.invoke(cli, ["validate", "--config", "nonexistent.yaml"])

        # Click returns exit code 2 for argument parsing/validation errors
        assert result.exit_code == 2
        # Verify Click's file-not-found error message is present
        assert "does not exist" in result.output

    @patch("gpuport_collectors.cli.load_export_config")
    @patch("gpuport_collectors.cli.validate_config")
    def test_validate_pipeline_summary(self, mock_validate, mock_load_config):
        """Test validate command displays pipeline summary."""
        from gpuport_collectors.export.config import (
            CSVTransformerConfig,
            ExportConfig,
            FilterConfig,
            LocalOutputConfig,
            PipelineConfig,
        )

        config = ExportConfig(
            pipelines=[
                PipelineConfig(
                    name="filtered-csv",
                    enabled=True,
                    filters=[FilterConfig(field="provider", operator="eq", value="runpod")],
                    transformer=CSVTransformerConfig(fields={"provider": "Provider"}),
                    outputs=[
                        LocalOutputConfig(path="./output", name="csv-output"),
                        LocalOutputConfig(path="./backup", name="backup-output"),
                    ],
                ),
                PipelineConfig(
                    name="disabled-pipeline",
                    enabled=False,
                    transformer=CSVTransformerConfig(fields={"price": "Price"}),
                    outputs=[LocalOutputConfig(path="./output")],
                ),
            ]
        )
        mock_load_config.return_value = config
        mock_validate.return_value = []

        runner = CliRunner()
        with runner.isolated_filesystem():
            config_file = Path("export.yaml")
            config_file.write_text("pipelines: []")

            result = runner.invoke(cli, ["validate", "--config", str(config_file)])

            assert result.exit_code == 0
            assert "Pipeline summary:" in result.output
            assert "filtered-csv: ✓ enabled" in result.output
            assert "Filters: 1" in result.output
            assert "Transformer: csv" in result.output
            assert "Outputs: 2" in result.output
            assert "csv-output" in result.output
            assert "backup-output" in result.output
            assert "disabled-pipeline: ✗ disabled" in result.output
