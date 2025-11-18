"""Tests for pipeline executor."""

import json
from pathlib import Path

import pytest

from gpuport_collectors.export.config import (
    CSVTransformerConfig,
    ExportConfig,
    FilterConfig,
    JSONTransformerConfig,
    LocalOutputConfig,
    MetricConfig,
    MetricsTransformerConfig,
    PipelineConfig,
)
from gpuport_collectors.export.pipeline import execute_pipeline, execute_pipelines
from gpuport_collectors.models import AvailabilityStatus, GPUInstance


@pytest.fixture
def sample_instances() -> list[GPUInstance]:
    """Create sample GPU instances for testing."""
    return [
        GPUInstance(
            provider="Provider1",
            instance_type="type1",
            accelerator_name="GPU1",
            accelerator_count=2,
            region="us-east-1",
            availability=AvailabilityStatus.HIGH,
            price=2.5,
        ),
        GPUInstance(
            provider="Provider1",
            instance_type="type2",
            accelerator_name="GPU2",
            accelerator_count=4,
            region="us-west-1",
            availability=AvailabilityStatus.MEDIUM,
            price=5.0,
        ),
        GPUInstance(
            provider="Provider2",
            instance_type="type3",
            accelerator_name="GPU1",
            accelerator_count=1,
            region="us-east-1",
            availability=AvailabilityStatus.LOW,
            price=1.0,
        ),
    ]


class TestPipelineResult:
    """Tests for PipelineResult class."""

    def test_success_property(self) -> None:
        """Test success property."""
        from gpuport_collectors.export.pipeline import PipelineResult

        # Successful result
        result = PipelineResult(
            pipeline_name="test",
            enabled=True,
            input_count=10,
            filtered_count=8,
            output_count=2,
            outputs=[],
        )
        assert result.success is True

        # Failed result
        result_with_error = PipelineResult(
            pipeline_name="test",
            enabled=True,
            input_count=10,
            filtered_count=0,
            output_count=0,
            outputs=[],
            error="Something went wrong",
        )
        assert result_with_error.success is False

    def test_to_dict(self) -> None:
        """Test to_dict method."""
        from gpuport_collectors.export.pipeline import PipelineResult

        result = PipelineResult(
            pipeline_name="test-pipeline",
            enabled=True,
            input_count=100,
            filtered_count=50,
            output_count=2,
            outputs=[{"type": "local", "success": True}],
        )

        data = result.to_dict()
        assert data["pipeline_name"] == "test-pipeline"
        assert data["success"] is True
        assert data["input_count"] == 100
        assert data["filtered_count"] == 50
        assert data["output_count"] == 2
        assert len(data["outputs"]) == 1


class TestExecutePipeline:
    """Tests for execute_pipeline function."""

    def test_disabled_pipeline(self, sample_instances: list[GPUInstance]) -> None:
        """Test that disabled pipeline returns without processing."""
        config = PipelineConfig(
            name="test-pipeline",
            enabled=False,
            transformer=JSONTransformerConfig(),
            outputs=[],
        )

        result = execute_pipeline(sample_instances, config)

        assert result.enabled is False
        assert result.input_count == 3
        assert result.filtered_count == 0
        assert result.output_count == 0
        assert result.success is True

    def test_json_transform_local_output(
        self, sample_instances: list[GPUInstance], tmp_path: Path
    ) -> None:
        """Test JSON transform with local output."""
        config = PipelineConfig(
            name="json-local",
            transformer=JSONTransformerConfig(),
            outputs=[
                LocalOutputConfig(path=str(tmp_path), filename_pattern="test.json", overwrite=True)
            ],
        )

        result = execute_pipeline(sample_instances, config)

        assert result.success is True
        assert result.input_count == 3
        assert result.filtered_count == 3  # No filters
        assert result.output_count == 1
        assert result.outputs[0]["success"] is True
        assert result.outputs[0]["type"] == "local"

        # Verify file was created
        output_file = tmp_path / "test.json"
        assert output_file.exists()
        data = json.loads(output_file.read_text())
        assert len(data) == 3

    def test_csv_transform(self, sample_instances: list[GPUInstance], tmp_path: Path) -> None:
        """Test CSV transform."""
        config = PipelineConfig(
            name="csv-local",
            transformer=CSVTransformerConfig(
                fields={
                    "provider": "provider",
                    "instance_type": "instance_type",
                    "price": "price",
                }
            ),
            outputs=[
                LocalOutputConfig(path=str(tmp_path), filename_pattern="test.csv", overwrite=True)
            ],
        )

        result = execute_pipeline(sample_instances, config)

        assert result.success is True
        output_file = tmp_path / "test.csv"
        assert output_file.exists()

        # Verify CSV content
        content = output_file.read_text()
        assert "provider,instance_type,price" in content
        assert "Provider1" in content

    def test_metrics_transform(self, sample_instances: list[GPUInstance], tmp_path: Path) -> None:
        """Test metrics transform."""
        config = PipelineConfig(
            name="metrics-local",
            transformer=MetricsTransformerConfig(
                metrics=[
                    MetricConfig(name="total", type="count"),
                    MetricConfig(name="avg_price", type="avg", field="price"),
                ]
            ),
            outputs=[
                LocalOutputConfig(
                    path=str(tmp_path), filename_pattern="metrics.json", overwrite=True
                )
            ],
        )

        result = execute_pipeline(sample_instances, config)

        assert result.success is True
        output_file = tmp_path / "metrics.json"
        assert output_file.exists()

        # Verify metrics content
        data = json.loads(output_file.read_text())
        assert data["metrics"]["total"] == 3
        assert "avg_price" in data["metrics"]

    def test_with_filters(self, sample_instances: list[GPUInstance], tmp_path: Path) -> None:
        """Test pipeline with filters."""
        config = PipelineConfig(
            name="filtered-pipeline",
            filters=[
                FilterConfig(field="provider", operator="eq", value="Provider1"),
            ],
            transformer=JSONTransformerConfig(),
            outputs=[
                LocalOutputConfig(
                    path=str(tmp_path), filename_pattern="filtered.json", overwrite=True
                )
            ],
        )

        result = execute_pipeline(sample_instances, config)

        assert result.success is True
        assert result.input_count == 3
        assert result.filtered_count == 2  # Only Provider1 instances
        assert result.output_count == 1

        # Verify filtered data
        output_file = tmp_path / "filtered.json"
        data = json.loads(output_file.read_text())
        assert len(data) == 2
        assert all(item["provider"] == "Provider1" for item in data)

    def test_multiple_outputs(self, sample_instances: list[GPUInstance], tmp_path: Path) -> None:
        """Test pipeline with multiple outputs."""
        config = PipelineConfig(
            name="multi-output",
            transformer=JSONTransformerConfig(),
            outputs=[
                LocalOutputConfig(
                    path=str(tmp_path), filename_pattern="output1.json", overwrite=True
                ),
                LocalOutputConfig(
                    path=str(tmp_path), filename_pattern="output2.json", overwrite=True
                ),
            ],
        )

        result = execute_pipeline(sample_instances, config)

        assert result.success is True
        assert result.output_count == 2
        assert all(output["success"] for output in result.outputs)

        # Verify both files were created
        assert (tmp_path / "output1.json").exists()
        assert (tmp_path / "output2.json").exists()

    def test_partial_output_failure(
        self, sample_instances: list[GPUInstance], tmp_path: Path
    ) -> None:
        """Test that pipeline continues when individual output fails."""
        config = PipelineConfig(
            name="partial-failure",
            transformer=JSONTransformerConfig(),
            outputs=[
                LocalOutputConfig(path=str(tmp_path), filename_pattern="good.json", overwrite=True),
                LocalOutputConfig(
                    path="/invalid/path/that/does/not/exist",
                    filename_pattern="bad.json",
                    create_dirs=False,
                ),
            ],
        )

        result = execute_pipeline(sample_instances, config)

        # Pipeline succeeds overall
        assert result.success is True
        assert result.output_count == 2

        # First output succeeds
        assert result.outputs[0]["success"] is True
        assert (tmp_path / "good.json").exists()

        # Second output fails
        assert result.outputs[1]["success"] is False
        assert "error" in result.outputs[1]

    def test_transform_error(self, sample_instances: list[GPUInstance], tmp_path: Path) -> None:
        """Test handling of transformation errors."""
        # Create config with invalid output path to trigger error
        config = PipelineConfig(
            name="invalid-output",
            transformer=JSONTransformerConfig(),
            outputs=[
                LocalOutputConfig(
                    path="/invalid/nonexistent/path",
                    filename_pattern="test.json",
                    create_dirs=False,  # Will fail because path doesn't exist
                )
            ],
        )

        result = execute_pipeline(sample_instances, config)

        # Pipeline fails because all outputs failed
        assert result.success is False
        assert result.output_count == 1
        assert result.outputs[0]["success"] is False
        assert result.failed_outputs == 1
        assert result.successful_outputs == 0


class TestExecutePipelines:
    """Tests for execute_pipelines function."""

    def test_multiple_pipelines(self, sample_instances: list[GPUInstance], tmp_path: Path) -> None:
        """Test executing multiple pipelines."""
        config = ExportConfig(
            pipelines=[
                PipelineConfig(
                    name="pipeline1",
                    transformer=JSONTransformerConfig(),
                    outputs=[
                        LocalOutputConfig(
                            path=str(tmp_path), filename_pattern="p1.json", overwrite=True
                        )
                    ],
                ),
                PipelineConfig(
                    name="pipeline2",
                    transformer=CSVTransformerConfig(
                        fields={"provider": "provider", "price": "price"}
                    ),
                    outputs=[
                        LocalOutputConfig(
                            path=str(tmp_path), filename_pattern="p2.csv", overwrite=True
                        )
                    ],
                ),
            ]
        )

        results = execute_pipelines(sample_instances, config)

        assert len(results) == 2
        assert all(result.success for result in results)
        assert results[0].pipeline_name == "pipeline1"
        assert results[1].pipeline_name == "pipeline2"

        # Verify both outputs were created
        assert (tmp_path / "p1.json").exists()
        assert (tmp_path / "p2.csv").exists()

    def test_disabled_and_enabled_pipelines(
        self, sample_instances: list[GPUInstance], tmp_path: Path
    ) -> None:
        """Test mix of enabled and disabled pipelines."""
        config = ExportConfig(
            pipelines=[
                PipelineConfig(
                    name="enabled",
                    enabled=True,
                    transformer=JSONTransformerConfig(),
                    outputs=[
                        LocalOutputConfig(
                            path=str(tmp_path), filename_pattern="enabled.json", overwrite=True
                        )
                    ],
                ),
                PipelineConfig(
                    name="disabled",
                    enabled=False,
                    transformer=JSONTransformerConfig(),
                    outputs=[
                        LocalOutputConfig(
                            path=str(tmp_path), filename_pattern="disabled.json", overwrite=True
                        )
                    ],
                ),
            ]
        )

        results = execute_pipelines(sample_instances, config)

        assert len(results) == 2
        assert results[0].enabled is True
        assert results[0].success is True
        assert results[1].enabled is False

        # Only enabled pipeline should create output
        assert (tmp_path / "enabled.json").exists()
        assert not (tmp_path / "disabled.json").exists()

    def test_empty_pipeline_list(self, sample_instances: list[GPUInstance]) -> None:
        """Test with no pipelines configured."""
        config = ExportConfig(pipelines=[])

        results = execute_pipelines(sample_instances, config)

        assert len(results) == 0

    def test_one_pipeline_fails_others_continue(
        self, sample_instances: list[GPUInstance], tmp_path: Path
    ) -> None:
        """Test that pipeline failures don't stop other pipelines."""
        config = ExportConfig(
            pipelines=[
                PipelineConfig(
                    name="good1",
                    transformer=JSONTransformerConfig(),
                    outputs=[
                        LocalOutputConfig(
                            path=str(tmp_path), filename_pattern="good1.json", overwrite=True
                        )
                    ],
                ),
                PipelineConfig(
                    name="bad",
                    transformer=JSONTransformerConfig(),
                    outputs=[
                        LocalOutputConfig(
                            path="/invalid/path",
                            filename_pattern="bad.json",
                            create_dirs=False,  # Will fail
                        )
                    ],
                ),
                PipelineConfig(
                    name="good2",
                    transformer=JSONTransformerConfig(),
                    outputs=[
                        LocalOutputConfig(
                            path=str(tmp_path), filename_pattern="good2.json", overwrite=True
                        )
                    ],
                ),
            ]
        )

        results = execute_pipelines(sample_instances, config)

        assert len(results) == 3
        assert results[0].success is True
        # Middle pipeline fails because all outputs failed
        assert results[1].success is False
        assert results[1].outputs[0]["success"] is False
        assert results[1].failed_outputs == 1
        assert results[1].successful_outputs == 0
        assert results[2].success is True

        # Good pipelines should create outputs
        assert (tmp_path / "good1.json").exists()
        assert (tmp_path / "good2.json").exists()
