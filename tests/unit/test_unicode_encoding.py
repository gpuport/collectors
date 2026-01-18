"""Tests for Unicode and encoding edge cases."""

import json
from pathlib import Path

import pytest

from gpuport_collectors.export.config import (
    CSVTransformerConfig,
    JSONTransformerConfig,
    LocalOutputConfig,
)
from gpuport_collectors.export.outputs import write_to_local
from gpuport_collectors.export.transformers import transform_to_csv, transform_to_json
from gpuport_collectors.models import AvailabilityStatus, GPUInstance


@pytest.fixture
def unicode_instances() -> list[GPUInstance]:
    """Create GPU instances with Unicode characters in various fields."""
    return [
        GPUInstance(
            provider="阿里云",  # Alibaba Cloud in Chinese
            instance_type="ecs.gn6i-c4g1.xlarge",
            accelerator_name="NVIDIA T4 Tensor Core",
            accelerator_count=1,
            accelerator_mem_gib=16.0,
            region="华北2(北京)",  # North China 2 (Beijing)
            availability=AvailabilityStatus.HIGH,
            quantity=10,
            price=1.5,
            v_cpus=4,
            memory_gib=15.0,
        ),
        GPUInstance(
            provider="Яндекс.Облако",  # Yandex Cloud in Russian
            instance_type="gpu-standard-v3",
            accelerator_name="NVIDIA® Tesla® V100",  # With registered trademark symbols
            accelerator_count=2,
            accelerator_mem_gib=32.0,
            region="ru-central1-a",
            availability=AvailabilityStatus.MEDIUM,
            quantity=5,
            price=2.8,
            v_cpus=8,
            memory_gib=96.0,
        ),
        GPUInstance(
            provider="AWS",
            instance_type="p3.2xlarge",
            accelerator_name="Tesla V100 🚀",  # With emoji
            accelerator_count=1,
            accelerator_mem_gib=16.0,
            region="ap-northeast-1",  # Tokyo
            availability=AvailabilityStatus.HIGH,
            quantity=15,
            price=3.06,
            v_cpus=8,
            memory_gib=61.0,
        ),
        GPUInstance(
            provider="GCP",
            instance_type="n1-standard-4",
            accelerator_name="NVIDIA K80™",  # With trademark symbol
            accelerator_count=1,
            accelerator_mem_gib=12.0,
            region="europe-west1",
            availability=AvailabilityStatus.LOW,
            quantity=2,
            price=0.75,
            v_cpus=4,
            memory_gib=15.0,
        ),
    ]


class TestUnicodeInJSON:
    """Tests for Unicode handling in JSON transformation."""

    def test_json_preserves_unicode(self, unicode_instances: list[GPUInstance]) -> None:
        """Test that JSON transformation preserves Unicode characters."""
        config = JSONTransformerConfig()
        result = transform_to_json(unicode_instances, config)

        # Parse JSON
        data = json.loads(result)

        # Check Chinese characters
        assert data[0]["provider"] == "阿里云"
        assert data[0]["region"] == "华北2(北京)"

        # Check Russian characters
        assert data[1]["provider"] == "Яндекс.Облако"

        # Check emoji
        assert "🚀" in data[2]["accelerator_name"]

        # Check special symbols
        assert "®" in data[1]["accelerator_name"]
        assert "™" in data[3]["accelerator_name"]

    def test_json_pretty_print_with_unicode(self, unicode_instances: list[GPUInstance]) -> None:
        """Test that pretty-printed JSON handles Unicode correctly."""
        config = JSONTransformerConfig(pretty_print=True)
        result = transform_to_json(unicode_instances, config)

        # Should be valid JSON
        data = json.loads(result)

        # Should preserve Unicode
        assert data[0]["provider"] == "阿里云"

        # Should be formatted (has newlines and indentation)
        assert "\n" in result
        assert "  " in result  # Indentation


class TestUnicodeInCSV:
    """Tests for Unicode handling in CSV transformation."""

    def test_csv_handles_unicode(self, unicode_instances: list[GPUInstance]) -> None:
        """Test that CSV transformation handles Unicode characters."""
        config = CSVTransformerConfig(
            fields={
                "provider": "Provider",
                "region": "Region",
                "accelerator_name": "GPU",
            }
        )
        result = transform_to_csv(unicode_instances, config)

        # Check that Unicode appears in output
        assert "阿里云" in result
        assert "Яндекс.Облако" in result
        assert "🚀" in result

    def test_csv_with_unicode_in_headers(self, unicode_instances: list[GPUInstance]) -> None:
        """Test CSV with Unicode characters in header names."""
        config = CSVTransformerConfig(
            fields={
                "provider": "供应商",  # "Provider" in Chinese
                "region": "区域",  # "Region" in Chinese
                "price": "价格",  # "Price" in Chinese
            }
        )
        result = transform_to_csv(unicode_instances, config)

        lines = result.strip().split("\n")
        header = lines[0]

        # Headers should contain Unicode
        assert "供应商" in header
        assert "区域" in header
        assert "价格" in header


class TestUnicodeInFileOutput:
    """Tests for Unicode handling in file output."""

    def test_local_output_writes_unicode_json(
        self, unicode_instances: list[GPUInstance], tmp_path: Path
    ) -> None:
        """Test that local output correctly writes Unicode to JSON files."""
        config = LocalOutputConfig(path=str(tmp_path))

        # Transform to JSON
        json_config = JSONTransformerConfig()
        data = transform_to_json(unicode_instances, json_config)

        # Write file
        file_path = write_to_local(
            data,
            config,
            metadata={"filename": "unicode_test.json", "provider": "test", "format": "json"},
        )

        # Read file back and verify Unicode
        with Path(file_path).open(encoding="utf-8") as f:
            content = f.read()
            parsed = json.loads(content)

        # Verify Unicode preserved
        assert parsed[0]["provider"] == "阿里云"
        assert parsed[1]["provider"] == "Яндекс.Облако"
        assert "🚀" in parsed[2]["accelerator_name"]

    def test_local_output_writes_unicode_csv(
        self, unicode_instances: list[GPUInstance], tmp_path: Path
    ) -> None:
        """Test that local output correctly writes Unicode to CSV files."""
        config = LocalOutputConfig(path=str(tmp_path))

        # Transform to CSV
        csv_config = CSVTransformerConfig(fields={"provider": "Provider", "region": "Region"})
        data = transform_to_csv(unicode_instances, csv_config)

        # Write file
        file_path = write_to_local(
            data,
            config,
            metadata={"filename": "unicode_test.csv", "provider": "test", "format": "csv"},
        )

        # Read file back and verify Unicode
        with Path(file_path).open(encoding="utf-8") as f:
            content = f.read()

        # Verify Unicode preserved
        assert "阿里云" in content
        assert "Яндекс.Облако" in content
        assert "华北2(北京)" in content


class TestEdgeCaseCharacters:
    """Tests for edge case characters that might cause issues."""

    def test_instances_with_special_csv_characters(self) -> None:
        """Test handling of characters that are special in CSV (quotes, commas, newlines)."""
        instances = [
            GPUInstance(
                provider='AWS "Premium"',  # Quotes
                instance_type="p3,2xlarge",  # Comma
                accelerator_name="NVIDIA\nTesla",  # Newline
                accelerator_count=1,
                accelerator_mem_gib=16.0,
                region="us-east-1",
                availability=AvailabilityStatus.HIGH,
                quantity=10,
                price=3.06,
                v_cpus=8,
                memory_gib=61.0,
            )
        ]

        config = CSVTransformerConfig(
            fields={
                "provider": "Provider",
                "instance_type": "Type",
                "accelerator_name": "GPU",
            }
        )
        result = transform_to_csv(instances, config)

        # Should handle special characters properly (CSV escaping per RFC 4180)
        # Quotes are escaped by doubling them: "AWS ""Premium"""
        assert 'AWS ""Premium""' in result
        # Commas trigger quoting: "p3,2xlarge"
        assert '"p3,2xlarge"' in result

    def test_instances_with_null_bytes(self) -> None:
        """Test handling of null bytes in strings."""
        instances = [
            GPUInstance(
                provider="AWS",
                instance_type="p3.2xlarge",
                accelerator_name="Tesla V100\x00",  # Null byte in name
                accelerator_count=1,
                accelerator_mem_gib=16.0,
                region="us-east-1",
                availability=AvailabilityStatus.HIGH,
                quantity=10,
                price=3.06,
                v_cpus=8,
                memory_gib=61.0,
            )
        ]

        # Should not crash (though null bytes may be stripped/replaced)
        config = JSONTransformerConfig()
        result = transform_to_json(instances, config)

        # Should produce valid JSON
        data = json.loads(result)
        assert len(data) == 1

    def test_instances_with_rtl_text(self) -> None:
        """Test handling of right-to-left text (Arabic, Hebrew)."""
        instances = [
            GPUInstance(
                provider="أمازون",  # Amazon in Arabic
                instance_type="p3.2xlarge",
                accelerator_name="تسلا V100",  # Tesla in Arabic
                accelerator_count=1,
                accelerator_mem_gib=16.0,
                region="me-south-1",  # Bahrain
                availability=AvailabilityStatus.HIGH,
                quantity=10,
                price=3.06,
                v_cpus=8,
                memory_gib=61.0,
            ),
            GPUInstance(
                provider="גוגל",  # Google in Hebrew
                instance_type="n1-standard-4",
                accelerator_name="K80",
                accelerator_count=1,
                accelerator_mem_gib=12.0,
                region="europe-west1",
                availability=AvailabilityStatus.MEDIUM,
                quantity=5,
                price=0.75,
                v_cpus=4,
                memory_gib=15.0,
            ),
        ]

        config = JSONTransformerConfig()
        result = transform_to_json(instances, config)

        data = json.loads(result)

        # Should preserve RTL text
        assert data[0]["provider"] == "أمازون"
        assert data[0]["accelerator_name"] == "تسلا V100"
        assert data[1]["provider"] == "גוגל"

    def test_instances_with_combining_characters(self) -> None:
        """Test handling of combining Unicode characters (diacritics, etc.)."""
        instances = [
            GPUInstance(
                provider="Café Computing",  # é with combining accent
                instance_type="gpu-café-1",
                accelerator_name="GPU™ Ñoño",  # Spanish ñ
                accelerator_count=1,
                accelerator_mem_gib=16.0,
                region="são-paulo",  # Portuguese ã
                availability=AvailabilityStatus.HIGH,
                quantity=10,
                price=2.5,
                v_cpus=8,
                memory_gib=32.0,
            )
        ]

        config = JSONTransformerConfig()
        result = transform_to_json(instances, config)

        data = json.loads(result)

        # Should preserve combining characters
        assert "Café" in data[0]["provider"]
        assert "Ñoño" in data[0]["accelerator_name"]
        assert "são-paulo" in data[0]["region"]
