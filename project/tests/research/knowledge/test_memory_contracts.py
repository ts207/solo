"""
Test memory contracts - verifies read/write and schema.

Tests the contract that:
- Write a finding to memory, read it back, verify schema
- Verify column presence, types, and content integrity
"""

import pandas as pd
import pytest
import tempfile
import shutil
from pathlib import Path
from datetime import datetime

from project.research.knowledge.memory import (
    write_reflection,
    read_reflections,
    MemoryPaths,
)


class TestMemoryContracts:
    @pytest.fixture
    def temp_memory_dir(self):
        tmp = tempfile.mkdtemp()
        yield Path(tmp)
        shutil.rmtree(tmp)

    def test_write_and_read_reflection(self, temp_memory_dir):
        """Write a reflection, read it back, verify schema."""
        memory = MemoryPaths(
            root=temp_memory_dir,
            tested_regions=temp_memory_dir / "tested_regions.parquet",
            region_statistics=temp_memory_dir / "region_statistics.parquet",
            event_statistics=temp_memory_dir / "event_statistics.parquet",
            template_statistics=temp_memory_dir / "template_statistics.parquet",
            context_statistics=temp_memory_dir / "context_statistics.parquet",
            failures=temp_memory_dir / "failures.parquet",
            proposals=temp_memory_dir / "proposals.parquet",
            reflections=temp_memory_dir / "reflections.parquet",
            belief_state=temp_memory_dir / "belief_state.json",
            next_actions=temp_memory_dir / "next_actions.json",
            proposals_dir=temp_memory_dir / "proposals",
        )
        memory.proposals_dir.mkdir(parents=True, exist_ok=True)

        reflection = pd.DataFrame([{
            "program_id": "test_campaign",
            "reflection_id": "ref_001",
            "timestamp": datetime.now().isoformat(),
            "observation": "LIQUIDATION_CASCADE showed negative edge in THIN liquidity",
            "insight_type": "negative",
            "confidence": 0.85,
            "action": "avoid",
            "status": "pending",
        }])

        write_reflection(memory, reflection)
        read_back = read_reflections(memory, "test_campaign")

        assert len(read_back) >= 1
        row = read_back.iloc[0]
        assert row["observation"] == "LIQUIDATION_CASCADE showed negative edge in THIN liquidity"
        assert row["confidence"] == 0.85

    def test_reflection_schema_columns(self, temp_memory_dir):
        """Verify reflection has required columns."""
        memory = MemoryPaths(
            root=temp_memory_dir,
            tested_regions=temp_memory_dir / "tested_regions.parquet",
            region_statistics=temp_memory_dir / "region_statistics.parquet",
            event_statistics=temp_memory_dir / "event_statistics.parquet",
            template_statistics=temp_memory_dir / "template_statistics.parquet",
            context_statistics=temp_memory_dir / "context_statistics.parquet",
            failures=temp_memory_dir / "failures.parquet",
            proposals=temp_memory_dir / "proposals.parquet",
            reflections=temp_memory_dir / "reflections.parquet",
            belief_state=temp_memory_dir / "belief_state.json",
            next_actions=temp_memory_dir / "next_actions.json",
            proposals_dir=temp_memory_dir / "proposals",
        )
        memory.proposals_dir.mkdir(parents=True, exist_ok=True)

        reflection = pd.DataFrame([{
            "program_id": "test",
            "reflection_id": "r1",
            "timestamp": datetime.now().isoformat(),
            "observation": "test",
            "insight_type": "negative",
            "confidence": 0.5,
            "action": "avoid",
            "status": "pending",
        }])
        write_reflection(memory, reflection)
        read_back = read_reflections(memory, "test")

        required_cols = {"program_id", "reflection_id", "observation", "insight_type", "confidence", "action", "status"}
        assert required_cols.issubset(set(read_back.columns)), f"Missing columns: {required_cols - set(read_back.columns)}"

    def test_write_and_read_reflection_by_program_id(self, temp_memory_dir):
        reflection = pd.DataFrame([{
            "reflection_id": "r2",
            "timestamp": datetime.now().isoformat(),
            "observation": "program-scoped reflection",
            "insight_type": "positive",
            "confidence": 0.9,
            "action": "exploit",
            "status": "complete",
        }])

        write_reflection("campaign_alpha", reflection, data_root=temp_memory_dir)
        read_back = read_reflections("campaign_alpha", data_root=temp_memory_dir)

        assert len(read_back) == 1
        row = read_back.iloc[0]
        assert row["program_id"] == "campaign_alpha"
        assert row["reflection_id"] == "r2"
        assert row["observation"] == "program-scoped reflection"


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
