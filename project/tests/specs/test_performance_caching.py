from __future__ import annotations

from pathlib import Path
from unittest.mock import patch

import project.specs.manifest as manifest
import project.specs.ontology as ontology
import project.specs.utils as utils


def test_get_spec_hashes_is_cached():
    utils._get_spec_hashes_cached.cache_clear()
    with patch("project.specs.utils.iter_spec_yaml_files") as mock_iter:
        mock_iter.return_value = []
        project_root = Path("/tmp/fake_repo")

        # First call
        utils.get_spec_hashes(project_root)
        # Second call
        utils.get_spec_hashes(project_root)

        assert mock_iter.call_count == 1

def test_ontology_component_hashes_is_cached():
    ontology._ontology_component_hashes_cached.cache_clear()
    with patch("project.specs.ontology.ontology_spec_paths") as mock_paths:
        mock_paths.return_value = {}
        project_root = Path("/tmp/fake_repo")

        # First call
        ontology.ontology_component_hashes(project_root)
        # Second call
        ontology.ontology_component_hashes(project_root)

        assert mock_paths.call_count == 1

def test_ontology_spec_hash_is_cached():
    ontology._ontology_spec_hash_cached.cache_clear()
    with patch("project.specs.ontology.ontology_spec_paths") as mock_paths:
        mock_paths.return_value = {}
        project_root = Path("/tmp/fake_repo")

        # First call
        ontology.ontology_spec_hash(project_root)
        # Second call
        ontology.ontology_spec_hash(project_root)

        assert mock_paths.call_count == 1

def test_normalization_avoids_cache_miss_on_equivalent_paths():
    utils._get_spec_hashes_cached.cache_clear()
    with patch("project.specs.utils.iter_spec_yaml_files") as mock_iter:
        mock_iter.return_value = []

        # Two different Path objects that resolve to the same thing
        path1 = Path("./project").absolute()
        path2 = Path(str(path1) + "/../project").resolve()

        assert path1 == path2
        assert path1 is not path2

        utils.get_spec_hashes(path1)
        utils.get_spec_hashes(path2)

        # If normalization works, the second call should be a cache hit
        assert mock_iter.call_count == 1

def test_git_commit_is_cached():
    manifest._git_commit_cached.cache_clear()
    with patch("project.specs.manifest.subprocess.check_output") as mock_run:
        mock_run.return_value = "fake_commit\n"

        # Call via start_manifest twice
        manifest.start_manifest("stage", "run", {}, [], [])
        manifest.start_manifest("stage", "run", {}, [], [])

        # Filter calls to only those starting with 'git' to avoid noise from platform.platform()
        git_calls = [call for call in mock_run.call_args_list if call.args[0][0] == "git"]
        assert len(git_calls) == 1
