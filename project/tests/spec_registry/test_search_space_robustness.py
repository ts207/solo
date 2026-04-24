
import logging

from project.spec_registry.search_space import (
    load_event_priority_weights,
)


def test_fragile_comment_parsing(tmp_path):
    # Case 1: Extra space before comment
    # Case 2: Lowercase quality
    # Case 3: Structured event key (currently ignored)
    # Case 4: Comment separated by multiple spaces

    content = """
- VALID_EVENT  # [QUALITY: HIGH]
- SPACE_EVENT   # [QUALITY: HIGH]
- LOWER_EVENT  # [quality: high]
- BRACE_EVENT: { param: 1 } # [QUALITY: HIGH]
- NO_ANNOTATION # just a comment
    """

    p = tmp_path / "search_space.yaml"
    p.write_text(content, encoding="utf-8")

    weights = load_event_priority_weights(p)

    # VALID_EVENT should be 3.0
    assert weights.get("VALID_EVENT") == 3.0

    # SPACE_EVENT should be 3.0 (currently works? regex is flexible)
    assert weights.get("SPACE_EVENT") == 3.0

    # LOWER_EVENT: Current regex is case-insensitive for the word QUALITY?
    # Let's check the code: re.IGNORECASE is used.
    assert weights.get("LOWER_EVENT") == 3.0

    # BRACE_EVENT: Should now be supported!
    assert weights.get("BRACE_EVENT") == 3.0

    # NO_ANNOTATION: Should be missing (default weight applied by caller)
    assert "NO_ANNOTATION" not in weights

def test_missing_annotation_warning(caplog, tmp_path):
    # We want to ensure we get a warning for malformed lines
    content = """
- MALFORMED  # [QUALITY: HIGH - typo]
    """
    p = tmp_path / "search_space.yaml"
    p.write_text(content, encoding="utf-8")

    with caplog.at_level(logging.WARNING):
        load_event_priority_weights(p)

    # Verify the warning
    assert "Malformed QUALITY annotation for event 'MALFORMED'" in caplog.text
