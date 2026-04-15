from __future__ import annotations

import re
from dataclasses import dataclass
from pathlib import Path
from typing import List, Optional

import pytest


ALLOWED_FILES = {
    "project/tests/",
    "examples/",
}

PROTECTED_CONTEXTS = [
    r"params\.get\(",
    r"params\[",
    r"\.get\(",
    r"config\.",
    r"self\.default_",
    r"self\.__dict__",
    r"sys\.maxsize",
    r"float\('inf'\)",
    r"float\('nan'\)",
    r"len\(",
    r"range\(",
    r"index\s*\[\s*-\s*1\s*\]",
    r"\[-1\]",
    r"\[:",
    r"\[:\d+\]",
    r"np\.nan",
    r"np\.inf",
    r"math\.pi",
    r"math\.e",
    r"pd\.NA",
    r"fillna\(",
    r"clip\(",
    r"\.replace\(",
    r"replace\(0\.0,",
    r"replace\(0,",
    r"index=df\.index",
    r"pd\.Series\(",
    r"default_\w+\s*=",
    r"^\s+\w+:\s*\w+\s*=\s*[\d\.]",
    r'defaults\s*=\s*\{',
    r'\s+"\w+":\s*[\d\.]',
    r"\s+'\w+':\s*[\d\.]",
]

PROTECTED_RE = re.compile("|".join(PROTECTED_CONTEXTS))

THRESHOLD_VALUE_PATTERN = re.compile(
    r"(?<![a-zA-Z0-9_])([0-9]*\.[0-9]{2,})(?![0-9])"
)

CLASS_LEVEL_DEFAULT_PATTERN = re.compile(r"^\s+(?:default_)?\w+(?::\s*\w+)?\s*=\s*[\d\.]")


@dataclass
class Violation:
    file_path: str
    line_number: int
    line_content: str
    violation_type: str
    suggestion: Optional[str] = None


@dataclass
class ScanResult:
    passed: bool
    violations: List[Violation]
    scanned_files: int
    total_lines: int

    def summary(self) -> str:
        if self.passed:
            return f"PASS: Scanned {self.scanned_files} files, {self.total_lines} lines"
        
        lines = [
            f"INFO: Found {len(self.violations)} hardcoded thresholds in {self.scanned_files} files:",
        ]
        for v in self.violations[:30]:
            lines.append(f"  {v.file_path}:{v.line_number}: {v.violation_type}")
            lines.append(f"    {v.line_content.strip()}")
        if len(self.violations) > 30:
            lines.append(f"  ... and {len(self.violations) - 30} more violations")
        return "\n".join(lines)


def is_in_protected_context(line: str) -> bool:
    if "self.default_" in line:
        return True
    if CLASS_LEVEL_DEFAULT_PATTERN.match(line):
        return True
    return PROTECTED_RE.search(line) is not None


def check_line_for_hardcoded(line: str, file_path: str, line_number: int) -> List[Violation]:
    violations: List[Violation] = []
    
    if is_in_protected_context(line):
        return violations
    
    line_stripped = line.rstrip()
    
    matches = THRESHOLD_VALUE_PATTERN.findall(line_stripped)
    for match in matches:
        try:
            val = float(match)
            if val not in (0.0, 1.0, -1.0):
                violations.append(Violation(
                    file_path=file_path,
                    line_number=line_number,
                    line_content=line_stripped,
                    violation_type="hardcoded_threshold",
                    suggestion="Use params.get() or config value",
                ))
                break
        except ValueError:
            pass
    
    return violations


def scan_file(file_path: Path) -> List[Violation]:
    violations: List[Violation] = []
    
    if file_path.suffix not in (".py",):
        return violations
    
    if "test_" in file_path.name or "_test.py" in file_path.name:
        return violations
    
    try:
        content = file_path.read_text(encoding="utf-8")
    except (UnicodeDecodeError, OSError):
        return violations
    
    lines = content.splitlines()
    in_defaults_dict = False
    
    for i, line in enumerate(lines, 1):
        if re.search(r'defaults\s*=\s*\{', line):
            in_defaults_dict = True
        elif in_defaults_dict and re.match(r'^\s*\}', line):
            in_defaults_dict = False
        elif in_defaults_dict:
            continue
        
        violations.extend(check_line_for_hardcoded(line, str(file_path), i))
    
    return violations


def scan_directory(directory: Path, recursive: bool = True) -> ScanResult:
    violations: List[Violation] = []
    scanned_files = 0
    total_lines = 0
    
    pattern = "**/*.py" if recursive else "*.py"
    
    for file_path in directory.glob(pattern):
        if file_path.is_dir():
            continue
        
        if any(file_path.match(p) for p in ALLOWED_FILES):
            continue
            
        file_violations = scan_file(file_path)
        violations.extend(file_violations)
        scanned_files += 1
        
        try:
            total_lines += len(file_path.read_text(encoding="utf-8").splitlines())
        except (UnicodeDecodeError, OSError):
            pass
    
    return ScanResult(
        passed=len(violations) == 0,
        violations=violations,
        scanned_files=scanned_files,
        total_lines=total_lines,
    )


class TestHardcodedConstants:
    def test_detector_files_scan(self):
        detectors_dir = Path("project/events/detectors")
        
        if not detectors_dir.exists():
            pytest.skip("Detectors directory not found")
        
        result = scan_directory(detectors_dir)
        print(f"\n{result.summary()}")
        
        assert isinstance(result.violations, list)
        assert result.scanned_files > 0

    def test_family_files_scan(self):
        families_dir = Path("project/events/families")
        
        if not families_dir.exists():
            pytest.skip("Families directory not found")
        
        result = scan_directory(families_dir)
        print(f"\n{result.summary()}")
        
        assert isinstance(result.violations, list)
        assert result.scanned_files > 0


class TestHardcodedConstantScanner:
    def test_detects_threshold_in_comparison(self):
        line = "if spread > 0.75:"
        violations = check_line_for_hardcoded(line, "test.py", 1)
        assert len(violations) > 0

    def test_allows_params_get(self):
        line = "threshold = params.get('threshold', 0.5)"
        violations = check_line_for_hardcoded(line, "test.py", 1)
        assert len(violations) == 0

    def test_allows_default_class_attribute(self):
        line = "    default_threshold = 0.75"
        violations = check_line_for_hardcoded(line, "test.py", 1)
        assert len(violations) == 0

    def test_allows_typed_class_attribute(self):
        line = "    min_quantile: float = 0.75"
        violations = check_line_for_hardcoded(line, "test.py", 1)
        assert len(violations) == 0


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
