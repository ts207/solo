from __future__ import annotations
from pathlib import Path
import re
ROOT = Path(__file__).resolve().parents[2]
PATTERNS=[("future_shift",re.compile(r"shift\s*\(\s*-\s*\d+"),"high"),("centered_rolling",re.compile(r"rolling\s*\([^\)]*center\s*=\s*True"),"high"),("backfill",re.compile(r"\.bfill\s*\(|fillna\s*\([^\)]*method\s*=\s*['\"]bfill"),"medium"),("zero_threshold_fill",re.compile(r"(threshold|median|quantile|std|q)\w*\.fillna\s*\(\s*0"),"medium"),("lookahead_name",re.compile(r"event_lookahead"),"medium")]
SKIP={".git",".venv","venv","__pycache__","data","data_codex","artifacts"}; SUF={".py",".yaml",".yml",".json",".md"}
def main():
    findings=[]
    for path in ROOT.rglob('*'):
        if path.is_dir() or any(part in SKIP for part in path.parts) or path.suffix.lower() not in SUF or path.name=='detector_static_audit.py': continue
        text=path.read_text(errors='ignore')
        for name,pat,severity in PATTERNS:
            for m in pat.finditer(text): findings.append((severity,name,path.relative_to(ROOT),text.count('\n',0,m.start())+1))
    for sev,name,path,line in findings: print(f"{sev.upper():6} {name:20} {path}:{line}")
    highs=[f for f in findings if f[0]=='high']; print(f"detector audit findings={len(findings)} high={len(highs)}"); return 1 if highs else 0
if __name__=='__main__': raise SystemExit(main())
