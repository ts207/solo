import argparse
from pathlib import Path

from project.research.portfolio_research_autopsy import generate_portfolio_autopsy

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--data-root", default="data")
    args = parser.parse_args()

    data_root = Path(args.data_root)
    out_dir = data_root / "reports" / "portfolio_autopsy"
    out_dir.mkdir(parents=True, exist_ok=True)
    
    autopsy = generate_portfolio_autopsy()

    with open(out_dir / "portfolio_research_autopsy.md", "w") as f:
        f.write("# Portfolio Research Autopsy\n\n")
        f.write("Global portfolio-level report summarizing the structural failure of all tested mechanisms.\n\n")
        
        for mech, data in autopsy.items():
            f.write(f"## {mech}\n\n")
            f.write(f"**Decision:** `{data['decision']}`\n\n")
            f.write("**Reopen only if:**\n")
            for condition in data["reopen_only_if"]:
                f.write(f"- {condition}\n")
            f.write("\n---\n\n")
            
    print("Wrote portfolio research autopsy.")

if __name__ == "__main__":
    main()
