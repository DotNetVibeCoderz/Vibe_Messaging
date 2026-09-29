"""Executes notebooks with dotnet-repl and reports cell errors.

    python tools/notebooks/run_notebooks.py <dotnet-repl exe> <notebook dir> [out dir]
"""
import json
import subprocess
import sys
from pathlib import Path

repl, nb_dir = sys.argv[1], Path(sys.argv[2])
out_dir = Path(sys.argv[3]) if len(sys.argv) > 3 else nb_dir / "executed"
out_dir.mkdir(parents=True, exist_ok=True)
failed = 0
for nb in sorted(nb_dir.glob("*.ipynb")):
    out = out_dir / nb.name
    subprocess.run([repl, "--run", str(nb), "--exit-after-run", "--working-dir", str(nb_dir), "--output-path", str(out)],
                   capture_output=True, text=True, timeout=900)
    errors = []
    if out.exists():
        for i, c in enumerate(json.loads(out.read_text(encoding="utf-8"))["cells"]):
            for o in c.get("outputs", []):
                if o.get("output_type") == "error":
                    errors.append(f"cell {i}: {o.get('evalue', '')[:400]}")
    else:
        errors.append("no output written")
    failed += bool(errors)
    print(f"[{'FAIL' if errors else 'PASS'}] {nb.name}")
    for e in errors:
        print("   ", e)
sys.exit(1 if failed else 0)
