#!/usr/bin/env python3
"""Capture actual application and owner flows in the selected owned fixture."""
import argparse
import json
from pathlib import Path
import sys
import tempfile

sys.path.insert(0, str(Path(__file__).resolve().parent / "qualification"))
from common import REPO, command, owned_run, read_json, safe_error, write_json
from context import phase_context


def interfaces(run: Path) -> dict:
    attempt = Path(tempfile.mkdtemp(prefix="interfaces-", dir=run))
    value = {"passed": False, "checks": [], "attempt": str(attempt)}
    write_json(run / "interfaces.json", value)
    try:
        before = phase_context(run)
        with (attempt / "interfaces.log").open("xb") as output:
            (attempt / "interfaces.log").chmod(0o600)
            completed = command(["node", "--max-old-space-size=256", "--import",
                str(REPO / "backend/node_modules/tsx/dist/loader.mjs"),
                str(REPO / "scripts/full-dev/interfaces.mjs"), str(attempt)],
                output_stream=output, timeout=600, check=False)
        if completed.stderr:
            (attempt / "interfaces.stderr").write_bytes(completed.stderr)
            (attempt / "interfaces.stderr").chmod(0o600)
        value = read_json(attempt / "interfaces-native.json") if (attempt / "interfaces-native.json").exists() else {"passed": False, "checks": []}
        after = phase_context(run)
        value.update({"context": before, "attempt": str(attempt), "exit_code": completed.returncode})
        value["checks"].append({"name": "interface-source-and-runtime-stable", "passed": before == after})
        value["passed"] = value["passed"] and completed.returncode == 0 and before == after
    except Exception as error:
        value["passed"] = False
        value["checks"].append({"name": "interface-run", "passed": False, "error": safe_error(error)})
    finally:
        write_json(run / "interfaces.json", value)
    return value


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--run")
    arguments = parser.parse_args()
    result = interfaces(owned_run(arguments.run))
    print(json.dumps({"passed": result["passed"], "attempt": result["attempt"]}))
    raise SystemExit(0 if result["passed"] else 1)
