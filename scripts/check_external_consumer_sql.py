#!/usr/bin/env python3
"""Fail when a runtime consumer queries a moved UKMesh relation without a schema.

The P1/P2 consolidation moves relations out of ``public``.  External consumers
(link worker, Discord bot, CoreScope compatibility readers) are separate build
and deployment units, so a green backend suite cannot catch them.  This checker
is deliberately source-level and cheap enough to run in CI or a pre-deploy step.

Usage:
    python3 scripts/check_external_consumer_sql.py \
        --root viewshed-worker \
        --root /path/to/meshcore-discord-bot/src
"""
from __future__ import annotations

import argparse
import os
import re
import sys
from pathlib import Path

# Relation -> the schema that now owns it.  Add new reservations in the same
# change as the migration that performs the move.
MOVED_RELATIONS: dict[str, str] = {
    "nodes": "core",
    "node_status_samples": "core",
    "node_links": "core",
    "observers": "core",
    "private_node_prefixes": "core",
    "packets": "mesh",
    "packet_paths": "mesh",
    "message_tags": "mesh",
    "message_tags_local": "mesh",
    "packet_visibility_materialization_state": "ops",
    "privacy_rematerialization_queue": "ops",
    "public_visibility_state": "ops",
}

_QUALIFIED_SCHEMAS = "core|mesh|ops|analytics|archive|public"
_RELATION_PATTERN = "|".join(re.escape(name) for name in sorted(MOVED_RELATIONS, key=len, reverse=True))
_SQL_RE = re.compile(
    rf"(?i)(?:\bfrom|\bjoin|\bupdate|\binsert\s+into|\bdelete\s+from)\s+"
    rf"(?![\"']?(?:{_QUALIFIED_SCHEMAS})\.)[\"']?({_RELATION_PATTERN})[\"']?\b"
)
_COMMENT_PREFIXES = ("--", "//", "#")
_SKIP_DIRS = {
    ".git", ".hg", "node_modules", "dist", "build", "__pycache__",
    "migrations", "tests", "test",
}


def _source_files(root: Path):
    if root.is_file():
        yield root
        return
    for directory, dirnames, filenames in os.walk(root):
        dirnames[:] = [d for d in dirnames if d not in _SKIP_DIRS]
        for filename in filenames:
            if ".test." in filename or ".spec." in filename:
                continue
            if filename.endswith((".py", ".ts", ".tsx", ".js", ".mjs", ".go", ".sh")):
                yield Path(directory) / filename


def scan_text(text: str, *, source: str = "<memory>") -> list[tuple[str, int, str]]:
    """Return (source, line, relation) for every unqualified moved relation."""
    violations: list[tuple[str, int, str]] = []
    for line_no, line in enumerate(text.splitlines(), 1):
        stripped = line.lstrip()
        if stripped.startswith(_COMMENT_PREFIXES):
            continue
        for match in _SQL_RE.finditer(line):
            violations.append((source, line_no, match.group(1).lower()))
    return violations


def scan_roots(roots: list[Path]) -> list[tuple[str, int, str]]:
    violations: list[tuple[str, int, str]] = []
    for root in roots:
        for path in _source_files(root):
            try:
                text = path.read_text(encoding="utf-8", errors="replace")
            except OSError as error:
                violations.append((str(path), 0, f"read-error:{error}"))
                continue
            violations.extend(scan_text(text, source=str(path)))
    return violations


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--root",
        action="append",
        type=Path,
        default=None,
        help="consumer source root (repeatable); defaults to viewshed-worker",
    )
    parser.add_argument("--json", action="store_true", help="emit a machine-readable result")
    args = parser.parse_args(argv)
    roots = args.root or [Path("viewshed-worker")]
    violations = scan_roots(roots)
    if args.json:
        import json
        print(json.dumps({"ok": not violations, "violations": violations}))
    elif violations:
        print("Unqualified moved-relation references:", file=sys.stderr)
        for source, line, relation in violations:
            print(f"  {source}:{line}: {relation}", file=sys.stderr)
    else:
        print(f"OK: no unqualified moved-relation references in {len(roots)} root(s)")
    return 0 if not violations else 1


if __name__ == "__main__":
    raise SystemExit(main())
