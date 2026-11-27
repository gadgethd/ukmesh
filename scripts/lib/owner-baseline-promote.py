#!/usr/bin/env python3
"""Publish the validated baseline passed by refresh-owner-baseline.sh."""

import os
from pathlib import Path
import shutil
import stat
import sys
import tempfile


def promote(candidate: Path, previous: Path) -> None:
    active = candidate.parent / "current.json"
    if previous.parent != active.parent:
        raise ValueError("baseline files must share the same directory")
    if not all(stat.S_ISREG(path.lstat().st_mode) for path in (active, candidate)):
        raise ValueError("only regular files can be used for baseline activation")

    active_stat = active.stat()
    with tempfile.NamedTemporaryFile(dir=active.parent, prefix=".owner-baseline-", delete=False) as staged:
        staged_path = Path(staged.name)
        try:
            # Readers keep access across the rename, even when the host operator
            # is a different user from the backend container.
            os.fchown(staged.fileno(), active_stat.st_uid, active_stat.st_gid)
            os.fchmod(staged.fileno(), stat.S_IMODE(active_stat.st_mode))
            with candidate.open("rb") as source:
                shutil.copyfileobj(source, staged)
            staged.flush()
            os.fsync(staged.fileno())
            # Keep the previous inode under a unique name before replacing it.
            os.link(active, previous, follow_symlinks=False)
            os.replace(staged_path, active)
            directory = os.open(active.parent, os.O_RDONLY | os.O_DIRECTORY)
            try:
                os.fsync(directory)
            finally:
                os.close(directory)
        finally:
            staged_path.unlink(missing_ok=True)


if __name__ == "__main__":
    promote(Path(sys.argv[1]), Path(sys.argv[2]))
