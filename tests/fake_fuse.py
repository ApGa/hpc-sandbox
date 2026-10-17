"""Stand-ins for ``fuse-overlayfs`` and ``fusermount3``, used by the unit tests.

``fuse-overlayfs -o lowerdir=L,upperdir=U,workdir=W MOUNTPOINT`` copies L, then what U holds, into MOUNTPOINT (the
fake singularity then runs commands on it as on any rootfs; what they write stays there, not in U) and records the
mount; ``fusermount3 -u [-z] MOUNTPOINT`` drops the
record. Every invocation is appended to $FAKE_SINGULARITY_STATE/fuse-calls.jsonl, and the mounts are files in
$FAKE_SINGULARITY_STATE/mounts/. FAKE_FUSE_MOUNT_FAILS=1 makes mounts fail.
"""

from __future__ import annotations

import hashlib
import json
import os
import shutil
import sys
from pathlib import Path

STATE = Path(os.environ["FAKE_SINGULARITY_STATE"])
MOUNTS = STATE / "mounts"


def record_path(mountpoint: str) -> Path:
    return MOUNTS / hashlib.sha256(mountpoint.encode()).hexdigest()


def main(argv: list[str]) -> int:
    tool = os.path.basename(argv[1])  # the wrappers pass the name they were run as
    args = argv[2:]
    with (STATE / "fuse-calls.jsonl").open("a") as stream:
        stream.write(json.dumps([tool, *args]) + "\n")
    MOUNTS.mkdir(parents=True, exist_ok=True)
    if tool == "fuse-overlayfs":
        if os.environ.get("FAKE_FUSE_MOUNT_FAILS") == "1":
            print("fuse-overlayfs: simulated failure", file=sys.stderr)
            return 1
        options = dict(item.split("=", 1) for item in args[args.index("-o") + 1].split(","))
        mountpoint = args[-1]
        shutil.copytree(options["lowerdir"], mountpoint, symlinks=True, dirs_exist_ok=True)
        shutil.copytree(options["upperdir"], mountpoint, symlinks=True, dirs_exist_ok=True)
        record_path(mountpoint).write_text(json.dumps({"mountpoint": mountpoint, **options}))
        return 0
    mountpoint = args[-1]
    path = record_path(mountpoint)
    if not path.exists():
        print(f"{tool}: entry for {mountpoint} not found", file=sys.stderr)
        return 1
    path.unlink()
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
