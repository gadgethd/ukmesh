#!/usr/bin/env python3
"""Retry CI image acquisition and apt failures, never application tests."""

import argparse
from collections import deque
import re
import subprocess
import sys
import time


TRANSIENT = re.compile(
    r"\b(?:408|429|500|502|503|504)\s+(?:Request Timeout|Too Many Requests|"
    r"Internal Server Error|Bad Gateway|Service Unavailable|Gateway Timeout)\b"
    r"|unexpected status[^\n]*\b(?:408|429|500|502|503|504)\b"
    r"|toomanyrequests|i/o timeout|TLS handshake timeout|connection (?:reset|timed out)"
    r"|temporary failure (?:in name resolution|resolving)|temporary error resolving"
    r"|Hash Sum mismatch|File has unexpected size[^\n]*|Mirror sync in progress",
    re.IGNORECASE,
)
PERMANENT = re.compile(
    r"unauthorized|authentication required|denied|manifest unknown|not found"
    r"|no matching manifest|certificate (?:has expired|signed by unknown authority)"
    r"|NO_PUBKEY|not signed|signatures couldn't be verified|Unable to locate package"
    r"|has no installation candidate|unmet dependencies|held broken packages",
    re.IGNORECASE,
)


def retryable(kind, output):
    if kind == "build":
        # BuildKit repeats the causal failure in its final ERROR line. Ignore
        # earlier network warnings from stages that succeeded or were cancelled.
        errors = [line for line in output.splitlines() if re.match(
            r"^(?:ERROR:|(?:target [^:]+: )?failed to solve:)", line,
        )]
        if not errors:
            return False
        failure = errors[-1]
        if 'process "' in failure:
            # Only apt-only layers may be replayed. Never retry npm/go builds,
            # dependency audits, test failures, or a shell layer containing both.
            if not re.search(r'process "/bin/sh -c apt-get\b', failure):
                return False
            evidence = output
        else:
            evidence = failure
    else:
        evidence = output
    return bool(TRANSIENT.search(evidence)) and not PERMANENT.search(evidence)


def run(kind, command, sleep=time.sleep):
    for attempt in range(1, 4):
        output = deque(maxlen=2000)
        with subprocess.Popen(
            command, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
            text=True, errors="replace",
        ) as process:
            for line in process.stdout:
                print(line, end="", flush=True)
                output.append(line)
            status = process.wait()
        if status == 0:
            return 0
        if status < 0:
            return 128 - status
        if attempt == 3 or not retryable(kind, "".join(output)):
            print(f"CI {kind} failed (attempt {attempt}/3, exit {status}).", file=sys.stderr)
            return status
        delay = 5 * 2 ** (attempt - 1)
        print(f"Transient {kind} failure; retrying in {delay}s ({attempt}/3).", flush=True)
        sleep(delay)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("kind", choices=("pull", "build", "apt"))
    parser.add_argument("command", nargs=argparse.REMAINDER)
    args = parser.parse_args()
    if not args.command:
        parser.error("a command is required")
    try:
        sys.exit(run(args.kind, args.command))
    except FileNotFoundError as error:
        print(error, file=sys.stderr)
        sys.exit(127)
