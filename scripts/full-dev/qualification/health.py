"""Own the local Health Check session used by invented channel traffic."""

from __future__ import annotations

import hashlib
import json
from pathlib import Path
import re
import time
import urllib.request

from common import STATE, read_json, utc_now, write_json
from loopback_http import http_get, open_local


PROOF = STATE / "runtime/health-proof.json"


def create_session(run: Path, fixture: dict, explicit_code: str | None = None) -> dict:
    if explicit_code:
        receipt = read_json(STATE / "health-session.json")
        if receipt.get("code") != explicit_code:
            raise ValueError("explicit Health Check code requires its matching local health-session.json receipt")
    else:
        request = urllib.request.Request("http://127.0.0.1:3090/api/sessions", data=b"{}", method="POST",
                                         headers={"Content-Type": "application/json"})
        with open_local(request, timeout=15) as response:
            raw = response.read(65537)
            if len(raw) > 65536:
                raise ValueError("Health Check session response exceeded its byte budget")
            receipt = json.loads(raw)
    if not re.fullmatch(r"[a-zA-Z0-9_-]{1,128}", receipt.get("id", "")) or not re.fullmatch(
            r"[a-zA-Z0-9_-]{1,64}", receipt.get("code", "")):
        raise ValueError("Health Check returned an invalid session identity")
    result = {"sessionId": receipt["id"], "code": receipt["code"], "fixtureToken": fixture["token"],
              "runId": run.name, "testStartedAt": int(time.time() * 1000)}
    write_json(PROOF, {key: value for key, value in result.items() if key != "code"} | {"passed": False,
        "codeSha256": hashlib.sha256(receipt["code"].encode()).hexdigest()})
    return result


def verify_session(session: dict, *, timeout: float = 120, stop_event=None) -> dict:
    deadline = time.monotonic() + timeout
    last_error = None
    value = {}
    checks = []
    cancelled = False
    while True:
        if stop_event is not None and stop_event.is_set():
            cancelled = True
            break
        try:
            body, _ = http_get("http://127.0.0.1:3090/api/sessions/" + session["sessionId"])
            value = json.loads(body)
            last_error = None
            checks = [
                {"name": "Health Check returned the requested session", "passed": value.get("id") == session["sessionId"]},
                {"name": "Health Check proof belongs to the active fixture", "passed":
                 read_json(STATE / "runtime/selected-fixture.json").get("token") == session["fixtureToken"]},
                {"name": "Health Check matched the new code", "passed": isinstance(value.get("matchedAt"), (int, float))
                 and value["matchedAt"] >= session["testStartedAt"]},
                {"name": "Health Check received both observers", "passed": value.get("observedCount", 0) >= 2
                 and isinstance(value.get("receipts"), list) and len(value["receipts"]) >= 2},
                {"name": "Health Check ignored duplicate use", "passed": value.get("useCount") == 1},
            ]
            if all(check["passed"] for check in checks) and not (stop_event is not None and stop_event.is_set()):
                break
        except Exception as error:
            from common import safe_error
            last_error = safe_error(error)
        if time.monotonic() >= deadline:
            break
        if stop_event is None: time.sleep(.5)
        elif stop_event.wait(.5):
            cancelled = True
            break
    cancelled = cancelled or (stop_event is not None and stop_event.is_set())
    if cancelled:
        last_error = 'native workload failed before publication; owned Health poll cancelled'
        checks.append({'name': 'native workload was published for this Health proof', 'passed': False, 'not_published': True})
    result = {"at": utc_now(), "sessionId": session["sessionId"], "fixtureToken": session["fixtureToken"],
              "runId": session["runId"], "passed": not cancelled and bool(checks) and all(check["passed"] for check in checks),
              "matchedAt": value.get("matchedAt"), "observedCount": value.get("observedCount", 0),
              "receiptCount": len(value.get("receipts", [])), "useCount": value.get("useCount"),
              "checks": checks, "error": last_error, 'cancelled': cancelled}
    write_json(PROOF, result)
    return result
