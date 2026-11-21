"""Retain exact-window resident logs and reject unexpected application failures."""
from __future__ import annotations

import datetime as dt
import hashlib
import json
from pathlib import Path
import re

from common import docker, service_manifest, write_json

FAILURE = re.compile(
    r'\blevel[=:]\s*"?(?:error|fatal|panic)\b'
    r'|\[(?:error|fatal|panic)\]'
    r'|\b(?:[A-Za-z]*Error|ERROR|FATAL|PANIC)\s*:'
    r'|\b(?:UnhandledPromiseRejection|UncaughtException)\b'
    r'|\b(?:uncaught|unhandled|panic|exception|failed|failure)\b'
    r'|\b(?:timed out|timeout exceeded|timeout acquiring|permission denied|out of memory|ECONNRESET|ECONNREFUSED|connection terminated unexpectedly)\b',
    re.IGNORECASE)
EMPTY_FIELD = re.compile(r'\b(?:failed|failure|error|err)=(?:0|false|null|none)(?=\s|,|$)', re.IGNORECASE)
ERROR_FIELD = re.compile(r'''\b(?:error|err)=(?:"([^"]*)"|'([^']*)'|(\S+))''', re.IGNORECASE)
# A PostgreSQL log record carries its own timestamp, timezone, pid and severity.
# STATEMENT/QUERY/DETAIL/HINT/CONTEXT/LOCATION records echo the failed SQL or its
# bind values across continuation lines that have no record prefix; their text
# is data, not a new failure. The severity record itself (ERROR/FATAL/PANIC)
# remains the failure evidence.
# PG_RECORD assumes the deployed log_line_prefix '%m [%p] ' (fractional-second
# timestamp, pid, severity directly before the colon). A different prefix would
# fall back to the ordinary message scan rather than misclassify records.
PG_RECORD = re.compile(r'^\d{4}-\d\d-\d\d \d\d:\d\d:\d\d\.\d+ \S+ \[\d+\] ([A-Z]+\d*):')
PG_DUMP_SEVERITIES = frozenset({'STATEMENT', 'QUERY', 'DETAIL', 'HINT', 'CONTEXT', 'LOCATION'})


def log_body(line: str) -> str:
    """The line without the docker --timestamps prefix, when present."""
    return line.split(' ', 1)[1] if re.match(r'^\d{4}-\d\d-\d\dT', line) else line


def pg_record_severity(line: str) -> str | None:
    """Severity of a PostgreSQL log record, or None for any other line."""
    match = PG_RECORD.match(log_body(line))
    return match.group(1) if match else None


def failed_value(value) -> bool:
    if isinstance(value, dict):
        for key, child in value.items():
            key = key.lower()
            if key in ('error', 'err', 'failed', 'failure', 'exception', 'panic', 'fatal') and child:
                return True
            if key in ('level', 'severity') and str(child).lower() in ('error', 'fatal', 'panic'):
                return True
            if key in ('event', 'outcome', 'status') and isinstance(child, str) and re.search(
                    r'(^|[_\W])(error|failure|failed|fatal|panic)([_\W]|$)', child, re.IGNORECASE):
                return True
            if failed_value(child):
                return True
        return False
    if isinstance(value, list):
        return any(failed_value(child) for child in value)
    return isinstance(value, str) and bool(FAILURE.search(EMPTY_FIELD.sub('', value)))


def failure_line(line: str) -> bool:
    # Docker prepends its timestamp. Parse structured fields before scanning
    # messages so false/null counters do not hide quoted failure messages.
    body = log_body(line)
    try:
        return failed_value(json.loads(body))
    except (ValueError, TypeError):
        for match in ERROR_FIELD.finditer(body):
            value = next(part for part in match.groups() if part is not None)
            if value.lower() not in ('', '0', 'false', 'null', 'none', 'nil', '<nil>'):
                return True
        return bool(FAILURE.search(EMPTY_FIELD.sub('', body)))


def validate_window(started: str, finished: str) -> None:
    start, end = (dt.datetime.fromisoformat(value) for value in (started, finished))
    if start.utcoffset() is None or end.utcoffset() is None or not start < end <= dt.datetime.now(dt.timezone.utc):
        raise ValueError('Log capture requires an ordered, aware, completed time window')


def inspect_log(path: Path) -> dict:
    checksum = hashlib.sha256()
    failures = 0
    examples = []
    with path.open('rb') as source:
        for number, line in enumerate(source, 1):
            checksum.update(line)
            text = line.decode('utf8', errors='replace')
            severity = pg_record_severity(text)
            if severity in PG_DUMP_SEVERITIES:
                # Dump records echo SQL/bind data; only their owning severity
                # record counts.
                continue
            if severity is None and log_body(text).startswith('\t'):
                # PostgreSQL continuation lines are tab-indented. Skip them
                # wherever they appear: inside a dump, and when the capture
                # window opened mid-dump (leading orphaned continuations).
                # Any other line ends the echo and is scanned normally, so a
                # non-PostgreSQL failure right after a dump stays visible.
                continue
            if failure_line(text):
                failures += 1
                if len(examples) < 20:
                    examples.append(number)
    return {'sha256': checksum.hexdigest(), 'failure_lines': failures, 'example_line_numbers': examples}


def collect_logs(run: Path, *, started: str, finished: str) -> dict:
    validate_window(started, finished)
    directory = run / 'steady-runtime-logs'
    directory.mkdir(mode=0o700)
    checks = []
    for service in service_manifest()['services']:
        if not service.get('required', True) or service.get('kind', 'container') != 'container':
            continue
        container = service['container']
        path = directory / (service['name'] + '.log')
        with path.open('xb') as output:
            path.chmod(0o600)
            result = docker(['logs', '--timestamps', '--since', started, '--until', finished, container],
                            output_stream=output, merge_stderr=True, check=False, timeout=30)
        checked = inspect_log(path)
        checks.append({'name': service['name'], 'passed': result.returncode == 0 and checked['failure_lines'] == 0,
                       'exit_code': result.returncode, **checked})
    value = {'passed': bool(checks) and all(check['passed'] for check in checks),
             'started': started, 'finished': finished, 'checks': checks}
    write_json(run / 'steady-runtime-logs.json', value)
    return value
