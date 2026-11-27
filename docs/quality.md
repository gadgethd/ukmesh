# Fleet pattern checks

`hcheck` runs at commit time. It covers the failure modes this fleet keeps
repeating that `ruff`, `eslint` and `tsc` do not: swallowed exceptions,
unbounded sleep polling, `shell=True` with interpolation, `os.system`/`pkill -f`,
TODO without a ticket, network calls without a timeout, and unsafe shell
patterns (`curl | sh`, `docker volume rm`).

The repo's CI already runs its own lint, type and test jobs; this is the extra
layer for those failure modes.

Allow a rule only with a reason and an expiry, on the line above the code:

```python
# hcheck: allow H002 reason="daemon poll interval, no event source" until=2027-01-01
```

An annotation without a reason, or past its expiry, is itself a violation (H900).

`.hcheck-baseline.json` records violations that predate the gate; new violations
fail and the baseline may only shrink.
