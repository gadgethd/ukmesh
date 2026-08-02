# Security waiver policy

Production images may not be released with an unwaived critical vulnerability.
The release workflow scans the exact pushed digest and reads
`.trivyignore.yaml`; a tag or a locally rebuilt image is never substituted for
that digest.

An exception is allowed only when all of the following are true:

- the vulnerability ID and affected path or package are specific;
- the statement records the applicability analysis, compensating control,
  `owner=<team-or-person>`, and `reviewed=YYYY-MM-DD`;
- `expired_at` is no more than 30 days after the review date;
- a security reviewer approved the change through normal code review; and
- a follow-up issue is linked in the statement.

Example:

```yaml
vulnerabilities:
  - id: CVE-2099-0001
    paths:
      - usr/lib/example.so
    statement: >-
      Not reachable in this image; issue=SEC-123; owner=platform;
      reviewed=2099-01-02
    expired_at: 2099-02-01
```

`node scripts/check-trivy-waivers.mjs` validates uniqueness, ownership, review
date, expiry, and the maximum lifetime. Remove the entry as soon as a fixed
base image or dependency is available. Waivers must never suppress an entire
scanner, severity, or image.
