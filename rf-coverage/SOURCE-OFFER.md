# HopReach corresponding source

The RF calculator's complete corresponding source pinned for this candidate
is public in the
[UK Mesh HopReach fork](https://github.com/gadgethd/hopreach/tree/v0.1.32-ukmesh.4)
at commit `58b2eb9ccdadd2e1b5e54c7548e48f92cc526119`, immutably tagged
`v0.1.32-ukmesh.4`. That revision is based directly on upstream HopReach
v0.1.32 commit `61efac0b4678f55496fe08f53eda0c79eb18655b` and contains the
identifiable UK Mesh modifications vendored in `third_party/hopreach`.

HopReach and the derived RF integration files are licensed under AGPL-3.0 plus
the Commons Clause. The complete license and notices are preserved in
`third_party/hopreach/LICENSE`. The HopReach container identifies the public
fork through `org.opencontainers.image.source`. Direct builds can identify the
fork commit through `SOURCE_REVISION`; the UKMesh release workflow overrides the
OCI revision label with the integration release commit and records that commit
in its signed inventory. It explicitly retains the fork source URL and
`AGPL-3.0-only WITH Commons-Clause` licence label instead of repository defaults.
The calculator build/version remains pinned to the
fork revision and tag above, also recorded in `third_party/hopreach/UPSTREAM_COMMIT`. Production rollout is gated on all of those
references being publicly accessible before the calculator or coverage UI is
exposed.

UK Mesh does not sell this software or a service based on it.
