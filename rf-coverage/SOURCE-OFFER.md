# HopReach corresponding source

The deployed RF calculator's complete corresponding source is public in the
[UK Mesh HopReach fork](https://github.com/gadgethd/hopreach/tree/v0.1.32-ukmesh.3)
at commit `0230702be70a2729c5acc5640401f56ab9d65fd4`, immutably tagged
`v0.1.32-ukmesh.3`. That revision is based directly on upstream HopReach
v0.1.32 commit `61efac0b4678f55496fe08f53eda0c79eb18655b` and contains the
identifiable UK Mesh modifications vendored in `third_party/hopreach`.

HopReach and the derived RF integration files are licensed under AGPL-3.0 plus
the Commons Clause. The complete license and notices are preserved in
`third_party/hopreach/LICENSE`. The HopReach container identifies the public
fork through `org.opencontainers.image.source`, its exact fork commit through
`org.opencontainers.image.revision`, and the integration release revision in
the signed release inventory. Production rollout is gated on all of those
references being publicly accessible before the calculator or coverage UI is
exposed.

UK Mesh does not sell this software or a service based on it.
