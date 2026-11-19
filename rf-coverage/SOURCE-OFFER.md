# HopReach corresponding source

The complete corresponding source for the remote GPU coordinator is in
`third_party/hopreach` in the [UKMesh repository](https://github.com/gadgethd/ukmesh).
Select the commit recorded by the image's OCI revision label. The vendored
base is [UKMesh HopReach v0.1.32-ukmesh.4](https://github.com/gadgethd/hopreach/tree/v0.1.32-ukmesh.4)
at `58b2eb9ccdadd2e1b5e54c7548e48f92cc526119`, based on upstream v0.1.32
commit `61efac0b4678f55496fe08f53eda0c79eb18655b`.

HopReach and the derived RF integration files are licensed under AGPL-3.0 plus
the Commons Clause. The complete license and notices are preserved in
`third_party/hopreach/LICENSE`. Integrated builds use the UKMesh repository as
their OCI source and record the integration revision. The signed release
inventory associates the image digest with that revision. Publish the exact
corresponding source before exposing a release image or the coverage UI.

UK Mesh does not sell this software or a service based on it.
