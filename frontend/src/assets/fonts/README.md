These OFL fonts are emitted with a content hash by Vite. The root HTML preloads
and global CSS must point to these source assets so both use the same immutable
build URLs. License notices remain in `frontend/public/fonts/OFL-Inter.txt` and
`frontend/public/fonts/OFL-ShareTechMono.txt` and are copied into every artifact.

The stable font copies in `frontend/public/fonts/` remain available to previous
clients. Retain them through the release rollback window; cleanup is a later
operator decision. Do not mark stable, replaceable font URLs immutable.
