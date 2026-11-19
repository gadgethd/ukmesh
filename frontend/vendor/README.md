# Picomatch security backport

Vite, tinyglobby and fdir share the local MIT-licensed package
`picomatch@4.0.8-ukmesh.1`. It is a UKMesh version built from the published
picomatch 4.0.7 tarball with its original license. The patch addresses
[GHSA-fc5q-39xc-vjpf](https://github.com/advisories/GHSA-fc5q-39xc-vjpf).

- [Base source tag](https://github.com/micromatch/picomatch/tree/6bb40679c218cefba5d4e9662408c5fdf699a8bb)
- [Parser change](https://github.com/micromatch/picomatch/pull/262/commits/3222059ac27492d9c0147fee5c65f751bbd851ab)
- Published tarball integrity `sha512-qcJu88Q2IWqJsDD529JKMdwGm/dvInW4HvQnRwiH9JtihJvzGOscDtHE3x1pBKeUOTysQ8kVmLnJ2kJu7yhcGA==`
- UKMesh tarball SHA-256 `4f0c817796cff370435a854d76b9ebfd2af4f6ab1592ebc9db800f7c94805d13`

To reproduce the package, verify the published tarball integrity, unpack it,
apply `picomatch-4.0.7-repeated-wildcard.patch` with `patch -p1 --fuzz=0`, set
the package version to `4.0.8-ukmesh.1` in `package.json`, and run
`npm pack --ignore-scripts` using npm 11.20.0. The patch contains only the
parser change relative to the proposal's parent. The lockfile pins the
resulting archive integrity, and the frontend's full npm audit remains a
required check. Remove the replacement after a compatible upstream fix passes
the audit and frontend checks.
