# UKMesh merge checklist — 2026-10-23

Source: the first ten priority rows of the [independent verification matrix](20261023-verify-ukmesh.md#merge-matrix), content commit `2a37774492f783ca7f7ce8c6b463cd17f0a0f8f0` on `verify/ukmesh-20261023`. All ten are **SAFE TO MERGE at the tested commits below** for the gates recorded there. The matrix does not certify their combined result or unexecuted integration/infrastructure checks.

GitHub PRs and published branch tips were checked on **2026-10-23 at 17:46 BST**. Only three of these branches currently have PR numbers; “not opened” means no PR exists for that exact head branch, across open and closed PRs. The row order preserves the matrix's suggested priority; the single required branch ordering is **regions before data**.

| Merged | Priority | Safe branch | PR | Tested commit |
|---|---:|---|---|---|
| ☐ | 1 | `burn/ukmesh-ingest-20261023` | [#151](https://github.com/gadgethd/ukmesh/pull/151) | `a542111fc7656be203c3a19e68230038750a6a77` |
| ☐ | 2 | `burn/ukmesh-ci-20261023` | [#150](https://github.com/gadgethd/ukmesh/pull/150) | `ec51e459dc9c3e06a0a29f7a5d9806ea7234cc29` |
| ☐ | 3 | `burn/ukmesh-readyz-20261023` | [#149](https://github.com/gadgethd/ukmesh/pull/149) | `a5bfa546af220804ce76ec7dd99b27a7671daf7f` |
| ☐ | 4 | `burn/ukmesh-converge-20261023` | Not opened | `7e157bb9988ad79938bbd3747902353754446c71` |
| ☐ | 5 | `burn/ukmesh-regions-20261023` | Not opened | `2116667845144289f42e6ce0c3066a58ddbe102d` |
| ☐ | 6 | `burn/ukmesh-data-20261023` | Not opened | `320e6c6ca79b6564811ebb4a063c845c771b9198` |
| ☐ | 7 | `burn/ukmesh-alerts-20261023` | Not opened | `0522ef4f40effe1d04e0f3ea12ec96c359370c52` |
| ☐ | 8 | `burn/ukmesh-preheat-20261023` | Not opened | `acddc0478b6bc58d73238896d79aa9bcc2b4952b` |
| ☐ | 9 | `burn/ukmesh-shm-20261023` | Not opened | `4330e704cb437ce4ba7dfff27774dae516bb5a9a` |
| ☐ | 10 | `burn/ukmesh-onboard-20261023` | Not opened | `f43aa0033a0d80dc07287da93aa77032c88cc203` |

- [ ] Publish onboard's tested local tip before opening its PR: its GitHub branch still points to the unchanged base `73ab7d4fc63e4968a5fd46af022696ad69d16d9b`. The other nine published tips match the tested commits.
- [ ] Open the seven missing PRs and fill in their numbers. Recheck each PR's head against the matrix; changes after verification need fresh checks.
- [ ] **Merge regions before data.** Keep regions' `backend/src/db/migrations/063_packet_region_scope_index.sql`. Update data on the resulting main branch and rename its `063_node_name_history.sql` to `064_node_name_history.sql` (or the next unused prefix). Update the filename reference in `backend/src/db/nodeNameHistory.integration.test.ts` and the data review document. Both branches currently allocate `063`; merge order alone does not resolve this collision.
- [ ] Before merging the updated data PR, run its migration/name-history integration checks and the normal backend/frontend gates against the combined tree. Preserve both region filtering and former-name search when resolving overlapping node API/repository and contract changes.
- [ ] Check CI and resolve conflicts for each resulting PR before marking its row merged. The matrix's individual-branch verdict does not establish that all ten can merge unchanged.

Ordering evidence: [`validateMigrationPrefixes`](https://github.com/gadgethd/ukmesh/blob/73ab7d4fc63e4968a5fd46af022696ad69d16d9b/backend/src/db/migrations.ts#L64) rejects new duplicate numeric prefixes. A local read-only check of the actual regions/data migration inventories reproduced `duplicate migration prefix 063`; substituting data's `064_node_name_history.sql` passed the same validator. No SQL was executed. There is no other required branch ordering established by the verification matrix; the remaining priority positions are a suggested sequence.
