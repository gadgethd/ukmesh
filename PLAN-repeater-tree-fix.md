# UKMesh — "Predicted Repeater Tree" Modal Fix Plan

**Date:** 2026-08-01
**Repo:** `~/meshcore-analytics` on 192.168.100.105 (remote: `github.com/gadgethd/ukmesh.git`, branch `main`)
**Deployed:** docker images `meshcore-analytics-app-ukmesh` (:3003) + `meshcore-analytics-website-ukmesh` (:3004), served via Cloudflare tunnel → Anubis

---

## 1. What's wrong (from live site screenshot + code)

The "Predicted Repeater Tree" modal on the live map / feed page:

1. **Modal shell is unstyled** — no panel background/border/padding; title sits flush to the edge; the Close button renders as a giant full-width bright-cyan button; the modal looks broken/regressed.
2. **Tree content is confusing** — hops with no resolved repeater (raw 2-char trace fragments like `AD`, `91`) render as full "Unmatched repeater" nodes; every node's meta line dumps cryptic raw hex (`predicted repeater CBG D9993F88 D9 1/1 seen`); all dots are the same colour so matched/ambiguous/unmatched hops are indistinguishable.
3. **Header contradicts the tree** — says "1 branch 2/5 hops matched" while the tree shows 3 named repeaters (the ambiguous one isn't counted as "matched").
4. **No map** — the modal shows only a text tree. The feed's right column already draws the path on a map; the stats page's Decoded Path modal (the good reference) shows a 420px map + node cards. The tree modal lost that context.

## 2. Root cause (the regression)

Commit **`83d770a`** ("fix(ui): UKMesh visual QA fixes — …", Aug 1, HEAD, deployed same day) **removed the 15 `.stats-page__path-modal*` rules from `frontend/src/styles/globals.css`** (they had lived there since the modal was first added in `9dd1e7a`, and were duplicated into `stats-page.css` since `93cc3e1`).

Today those styles exist **only** in `frontend/src/pages/stats-page.css`, which is imported **only** by `StatsPage.tsx` (lazy route chunk). The feed modal (`frontend/src/pages/ukmesh/FeedDialogs.tsx`) uses the same `stats-page__path-modal*` classes but **never imports that CSS** → on the feed page the modal falls back to:
- `.ui-dialog-modal` wrapper (760px) — overridden to 1180px by `.uk-feed-path-modal` (feed-page.css, loaded)
- **no panel bg, no border, no radius, no padding, no flex column** (all from the missing `.stats-page__path-modal`)
- Close button = global `.disclaimer-modal__close` (full-width cyan accent button)

Verified in the **deployed bundles** (fetched from live :3003): the feed chunk contains the class names but **zero** `path-modal` CSS rules; the main CSS has none either. So the live site == current source. The stats page's Decoded Path modal looks fine because StatsPage imports the CSS.

## 3. Live data check (packet in screenshot)

`GET /api/path-lazy/resolve?hash=BC1D6965…FD131&network=ukmesh` returns 1 branch:
- pos 0: `hash:"AD"`, nodeId **null** → unmatched
- pos 1: `hash:"91"`, nodeId **null** → unmatched
- pos 2: "Bluntisham RPTR" (nodeId D9993F88…) → matched
- pos 3: "Cambridge South" (nodeId 4E879CAB…, `ambiguous:true`) → matched-but-ambiguous
- pos 4: "EWR 🧭 Stotfold Omni" (FECE48AF…) → matched
- pos 5: "dm-stotfold 📡" → observer (RX)
- `matchedHops: 2, totalHops: 5` (ambiguous **not** counted as matched)

Data itself is sane; presentation is the problem.

## 4. Fix design

### Phase 1 — Restore modal styling (the regression, must-do)
Extract the shared path-modal styles into a file both consumers import:
1. New `frontend/src/pages/path-modal.css` containing the `.stats-page__path-modal*` rules (panel, header, title, sub, close, map, list, node, mobile media query) currently in `stats-page.css` (lines ~402–545).
2. `StatsPage.tsx`: `import './path-modal.css';` (replaces the stats-page.css copy — delete the moved block).
3. `FeedDialogs.tsx`: `import '../path-modal.css';`
4. (Alternative if we want zero new files: `import '../stats-page.css'` in FeedDialogs — quicker but drags unrelated stats styles into the feed chunk and couples feed → stats page. Prefer the shared file.)

Result: modal renders as a proper dark panel, compact Close button, mono uppercase accent title — identical look to the stats page modal.

### Phase 2 — Tree readability (FeedPathViews.tsx + feed-page.css)
1. **Node identity/fallback:**
   - Unmatched hop → title **"Unknown hop"** (not "Unmatched repeater"), meta shows only its short hash + seen count; grey **dashed** dot.
   - Matched hop → repeater name (existing lookup), accent solid dot.
   - Ambiguous hop → amber dot + "ambiguous" tag (data already has `ambiguous`).
   - Observer → unchanged (green RX dot).
2. **Meta declutter** — matched nodes: `IATA · n/n seen` (drop role word, raw nodeId prefix, raw hash — name is already the title; put full nodeId in `title` tooltip). Unmatched: `hop AD · 1/1 seen`. Observer: `observer · E0218FD9`.
3. **Header** — `5 hops · 2 matched · 1 ambiguous` (compute ambiguous count from paths) + `N branches` only when >1. Drop the confusing bare "2/5 hops matched".
4. **Sizing/typography** — modal width 1180px → `min(960px, calc(100vw - 32px))` (matches stats modal); tree text 12px → 13–14px; more row padding.

### Phase 3 — Put the map back in the modal (the "easy to see" win)
1. Reuse the existing `PathMap` (already used by `FeedMapPanel` / `PacketDetailPanel`) — render it in the modal body **above** the tree: header → map (`stats-page__path-modal-map` style, height ~340–420px) → tree.
2. Inputs already available: `lazyPath.paths[].coordinates`, `observerPositions` from packet, `nodeMap`.
3. Optional v1 skip: hover/click tree node → highlight on map (note for later).

### Phase 4 — Housekeeping
- The July-29 feed refactor (untracked: `FeedDialogs.tsx`, `FeedPathViews.tsx`, `feedModel.ts`, `feedState.ts`, `PacketPathMap.tsx` + UKFeedPage rewrite) is already live in the deployed build but uncommitted. **Commit the refactor + this fix together** as one PR to `gadgethd/ukmesh` (branch from `main`). Do **not** `git add -A` (dirty tree carries unrelated changes; `.tokensave/` trap).

## 5. Implementation steps (fix session)

1. `cd ~/meshcore-analytics` — branch `fix/repeater-tree-modal` from main.
2. Create `frontend/src/pages/path-modal.css` (move block from stats-page.css), update imports in `StatsPage.tsx` + `FeedDialogs.tsx`.
3. Update `FeedPathViews.tsx` (labels, meta, header, dot variants) + `feed-page.css` (dot modifiers, typography, modal width).
4. Add modal map: embed `PathMap` in `FeedDialogs.tsx` body above `PacketPathTree`.
5. Update/extend `feedState.test.ts` if it covers tree status logic; add component-level checks where cheap.
6. Quality gates: `npx tsc --noEmit`, `npm test`, `npm run lint:css`, `npm run build`.
7. Deploy (with Ben's go-ahead): `docker compose -f docker-compose.yml -f docker-compose.live.yml build app-ukmesh website-ukmesh && … up -d` — or Ben deploys himself. **Restart via compose only, never bare `up`** (shared external network).
8. Verify live: open a packet's Repeater tree on app.ukmesh.com → screenshot; confirm styled panel, readable tree, map present. Screenshot to Ben.

## 6. Risks / notes
- **Working tree is dirty** (many unrelated M files) — touch only the fix files; never reset/stash.
- Shared external network `meshcore-analytics_default` — compose down/up must keep the `live` overlay file.
- Anubis sits in front of the app — after deploy, first load may be challenged; bot policy already allows normal browsers.
- Website build (`public-website` target) shares `UKFeedPage` (`feed` route in main.tsx) — same fix covers both images; rebuild both.
- No backend changes needed (path-lazy API is fine).
