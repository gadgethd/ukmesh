# MeshCore Analytics — Frontend UI/UX Audit

**Date:** 2026-08-01
**Branch audited:** `fix/repeater-tree-modal` (working tree, uncommitted changes included)
**Scope:** `frontend/src/**` (57 `.tsx`, 63 `.ts`, 12 `.css`), `frontend/index.html`, `backend/src/backend-site/login.html`
**Type:** Read-only static audit. **No fixes were applied.**

> **Resolution update — 2026-08-01:** The audit itself was read-only, but the
> follow-up implementation has now resolved UI-01 through UI-28 in the current
> working tree. Verification includes 38 frontend unit tests, 27 desktop/axe
> browser tests, 21 responsive/mobile browser tests, production builds, the CSS
> structure gate, and the asset-size budget. The browser suite covers every
> public route, keyboard/focus flows, touch-only map controls, and modal geometry
> from 320px through 1440px. The source gates report no undefined CSS custom
> properties, no text below the 10px floor, no duplicate selectors, no missing
> button `type`, no missing table-header `scope`, and no diff whitespace errors.

---

## 0. How to use this document

Each finding has a stable ID (`UI-01`…`UI-28`), a severity, exact file/line anchors, the *measured* evidence, the user-visible impact, and a concrete fix with acceptance criteria. Findings are independent unless a **Depends on** line says otherwise.

**Recommended order of work:** §7 has a sequenced plan. Do not work top-to-bottom by ID — several P1 items share a root cause and are cheaper to fix together.

**Before you start:** read §6 ("Verified correct — do not re-investigate"). Several plausible-looking problems were checked and are *not* bugs. Re-chasing them wastes time and risks regressions.

---

## 1. Method

1. **Full read** of `tokens.css`, `globals.css`, `map-app.css`, `site-shell.css`, `site-content.css`, `network-intelligence.css`, `path-modal.css`, plus targeted reads of `feed-page.css`, `spam-page.css`, `stats-page.css`, `docs-pages.css`, `owner-portal.css`.
2. **Undefined CSS custom property scan** — parsed every `--x:` definition and every `var(--x)` usage across all CSS/TSX, flagged usages with no definition and no fallback.
3. **WCAG 2.1 contrast computation** — a scripted sRGB relative-luminance calculator (script in §8) was run on every foreground/background pair discovered, including alpha compositing for `rgba()` and `opacity`.
4. **Typography scan** — every `font-size` and `font` shorthand across all CSS, normalised px/rem, flagged `< 11px`.
5. **Cascade verification** — checked the *built* CSS in `frontend/dist/assets/` and Vite's preload manifest to confirm real-world stylesheet load order, because specificity ties are resolved by load order here.
6. **Dead-code cross-check** — every class named in a finding was checked against all TSX/HTML to confirm it actually renders. Findings on dead classes are demoted to P3 and labelled.
7. **Structured a11y scan** — form-control labelling, `<th scope>`, `role=` correctness, icon-only buttons, `<button type>`, focus indicators, touch-target sizes.

Scripts used are reproduced in §8 so results can be regenerated.

---

## 2. Severity definitions

| Sev | Meaning |
|---|---|
| **P0** | Content is unreadable, invisible, or unreachable. Blocks the user. |
| **P1** | Serious degradation — feature unusable for a class of users (keyboard, touch, low vision), or visibly broken layout. |
| **P2** | Clear defect with a workaround; fails a WCAG AA criterion; noticeably wrong visual. |
| **P3** | Polish, consistency, dead code, latent risk. |

---

## 3. Summary

| ID | Sev | Area | One-line |
|---|---|---|---|
| UI-01 | P0 | Map | Path-segment popover text is dark-on-dark at **1.13:1** — effectively invisible |
| UI-02 | P0 | Feed modal | Repeater-tree modal is 960px inside a hard-coded 760px shell — **clipped off-screen at 810–1160px viewports** |
| UI-03 | P0 | Map | Light basemap mode: 9 of 13 marker colours below 3:1; selected node at **1.13:1** |
| UI-04 | P1 | Repeater search | `--bg-tertiary` undefined → **no hover/keyboard-focus highlight** in the search dropdown |
| UI-05 | P1 | Focus | Global focus ring uses `:where()` (specificity 0) and is defeated by 8 `outline:none` rules; range slider has **no** focus indicator |
| UI-06 | P1 | Map tools | LOS + Plan-repeater tools `display:none` on **all** coarse-pointer devices, no alternative entry point |
| UI-07 | P1 | Owner portal | `--border-soft` undefined → telemetry chart/stat cards render with **no border at all** |
| UI-08 | P1 | Typography | **79** declarations below 11px, down to **7px**; worst is 7px uppercase mono |
| UI-09 | P1 | Packet feed | `.packet-item__pin` is 7px text animating down to **1.92:1** |
| UI-10 | P2 | Topology | `role="img"` on the SVG hides its own focusable nodes from screen readers |
| UI-11 | P2 | Topology | RF-validation table has **no header row** (neither visual nor ARIA) |
| UI-12 | P2 | Tables | All 5 data tables: `<th>` missing `scope` |
| UI-13 | P2 | Buttons | 2 close buttons have no accessible name (bare `✕`) |
| UI-14 | P2 | Feed | Packet search input is placeholder-only — no accessible name |
| UI-15 | P2 | Map | Link quality is red/amber/green **colour-only** (width differs by 0.4px) |
| UI-16 | P2 | Status | `--offline` (`#546e7a`) used as status text at **3.40:1** |
| UI-17 | P2 | Touch | Topbar info button is 22px wide; packet watch button 20px — below the 24px floor |
| UI-18 | P2 | Nav | `.site-nav__badge` white-on-red at **3.76:1** |
| UI-19 | P3 | Repeater page | `--bg-secondary` undefined → 5 live surfaces lose their background |
| UI-20 | P3 | Buttons | 13 `<button>` without `type` |
| UI-21 | P3 | Dead CSS | ~38 orphaned class rules (`dev-monitor__*`, `dev-telemetry*`, `dev-status-*`, 4 others) |
| UI-22 | P3 | Tokens | `--accent-rgb` fallback is green while `--accent` is cyan |
| UI-23 | P3 | Site | `.site-home__card` declares background/border/radius twice |
| UI-24 | P3 | Tokens | `--map-label-color` / `--map-link-color` / `--color-gold` / `--color-purple` / `--font-sans` never consumed |
| UI-25 | P3 | Backend login | Input boundary contrast 1.06–1.82:1; errors use `role="status"` not `alert` |
| UI-26 | P3 | Nav | Mobile nav styled twice (globals.css + site-shell.css) and reconciled with an `html ` specificity hack |
| UI-27 | P3 | Map | `circle-stroke-width: 0` with live `stroke-color`/`stroke-opacity` — dead paint |
| UI-28 | P3 | Map | `.map-annotation` squeezed to ~140px between 641–800px viewports |

---

## 4. P0 findings

### UI-01 — Path-segment evidence popover is invisible (1.13:1)

**Files**
- `frontend/src/styles/map-app.css:150–156` (the popover styles)
- `frontend/src/styles/map-app.css:914–921` (`.maplibregl-popup-content`)
- `frontend/src/components/Map/DeckGLOverlay.tsx:484–517` (constructs it)

**What's wrong**
`DeckGLOverlay.tsx:488` builds a `div.path-explanation-popover` and injects it into a MapLibre popup via `setDOMContent`. The popup shell is forced dark:

```css
/* map-app.css:914 */
.maplibregl-popup-content {
  background: var(--bg-panel) !important;   /* #0d1520 */
  color: var(--text-primary) !important;
}
```

But the popover overrides the inherited colour with a **light-theme slate palette**:

```css
/* map-app.css:150–156 */
.path-explanation-popover { min-width: 220px; color: #172033; … }
.path-explanation-popover dt { color: #64748b; }
.path-explanation-popover p  { margin: 7px 0 0; color: #475569; font-size: 11px; }
```

`<strong>` (title) and `<dd>` (all four values) have no colour of their own, so they inherit `#172033`.

**Measured**

| Element | Colour | On `#0d1520` | WCAG AA (4.5:1) |
|---|---|---|---|
| Title `<strong>` + all `<dd>` values | `#172033` | **1.13:1** | ✗ catastrophic |
| Labels `<dt>` | `#64748b` | 3.85:1 | ✗ |
| Summary `<p>` | `#475569` | 2.42:1 | ✗ |

**Impact** Clicking a beta path segment on the map opens a popup whose entire payload — Confidence, Observers, Evidence, Top alternatives, and the summary — is unreadable. Only the four `<dt>` labels are faintly visible. This is a total content failure, not a contrast nit.

**Fix** Replace the three hard-coded slate values with design tokens in `map-app.css:150–156`:

```css
.path-explanation-popover { min-width: 220px; color: var(--text-primary); font-family: var(--font-body); }
.path-explanation-popover > strong { display: block; margin-bottom: 7px; font-size: 13px; color: var(--text-primary); }
.path-explanation-popover dt { color: var(--text-secondary); }
.path-explanation-popover dd { margin: 0; font-weight: 600; color: var(--text-primary); }
.path-explanation-popover p { margin: 7px 0 0; color: var(--text-secondary); font-size: 11px; }
```

Note `#64748b` → `var(--text-secondary)` (`#8aa6c4`, 7.28:1) and `#475569` → `var(--text-secondary)`; do **not** use `--text-muted` for the 11px summary (6.17:1 is fine but `--text-secondary` is the established pairing for popup body text elsewhere in this file).

**Acceptance** Every string in the popover ≥ 4.5:1 against `#0d1520`. Verify with the script in §8.

**Repro** Open the app map, enable a beta path overlay, click a path segment.

---

### UI-02 — Repeater-tree modal overflows its shell and is clipped off-screen

**Files**
- `frontend/src/components/ui/Dialog.tsx:46` — `<Modal className="ui-dialog-modal">` (hard-coded, not overridable)
- `frontend/src/styles/globals.css:2086–2089` — `.ui-dialog-modal { width: min(760px, 100%); … }`
- `frontend/src/pages/ukmesh/feed-page.css:406–409` — `.uk-feed-path-modal { width: min(960px, calc(100vw - 32px)); … }`
- `frontend/src/pages/path-modal.css:7–20` — `.stats-page__path-modal { width: min(960px, 100%); … }`
- `frontend/src/pages/ukmesh/FeedDialogs.tsx:39–45` — the dialog that combines them

**What's wrong**
`Dialog.tsx` always renders `<ModalOverlay><Modal className="ui-dialog-modal"><Dialog className={…}>`. The `className` prop styles the **innermost** element, but the **middle** element is permanently capped at `760px`. The repeater-tree dialog asks for `960px`, which its containing block cannot provide, so the dialog overflows the modal box to the right.

Cascade was verified against the build: `dist/assets/path-modal-*.css` is preloaded **before** `dist/assets/UKFeedPage-*.css` (confirmed in the Vite preload manifest inside `dist/assets/index-*.js`), and both selectors are specificity `(0,1,0)`, so **`.uk-feed-path-modal` wins** → the dialog computes to `min(960px, 100vw - 32px)` inside a `760px` parent.

**Measured geometry** (overlay has `padding: 16px`, `justify-content: center`)

```
modalLeft  = 16 + (100vw - 32 - 760) / 2
dialogW    = min(960, 100vw - 32)
dialogRight = modalLeft + dialogW
```

| Viewport | Modal | Dialog | Dialog right edge | Result |
|---|---|---|---|---|
| 792px | 760px | 760px | 776px | fits |
| 850px | 760px | 818px | 863px | **13px clipped** |
| 900px | 760px | 868px | 938px | **38px clipped** |
| 1024px | 760px | 960px | 1092px | **68px clipped** |
| 1160px | 760px | 960px | 1160px | boundary |
| 1440px | 760px | 960px | 1300px | fits but **100px off-centre** |

Because `ModalOverlay` is `position: fixed`, the overflow does **not** create a document scrollbar — the right-hand strip is simply unreachable.

**Impact** On tablets and small laptops (~810–1160px, a very common range) the right edge of the Predicted Repeater Tree modal — including part of the map and the right side of the tree rows — is cut off with no way to scroll to it. Above 1160px the modal is visibly off-centre by 100px. This is the same class of defect the branch name suggests is being chased.

**Fix — pick one, (A) preferred**

**(A) Make the modal shell width-neutral.** In `globals.css:2086`:

```css
.ui-dialog-modal {
  width: auto;              /* was: min(760px, 100%) */
  max-width: calc(100vw - 32px);
  max-height: calc(100dvh - 32px);
}
```
Then give the *default* dialog its own width so nothing else regresses — add to `globals.css` near `.ui-dialog` (line 2091):
```css
.ui-dialog { width: min(760px, calc(100vw - 32px)); }
```
Audit every current `Dialog` consumer afterwards (`FeedDialogs.tsx`, `StatsDecodedPathDialog.tsx`, `App.tsx:656`) to confirm none relied on the 760px cap.

**(B) Add a size prop to `Dialog`.** Extend `DialogProps` with `modalClassName?: string`, pass it through to `<Modal>`, and give the path modal a `uk-feed-path-modal-shell` class with `width: min(960px, calc(100vw - 32px))`. Lower blast radius, more code.

**Also fix regardless of option:** `.stats-page__path-modal` (`path-modal.css:11`) and `.uk-feed-path-modal` (`feed-page.css:407`) both set `width` on the same element with equal specificity. Delete the `width` and `max-height` from one of them so the winner is not decided by chunk load order. Keep the value on `.uk-feed-path-modal` and reduce `path-modal.css` to the shared visual styling only.

**Acceptance** At 850, 900, 1024, 1280 and 1440px the modal is fully visible, horizontally centred, and its right edge is ≥ 16px inside the viewport. At 375px it still fits with the `≤640px` overrides.

---

### UI-03 — Light basemap mode makes most map markers invisible

**Files**
- `frontend/src/components/Map/mapSourceLayers.ts:22–47` (node dots), `:64` (selected halo), `:79–83` (selected marker), `:127–132`, `:165–170`, `:251–263`
- `frontend/src/components/Map/geojsonBuilders.ts:299–304`, `:345–350` (link colours)
- `frontend/src/components/Map/mapConfig.ts:183–207` (`MAP_STYLE_LIGHT`, `background-color: #edf2f7`)
- `frontend/src/App.tsx:121, 257` (`map-theme` toggle)

**What's wrong**
`mapConfig.ts` correctly themes the basemap raster **and the vector label colours** (`MAP_LABEL_COLORS.light/dark`). But every overlay drawn on top — node dots, selection rings, links, coverage fills — uses a single hard-coded palette tuned for the dark basemap. `MapLibreMap.tsx:217–263` swaps the raster source and label colours on theme change and nothing else.

Making this worse, the main node layer has `'circle-stroke-width': 0` (`mapSourceLayers.ts:45`), so there is no outline to rescue a low-contrast fill.

**Measured** — WCAG 1.4.11 non-text contrast requires **≥ 3:1**. Measured against each theme's `background-color` (`#080d14` dark, `#edf2f7` light):

| Marker | Colour | Dark | Light |
|---|---|---|---|
| Repeater (default) | `#00c4ff` | 9.59 ✓ | **1.80 ✗** |
| ChatNode (role 1) | `#ff9f43` | 9.55 ✓ | **1.81 ✗** |
| RoomServer (role 3) | `#a78bfa` | 7.16 ✓ | **2.42 ✗** |
| Sensor (role 4) | `#34d399` | 10.13 ✓ | **1.71 ✗** |
| Inferred | `#7dd3fc` | 11.68 ✓ | **1.48 ✗** |
| Replay active | `#fbbf24` | 11.67 ✓ | **1.48 ✗** |
| Hex-clash relay | `#22c55e` | 8.55 ✓ | **2.02 ✗** |
| **Selected node** | `#8af4ff` | 15.27 ✓ | **1.13 ✗** |
| **Selected stroke** | `#ffffff` | 19.48 ✓ | **1.13 ✗** |
| Selected halo | `#22e0ff` | 12.22 ✓ | **1.42 ✗** |
| Stale / offline | `#6b7280` | 4.03 ✓ | 4.29 ✓ |
| Link-only stale | `#4b5563` | **2.58 ✗** | 6.71 ✓ |
| Hex-clash offender | `#ef4444` | 5.18 ✓ | 3.34 ✓ |

Link colours (`geojsonBuilders.ts`) fail identically in light mode: `#22c55e` 2.02, `#fbbf24` 1.48, `#d1d5db` ≈1.2.

**Impact** Turning on the light basemap makes the map largely unusable: online repeaters, chat nodes, sensors, inferred nodes and all link lines wash out, and **the currently selected node is the single least visible element on screen (1.13:1)** — the user cannot see what they clicked. Note also `#4b5563` fails in *dark* mode (2.58:1).

**Fix** Introduce a theme-aware overlay palette next to `MAP_LABEL_COLORS` in `mapConfig.ts`:

```ts
export const MAP_OVERLAY_COLORS = {
  dark:  { repeater: '#00c4ff', companion: '#ff9f43', roomServer: '#a78bfa', sensor: '#34d399',
           inferred: '#7dd3fc', replay: '#fbbf24', stale: '#6b7280', linkOnlyStale: '#7b8794',
           clashRelay: '#22c55e', clashOffender: '#ef4444',
           selected: '#8af4ff', selectedStroke: '#ffffff', selectedHalo: '#22e0ff' },
  light: { /* darkened equivalents, each verified ≥ 3:1 on #edf2f7 */ },
} as const;
```
Then:
1. Thread `mapLight` into `mapSourceLayers.ts` and use `MAP_OVERLAY_COLORS[theme]` for every `circle-color`, `fill-color`, `line-color` literal.
2. Thread the theme into `geojsonBuilders.ts` (`buildLinksGeoJSON`, and the predicted-links builder at :299) so link colours are chosen per theme.
3. Add a contrasting outline to the node layer instead of `stroke-width: 0`: `'circle-stroke-width': 1`, `'circle-stroke-color'` = `#080d14` in dark / `#ffffff` in light. This alone lifts every marker over 3:1 and is the cheapest partial fix if the full palette work is deferred.
4. Fix `#4b5563` → a lighter grey for dark mode (it fails there today).
5. `MapLibreMap.tsx:217–263` already re-applies style on `mapLight` change — extend that effect to call `map.setPaintProperty` for the overlay layers, or rebuild the layer set.

**Acceptance** Every marker/link colour ≥ 3:1 against its theme's background, in both themes. Selected node clearly distinguishable in light mode.

**Note** `SpamTransparencyPage.tsx:177,488` and `OwnerMapView.tsx:111` use `MAP_STYLE` (dark) only, so they are unaffected — do not change them.

---

## 5. P1–P3 findings

### UI-04 — P1 — Repeater search dropdown has no hover or keyboard-focus highlight

**File** `frontend/src/styles/globals.css:775–778`

```css
.repeater-search-box__result:hover,
.repeater-search-box__result[data-focused] {
  background: var(--bg-tertiary);   /* NEVER DEFINED ANYWHERE */
}
```

`--bg-tertiary` is not defined in any stylesheet. Per the CSS Custom Properties spec, a `var()` referencing an undefined property with no fallback makes the declaration *invalid at computed-value time*; `background` is a shorthand, so **all** background longhands reset to their initial values → `background-color: transparent`. The rule does nothing.

`[data-focused]` is the attribute react-aria-components sets on the active `ListBoxItem` (`Combobox.tsx:105–108` passes `optionClassName` straight to `ListBoxItem`). So arrow-keying through the repeater search results produces **zero visual feedback** — the user cannot tell which option Enter will select. Mouse hover is equally dead.

**Impact** Keyboard operation of the primary search on `/repeater` is effectively broken (WCAG 2.4.7 Focus Visible).

**Fix** Use a defined token. In `globals.css:777`:
```css
.repeater-search-box__result:hover,
.repeater-search-box__result[data-focused] { background: var(--bg-hover); }
```
`--bg-hover` (`#162030`) is the established hover surface and is what the equivalent map search uses (`map-app.css:115`). Consider also adding `color: var(--accent);` to match `.node-search__result[data-focused]`.

**Acceptance** Arrow keys visibly move a highlight through the dropdown; hover highlights too.

---

### UI-05 — P1 — Global focus ring is defeated by `outline: none` in 8 places

**Files**
- Rule: `frontend/src/styles/globals.css:2124–2127`
- Overrides: `globals.css:726`, `globals.css:1281`, `globals.css:1740`, `map-app.css:80`, `owner-portal.css:24`, `feed-page.css:839`, `docs-pages.css:247`, `network-intelligence.css:40`

**What's wrong** The app defines one global focus indicator:

```css
/* globals.css:2124 */
:where(a, button, input, select, textarea, [tabindex]):focus-visible,
[data-focus-visible] { outline: 3px solid var(--accent); outline-offset: 2px; }
```

`:where()` contributes **zero** specificity, so this compound weighs only `(0,1,0)` — the same as a single class. Every component rule that sets `outline: none` therefore wins, and page CSS chunks load *after* `index-*.css` (verified in `dist/`), so even ties go to the component.

| Override | Replacement indicator | Verdict |
|---|---|---|
| `globals.css:726` `.repeater-search-box__input:focus` | `border-color: var(--accent)` | weak (1px) but present |
| `globals.css:1281` `.regions-search__input:focus` | `border-color: var(--accent)` | weak but present |
| **`globals.css:1740` `.regions-rel__neighbour-range`** | **none** | **✗ no indicator at all** |
| `map-app.css:80` `.node-search__input` | `:focus { border-color }` | weak but present |
| `owner-portal.css:24` `.owner-login__input:focus` | `border-color: var(--accent)` | weak but present |
| `feed-page.css:839` `.uk-feed-search` | `:focus { border-color }` | weak but present |
| **`docs-pages.css:247` `.health-card--interactive:focus-visible`** | shares hover styling; `border-color: var(--border-bright)` (rgba .25) | **✗ near-invisible, indistinguishable from hover** |
| **`network-intelligence.css:40` `.topology-page__node:focus`** | fill change identical to `:hover` | **✗ weak, and see UI-10** |

The range slider (`globals.css:1733–1742`) is `<input type="range">` — fully keyboard-operable with arrow keys — and has **no** focus indicator whatsoever.

**Impact** Keyboard users lose or barely retain the focus indicator on the login field, both search fields, the neighbour-range slider, the health cards and the topology nodes. WCAG 2.4.7 (A) failure on at least the three bolded rows.

**Fix**
1. **Raise the global rule above component rules.** In `globals.css:2124`, drop `:where()` so the selector carries real weight, and add `!important` only if needed after re-testing:
   ```css
   a:focus-visible, button:focus-visible, input:focus-visible, select:focus-visible,
   textarea:focus-visible, [tabindex]:focus-visible, [data-focus-visible] {
     outline: 3px solid var(--accent);
     outline-offset: 2px;
   }
   ```
   This makes it `(0,1,1)`, beating bare-class rules.
2. **Delete the eight `outline: none` declarations** listed above. Keep the `border-color` transitions — they are a nice *additional* affordance, just not a substitute.
3. For `.regions-rel__neighbour-range` specifically, if a ring on the track looks wrong, style the thumb instead:
   ```css
   .regions-rel__neighbour-range:focus-visible::-webkit-slider-thumb { box-shadow: 0 0 0 3px var(--accent-glow); }
   .regions-rel__neighbour-range:focus-visible::-moz-range-thumb     { box-shadow: 0 0 0 3px var(--accent-glow); }
   ```
4. For `.health-card--interactive`, split hover and focus so they are not the same rule; let the global ring apply on focus.
5. For `.topology-page__node`, see **UI-10** — fix together.

**Acceptance** Tab through `/`, `/repeater`, `/feed`, `/topology`, `/docs`, `/login` and the app map: every stop shows a 3px cyan ring or an equally prominent, focus-only indicator.

---

### UI-06 — P1 — LOS and Plan-repeater tools are unreachable on every touch device

**Files**
- `frontend/src/styles/map-app.css:1673–1677`
- `frontend/src/components/Map/MapLibreMap.tsx:1282–1310`
- `frontend/src/components/app/MobileControls.tsx` (whole file — no equivalent)

```css
/* map-app.css:1673 */
@media (max-width: 640px), (pointer: coarse) {
  .map-tools { display: none !important; }
}
```

`.map-tools` holds the two map tool buttons: **LOS** (custom line-of-sight, `MapLibreMap.tsx:1283–1292`) and **Plan repeater** (`:1294–1310`, gated on `viewshedEnabled`). `MobileControls.tsx` renders map modes, the layer filter grid, node search and the watchlist — but has no LOS or planner entry, and no other component references `customLosMode` / `planRepeaterMode`.

**Impact** `(pointer: coarse)` matches **any** touch-primary device, including 12.9" tablets and touchscreen laptops at 1400px+, not just phones. On all of them two whole features silently disappear with no alternative path. This is functionality loss, not just a layout adaptation.

**Fix — choose based on product intent**
- **If the tools should work on touch:** drop `(pointer: coarse)` from the media query so only `max-width: 640px` hides them, and add both buttons to `MobileControls.tsx` for the phone case. The buttons already carry text labels ("LOS") so they only need `min-height: 44px` and adequate padding — the global coarse-pointer rule (`globals.css:2130`) supplies the height.
- **If they are genuinely desktop-only:** keep the query but surface *why*. Add a disabled/explanatory affordance in `MobileControls.tsx` (e.g. a note in the Layers section: "Line-of-sight and repeater planning require a mouse"). Silent removal is the actual defect.

**Acceptance** On an iPad-class viewport, either the tools are usable, or the UI explains their absence.

---

### UI-07 — P1 — Owner-portal telemetry cards render with no border

**Files** `frontend/src/pages/owner-portal.css:205`, `:218`

```css
.owner-telemetry-metric__chart { … border: 1px solid var(--border-soft); … }
.owner-telemetry-metric__stat  { … border: 1px solid var(--border-soft); … }
```

`--border-soft` is **never defined**. As in UI-04, the `border` shorthand is invalid at computed-value time, so all three longhands reset — critically `border-style` → `none`. **The border does not render at all** (it is not merely the wrong colour).

Both classes are live — confirmed used by `frontend/src/pages/owner/OwnerPortalCharts.tsx`.

Their only remaining separation from the panel is `background: linear-gradient(180deg, rgba(255,255,255,0.03), rgba(255,255,255,0.01))` — a 1–3% white wash, well under the 3:1 required for a UI component boundary (WCAG 1.4.11).

**Impact** In the owner portal, telemetry chart tiles and stat tiles have no visible edge and bleed into the panel background; the grid reads as one undifferentiated block.

**Fix** Either define the token once in `globals.css:26` (`:root`) —
```css
--border-soft: rgba(32, 80, 140, 0.22);
```
— or, simpler and consistent with the rest of the file, replace both usages with the existing `var(--border)`.

**Note** The other 9 `--border-soft` usages in `site-content.css` sit on **dead classes** (see UI-21) and have no user impact. Fix them only as part of the dead-code cleanup.

**Acceptance** Owner portal telemetry tiles have a visible 1px edge.

---

### UI-08 — P1 — 79 declarations render text below 11px, down to 7px

**Scan result** 79 `font-size`/`font` declarations across all CSS compute to `< 11px`. Body text is 13px (`globals.css:84`). Worst offenders:

| Size | File:line | Selector | Notes |
|---|---|---|---|
| **7px** | `map-app.css:629` | `.watchlist-panel li small` | + `text-transform: uppercase`, `--font-mono` |
| **7px** | `map-app.css:1218` | `.packet-item__pin` | see **UI-09** |
| **8px** | `map-app.css:662` | `.planner-comparison__summary span` | metric labels |
| 9px ×14 | `map-app.css:68,454,590,593,598,656,667,671,967,1554`; `network-intelligence.css:66,69`; `spam-page.css:233,287,438` | timeline meta, planner labels, validation stats, **`.sm-timeline th`** | `.sm-timeline th` is a public-page table header |
| 10px ×60 | across all files | — | pervasive |

`--font-mono` is `'Share Tech Mono'`, a narrow face with a small x-height, so 7–9px mono renders smaller than 7–9px Inter would. Uppercase + `letter-spacing` at these sizes compounds the problem.

**Impact** Genuinely hard to read at normal viewing distance, and unreadable for many users with low vision. None of it is decorative — `.watchlist-panel li small` carries the watch category, `.planner-comparison__summary span` labels the planner metrics, `.sm-timeline th` labels a public data table.

**Fix — staged, do not do a blanket find/replace**
1. **Floor at 10px, immediately:** raise every `< 10px` declaration to `10px`. That is 18 declarations (all the 7px, 8px and 9px rows above). Low risk — these are in fixed-width panels with room.
2. **Then raise the floor to 11px** for anything that is a *label the user must read* rather than a dense-grid affordance: `.watchlist-panel li small`, `.planner-comparison__summary span`, `.planner-comparison li strong`, `.sm-timeline th`, `.topology-page__validation-stats span`, `.topology-page__validation-table [role='row']`, `.status-page__checks strong`, `.status-page__privacy`.
3. **Check for reflow after each step.** The at-risk containers are `.planner-comparison` (290px fixed, `map-app.css:643`), `.filter-panel` (200px fixed, `map-app.css:159`), `.node-legend` (max 190px, `map-app.css:1462`) and `.packet-item` (see UI-09 note on its 9-column grid). Widen those panels rather than reverting the font sizes.
4. Add a lint guard: extend `frontend/scripts/check-css-structure.mjs` to fail on any computed `font-size < 10px`.

**Acceptance** No CSS declaration under 10px; the eight listed label selectors at ≥ 11px; no clipped panel content at 1280×800.

---

### UI-09 — P1 — Pinned-packet marker is 7px text pulsing to 1.92:1

**File** `frontend/src/styles/map-app.css:1213–1227`

```css
.packet-item__pin { … font-size: 7px; color: var(--accent); opacity: 0.7;
                    animation: pin-pulse 2s ease-in-out infinite; }
@keyframes pin-pulse { 0%, 100% { opacity: 0.7; } 50% { opacity: 0.3; } }
```

**Measured** against the pinned row background (`rgba(0,196,255,0.08)` over `rgba(8,16,27,0.97)` ≈ `#071e2d`):

| Animation phase | Effective colour | Contrast |
|---|---|---|
| Peak (`opacity: 0.7`) | ≈ `#0292c0` | 4.77:1 — marginal at 7px |
| **Trough (`opacity: 0.3`)** | ≈ `#05506c` | **1.92:1 — invisible** |

**Impact** The pin indicator is the smallest text in the app *and* spends half of every 2s cycle below the visibility threshold. It also runs indefinitely; `prefers-reduced-motion` is honoured globally (`globals.css:2136–2146` forces `animation-duration: 0.01ms`), which covers WCAG 2.2.2 for users who set that preference but not for anyone else.

**Fix**
```css
.packet-item__pin { font-size: 10px; color: var(--accent); opacity: 1; }
```
Drop the `pin-pulse` animation and the `@keyframes` block entirely (also remove `.packet-item__pin` from the `prefers-reduced-motion` list at `map-app.css:1378`). If a "recently pinned" cue is wanted, use a one-shot 0.4s flash rather than an infinite loop.

**Watch out:** `.packet-item` is a 9-column grid (`map-app.css:1174`) whose fixed columns total 302px including gaps. `.packet-feed` is `min(420px, 100vw - 32px)` on desktop but `calc(100% - 32px)` with `max-height: 180px` on mobile (`map-app.css:1619`). At a 360px viewport the content box is ~292px — already 10px short of the fixed columns, so the flexible summary column collapses to 0 and `overflow-x: hidden` clips the right-hand cells. Raising the pin to 10px slightly worsens this. **Fix the grid at the same time**: on `≤640px`, reduce `.packet-item` to the columns that matter (time, type, summary, hops) and hide the rest, rather than relying on clipping.

**Acceptance** Pin indicator legible and static; packet rows show all their cells at 360px.

---

### UI-10 — P2 — `role="img"` hides the topology graph's own focusable nodes

**File** `frontend/src/pages/TopologyPage.tsx:228` and `:255–284`

```tsx
<svg viewBox="0 0 1000 600" role="img" aria-label={`${plot.nodes.length} positioned repeaters and ${plot.links.length} links`}>
  …
  <circle role="button" tabIndex={0} aria-label={`${node.name …}, ${node.degree} links`} … >
    <title>…</title>
  </circle>
```

`role="img"` makes the SVG a single leaf node in the accessibility tree — **all descendants are removed from it**. The `role="button" tabIndex={0}` circles remain keyboard-focusable but are no longer exposed to assistive tech.

**Impact** A screen-reader user tabs into the graph and lands on N focus stops that announce nothing, with no way to know what they are or that Enter/Space selects a repeater. Combined with **UI-05** (`network-intelligence.css:40` `outline: none` on `:focus`), a sighted keyboard user also gets only a subtle fill change.

**Fix**
1. Change `role="img"` → `role="group"` (or drop the role and keep `aria-label`) on `TopologyPage.tsx:228` so descendants stay in the a11y tree.
2. Move the graph-level summary into a visually-hidden live description so the count is still announced:
   ```tsx
   <p className="ui-visually-hidden" id="topology-graph-desc">
     {plot.nodes.length} positioned repeaters and {plot.links.length} links
   </p>
   <svg viewBox="0 0 1000 600" role="group" aria-labelledby="topology-graph-desc">
   ```
   (`.ui-visually-hidden` already exists at `globals.css:2064`.)
3. Remove `outline: none` from `network-intelligence.css:40` and split `:focus` out of the `:hover, :focus, --active` group so focus has a distinct look. SVG `outline` support is patchy — prefer an explicit ring:
   ```css
   .topology-page__node:focus-visible { stroke: #fff; stroke-width: 3.5; }
   ```

**Acceptance** VoiceOver/NVDA announces each focused repeater's name and link count; keyboard focus is visually distinct from hover.

---

### UI-11 — P2 — RF-validation table has no header row

**File** `frontend/src/pages/TopologyPage.tsx:339–347`; CSS `frontend/src/pages/network-intelligence.css:67–69`

```tsx
<div className="topology-page__validation-table" role="table" aria-label="RF model mismatches">
  {rfValidation.mismatches.slice(0, 20).map((link) => (
    <div role="row" key={…}>
      <span role="cell">…</span>   {/* link pair */}
      <span role="cell">…</span>   {/* observations */}
      <span role="cell">…</span>   {/* path loss dB */}
      <strong role="cell">…</strong> {/* classification */}
```

There is no header row — neither a `role="row"` of `role="columnheader"` cells, nor any visual heading. The CSS confirms it: `.topology-page__validation-table` only styles `[role='row']` and `strong`.

**Impact** Four unlabelled columns. A sighted user sees `Node A ↔ Node B | 42 obs · 8 strong | 131.2 dB | model mismatch` and must infer every column. A screen-reader user navigating the ARIA table gets no column context at all. The ARIA table role actively *promises* headers that do not exist.

**Fix** Add a header row as the first child, and style it:

```tsx
<div className="topology-page__validation-table" role="table" aria-label="RF model mismatches">
  <div role="row" className="topology-page__validation-table-head">
    <span role="columnheader">Link</span>
    <span role="columnheader">Observations</span>
    <span role="columnheader">Modelled path loss</span>
    <span role="columnheader">Classification</span>
  </div>
  …
```
```css
/* network-intelligence.css, next to :68 */
.topology-page__validation-table-head {
  color: var(--text-muted);
  font: 10px var(--font-mono);
  text-transform: uppercase;
  letter-spacing: 0.06em;
}
```
The existing `[role='row']` grid rule already supplies the 4-column template, so the header aligns automatically. Bump the row font-size per **UI-08** while here.

---

### UI-12 — P2 — No `<th>` in the codebase has `scope`

**Files**
- `frontend/src/pages/SpamTransparencyPage.tsx:363`
- `frontend/src/pages/StatusPage.tsx:232`
- `frontend/src/pages/ukmesh/UKRepeaterSearchPage.tsx:601–605`
- `frontend/src/pages/ukmesh/PacketDetailPanel.tsx:137`, `:329`, `:393`

All 6 `<thead>` header rows use bare `<th>`. Without `scope="col"`, screen readers must guess the header/data association; in multi-header tables they frequently get it wrong.

**Fix** Add `scope="col"` to every `<th>` in a `<thead>` row. Example for `PacketDetailPanel.tsx:137`:
```tsx
<tr><th scope="col">Bits</th><th scope="col">Field</th><th scope="col">Value</th><th scope="col">Binary</th></tr>
```
Purely additive, no visual change.

---

### UI-13 — P2 — Two close buttons have no accessible name

**Files**
- `frontend/src/pages/ukmesh/PacketDetailPanel.tsx:272` — `<button type="button" className="feed-detail__close" onClick={onClose}>✕</button>`
- `frontend/src/pages/ukmesh/UKFeedPage.tsx:680` — `<button className="uk-feed-stats__close" onClick={…}>✕</button>`

Both are labelled only by the bare glyph `✕` (U+2715). Screen readers announce it as "multiplication X", "times", or nothing depending on the AT and its punctuation settings.

Every other close control in the codebase is done correctly — `App.tsx:656` (`aria-label="Close"`), `MapLibreMap.tsx:1393` (`aria-label="Close node details"`), `WatchlistPanel.tsx:54`, `NodeSearch.tsx:172`, `Combobox.tsx:91`. These two are the outliers.

**Fix**
```tsx
/* PacketDetailPanel.tsx:272 */
<button type="button" className="feed-detail__close" onClick={onClose} aria-label="Close packet details">✕</button>

/* UKFeedPage.tsx:680 */
<button type="button" className="uk-feed-stats__close" onClick={() => setSelectedPacketHash(null)} aria-label="Clear selected packet">✕</button>
```
(The `type="button"` on the second also resolves one instance of **UI-20**.)

---

### UI-14 — P2 — Feed packet search has no accessible name

**File** `frontend/src/pages/ukmesh/UKFeedPage.tsx:530–536`

```tsx
<input type="search" className="uk-feed-search" placeholder="Search packets…" value={searchQuery} onChange={…} />
```

No `<label>`, no `aria-label`, no `id`/`htmlFor`. Placeholder text is not an accessible name (WCAG 4.1.2), and it disappears the moment the user types, removing the only cue about what the field filters.

This was the **only** unlabelled control found — every other input in the app either has an `aria-label` or is wrapped in a `<label>` (verified across all TSX).

**Fix**
```tsx
<input
  type="search"
  className="uk-feed-search"
  aria-label="Search packets"
  placeholder="Search packets…"
  value={searchQuery}
  onChange={(e) => setSearchQuery(e.target.value)}
/>
```

---

### UI-15 — P2 — Link quality is encoded by colour alone

**Files** `frontend/src/components/Map/geojsonBuilders.ts:299–316` and `:345–352`; thresholds in `mapConfig.ts:12–13`

```ts
const color = pathLoss == null ? '#a78bfa'
  : pathLoss <= LINK_GREEN_THRESHOLD_DB ? '#22c55e'   // good
  : pathLoss <= LINK_AMBER_THRESHOLD_DB ? '#fbbf24'   // marginal
  : '#ef4444';                                        // poor
width: pathLoss == null ? 1.6 : pathLoss <= …GREEN… ? 2.4 : pathLoss <= …AMBER… ? 2.0 : 1.6,
```

There *is* a width variation, but green→amber→red spans only **2.4px → 2.0px → 1.6px**. A 0.4px step on an anti-aliased map line is not a perceivable redundant channel. In practice the encoding is red/amber/green only — the exact pairing that deuteranopia and protanopia collapse (~8% of men).

**Impact** Users with red-green CVD cannot distinguish good from poor links, which is the core signal of the links overlay. WCAG 1.4.1 (Use of Color, level A).

**Fix — add a second channel.** Cheapest option that needs no legend change:
```ts
// in the link feature properties
dash: pathLoss == null ? [2, 2] : pathLoss <= GREEN ? [1, 0] : pathLoss <= AMBER ? [6, 3] : [2, 3],
width: pathLoss == null ? 1.6 : pathLoss <= GREEN ? 3.0 : pathLoss <= AMBER ? 2.0 : 1.4,
```
and in `mapSourceLayers.ts:112–115` add `'line-dasharray': ['get', 'dash']`.
*Caveat:* MapLibre does not support data-driven `line-dasharray` on a single layer. Either split into three filtered layers (one per band, each with a static dasharray) or widen the width spread to at least 1.4px → 3.0px, which is perceivable on its own. Update `.links-legend-inline` to show the line style, not just the swatch colour.

---

### UI-16 — P2 — `--offline` used as status text at 3.40:1

**Files**
- Token: `frontend/src/styles/globals.css:54` — `--offline: #546e7a`
- `frontend/src/styles/map-app.css:460` — `.node-dock__status--offline { color: var(--offline); }` at `font: 10px/1 mono`
- `frontend/src/components/Map/NodePopupContent.tsx:64, 101, 137` — `<span style={{ color: 'var(--offline)' }}>OFFLINE</span>` at 11px

**Measured**

| Background | Contrast | AA (4.5:1) |
|---|---|---|
| `--bg-base` `#080d14` | 3.61:1 | ✗ |
| `--bg-panel` `#0d1520` | **3.40:1** | ✗ |
| `--bg-panel-alt` `#111c2b` | 3.17:1 | ✗ |
| `--bg-active` `#1a2840` | **2.74:1** | ✗ (fails even large-text) |

Used as a background for dots (`map-app.css:782`, `:1349`) it is fine (non-text, 3:1). Used as **text** it fails everywhere.

**Fix** Do not change `--offline` itself — it is correct as a dot fill. Add a text-safe variant in `globals.css:52–56`:
```css
--offline:      #546e7a;   /* dots / fills only */
--offline-text: #93a7b2;   /* 7.34:1 on --bg-panel */
```
and switch the two text usages (`map-app.css:460`, `NodePopupContent.tsx:64`) to `var(--offline-text)`.

**Related, verify while here:** `--danger` (`#ff1744`) measures 4.46:1 on `--bg-panel-alt` and 3.84:1 on `--bg-active` — below AA for normal text on those two surfaces. It passes on `--bg-base` (5.06) and `--bg-panel` (4.77). Check whether `.node-dock__status--stale` (`map-app.css:461`) or `.packet-item` variants ever sit on `--bg-active`.

---

### UI-17 — P2 — Two touch targets below the 24px floor

**Files** `frontend/src/styles/globals.css:298–301`, `frontend/src/styles/map-app.css:633–641`

There *is* a coarse-pointer rule (`globals.css:2130–2134`):
```css
@media (pointer: coarse) {
  :where(button, input, select, textarea, [role="button"], [role="tab"], [role="option"]) { min-height: 44px; }
}
```
`min-height` clamps `height` regardless of specificity, so **heights are fixed**. But it sets no `min-width`, so explicit widths survive:

| Control | Declared | Effective on touch | WCAG 2.5.8 (24×24) |
|---|---|---|---|
| `.topbar__info-btn` (`globals.css:298`) | `22px × 22px` | **22 × 44** | ✗ width |
| `.packet-item__watch` (`map-app.css:633`) | `width: 20px` | **20 × 44** | ✗ width |
| `.topbar__shortcut-btn` (`globals.css:329`) | `28px` | 28 × 44 | ✓ |
| `.filter-panel__collapse` (`map-app.css:217`) | `24 × 24` | 24 × 44 | ✓ (boundary) |

Both failing controls remain visible on mobile: `globals.css:511` hides `.topbar__tool-btn:not(.topbar__shortcut-btn)` but not `.topbar__info-btn`.

**Fix** Add `min-width` to the coarse-pointer rule in `globals.css:2130`:
```css
@media (pointer: coarse) {
  :where(button, input, select, textarea, [role="button"], [role="tab"], [role="option"]) {
    min-height: 44px;
    min-width: 44px;
  }
}
```
Then check for layout damage — `min-width: 44px` on every button is broad. If it breaks the packet-row grid, instead widen the two offenders directly:
```css
.topbar__info-btn { width: 24px; height: 24px; }          /* globals.css:299 */
.packet-item__watch { width: 24px; min-width: 24px; }      /* map-app.css:634 — also widen the grid column at map-app.css:1174 */
```

---

### UI-18 — P2 — Nav badge fails contrast

**File** `frontend/src/pages/site-shell.css:219–230`

```css
.site-nav__badge { background: var(--color-red); color: #fff; font: 10px var(--font-mono); }
```
`--color-red` is `#ef4444` (`tokens.css:7`). White on `#ef4444` = **3.76:1** — below AA (4.5:1) for 10px text. Under `[data-contrast='high']` (`tokens.css:31`) `--color-red` becomes `#ff6b6b`, making it **worse** (~2.9:1) — the high-contrast theme actively regresses this element.

**Fix** Darken the badge background and keep white text:
```css
.site-nav__badge { background: #b91c1c; color: #fff; font: 11px var(--font-mono); }
```
`#fff` on `#b91c1c` = 6.47:1. Do not source it from `--color-red`, which is tuned as a *foreground* colour. Bump 10px → 11px per **UI-08**.

---

### UI-19 — P3 — `--bg-secondary` undefined on 5 live surfaces

**Files** `globals.css:912`, `:971`, `:986`, `:1021`; `feed-page.css:1027`, `:1148`

Same mechanism as UI-04/UI-07 — `background: var(--bg-secondary)` with no definition and no fallback → `background-color: transparent`.

| Selector | Live? | Effect |
|---|---|---|
| `.repeater-details-card__neighbour` (`globals.css:912`) | yes | neighbour tiles lose their inset fill; only the `--border` outline remains |
| `.repeater-details-card__table th` (`globals.css:971`) | yes | header row no longer distinguished from body rows |
| `.repeater-search-box__count` (`globals.css:986`) | yes | result-count strip blends into the dropdown |
| `.repeater-details-card__empty-msg` (`globals.css:1021`) | yes | empty-state block has no surface |
| `.uk-feed-inline-map` (`feed-page.css:1027`, `:1148`) | yes (mobile) | no placeholder fill behind the map while it loads |

Low severity because a border or parent background still delimits each one — the surfaces are flatter than intended, not broken.

**Fix** Define once in `globals.css:26` (`:root`):
```css
--bg-secondary: var(--bg-panel-alt);   /* #111c2b */
```
That gives a surface one step lighter than `--bg-panel`, which is what each of these five call sites clearly wants. Note `feed-page.css:1027` and `:1148` are duplicate rules inside two different media queries — consolidate them while you are there.

---

### UI-20 — P3 — 13 `<button>` elements without `type`

`DisclaimerModal.tsx:47`; `FeedPathViews.tsx:195`, `:203`; `UKFeedPage.tsx:456, 462, 468, 477, 484, 624, 680, 684, 690`; `UKRepeaterSearchPage.tsx:468`.

The HTML default is `type="submit"`. **None of these files contain a `<form>`**, so there is no live submit bug today — this is a latent risk and a consistency gap (the other ~90 buttons in the codebase set it explicitly).

**Fix** Add `type="button"` to each. Consider an ESLint rule (`react/button-has-type`) to prevent regression.

---

### UI-21 — P3 — ~38 orphaned CSS class rules

Cross-checked every class name against all TSX/HTML; these are referenced by **no** component (verified they are not built dynamically via template literals either):

| Prefix | Dead classes | File |
|---|---|---|
| `dev-monitor__*` | 14 (`__card`, `__card--wide`, `__chip`, `__chips`, `__empty`, `__empty-row`, `__grid`, `__meta`, `__mono`, `__row`, `__section-head`, `__summary`, `__table`, `__table-wrap`) | `site-content.css` |
| `dev-telemetry*` | 11 (`dev-telemetry`, `-chart`, `-chart__body/__empty/__foot/__head/__tooltip`, `__charts`, `__grid`, `__item`, `__section`) | `site-content.css` |
| `dev-status-*` | 13 (`-card`, `-card--fixed`, `-grid`, `-list`, `-list--compact`, `-list__summary`, `-note`, `-page`, `-page__header`, `-page__last-seen`, `-shell`, `-table`, `-table-wrap`) | `site-content.css`, `globals.css` |
| `repeater-details-card__*` | 4 (`__map-icon`, `__map-link`, `__packet-type`, `__spinner`) | `globals.css` |

`dev-status-mono` and `dev-status-empty` **are** live (`UKFeedPage.tsx:582`, `:592`) — keep those two.

This is where 9 of the 11 `--border-soft` usages and 1 of the 2 `--bg-tertiary` usages live, which is why they were never noticed.

**Fix** Delete the dead rules, plus their `@media` overrides (`globals.css:661–669`, `site-content.css` responsive blocks). Re-run the dead-class script in §8 afterwards to confirm nothing else broke. Do this **after** UI-07 and UI-19, so the token definitions land first and you can tell dead rules from live ones.

---

### UI-22 — P3 — `--accent-rgb` fallback is the wrong colour

**File** `frontend/src/styles/globals.css:1753`

```css
box-shadow: 0 0 0 3px rgba(var(--accent-rgb, 80, 220, 180), 0.2);
```
`--accent-rgb` is never defined, so the fallback always applies: `rgb(80, 220, 180)` — a mint green. The actual accent is `#00c4ff` = `rgb(0, 196, 255)`, cyan. The range-slider thumb therefore carries a green glow in an otherwise cyan UI.

**Fix** Either define `--accent-rgb: 0, 196, 255;` in `globals.css:39` next to `--accent`, or replace the whole expression with the existing token: `box-shadow: 0 0 0 3px var(--accent-dim);`.

---

### UI-23 — P3 — Duplicated declarations

**File** `frontend/src/pages/site-content.css:181–192`

`.site-home__card` declares `background: var(--bg-panel)`, `border: 1px solid var(--border)` and `border-radius: 8px` **twice**, back to back. Harmless but confusing.

**Fix** Delete lines 185–187.

---

### UI-24 — P3 — Unused design tokens

- `frontend/src/styles/tokens.css:69–70` — `--map-label-color`, `--map-link-color` are defined on `.maplibregl-map` and consumed nowhere. Map label colours are actually set in `mapConfig.ts:32–45`.
- `frontend/src/styles/tokens.css:6` `--color-gold`, `:10` `--color-purple`, `:22` `--font-sans` — zero consumers.

**Fix** Delete, or wire `--map-label-color`/`--map-link-color` into the real label pipeline if the intent was theme-driven map labels (see UI-03, which would benefit from exactly that plumbing).

---

### UI-25 — P3 — Backend operator login: invisible field boundaries

**File** `backend/src/backend-site/login.html:14–17`

**Measured**

| Pair | Contrast | Needs |
|---|---|---|
| Input background `#0f1512` vs card `#151b18` | **1.06:1** | 3:1 (1.4.11) |
| Input border `#3b4842` vs card `#151b18` | **1.82:1** | 3:1 |
| Card border `#2f3a35` vs body `#0b0f0d` | 1.63:1 | 3:1 |
| Submit `#fff` on `#267d51` | 5.08:1 | ✓ |
| Body text `#a9b5af` on `#151b18` | 8.25:1 | ✓ |

The token field is effectively invisible until focused — there is no `:focus` style either.

Separately, `<p id="status" role="status">` (`:27`) receives **error** messages (`:47`). `role="status"` is polite; failures should be assertive.

**Fix**
```css
input { background: #0b100d; color: #fff; border: 1px solid #6b7d74; }
input:focus-visible { outline: 2px solid #5ad18f; outline-offset: 2px; border-color: #5ad18f; }
main { border-color: #55635c; }
```
and change `role="status"` → `role="alert"` on the status paragraph (or keep `status` and add `aria-live="assertive"`).

Low priority — this page is operator-only behind the local HTTPS/tunnel endpoint.

---

### UI-26 — P3 — Mobile nav is styled twice, reconciled by a specificity hack

**Files** `frontend/src/styles/globals.css:588–623` (`@media (max-width: 640px)`) and `frontend/src/pages/site-shell.css:137–215` (`@media (max-width: 860px)`)

Both blocks style `.site-nav`, `.site-nav__links`, `.site-nav__links--open`, `.site-nav__link` and `.site-nav__app-btn`, with **different** layouts: globals uses a full-bleed `flex-direction: column` dropdown anchored at `top: 52px`; site-shell uses a `360px`, 2-column `grid` anchored at `top: calc(100% + 8px)`.

Today site-shell wins, but only because every one of its selectors is prefixed with `html ` (e.g. `html .site-nav__links`) to buy specificity — `site-content.css`/`site-shell.css` load in a lazy chunk **after** `index-*.css`, so bare-class rules would tie and the wrong one could win. The same `html ` hack appears in `feed-page.css` (`html .uk-feed-*`), and `map-app.css:1384` carries a comment explaining the same fight: *"App-specific responsive rules live after the app base rules so lazy-loaded CSS cannot reverse the mobile cascade."*

Note the residue: globals' `.site-nav { height: 52px }` is **not** overridden by site-shell (which sets `min-height`), so the final nav mixes declarations from both files.

**Impact** No visible bug today. It is a maintenance trap — this is the same cascade-ordering class of problem that produced **UI-05** and the `path-modal.css` extraction comment (`path-modal.css:1–5`) documenting a real regression from commit `83d770a`.

**Fix** Delete the duplicated mobile-nav block from `globals.css:588–623` (globals should not know about `.site-nav`, which belongs to the UK site shell), and drop the now-unneeded `html ` prefixes in `site-shell.css`. Longer term, consider CSS layers (`@layer base, components, pages;`) so lazy-chunk order stops mattering.

---

### UI-27 — P3 — Dead paint properties on the node layer

**File** `frontend/src/components/Map/mapSourceLayers.ts:45–47` (and an identical dead trio at `:237`)

```ts
'circle-stroke-width': 0,
'circle-stroke-color': '#00c4ff',
'circle-stroke-opacity': 0.7,
```
With `stroke-width: 0` the colour and opacity never render. Either delete the two dead lines or — better — set `circle-stroke-width: 1` as part of **UI-03**, which is the cheapest way to lift every marker over 3:1 in light mode.

---

### UI-28 — P3 — Map annotation squeezed on narrow desktop widths

**File** `frontend/src/styles/map-app.css:136`

```css
.map-annotation { max-width: min(520px, calc(100vw - 500px)); … white-space: nowrap; text-overflow: ellipsis; }
```
The `≤640px` override (`map-app.css:1409`) resets this to `calc(100vw - 30px)`, so phones are fine. But between **641px and ~800px** the annotation is capped at 141–300px and truncated to an ellipsis almost immediately. At exactly 641px it is 141px wide.

The same pattern in `.node-drawer` (`map-app.css:380`, `width: min(370px, calc(100vw - 480px))`) yields 161px at 641px — tight but not broken, and also reset at `≤640px`.

**Fix** Raise the breakpoint for the annotation reset, or use a floor:
```css
.map-annotation { max-width: min(520px, max(260px, calc(100vw - 500px))); }
```

---

## 6. Verified correct — do NOT re-investigate

These were flagged by automated scans or looked suspicious on reading, and were then **checked and cleared**. Re-chasing them will waste time.

| Thing | Verdict |
|---|---|
| `var(--trigger-width)` (`globals.css:731`, `:2105`; `map-app.css:87`) undefined | **Not a bug.** Injected at runtime by react-aria-components' `Popover`/`ComboBox`. Confirmed in `node_modules/react-aria-components/dist/private/Popover.mjs`. |
| `--color-text-dim: #6b7280` (`tokens.css:12`) measures 3.97:1 | **Not a bug.** Only consumed inside `tokens.css` to build `--text-muted` in the **high-contrast** blocks, where it is redefined to `#c7d2df`. The default theme's `--text-muted` comes from `globals.css:50` (`#8697b0`, 6.17:1). The `#6b7280` value never reaches the screen. |
| `<img>` without `alt` | **None exist.** Every `<img>` in the codebase has `alt`. |
| Text clipped without ellipsis (`nowrap` + `overflow:hidden`, no `text-overflow`) | **Only `.ui-visually-hidden`** (`globals.css:2064`), which is intentional. Everything else pairs them correctly. |
| `.sm-timeline` table overflowing on mobile | **Handled.** `spam-page.css:422` has `.sm-timeline { overflow-x: auto; }` (single-line rule, easy to miss). Same for `.repeater-details-card__table-wrap` and `.dev-monitor__table-wrap`. |
| "51 buttons missing `type`" (naive line-grep) | **Actually 13**, and none inside a `<form>`. See UI-20. The other 38 have `type` on a following line. |
| "4 unlabelled form controls" | **Actually 1** (UI-14). `ObserverRegistrationForm.tsx:34` and `OwnerPortalSections.tsx:165,166` use valid implicit `<label>` wrapping. |
| Filter toggle on/off state colour-only | **Not colour-only.** `.filter-toggle--on::after` translates the knob 12px (`map-app.css:720`) and `aria-pressed` is set (`MobileControls.tsx:73`, `FilterPanel.tsx`). |
| Public-site body/nav/footer/card text contrast | **All pass AA.** `#b4c6d8` measures 10.5–11.1:1 on its backgrounds; `#66829e` 4.87:1; `#b8c9dc` 10.85:1; `#bcefff` 13.5:1; `#6b8aaa` 5.10:1; `.site-stat__hash` (accent @ 0.7) 5.00:1. |
| Core dark palette (`--text-primary/secondary/muted`, `--accent`, `--amber`, `--online`) | **All ≥ 4.97:1** on all five app surfaces. Solid. |
| `.stats-page__path-node-label { color: #ffffff }` on a map | **Fine.** StatsPage/Spam/Owner maps use `MAP_STYLE` (dark) only; the light theme is exclusive to the main app map. |
| `aria-live` / `role="status"` coverage | **Good.** 17 correct usages across loading, error and connection states. |
| `role="img"` on the four Recharts wrappers (`LinkQualitySparkline.tsx:90`, `ActivitySparkline.tsx:102`, `OwnerPortalCharts.tsx:242,282,395`) | **Correct pattern** — those wrap non-interactive charts. Only the topology SVG (UI-10) is wrong, because it contains focusable children. |
| `prefers-reduced-motion` handling | **Present and global** (`globals.css:2136–2146`, `map-app.css:1372–1382`). |
| Modal scroll containment | **Correct.** `.disclaimer-modal`, `.stats-page__path-modal`, `.ui-dialog` all set `max-height` + `overflow-y: auto` + `overscroll-behavior: contain`, with `100dvh` used for mobile browser chrome. |
| `onClick` on non-interactive elements | **Only one** (`FilterPanel.tsx:158`), and it is a `stopPropagation` guard on a wrapper, not an interactive control. |

---

## 7. Suggested work order

**Batch 1 — undefined tokens (single commit, ~20 min, fixes 3 findings)**
Define `--bg-secondary`, `--border-soft`, `--bg-tertiary` (or substitute existing tokens) → **UI-04, UI-07, UI-19**. Highest value per line changed; UI-04 alone restores keyboard operation of the repeater search.

**Batch 2 — P0 readability**
**UI-01** (5-line CSS fix), then **UI-02** (modal shell — touches `Dialog.tsx`, re-test every dialog consumer).

**Batch 3 — focus & keyboard**
**UI-05** (de-`:where()` the global ring, remove 8 `outline:none`), then **UI-10** and **UI-11** together since both are in `TopologyPage.tsx`.

**Batch 4 — typography**
**UI-08** step 1 (floor at 10px) and **UI-09** together — they share the `.packet-item` grid risk. Re-test the map side panels at 1280×800.

**Batch 5 — map theming**
**UI-03** + **UI-27** + **UI-15**. Largest single piece of work; needs a designed light palette. Ship the `circle-stroke-width: 1` partial fix first if the palette work is deferred.

**Batch 6 — a11y polish**
**UI-12, UI-13, UI-14, UI-16, UI-17, UI-18, UI-20**. All small and independent.

**Batch 7 — cleanup**
**UI-21** (dead CSS — do after Batch 1), **UI-22, UI-23, UI-24, UI-26, UI-28, UI-25**.

**Regression guards worth adding**
- Extend `frontend/scripts/check-css-structure.mjs` to fail on (a) `var(--x)` with no definition and no fallback, (b) computed `font-size < 10px`. Both scanners are in §8 and can be dropped in nearly as-is — they would have caught UI-04, UI-07, UI-08, UI-19 and UI-22 automatically.
- The repo already has `@axe-core/playwright` in `devDependencies`. Wiring an axe pass into `test:e2e` would catch UI-12, UI-13, UI-14 and parts of UI-05/UI-10.

---

## 8. Reproducing the measurements

Save these to a scratch directory and run with Node ≥ 20 from `frontend/`.

### 8.1 `contrast.mjs` — WCAG contrast with alpha compositing

```js
function parse(c) {
  c = c.trim().replace('#', '');
  if (c.length === 3) c = c.split('').map((x) => x + x).join('');
  return [0, 2, 4].map((i) => parseInt(c.slice(i, i + 2), 16));
}
function lum([r, g, b]) {
  const f = (v) => { v /= 255; return v <= 0.04045 ? v / 12.92 : Math.pow((v + 0.055) / 1.055, 2.4); };
  return 0.2126 * f(r) + 0.7152 * f(g) + 0.0722 * f(b);
}
export function ratio(fgHex, bgHex, alpha = 1) {
  let fg = parse(fgHex); const bg = parse(bgHex);
  if (alpha < 1) fg = fg.map((c, i) => Math.round(c * alpha + bg[i] * (1 - alpha)));
  const l1 = lum(fg), l2 = lum(bg);
  const [hi, lo] = l1 > l2 ? [l1, l2] : [l2, l1];
  return (hi + 0.05) / (lo + 0.05);
}
```
Thresholds: normal text **4.5:1** (AA) / 7:1 (AAA); large text (≥18.66px bold or ≥24px) **3:1** / 4.5:1; non-text UI and graphics **3:1** (WCAG 1.4.11).

### 8.2 Undefined custom-property scanner

```js
import { readFileSync, readdirSync, statSync } from 'fs';
import { join } from 'path';
const files = [];
(function walk(d) { for (const e of readdirSync(d)) {
  if (['node_modules','dist','.git'].includes(e)) continue;
  const p = join(d, e);
  if (statSync(p).isDirectory()) walk(p); else if (/\.(css|tsx|ts|html)$/.test(p)) files.push(p);
} })('src');
const defined = new Set(), used = new Map();
for (const f of files) readFileSync(f, 'utf8').split('\n').forEach((line, i) => {
  for (const m of line.matchAll(/(^|[;{\s])(--[a-zA-Z0-9_-]+)\s*:/g)) defined.add(m[2]);
  for (const m of line.matchAll(/var\(\s*(--[a-zA-Z0-9_-]+)\s*(,)?/g)) {
    if (!used.has(m[1])) used.set(m[1], []);
    used.get(m[1]).push({ file: f, line: i + 1, hasFallback: !!m[2] });
  }
});
for (const [name, locs] of used) if (!defined.has(name)) {
  const noFb = locs.filter((l) => !l.hasFallback);
  if (noFb.length) console.log(`${name}: ${noFb.length} usages without fallback\n` +
    noFb.map((l) => `    ${l.file}:${l.line}`).join('\n'));
}
```
**Expected clean output after fixes:** only `--trigger-width` (runtime-injected by react-aria — allowlist it).

### 8.3 Sub-11px typography scanner

Walk all `.css`, match `font-size:\s*([0-9.]+)(px|rem)` and `font:\s*(?:[0-9]{3}\s+)?([0-9.]+)px`, normalise rem × 16, report `< 11`.
**Current count: 79.** Target after UI-08: 0 below 10px.

### 8.4 Dead-class detector

Collect every `.class-name` from all CSS, then check each against the concatenation of all `.tsx`/`.ts`/`index.html`. Flag names that never appear. Manually confirm each hit is not assembled from a template literal (e.g. `` `filter-row${on ? '--on' : ''}` ``) before deleting.

### 8.5 Cascade-order verification

Specificity ties in this codebase are resolved by **stylesheet load order**, which is determined by Vite's chunking, not by import statements. To check which rule really wins:

```bash
cd frontend
grep -o "\.your-selector{[^}]*}" dist/assets/*.css      # which chunk holds it
grep -o "\[[^]]*YourPage-[a-zA-Z0-9_-]*\.css[^]]*\]" dist/assets/index-*.js   # preload order
```
Chunk CSS is injected in the order it appears in the preload manifest array. `index-*.css` (containing `globals.css` + `tokens.css`) always loads first, so **every page-level stylesheet outranks the design system on ties.** This is the root cause of UI-05 and a contributing factor in UI-02 and UI-26.

---

## 9. Coverage notes

**Fully read:** `tokens.css`, `globals.css`, `map-app.css`, `site-shell.css`, `network-intelligence.css`, `path-modal.css`, `mapConfig.ts`, `mapSourceLayers.ts` (layer definitions), `Dialog.tsx`, `Combobox.tsx`, `MobileControls.tsx`, `FeedPathViews.tsx`, `FeedDialogs.tsx`, `pathNodePopup.ts`, `login.html`.

**Read in relevant part + scanned in full:** `site-content.css`, `feed-page.css`, `spam-page.css`, `stats-page.css`, `docs-pages.css`, `owner-portal.css`, `App.tsx`, `MapLibreMap.tsx`, `DeckGLOverlay.tsx`, `geojsonBuilders.ts`, `TopologyPage.tsx`, `StatusPage.tsx`, `UKFeedPage.tsx`, `UKRepeaterSearchPage.tsx`, `SpamTransparencyPage.tsx`, `PacketDetailPanel.tsx`, `NodePopupContent.tsx`, `OwnerPortalCharts.tsx`, `OwnerPortalSections.tsx`.

**Scanned by automation only** (all CSS/TSX): undefined tokens, contrast pairs, font sizes, `z-index` inventory, low-opacity text, `calc(100vw - N)` negatives, text-clipping, `role=`/`aria-` usage, form labelling, `<th scope>`, `<button type>`, icon-only buttons, dead classes, `!important` distribution.

**Not covered — recommend a follow-up pass**
- `backend/src/backend-site/template.html` (573 lines) — only spot-checked.
- `healthcheck-overrides/index.html` and `share.html` (369/370 lines) — not reviewed; they are deploy-time overrides for a third-party health page.
- Runtime/visual verification. Everything here is static analysis. Geometry claims in **UI-02** are computed from the CSS box model and should be confirmed in a browser at 850/900/1024/1280px before and after the fix.
- `frontend/test/` Playwright specs were not reviewed for coverage gaps against these findings.
