/** Map layer/filter state, shared by the app shell, URL state, map modes and
 *  the filter panel. */
export interface Filters {
  livePackets:       boolean;
  links:             boolean;
  terrain:           boolean;
  clientNodes:       boolean;
  packetHistory:     boolean;
  coverage:          boolean;
  heatmap:           boolean;
  betaPaths:         boolean;
  betaPathThreshold: number;  // 0–1
  hexClashes:        boolean;
  hexClashMaxHops:   number;  // 0–3 (0 = direct only)
}
