# UK Mesh operational RF boundary v1

`uk-operational-v1.geojson` is a versioned, standalone boundary for the new
HopReach pipeline. It is not derived from the retired viewshed worker's
`uk_mainland.json` assumptions.

The FeatureCollection contains Natural Earth 1:10m Admin 0 geometries for:

- the United Kingdom (Great Britain and Northern Ireland);
- the Isle of Man;
- Jersey; and
- Guernsey.

Natural Earth data is in the public domain. Source dataset:
`natural-earth-vector/geojson/ne_10m_admin_0_countries.geojson`, retrieved for
boundary version 1 on 2026-08-02. The committed geometry is the immutable
runtime input; production does not download a mutable boundary URL.
