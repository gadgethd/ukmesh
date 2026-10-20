-- 052: add raw receiver/sender columns to the canonical packets view.
--
-- The owner portal queries node_identity_packets.rx_node_id_raw and
-- src_node_id_raw. Those columns existed on production only as an out-of-band
-- view change, so a database restored from these migrations (or any fresh
-- environment) failed every owner dashboard request with
-- `column "rx_node_id_raw" does not exist`.
--
-- This view also replaces the previous `meshcore_canonical_node_id()` scalar
-- calls with alias joins, matching the deployed production definition.

CREATE OR REPLACE VIEW node_identity_packets AS
SELECT p."time",
       p.packet_hash,
       COALESCE(la.canonical_node_id, upper(btrim(p.rx_node_id))) AS rx_node_id,
       COALESCE(ls.canonical_node_id, upper(btrim(p.src_node_id))) AS src_node_id,
       p.topic,
       p.packet_type,
       p.route_type,
       p.hop_count,
       p.rssi,
       p.snr,
       p.payload,
       p.raw_hex,
       p.advert_count,
       p.path_hashes,
       p.network,
       p.path_hash_size_bytes,
       p.transport_codes,
       p.region_scope,
       p.companion_sender,
       p.topic_prefix,
       p.iata,
       p.is_private,
       p.visibility_ok,
       p.rx_node_id AS rx_node_id_raw,
       p.src_node_id AS src_node_id_raw
  FROM packets p
  LEFT JOIN node_identity_aliases la
    ON la.source_node_id = upper(btrim(p.rx_node_id))
  LEFT JOIN node_identity_aliases ls
    ON ls.source_node_id = upper(btrim(p.src_node_id));
