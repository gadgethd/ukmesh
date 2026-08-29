# Public API response profiles

`GET /api/nodes` and `GET /api/packets/recent` accept `fields=slim|full`.
The compatibility default is `full`; callers must explicitly request `slim`.
Responses identify the selected projection in `X-Response-Profile`.

The node slim profile retains `node_id`, `name`, coordinates, IATA, role,
presence state, advert count, and elevation. The recent-packet slim profile
retains packet/time identity, receiver/source identities, observer identities
and IATAs, type, hop/radio/path data, summary, advert count, and RX/TX counts.
The full packet profile additionally returns topic/network/transport metadata.
`raw=true` remains a separate, unchanged observation-event response.

The UK homepage opts into the packet slim profile. The main dashboard poll and
all requests without `fields=slim` retain the full response so existing public
consumers do not lose fields.

Validation failures preserve the human-readable `error` member and add a
stable uppercase `code` plus `requestId`. The OpenAPI contract documents both
response profiles and the validation-error envelope.
