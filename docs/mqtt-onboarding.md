# MQTT observer setup and invisible-device troubleshooting

An MQTT connection can succeed while the broker rejects every publish. If your
firmware still uses the default Topic root `meshcore`, your observer will not
feed the dashboard: that literal topic contains neither your IATA code nor your
node public key. Changing your password will not fix this topic error.

## Fix the built-in MQTT firmware

1. Open the [UK Mesh flasher](https://flasher.ukmesh.com), connect to your device
   and read its settings. Select the MQTT broker slot for `mqtt.ukmesh.com`.
2. Keep the credentials issued for your account. Set **IATA** to your location
   code, for example `NCL`.
3. Set **Topic root** to exactly `meshcore/{IATA}/{PUBLIC_KEY}/packets`.
   Keep the braces: the firmware expands both tokens from the device settings
   and its current identity. Include `/packets`; a namespace alone is incomplete.
4. Apply the settings, read them back, and check that MQTT reconnects. For broker
   slot 1, the equivalent serial commands are:

   ```text
   set mqtt.1.iata NCL
   set mqtt.1.topic.root meshcore/{IATA}/{PUBLIC_KEY}/packets
   get mqtt.1.iata
   get mqtt.1.topic.root
   ```

   Replace `NCL` with your location code. If UK Mesh is in another broker slot,
   replace `1` in all four commands with that slot number.
5. Check the [owner dashboard](https://ukmesh.com/login) with your MQTT username
   and password. Once the node is linked and sends a valid status or packet,
   it can appear there. Public dashboards also depend on their network and
   visibility filters.

For example, a device configured with IATA `NCL` publishes packets on
`meshcore/NCL/<its-full-64-character-public-key>/packets` and status on the
equivalent `/status` topic. The broker permits the supported suffixes only for
public keys explicitly granted to your account. Do not put angle-bracket
examples or a shortened key into the actual topic.

The firmware's
[topic builders and token expansion](https://github.com/gadgethd/MeshCore-MQTT/blob/df46fb8d39e2970faf60269cd42ed8e88ad15874/examples/simple_repeater/MqttReporter.cpp)
use a bare `meshcore` literally, replace `/packets` with `/status` for status,
and substitute `{IATA}` and `{PUBLIC_KEY}` at runtime. This is why both tokens
and the packet suffix belong in the stored firmware template.

## Read the denial message

After you submit valid MQTT credentials, the owner dashboard can show a recent
broker denial of `meshcore` (or its direct `packets`, `status`, `neighbors` or
`neighbours` child). An unlinked owner sees the repair instructions on the login
form; an existing owner sees a **Recent MQTT publish denial** notice above their
nodes, with the observation time. The login error includes a link to the same
setup instructions on the public Install page.

This is a recent observation, not a live connectivity test. The notice expires
15 minutes after the last observed denial. It depends on the backend seeing
both an authenticated connection and the denial for that exact client ID while
its connection correlation is retained (up to 15 minutes, bounded by the client
cap). Reconnect the device if no hint appears. A missing hint does not prove
that publishing works. Historical denials older than 15 minutes do not create
new notices, and hints are kept only in backend memory.

The firmware may still report MQTT connected: it uses QoS 0, where there is no
per-publish acknowledgement to carry a detailed denial reason back to the
device. The portal explains the broker log evidence after authenticating you.

## Correct topic, but still denied or not linked

Send **sudogadget** on Discord your MQTT username, IATA code and the **full
64-character public key** shown by your device. Never send a password or private
key. Ask for the exact key to be linked to your account and its publish grant
verified. A factory reset or identity change can require a new grant even though
the firmware topic template now expands correctly.

There is no generic unattended new-device linker in this repository. Broker log
topics and client IDs are claims, not ownership proofs. The operator can use the
existing `newuser --link <username> <full-public-key>` flow for an existing owner,
or finish first-device provisioning for a credential-only account. Those are
operator provisioning actions; users should request the check, not edit ACLs.
After the grant is confirmed, reconnect, send a valid status/packet, and sign in
again. See the [linking design note](designs/generic-owner-linking.md) for the
remaining automation gap.

For a Linux `mctomqtt` bridge, use the bridge's IATA/public-key topic generation
described on the [Install page](https://ukmesh.com/install#mqtt-observer).
The `set mqtt.1.*` commands above configure the built-in firmware, not the Linux
bridge. `/raw` is not a supported MQTT topic; packet bytes belong in the `raw`
JSON field of the `/packets` envelope.
