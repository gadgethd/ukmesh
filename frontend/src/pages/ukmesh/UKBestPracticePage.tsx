import React from 'react';

export const UKBestPracticePage: React.FC = () => (
  <>

    <div className="site-content site-prose">

      <section className="prose-section">
        <h2>Radio settings &amp; profile</h2>
        <p>
          The UK network uses the <strong>EU/UK Narrow</strong> radio profile. This sits in the
          869.4&ndash;869.65&thinsp;MHz sub-band, which is licence-exempt up to <strong>27&thinsp;dBm ERP
          (500&thinsp;mW)</strong> with a 10% duty cycle limit under Ofcom IR2030 / ERC Recommendation 70-03.
          Staying on this profile keeps you legal and compatible with every other node on the network.
        </p>
        <div className="prose-facts prose-facts--3x2">
          <div className="prose-fact">
            <span className="prose-fact__value">869.618</span>
            <span className="prose-fact__label">Frequency (MHz)</span>
          </div>
          <div className="prose-fact">
            <span className="prose-fact__value">62.5</span>
            <span className="prose-fact__label">Bandwidth (kHz)</span>
          </div>
          <div className="prose-fact">
            <span className="prose-fact__value">SF8</span>
            <span className="prose-fact__label">Spreading factor</span>
          </div>
          <div className="prose-fact">
            <span className="prose-fact__value">CR8</span>
            <span className="prose-fact__label">Coding rate (default)</span>
          </div>
          <div className="prose-fact">
            <span className="prose-fact__value">27 dBm</span>
            <span className="prose-fact__label">Regulatory ERP max</span>
          </div>
          <div className="prose-fact">
            <span className="prose-fact__value">10%</span>
            <span className="prose-fact__label">Duty cycle limit</span>
          </div>
        </div>

        <h3>Fixed parameters — do not change</h3>
        <p>
          Frequency (869.618&thinsp;MHz), bandwidth (62.5&thinsp;kHz), and spreading factor (SF8)
          are <strong>fixed for network compatibility</strong>. LoRa modulation is orthogonal across
          spreading factors — a node running SF9 cannot demodulate SF8 packets, and vice versa.
          Changing the SF doesn't give you a stronger signal; it disconnects you from the entire UK
          network. The same applies to bandwidth and frequency: they must match exactly.
        </p>

        <h3>Coding rate — the one parameter you can adjust</h3>
        <p>
          Coding rate (CR) is adjustable without breaking compatibility. MeshCore uses LoRa explicit
          header mode, which embeds the CR value in every packet header — the receiver decodes
          whatever CR the sender used, automatically.
        </p>
        <p>
          The network default is <strong>CR8 (4/8)</strong>, the most robust setting: maximum forward
          error correction at the cost of slightly longer packets. <strong>CR5 (4/5)</strong> gives
          the shortest airtime with the least error correction. CR6 and CR7 sit in between.
        </p>
        <p>
          For most users, stay on CR8. If you are experimenting on a strong short-range link you might
          try CR5; if a marginal long-range path is dropping packets, CR8 gives the best chance of
          recovery. CR is set in the MeshCore app under the radio configuration.
        </p>

        <h3>TX power and antenna gain</h3>
        <p>
          The MeshCore app sets TX power in <strong>dBm</strong>. Most supported hardware (Heltec,
          LILYGO, Mesh Node) caps at around 20–22&thinsp;dBm. The regulatory limit of 27&thinsp;dBm
          is an ERP figure — <em>effective radiated power</em>, referenced to a half-wave dipole.
        </p>
        <p>
          Antenna gain adds directly to your ERP. A 3&thinsp;dBi antenna at 20&thinsp;dBm TX produces
          roughly 21.5&thinsp;dBm ERP. A 6&thinsp;dBi antenna at the same TX power pushes around
          23.5&thinsp;dBm ERP. This is well within the 27&thinsp;dBm limit, but if you use a
          high-gain antenna (&gt;8&thinsp;dBi) you should reduce TX power accordingly to stay legal.
        </p>
        <p>
          Higher power is rarely the answer. A well-elevated antenna at 17&ndash;20&thinsp;dBm
          will consistently outperform a ground-level node running maximum power, and reduces
          unnecessary interference for nearby nodes.
        </p>
      </section>

      <section className="prose-section">
        <h2>Repeater placement</h2>
        <p>
          Elevation is the single biggest factor in repeater range. A node at 50&thinsp;m above
          surrounding terrain will outperform a high-powered node at ground level. Aim for clear
          line of sight to as much of your intended coverage area as possible.
        </p>
        <p>
          Before choosing a site, check the <a href="https://app.ukmesh.com/" target="_blank" rel="noopener noreferrer">live map</a> to see what
          nodes already cover your area. The goal is to fill coverage gaps, not to duplicate existing
          infrastructure. A well-placed node in an uncovered valley is more useful than a fifth node
          on a hilltop that already has good coverage.
        </p>

        <h3>Antenna</h3>
        <p>
          Use a vertical omnidirectional antenna for general repeater use. 3&ndash;6&thinsp;dBi gain
          is typical and appropriate — very high-gain antennas narrow the vertical beam and can
          actually reduce coverage to nearby users. Mount it as high as you safely can and keep
          the antenna vertical. Avoid indoor deployment behind walls or windows if at all possible.
        </p>
        <p>
          Check your connector type before ordering an antenna. Most boards use SMA or U.FL/IPEX
          connectors on the board itself, often with a short pigtail ending in SMA. A loose or
          mismatched connector at the SMA joint is one of the most common causes of poor RF
          performance — it can account for 3&ndash;6&thinsp;dB of unexpected loss.
        </p>

        <h3>Cabling</h3>
        <p>
          Keep coax runs short. Use low-loss cable (LMR-400 or equivalent) for any run over a few
          metres — cheap RG58 loses around 1.5&thinsp;dB per 10&thinsp;m at 870&thinsp;MHz, which
          quickly cancels out any antenna gain. Use weatherproof connectors and apply self-amalgamating
          tape over outdoor joins.
        </p>
        <p>
          If your antenna is elevated or roof-mounted, fit a lightning arrestor / surge protector
          on the feedline at the point it enters the building. This protects both the node hardware
          and anything else on the same circuit.
        </p>

        <h3>Power &amp; weatherproofing</h3>
        <p>
          Use a stable, regulated 5&thinsp;V supply rated for continuous use. Phone chargers and
          USB hubs are unreliable for permanent installs. Some nodes draw up to 500&thinsp;mA
          during transmit — a supply that is fine at idle may brown out mid-packet. Use a dedicated
          PSU rated at <strong>1&thinsp;A minimum</strong>.
        </p>
        <p>
          If your node is outdoors, house it in a sealed enclosure with a drip loop on the coax
          entry. Check connectors and seals annually — UV and moisture degrade everything over time.
        </p>
      </section>

      <section className="prose-section">
        <h2>Network etiquette</h2>
        <p>
          The UK mesh is a shared resource. A few simple habits keep it usable for everyone.
        </p>
        <ul>
          <li>
            <strong>Use the Public channel for real traffic only.</strong> Don't flood it with
            automated test messages or scripts. Use the <strong>Test</strong> channel (or a private
            encrypted channel) for development and experimentation.
          </li>
          <li>
            <strong>Set a meaningful node name.</strong> The default "MeshCore" name makes it
            impossible for others to identify your node on the map or in the feed. Pick something
            unique — your callsign, location, or a short handle.
          </li>
          <li>
            <strong>Set your advert interval to 168 hours (1 week).</strong> The absolute minimum
            is 24 hours. The network map is built from live packet traffic, not adverts — frequent
            adverts waste airtime and congest the shared channel with no benefit. A fixed repeater
            has no reason to advertise more than once a week.
          </li>
          <li>
            <strong>Direct messages over group messages where appropriate.</strong> Group messages
            are rebroadcast by repeaters; direct messages are routed. If you're chatting with one
            person, use a DM — it uses less airtime.
          </li>
          <li>
            <strong>Be thoughtful about position sharing.</strong> If you're privacy-conscious, you
            can disable GPS or set a coarse manual location. Be aware that position data powers the
            coverage maps and path loss estimates — inaccurate coordinates degrade the picture for
            everyone.
          </li>
          <li>
            <strong>Treat channel keys like passwords.</strong> If you run a private encrypted
            channel, only share the key with people you intend to include. Don't post it publicly —
            once shared widely, a channel key cannot be revoked without everyone migrating to a
            new channel.
          </li>
        </ul>
      </section>

      <section className="prose-section">
        <h2>Troubleshooting</h2>

        <h3>RSSI and SNR</h3>
        <p>
          RSSI above &minus;115&thinsp;dBm is usable; above &minus;100&thinsp;dBm is good. SNR
          above &minus;10&thinsp;dB is workable; positive SNR is excellent. If both figures are
          poor, the problem is distance or obstruction — not power. Check the{' '}
          <a href="https://app.ukmesh.com/" target="_blank" rel="noopener noreferrer">live map</a> to see what path loss the network is estimating
          between nodes.
        </p>

        <h3>Packets not appearing on the map or feed</h3>
        <p>
          Check the observer bridge service is running and connected:
        </p>
        <div className="code-block">
          <pre>{'journalctl -u mctomqtt -f'}</pre>
        </div>
        <p>
          Confirm your MQTT credentials are correct in the config file and that the broker hostname
          is <code>mqtt.ukmesh.com</code> on port <code>443</code> with WebSockets and TLS enabled.
          Your node may also take up to one advert interval to appear on the map after first
          connecting — if your advert interval is set to 168 hours, the position won't update until
          the next advert fires or the node sends a traceroute.
        </p>

        <h3>Node visible on feed but showing wrong position</h3>
        <p>
          The node is broadcasting a stale or zero GPS fix. Boards like the Heltec WiFi LoRa 32
          have no GPS module — you must set a <strong>manual position</strong> in the MeshCore app.
          A node that has never had a position set will broadcast (0°, 0°), which places it in
          the Gulf of Guinea. Set your location once in the app and it will be included in future
          adverts.
        </p>

        <h3>Observer bridge running but node not appearing</h3>
        <p>
          If the bridge is connected (no errors in the journal) but the node doesn't appear, check:
        </p>
        <ul>
          <li>Your IATA code is set and recognised (e.g. <strong>MME</strong>, <strong>NCL</strong>, <strong>MAN</strong>) — unknown codes are rejected by the broker</li>
          <li>The broker logs show a successful CONNECT and no ACL-denied messages</li>
          <li>The node's advert interval — if set to 168 hours and the node just connected, you may need to trigger a manual advert or traceroute to seed the position</li>
        </ul>

        <h3>USB stability / serial watchdog restarting</h3>
        <p>
          The most common cause is a poor USB cable. Swap it for a known-good data cable (not a
          charge-only cable). If the problem persists, disable USB autosuspend — the meshcoretomqtt
          install script offers this as a udev rule, or you can add it manually:
        </p>
        <div className="code-block">
          <pre>{'echo \'ACTION=="add", SUBSYSTEM=="usb", ATTR{idVendor}=="303a", ATTR{power/autosuspend}="-1"\' | sudo tee /etc/udev/rules.d/99-meshcore-usb.rules\nsudo udevadm control --reload-rules && sudo udevadm trigger'}</pre>
        </div>

        <h3>Two observers showing the same node</h3>
        <p>
          This is expected and correct. Multiple observers can hear the same node simultaneously —
          the feed and map deduplicate by packet hash, so each unique packet appears once regardless
          of how many observers received it. Seeing the same node reported by both MME and NCL
          observers just means you have good coverage.
        </p>
      </section>

    </div>
  </>
);
