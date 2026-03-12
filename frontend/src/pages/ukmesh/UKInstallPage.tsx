import React from 'react';

export const UKInstallPage: React.FC = () => (
  <>
    <section className="site-page-hero">
      <div className="site-content">
        <h1 className="site-page-hero__title">Install MeshCore</h1>
        <p className="site-page-hero__sub">
          Get a companion node on the air in about 10 minutes. No soldering, no special tools. Just a browser, a USB cable, and a phone.
        </p>
      </div>
    </section>

    <div className="site-content site-prose">

      <section className="prose-section">
        <h2>
          <span className="prose-step">1</span>
          What you need
        </h2>
        <p>
          A companion node is a handheld or portable device you use to send and receive messages on the mesh.
          Any ESP32-based LoRa board supported by MeshCore will work. The most common choice in the UK
          is the <strong>Heltec WiFi LoRa 32 V4</strong>. It has a built-in OLED display,
          USB-C charging, integrated battery management, and costs around &pound;25&ndash;&pound;35.
        </p>

        <div className="hw-cards">
          <div className="hw-card hw-card--recommended">
            <div className="hw-card__badge">Recommended</div>
            <div className="hw-card__name">Heltec WiFi LoRa 32 V4</div>
            <div className="hw-card__detail">ESP32-S3 · 868 MHz · OLED · USB-C · Li-Po</div>
          </div>
          <div className="hw-card">
            <div className="hw-card__name">Heltec WiFi LoRa 32 V3</div>
            <div className="hw-card__detail">ESP32 · 868 MHz · OLED · USB-C</div>
          </div>
          <div className="hw-card">
            <div className="hw-card__name">LILYGO T3S3</div>
            <div className="hw-card__detail">ESP32-S3 · 868 MHz · E-Paper optional</div>
          </div>
          <div className="hw-card">
            <div className="hw-card__name">Heltec Mesh Node T114</div>
            <div className="hw-card__detail">nRF52840 · 868 MHz · Ultra-compact</div>
          </div>
        </div>

        <p className="prose-note">
          <strong>You will also need:</strong> a USB cable to match your board (USB-C for V4/V3), an Android
          phone to run the MeshCore companion app, and optionally a 3.7 V Li-Po battery to run the board untethered.
        </p>
      </section>

      <section className="prose-section">
        <h2>
          <span className="prose-step">2</span>
          Flash the firmware
        </h2>
        <p>
          Everything is done in the browser, no software to install. You need Chrome or Edge (Web Serial API required).
        </p>
        <ol className="prose-steps">
          <li>
            Connect your board to your PC via USB and open the{' '}
            <a href="https://flasher.meshcore.co.uk/" target="_blank" rel="noopener noreferrer">
              MeshCore web flasher
            </a>.
          </li>
          <li>
            Select your <strong>device hardware</strong> from the dropdown, then select{' '}
            <strong>Companion Radio (Bluetooth)</strong> as the firmware type.
          </li>
          <li>
            Click <strong>Enter DFU Mode</strong>, then <strong>Erase Flash</strong>, then <strong>Flash</strong>.
            The flash takes about 30 seconds.
          </li>
          <li>
            Once complete, your board will reboot and show the MeshCore splash screen on its OLED (if it has one).
          </li>
        </ol>
        <p className="prose-note">
          If your board does not appear in the port list, try holding the <strong>BOOT</strong> button
          while plugging in the USB cable to enter bootloader mode.
        </p>
      </section>

      <section className="prose-section">
        <h2>
          <span className="prose-step">3</span>
          Configure your node
        </h2>
        <p>
          Install the <strong>MeshCore</strong> companion app on your phone. Official clients are
          available on Android (Google Play) and iOS (App Store).
        </p>
        <ol className="prose-steps">
          <li>
            Open the MeshCore app and tap <strong>Add device</strong>. Your node will appear in the
            Bluetooth scan.
          </li>
          <li>
            Set a <strong>node name</strong>. Your callsign, name, or location works well.
          </li>
          <li>
            Set the <strong>device role</strong> to <em>Client</em> for a handheld companion node.
          </li>
          <li>
            Set the radio to the UK network configuration:
            <br />
            <strong>Profile:</strong> EU/UK Narrow &nbsp;|&nbsp;
            <strong>Freq:</strong> 869.618 MHz &nbsp;|&nbsp;
            <strong>BW:</strong> 62.5 kHz &nbsp;|&nbsp;
            <strong>SF8 / CR8</strong>
          </li>
          <li>
            Leave the channel set to the default <strong>Public</strong> channel.
          </li>
          <li>
            Add your <strong>GPS coordinates</strong> so the network can place you on the map.
          </li>
        </ol>
      </section>

      <section className="prose-section">
        <h2>
          <span className="prose-step">4</span>
          Get on the network
        </h2>
        <p>
          If your node is on and configured to the Public channel, and within range of any
          UK Mesh repeater, you should be able to send and receive messages straight away.
          Send a message in the Public channel. If it shows <strong>"Heard X Repeats"</strong> instead
          of just "Sent", you are on the network.
        </p>
        <p>
          Check the{' '}
          <a href="https://app.ukmesh.com">live map</a>{' '}
          to see if your node appears. Come say hello on the MeshCore Discord.
          DM <strong>ibengr</strong> if you have any questions.
        </p>
        <a
          href="https://discord.gg/bSuST8xvet"
          target="_blank"
          rel="noopener noreferrer"
          className="site-btn site-btn--primary"
        >
          Join us on Discord →
        </a>
      </section>

      <section className="prose-section">
        <h2>
          <span className="prose-step">5</span>
          Add an MQTT observer
        </h2>
        <p>
          If you want your repeater or room server to feed the public dashboards, run an observer bridge on a Linux host
          connected to the node over USB. The bridge publishes what the node hears to the shared UK Mesh broker.
        </p>
        <div className="prose-note">
          <strong>Access is by request.</strong> Message <strong>ibengr</strong> on Discord to get MQTT credentials before setting this up.
        </div>
        <div className="code-block">
          <pre>{'curl -fsSL https://raw.githubusercontent.com/Cisien/meshcoretomqtt/main/install.sh | bash'}</pre>
        </div>
        <p>
          During setup, enable packet logging, choose the correct IATA code for your location, and add one extra broker with:
        </p>
        <div className="code-block">
          <pre>{`Server hostname/IP: mqtt.ukmesh.com
Port [1883]: 443
Use WebSockets transport? [y/N]: y
Use TLS/SSL encryption? [y/N]: y
Verify TLS certificates? [Y/n]: y
Choose authentication method [1-3] [1]: 1
Username: <your username>
Password: <your password>`}</pre>
        </div>
        <p className="prose-note">
          Topic format:
        </p>
        <div className="code-block">
          <pre>{'meshcore/<IATA>/{PUBLIC_KEY}/packets'}</pre>
        </div>
        <p>
          Observers using <strong>MME</strong> also appear in the Teesside-specific views. All other IATA codes appear in the wider UK feed only.
        </p>
      </section>

    </div>
  </>
);
