import React from 'react';

export const MqttPage: React.FC = () => (
  <>
    <section className="site-page-hero">
      <div className="site-content">
        <h1 className="site-page-hero__title">MQTT Integration</h1>
        <p className="site-page-hero__sub">
          Connect your repeater node to the global UK Mesh MQTT broker and contribute live packet
          data to the dashboards.
        </p>
      </div>
    </section>

    <div className="site-content site-prose">

      {/* ── What is this ─────────────────────────────────────────────── */}
      <section className="prose-section">
        <h2>What is this?</h2>
        <p>
          <strong>meshcoretomqtt</strong> is a piece of software that runs on a Linux device (such
          as a Raspberry Pi) connected to your MeshCore repeater node via USB. It reads the packet
          stream from the node's serial port and publishes it to an MQTT broker over the internet.
        </p>
        <p>
          Once connected, every packet your node hears will appear on the live map in real time,
          your node's position and coverage will be shown, and you'll be contributing to the
          shared picture of the network.
        </p>
        <p>
          Teesside and UK views now share the same ingest path. If your observer IATA is{' '}
          <strong>MME</strong>, packets are included in Teesside views. Other IATA regions appear
          in the wider UK/global views.
        </p>
        <div className="prose-note">
          <strong>Access is by request.</strong> Message <strong>ibengr</strong> on the{' '}
          <a href="https://discord.gg/bSuST8xvet" target="_blank" rel="noopener noreferrer">Discord</a>{' '}
          to get your credentials before going through this setup. We're in the{' '}
          <strong>North East England</strong> regional channel.
        </div>
      </section>

      {/* ── Step 1: Firmware ─────────────────────────────────────────── */}
      <section className="prose-section">
        <h2>
          <span className="prose-step">1</span>
          Flash firmware with packet logging
        </h2>
        <p>
          We use the pre-built firmware from the team at{' '}
          <a href="https://analyzer.letsmesh.net" target="_blank" rel="noopener noreferrer">letsmesh</a>
          {' '}, a great project for global MeshCore network stats. You will need a letsmesh build
          with packet logging enabled. Use the latest packet-logging build shown in their onboarding flow.
        </p>
        <ol className="prose-steps">
          <li>
            Go to the{' '}
            <a href="https://analyzer.letsmesh.net/observer/onboard?type=repeater" target="_blank" rel="noopener noreferrer">
              letsmesh firmware flasher
            </a>{' '}
            and select your device variant.
          </li>
          <li>
            Choose <strong>Custom</strong> as the flash option to get the packet logging build.
          </li>
          <li>
            Connect your node via USB and flash it using the web flasher. No software install
            required, just Chrome or Edge.
          </li>
        </ol>
        <p className="prose-note">
          This only applies if you're setting up a fresh node. If your repeater is already running
          a recent letsmesh packet-logging build, you can usually skip this step.
        </p>
      </section>

      {/* ── Step 2: Run the installer ─────────────────────────────────── */}
      <section className="prose-section">
        <h2>
          <span className="prose-step">2</span>
          Run the installer
        </h2>
        <p>
          Connect your node to your Raspberry Pi (or other Linux device) via USB, then run the
          install script:
        </p>
        <div className="code-block">
          <pre>{'curl -fsSL https://raw.githubusercontent.com/Cisien/meshcoretomqtt/main/install.sh | bash'}</pre>
        </div>
        <p className="prose-note">
          meshcoretomqtt is for <strong>Repeater</strong> or <strong>Room Server</strong> nodes
          only. A different install script is used for Companion nodes.
        </p>
        <p>
          The script will walk you through setup interactively. It auto-detects your LoRa device
          on the serial port. Just confirm the detected device. The key choices to make:
        </p>

        <h3>Enable LetsMesh Packet Analyzer</h3>
        <p>
          Choose <strong>y</strong>. This enables the packet logging functionality and registers
          your node with the letsmesh global network stats platform.
        </p>
        <div className="code-block">
          <pre>{'Enable LetsMesh Packet Analyzer MQTT servers? [Y/n]: y'}</pre>
        </div>

        <h3>IATA region code</h3>
        <p>
          You'll be asked to search for your region by airport code or city name. This is the
          three-letter code for your nearest commercial airport, for example:
        </p>
        <div className="prose-facts">
          <div className="prose-fact">
            <span className="prose-fact__value">MME</span>
            <span className="prose-fact__label">Darlington</span>
          </div>
          <div className="prose-fact">
            <span className="prose-fact__value">NCL</span>
            <span className="prose-fact__label">Newcastle</span>
          </div>
          <div className="prose-fact">
            <span className="prose-fact__value">LBA</span>
            <span className="prose-fact__label">Leeds</span>
          </div>
          <div className="prose-fact">
            <span className="prose-fact__value">HUY</span>
            <span className="prose-fact__label">Humberside</span>
          </div>
        </div>
        <p className="prose-note">
          <strong>Use the correct IATA code for your location.</strong> IATA codes are not the
          same as ICAO codes. A quick Google search will confirm the right one. Using the wrong
          code may result in losing the ability to configure it later.
        </p>

        <h3>Owner identification &amp; remote serial</h3>
        <p>
          Owner public key and email are optional, you can skip both. When asked about remote
          serial access, choose <strong>n</strong>.
        </p>

        <h3>Add the global broker</h3>
        <p>
          When asked if you'd like to configure additional MQTT brokers, choose <strong>y</strong>{' '}
          and add <strong>1</strong> broker. Enter the following details using the credentials
          you received from ibengr:
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
          If your installer asks for topic format, use:
        </p>
        <div className="code-block">
          <pre>{'meshcore/<IATA>/{PUBLIC_KEY}/packets'}</pre>
        </div>
        <p>
          Once the installer finishes, your node should appear on the{' '}
          <a href="https://app.teessidemesh.com" target="_blank" rel="noopener noreferrer">live map</a>{' '}
          within a few minutes, once an advert packet is heard. It can take up to 5 minutes to
          appear in all parts of the dashboard.
        </p>
      </section>

      {/* ── Tips ─────────────────────────────────────────────────────── */}
      <section className="prose-section">
        <h2>Tips for a stable setup</h2>
        <ul>
          <li>
            <strong>Use a good USB cable.</strong> Cheap or long cables cause intermittent serial
            disconnects. Shorter is generally better, but keep the node physically away from the
            Pi to avoid RF noise on the 868 MHz band.
          </li>
          <li>
            <strong>Disable USB autosuspend</strong> for the node. Linux will power-suspend idle
            USB devices by default, which kills the serial connection. To fix this permanently,
            first find your device's vendor ID:
            <div className="code-block" style={{ marginTop: '10px' }}>
              <pre>{'lsusb'}</pre>
            </div>
            Look for your LoRa device in the output (e.g. <em>Espressif</em>, <em>Silicon Labs</em>,
            or <em>QinHeng</em>). Note the 4-character vendor ID, it's the first hex value after
            "ID", e.g. <code>303a</code> for Heltec V4, <code>10c4</code> for CP2102-based boards.
            Then create a udev rule:
            <div className="code-block" style={{ marginTop: '10px' }}>
              <pre>{'sudo nano /etc/udev/rules.d/99-lora-no-autosuspend.rules'}</pre>
            </div>
            Add this line, replacing <code>XXXX</code> with your vendor ID:
            <div className="code-block" style={{ marginTop: '10px' }}>
              <pre>{'ACTION=="add", SUBSYSTEM=="usb", ATTRS{idVendor}=="XXXX", ATTR{power/autosuspend}="-1"'}</pre>
            </div>
            Then reload the rules and replug the device:
            <div className="code-block" style={{ marginTop: '10px' }}>
              <pre>{'sudo udevadm control --reload-rules && sudo udevadm trigger'}</pre>
            </div>
          </li>
          <li>
            <strong>Power stability matters.</strong> A good quality 5 V supply prevents brownouts
            that can lock up the connected node. A powered USB hub between the Pi and the node
            also helps isolate the power rails.
          </li>
          <li>
            <strong>If the node stops sending data</strong>, check{' '}
            <code>sudo journalctl -u mctomqtt -n 20</code>. If the serial watchdog is cycling,
            try <code>sudo systemctl restart mctomqtt</code>. If the serial port has disappeared
            entirely, reseat the USB cable on the node itself.
          </li>
        </ul>
      </section>

      {/* ── Get access ───────────────────────────────────────────────── */}
      <section className="prose-section prose-section--muted">
        <h2>Get access</h2>
        <p>
          Come and find us on Discord in the <strong>North East England</strong> regional channel.
          Message <strong>ibengr</strong> with your node name, location, and IATA code and we'll
          get you set up with credentials for the global broker.
        </p>
        <a
          href="https://discord.gg/bSuST8xvet"
          target="_blank"
          rel="noopener noreferrer"
          className="site-btn site-btn--primary"
          style={{ marginTop: '8px', display: 'inline-flex' }}
        >
          Join Discord →
        </a>
      </section>

    </div>
  </>
);
