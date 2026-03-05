import React from 'react';

export const UKMqttPage: React.FC = () => (
  <>
    <section className="site-page-hero">
      <div className="site-content">
        <h1 className="site-page-hero__title">MQTT Observer Setup</h1>
        <p className="site-page-hero__sub">
          Connect your repeater node to the global UK Mesh MQTT broker and contribute live packet
          data to the national dashboard.
        </p>
      </div>
    </section>

    <div className="site-content site-prose">

      <section className="prose-section">
        <h2>What is this?</h2>
        <p>
          <strong>meshcoretomqtt</strong> is software that runs on a Linux device (such as a
          Raspberry Pi) connected to your MeshCore repeater node via USB. It reads the packet
          stream from the node's serial port and publishes it to an MQTT broker over the internet.
        </p>
        <p>
          Once connected, every packet your node hears will appear on the live map in real time,
          your node's position and RF coverage will be shown, and you will be contributing to the
          UK-wide picture of the network.
        </p>
        <p>
          Teesside and UK views use the same ingest feed. Observers using IATA <strong>MME</strong>{' '}
          are included in Teesside views, while other IATA regions appear in the wider UK/global
          views.
        </p>
        <div className="prose-note">
          <strong>Access is by request.</strong> Message <strong>ibengr</strong> on the{' '}
          <a href="https://discord.gg/bSuST8xvet" target="_blank" rel="noopener noreferrer">Discord</a>{' '}
          to get your credentials before going through this setup.
        </div>
      </section>

      <section className="prose-section">
        <h2>
          <span className="prose-step">1</span>
          Flash firmware with packet logging
        </h2>
        <p>
          We use the pre-built firmware from the team at{' '}
          <a href="https://analyzer.letsmesh.net" target="_blank" rel="noopener noreferrer">letsmesh</a>,
          a great project for global MeshCore network stats. You will need a letsmesh build with
          packet logging enabled. Use the latest packet-logging build shown in their onboarding flow.
        </p>
        <ol className="prose-steps">
          <li>
            Go to the{' '}
            <a href="https://analyzer.letsmesh.net/observer/onboard?type=repeater" target="_blank" rel="noopener noreferrer">
              letsmesh firmware flasher
            </a>{' '}
            and select your device variant.
          </li>
          <li>Choose <strong>Custom</strong> as the flash option to get the packet logging build.</li>
          <li>
            Connect your node via USB and flash it using the web flasher. No software install
            required, just Chrome or Edge.
          </li>
        </ol>
        <p className="prose-note">
          This only applies if you are setting up a fresh node. If your repeater is already running
          a recent letsmesh packet-logging build it likely already has packet logging enabled.
        </p>
      </section>

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
          meshcoretomqtt is for <strong>Repeater</strong> or <strong>Room Server</strong> nodes only.
        </p>
        <p>
          The script will walk you through setup interactively. Key choices to make:
        </p>

        <h3>Enable LetsMesh Packet Analyzer</h3>
        <p>
          Choose <strong>y</strong>. This enables packet logging and registers your node with
          the letsmesh global stats platform.
        </p>
        <div className="code-block">
          <pre>{'Enable LetsMesh Packet Analyzer MQTT servers? [Y/n]: y'}</pre>
        </div>

        <h3>IATA region code</h3>
        <p>
          Enter the three-letter IATA code for your nearest commercial airport. Some examples:
        </p>
        <div className="prose-facts prose-facts--3x2">
          <div className="prose-fact">
            <span className="prose-fact__value">MME</span>
            <span className="prose-fact__label">Teesside</span>
          </div>
          <div className="prose-fact">
            <span className="prose-fact__value">NCL</span>
            <span className="prose-fact__label">Newcastle</span>
          </div>
          <div className="prose-fact">
            <span className="prose-fact__value">MAN</span>
            <span className="prose-fact__label">Manchester</span>
          </div>
          <div className="prose-fact">
            <span className="prose-fact__value">LHR</span>
            <span className="prose-fact__label">London</span>
          </div>
          <div className="prose-fact">
            <span className="prose-fact__value">LBA</span>
            <span className="prose-fact__label">Leeds</span>
          </div>
          <div className="prose-fact">
            <span className="prose-fact__value">BHX</span>
            <span className="prose-fact__label">Birmingham</span>
          </div>
        </div>
        <p className="prose-note">
          Use the correct IATA code for your location. A quick search will confirm the right one.
        </p>

        <h3>Add the global broker</h3>
        <p>
          When asked if you would like to configure additional MQTT brokers, choose{' '}
          <strong>y</strong> and add <strong>1</strong> broker using the credentials you received
          from ibengr:
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
          Your node should appear on the{' '}
          <a href="https://app.ukmesh.com" target="_blank" rel="noopener noreferrer">live map</a>{' '}
          within a few minutes once an advert packet is heard.
        </p>
      </section>

      <section className="prose-section">
        <h2>Tips for a stable setup</h2>
        <ul>
          <li>
            <strong>Use a good USB cable.</strong> Cheap or long cables cause intermittent serial
            disconnects. Keep the node physically away from the Pi to avoid RF noise on 868 MHz.
          </li>
          <li>
            <strong>Disable USB autosuspend</strong> for the node. Find your device vendor ID with{' '}
            <code>lsusb</code>, then create a udev rule:
            <div className="code-block" style={{ marginTop: '10px' }}>
              <pre>{'ACTION=="add", SUBSYSTEM=="usb", ATTRS{idVendor}=="XXXX", ATTR{power/autosuspend}="-1"'}</pre>
            </div>
          </li>
          <li>
            <strong>If the node stops sending data</strong>, check{' '}
            <code>sudo journalctl -u mctomqtt -n 20</code>. If the serial watchdog is cycling,
            run <code>sudo systemctl restart mctomqtt</code>. If the serial port has disappeared,
            reseat the USB cable on the node.
          </li>
        </ul>
      </section>

      <section className="prose-section prose-section--muted">
        <h2>Get access</h2>
        <p>
          Message <strong>ibengr</strong> on the MeshCore Discord with your node name, location,
          and IATA code and we will get you set up with credentials for the global broker.
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
