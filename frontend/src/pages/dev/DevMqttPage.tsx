import React from 'react';

export const DevMqttPage: React.FC = () => (
  <>
    <section className="site-page-hero">
      <div className="site-content">
        <h1 className="site-page-hero__title">Development MQTT</h1>
        <p className="site-page-hero__sub">
          This site expects the same packet payload contract as production, but on the isolated test MQTT
          namespace.
        </p>
      </div>
    </section>

    <div className="site-content site-prose">
      <section className="prose-section">
        <h2>Use the test topic contract</h2>
        <p>
          Publish status and packets to <code>meshcore-test/{'{'}IATA{'}'}/{'{'}PUBLIC_KEY{'}'}/status</code> and
          <code>meshcore-test/{'{'}IATA{'}'}/{'{'}PUBLIC_KEY{'}'}/packets</code>. Keep the same JSON payload
          shape as production so firmware changes can be validated directly without a second parser or payload
          schema.
        </p>
      </section>

      <section className="prose-section">
        <h2>Recommended checks</h2>
        <p>Use this site to verify:</p>
        <ul>
          <li>status arrives and updates reliably</li>
          <li>packet `raw` hex is complete and decodes cleanly</li>
          <li>packet hash, type, RSSI, and SNR are populated</li>
          <li>multibyte path hashes are preserved in the raw packet</li>
          <li>test traffic stays isolated from the public dashboards</li>
        </ul>
      </section>
    </div>
  </>
);
