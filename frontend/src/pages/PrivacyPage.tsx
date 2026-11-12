import './docs-pages.css';
import React from 'react';

/**
 * Public privacy notice covering BOTH the error-diagnostics telemetry and the
 * mesh packet database. Lawful basis for both: legitimate interest (UK GDPR
 * Art 6(1)(f)). Full assessment: docs/privacy.md in the repository.
 */
export const PrivacyPage: React.FC = () => (
  <div className="site-content site-prose">
    <section className="prose-section">
      <h1>Privacy</h1>
      <p>
        This page explains what data the site collects, why, how long it is kept,
        and how to exercise your rights. Both the browser diagnostics and the mesh
        packet database are processed under the lawful basis of{' '}
        <strong>legitimate interest</strong> (reliability, bug fixing, and operating
        the network) — the full assessment is in the project repository.
      </p>
    </section>

    <section className="prose-section">
      <h2>Error diagnostics</h2>
      <p>
        The site automatically records errors that happen in your browser so we can
        find and fix faults. When an error occurs we store:
      </p>
      <ul>
        <li>a truncated error message and stack trace,</li>
        <li>the page path you were on (no query string),</li>
        <li>your browser type, and</li>
        <li>a pseudonymous hash of your IP address, rather than the address itself.</li>
      </ul>
      <p>
        Diagnostic records omit account IDs, query strings and cookie contents.
        Error text can contain contextual information. Our retention policy for
        diagnostics is <strong>30 days</strong>. Add <code>?telemetry=off</code> to
        the page URL to disable browser diagnostic and performance reports.
      </p>
    </section>

    <section className="prose-section">
      <h2>Mesh network data</h2>
      <p>
        The site stores radio packets observed on the MeshCore network so the
        network can be monitored and improved:
      </p>
      <ul>
        <li>
          <strong>Message content and raw packet data</strong> — kept for{' '}
          <strong>30 days</strong> under the content-retention policy.
        </li>
        <li>
          <strong>Decrypted message content</strong> — kept for{' '}
          <strong>30 days</strong> under the content-retention policy.
        </li>
        <li>
          <strong>Path metadata</strong> (which nodes a packet travelled through,
          timing, signal quality) — retained for network analytics. It contains
          node identifiers, timing and radio metadata, never message content or
          coordinates, and is stored
          indefinitely in pseudonymous form.
        </li>
        <li>
          <strong>Aggregate statistics</strong> (per-day/hour counts) — retained
          indefinitely; these are anonymous.
        </li>
      </ul>
      <p>
        Node identifiers may be linked to individuals through owner registration
        or other identifying context. Contact us about access, correction or
        deletion of data relating to you.
      </p>
    </section>

    <section className="prose-section">
      <h2>Performance reports</h2>
      <p>
        Public pages report a small set of loading, layout and interaction timings
        for fixed page categories. These reports omit account and node identifiers,
        cookies, referrers and interaction details, and contribute to aggregate
        performance metrics. The same telemetry opt-out applies.
      </p>
    </section>

    <section className="prose-section">
      <h2>Browser storage and login</h2>
      <p>
        Preferences and saved local views use browser storage. Owner login uses
        a secure session cookie on HTTPS and a separate cookie that protects
        account changes. The session expires after seven days and can be revoked;
        signing out ends that session. These cookies support authentication and
        security, rather than advertising.
      </p>
    </section>

    <section className="prose-section">
      <h2>Your rights</h2>
      <p>
        Under the UK GDPR you can request access to, correction of, or deletion of
        personal data we hold about you. Include enough information to help us
        locate relevant records without sending passwords or private keys.
        Routine diagnostic expiry follows the 30-day retention policy.
      </p>
      <p>
        Contact: <a href="mailto:ukmesh@proton.me">ukmesh@proton.me</a>
      </p>
    </section>
  </div>
);
