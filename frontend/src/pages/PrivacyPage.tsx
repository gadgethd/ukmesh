import React from 'react';

/**
 * Public privacy notice covering BOTH the error-diagnostics telemetry and the
 * mesh packet database. Lawful basis for both: legitimate interest (UK GDPR
 * Art 6(1)(f)). Full assessment: docs/privacy.md in the repository.
 */
export const PrivacyPage: React.FC = () => (
  <article className="privacy-page">
    <h1>Privacy</h1>

    <section>
      <h2>Error diagnostics</h2>
      <p>
        The site automatically records errors that happen in your browser so we
        can find and fix faults. When an error occurs we store:
      </p>
      <ul>
        <li>a truncated error message and stack trace,</li>
        <li>the page path you were on (no query string),</li>
        <li>your browser type, and</li>
        <li>an anonymised hash of your IP address — never the address itself.</li>
      </ul>
      <p>
        We do <strong>not</strong> collect your name, email, account details, or
        anything that identifies you personally. This is processed under the
        lawful basis of <strong>legitimate interest</strong> (reliability and
        bug fixing) and is kept for <strong>30 days</strong>.
      </p>
    </section>

    <section>
      <h2>Mesh network data</h2>
      <p>
        The site stores radio packets observed on the MeshCore network so the
        network can be monitored and improved:
      </p>
      <ul>
        <li>
          <strong>Message content and raw packet data</strong> — kept for{' '}
          <strong>30 days</strong>, then deleted automatically.
        </li>
        <li>
          <strong>Decrypted message content</strong> — kept for{' '}
          <strong>30 days</strong>, then deleted automatically.
        </li>
        <li>
          <strong>Path metadata</strong> (which nodes a packet travelled
          through, timing, signal quality) — retained for network analytics.
          It contains node identifiers and timing only, never message content,
          and is stored indefinitely in pseudonymous form.
        </li>
        <li>
          <strong>Aggregate statistics</strong> (per-day/hour counts) — retained
          indefinitely; these are anonymous.
        </li>
      </ul>
      <p>
        This is processed under the lawful basis of{' '}
        <strong>legitimate interest</strong> (operating and improving the
        network). Node identifiers can be linked to individuals only when a
        node owner has registered their node in the owner portal; in that case
        we can delete that node's data on request.
      </p>
    </section>

    <section>
      <h2>Your rights</h2>
      <p>
        Under the UK GDPR you can request access to, correction of, or deletion
        of personal data we hold about you. Because error diagnostics cannot be
        linked to individuals, deletion there happens automatically via the
        30-day retention. For node-linked data, contact us and we will remove
        the relevant records.
      </p>
      <p>
        Contact: <a href="mailto:ukmesh@proton.me">ukmesh@proton.me</a>
      </p>
    </section>
  </article>
);
