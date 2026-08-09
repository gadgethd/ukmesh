import React from 'react';

export const ContactPage: React.FC = () => (
  <div className="site-content site-prose">
    <section className="prose-section">
      <h1>Contact</h1>
      <p>
        Questions about the site, the network, or joining as an observer? Get in touch
        directly — UK Mesh is run by one person and everything sent this way gets read.
      </p>
    </section>
    <section className="prose-section">
      <h2>Ways to reach me</h2>
      <ul>
        <li>
          <strong>Discord:</strong> <code>sudogadget</code> — best for quick questions about
          the site, the mesh, or getting your observer station added.
        </li>
        <li>
          <strong>Email:</strong> <a href="mailto:ukmesh@proton.me">ukmesh@proton.me</a> — for
          anything longer, or if you prefer email.
        </li>
      </ul>
    </section>
    <section className="prose-section">
      <h2>Adding an observer</h2>
      <p>
        Want your station feeding the live map? Message me on Discord or email with your
        node&apos;s name and public key and I&apos;ll get you set up with an account.
      </p>
    </section>
  </div>
);
