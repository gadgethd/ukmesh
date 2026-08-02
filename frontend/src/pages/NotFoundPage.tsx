import React from 'react';
import { Link } from 'react-router';

export const NotFoundPage: React.FC = () => (
  <main className="site-layout__inner site-content">
    <section className="docs-page">
      <p className="docs-page__eyebrow">404</p>
      <h1>That page is not on the mesh</h1>
      <p>The address may be old or mistyped. The live map, feed, and guides are still available.</p>
      <p><Link to="/">Return to the UK Mesh home page</Link></p>
    </section>
  </main>
);
