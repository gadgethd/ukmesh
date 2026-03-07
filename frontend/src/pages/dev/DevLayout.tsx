import React from 'react';
import { Outlet } from 'react-router-dom';

export const DevLayout: React.FC = () => (
  <main className="dev-status-shell">
    <Outlet />
  </main>
);
