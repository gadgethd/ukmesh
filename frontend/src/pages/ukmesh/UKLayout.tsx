import React, { useState } from 'react';
import { NavLink, Outlet, Link, useNavigate } from 'react-router-dom';

const APP_URL = 'https://app.ukmesh.com';

export const UKLayout: React.FC = () => {
  const [menuOpen, setMenuOpen] = useState(false);
  const navigate = useNavigate();

  const closeMenu = () => setMenuOpen(false);
  const handleNavClick = (to: string) => { closeMenu(); navigate(to); };

  return (
  <div className="site-layout">
    <nav className="site-nav">
      <Link to="/" className="site-nav__brand" onClick={closeMenu}>
        <span className="site-nav__icon">◈</span>
        <span className="site-nav__name">UK Mesh</span>
      </Link>

      <div className={`site-nav__links${menuOpen ? ' site-nav__links--open' : ''}`}>
        <NavLink to="/" end onClick={() => handleNavClick('/')} className={({ isActive }) => isActive ? 'site-nav__link site-nav__link--active' : 'site-nav__link'}>
          Home
        </NavLink>
        <NavLink to="/about" onClick={() => handleNavClick('/about')} className={({ isActive }) => isActive ? 'site-nav__link site-nav__link--active' : 'site-nav__link'}>
          What is MeshCore
        </NavLink>
        <NavLink to="/install" onClick={() => handleNavClick('/install')} className={({ isActive }) => isActive ? 'site-nav__link site-nav__link--active' : 'site-nav__link'}>
          Install
        </NavLink>
        <NavLink to="/mqtt" onClick={() => handleNavClick('/mqtt')} className={({ isActive }) => isActive ? 'site-nav__link site-nav__link--active' : 'site-nav__link'}>
          MQTT
        </NavLink>
        <NavLink to="/open-source" onClick={() => handleNavClick('/open-source')} className={({ isActive }) => isActive ? 'site-nav__link site-nav__link--active' : 'site-nav__link'}>
          Open Source
        </NavLink>
        <a href={APP_URL} className="site-nav__link">Live Map</a>
      </div>

      <button
        className="site-nav__hamburger"
        aria-label={menuOpen ? 'Close menu' : 'Open menu'}
        onClick={() => setMenuOpen((o) => !o)}
      >
        {menuOpen ? '✕' : '☰'}
      </button>
    </nav>

    <main className="site-main">
      <Outlet />
    </main>

    <footer className="site-footer">
      <span>UK Mesh Network</span>
      <span className="site-footer__sep">·</span>
      <a href="https://discord.gg/bSuST8xvet" target="_blank" rel="noopener noreferrer">Discord</a>
      <span className="site-footer__sep">·</span>
      <Link to="/open-source">Open Source</Link>
      <span className="site-footer__sep">·</span>
      <a href={APP_URL}>Live Map</a>
    </footer>
  </div>
  );
};
