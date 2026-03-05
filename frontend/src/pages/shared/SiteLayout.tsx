import React, { useState } from 'react';
import { Link, NavLink, Outlet, useNavigate } from 'react-router-dom';

type SiteLayoutProps = {
  brandName: string;
  footerName: string;
  appUrl: string;
  showPackets: boolean;
  showStats: boolean;
};

type NavItem = {
  to: string;
  label: string;
  enabled: boolean;
};

function navClassName({ isActive }: { isActive: boolean }): string {
  return isActive ? 'site-nav__link site-nav__link--active' : 'site-nav__link';
}

export const SiteLayout: React.FC<SiteLayoutProps> = ({
  brandName,
  footerName,
  appUrl,
  showPackets,
  showStats,
}) => {
  const [menuOpen, setMenuOpen] = useState(false);
  const navigate = useNavigate();

  const navItems: NavItem[] = [
    { to: '/', label: 'Home', enabled: true },
    { to: '/about', label: 'What is MeshCore', enabled: true },
    { to: '/install', label: 'Install', enabled: true },
    { to: '/mqtt', label: 'MQTT', enabled: true },
    { to: '/health', label: 'Health', enabled: true },
    { to: '/packets', label: 'Packets', enabled: showPackets },
    { to: '/open-source', label: 'Open Source', enabled: true },
    { to: '/stats', label: 'Stats', enabled: showStats },
  ];

  const closeMenu = () => setMenuOpen(false);
  const handleNavClick = (to: string) => {
    closeMenu();
    navigate(to);
  };

  return (
    <div className="site-layout">
      <nav className="site-nav">
        <Link to="/" className="site-nav__brand" onClick={closeMenu}>
          <span className="site-nav__icon">◈</span>
          <span className="site-nav__name">{brandName}</span>
        </Link>

        <div className={`site-nav__links${menuOpen ? ' site-nav__links--open' : ''}`}>
          {navItems.filter((item) => item.enabled).map((item) => (
            <NavLink
              key={item.to}
              to={item.to}
              end={item.to === '/'}
              onClick={() => handleNavClick(item.to)}
              className={navClassName}
            >
              {item.label}
            </NavLink>
          ))}
          <a href={appUrl} className="site-nav__link">Live Map</a>
          <NavLink
            to="/login"
            onClick={() => handleNavClick('/login')}
            className={({ isActive }) => isActive ? 'site-nav__app-btn site-nav__app-btn--active' : 'site-nav__app-btn'}
          >
            Login
          </NavLink>
        </div>

        <button
          className="site-nav__hamburger"
          aria-label={menuOpen ? 'Close menu' : 'Open menu'}
          onClick={() => setMenuOpen((open) => !open)}
        >
          {menuOpen ? '✕' : '☰'}
        </button>
      </nav>

      <main className="site-main">
        <Outlet />
      </main>

      <footer className="site-footer">
        <span>{footerName}</span>
        <span className="site-footer__sep">·</span>
        <a href="https://discord.gg/bSuST8xvet" target="_blank" rel="noopener noreferrer">Discord</a>
        <span className="site-footer__sep">·</span>
        <Link to="/open-source">Open Source</Link>
        <span className="site-footer__sep">·</span>
        <a href={appUrl}>Live Map</a>
      </footer>
    </div>
  );
};
