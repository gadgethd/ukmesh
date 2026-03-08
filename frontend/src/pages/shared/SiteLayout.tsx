import React, { useEffect, useState } from 'react';
import { Link, NavLink, Outlet, useNavigate } from 'react-router-dom';

type SiteLayoutProps = {
  brandName: string;
  footerName: string;
  appUrl: string;
  showFeed?: boolean;
  showLiveMap?: boolean;
  showAbout?: boolean;
  showInstall?: boolean;
  showMqtt?: boolean;
  showHealth?: boolean;
  showOpenSource?: boolean;
  showPackets: boolean;
  showStats: boolean;
};

type NavItem = {
  to: string;
  label: string;
  enabled: boolean;
};

const OWNER_SESSION_EVENT = 'meshcore-owner-session';

type OwnerSessionSummary = {
  ok: boolean;
  mqttUsername?: string | null;
};

function navClassName({ isActive }: { isActive: boolean }): string {
  return isActive ? 'site-nav__link site-nav__link--active' : 'site-nav__link';
}

export const SiteLayout: React.FC<SiteLayoutProps> = ({
  brandName,
  footerName,
  appUrl,
  showFeed = false,
  showLiveMap = true,
  showAbout = true,
  showInstall = true,
  showMqtt = true,
  showHealth = true,
  showOpenSource = true,
  showPackets,
  showStats,
}) => {
  const [menuOpen, setMenuOpen] = useState(false);
  const [ownerLabel, setOwnerLabel] = useState<string | null>(null);
  const navigate = useNavigate();

  const navItems: NavItem[] = [
    { to: '/', label: 'Home', enabled: true },
    { to: '/feed', label: 'Feed', enabled: showFeed },
    { to: '/about', label: 'What is MeshCore', enabled: showAbout },
    { to: '/install', label: 'Install', enabled: showInstall },
    { to: '/mqtt', label: 'MQTT', enabled: showMqtt },
    { to: '/health', label: 'Health', enabled: showHealth },
    { to: '/packets', label: 'Packets', enabled: showPackets },
    { to: '/open-source', label: 'Open Source', enabled: showOpenSource },
    { to: '/stats', label: 'Stats', enabled: showStats },
  ];

  const closeMenu = () => setMenuOpen(false);
  const handleNavClick = (to: string) => {
    closeMenu();
    navigate(to);
  };

  useEffect(() => {
    let cancelled = false;

    const loadOwnerSession = () => {
      fetch('/api/owner/session', { cache: 'no-store' })
        .then(async (res) => {
          if (!res.ok) return null;
          return (await res.json()) as OwnerSessionSummary;
        })
        .then((json) => {
          if (cancelled) return;
          setOwnerLabel(json?.mqttUsername?.trim() || null);
        })
        .catch(() => {
          if (cancelled) return;
          setOwnerLabel(null);
        });
    };

    const handleOwnerSession = (event: Event) => {
      const detail = (event as CustomEvent<{ mqttUsername?: string | null }>).detail;
      setOwnerLabel(detail?.mqttUsername?.trim() || null);
    };

    loadOwnerSession();
    window.addEventListener(OWNER_SESSION_EVENT, handleOwnerSession as EventListener);
    return () => {
      cancelled = true;
      window.removeEventListener(OWNER_SESSION_EVENT, handleOwnerSession as EventListener);
    };
  }, []);

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
          {showLiveMap && <a href={appUrl} className="site-nav__link">Live Map</a>}
          <NavLink
            to="/login"
            onClick={() => handleNavClick('/login')}
            className={({ isActive }) => isActive ? 'site-nav__app-btn site-nav__app-btn--active' : 'site-nav__app-btn'}
          >
            {ownerLabel ?? 'Login'}
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
        {showLiveMap && (
          <>
            <span className="site-footer__sep">·</span>
            <a href={appUrl}>Live Map</a>
          </>
        )}
      </footer>
    </div>
  );
};
