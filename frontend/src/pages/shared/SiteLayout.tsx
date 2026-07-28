import React, { useEffect, useRef, useState } from 'react';
import { Link, NavLink, Outlet } from 'react-router-dom';

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
  showBestPractice?: boolean;
  showPackets: boolean;
  showStats: boolean;
  showRepeaterSearch?: boolean;
  showCompanion?: boolean;
  showRegions?: boolean;
  showTopology?: boolean;
  showSpam?: boolean;
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
  showBestPractice = false,
  showPackets,
  showStats,
  showRepeaterSearch = false,
  showCompanion = false,
  showRegions = false,
  showTopology = false,
  showSpam = false,
}) => {
  const COOKIE_CONSENT_KEY = 'meshcore-cookie-consent-v1';
  const [menuOpen, setMenuOpen] = useState(false);
  const navRef = useRef<HTMLElement>(null);
  const [ownerLabel, setOwnerLabel] = useState<string | null>(null);
  const [activeSpamIncidents, setActiveSpamIncidents] = useState(0);
  const [cookieConsent, setCookieConsent] = useState<boolean>(() => {
    try {
      return localStorage.getItem(COOKIE_CONSENT_KEY) === '1';
    } catch {
      return false;
    }
  });
  const navItems: NavItem[] = [
    { to: '/feed', label: 'Feed', enabled: showFeed },
    { to: '/repeater', label: 'Repeaters', enabled: showRepeaterSearch },
    { to: '/companion', label: 'Companions', enabled: showCompanion },
    { to: '/regions', label: 'Regions', enabled: showRegions },
    { to: '/topology', label: 'Topology', enabled: showTopology },
    { to: '/spam', label: 'Spam', enabled: showSpam },
    { to: '/about', label: 'What is MeshCore', enabled: showAbout },
    { to: '/install', label: 'Install', enabled: showInstall },
    { to: '/mqtt', label: 'MQTT', enabled: showMqtt },
    { to: '/health', label: 'Health', enabled: showHealth },
    { to: '/packets', label: 'Packets', enabled: showPackets },
    { to: '/open-source', label: 'Open Source', enabled: showOpenSource },
    { to: '/stats', label: 'Stats', enabled: showStats },
    { to: '/docs', label: 'Docs', enabled: showBestPractice },
  ];

  const closeMenu = () => setMenuOpen(false);

  useEffect(() => {
    if (!showSpam) return;
    const controller = new AbortController();
    fetch('/api/spam/messages/status', { signal: controller.signal })
      .then((response) => response.ok ? response.json() as Promise<{ activeIncidents?: number }> : null)
      .then((value) => setActiveSpamIncidents(Number(value?.activeIncidents ?? 0)))
      .catch(() => {});
    return () => controller.abort();
  }, [showSpam]);

  useEffect(() => {
    if (!menuOpen) return undefined;

    const handleKeyDown = (event: KeyboardEvent) => {
      if (event.key === 'Escape') closeMenu();
    };
    const handlePointerDown = (event: PointerEvent) => {
      if (!navRef.current?.contains(event.target as Node)) closeMenu();
    };

    document.addEventListener('keydown', handleKeyDown);
    document.addEventListener('pointerdown', handlePointerDown);
    return () => {
      document.removeEventListener('keydown', handleKeyDown);
      document.removeEventListener('pointerdown', handlePointerDown);
    };
  }, [menuOpen]);

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

  useEffect(() => {
    if (!menuOpen) return;

    const handleKeyDown = (event: KeyboardEvent) => {
      if (event.key === 'Escape') setMenuOpen(false);
    };

    window.addEventListener('keydown', handleKeyDown);
    return () => window.removeEventListener('keydown', handleKeyDown);
  }, [menuOpen]);

  const acceptCookies = () => {
    try {
      localStorage.setItem(COOKIE_CONSENT_KEY, '1');
    } catch {
      // Ignore storage failures and just hide the banner for this session.
    }
    setCookieConsent(true);
  };

  return (
    <div className="site-layout">
      <nav ref={navRef} className="site-nav">
        <Link to="/" className="site-nav__brand" onClick={closeMenu}>
          <span className="site-nav__icon">◈</span>
          <span className="site-nav__name">{brandName}</span>
        </Link>

        <div
          id="site-navigation"
          className={`site-nav__links${menuOpen ? ' site-nav__links--open' : ''}`}
        >
          {showLiveMap && <a href={appUrl} className="site-nav__link site-nav__link--map">Live Map ↗</a>}
          {navItems.filter((item) => item.enabled).map((item) => (
            <NavLink
              key={item.to}
              to={item.to}
              end={item.to === '/'}
              onClick={closeMenu}
              className={navClassName}
            >
              {item.label}
              {item.to === '/spam' && activeSpamIncidents > 0 && <span className="site-nav__badge" aria-label={`${activeSpamIncidents} active incidents`}>{activeSpamIncidents}</span>}
            </NavLink>
          ))}
          <a href="https://healthcheck.ukmesh.com" className="site-nav__link">Health Check</a>
          <NavLink
            to="/login"
            onClick={closeMenu}
            className={({ isActive }) => isActive ? 'site-nav__app-btn site-nav__app-btn--active' : 'site-nav__app-btn'}
          >
            {ownerLabel ?? 'Login'}
          </NavLink>
        </div>

        <button
          type="button"
          className="site-nav__hamburger"
          aria-label={menuOpen ? 'Close menu' : 'Open menu'}
          aria-controls="site-navigation"
          aria-expanded={menuOpen}
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
        <a href="https://meshcore.gg/" target="_blank" rel="noopener noreferrer">Discord</a>
        <span className="site-footer__sep">·</span>
        <Link to="/stats">Stats</Link>
        <span className="site-footer__sep">·</span>
        <Link to="/open-source">Open Source</Link>
        {showLiveMap && (
          <>
            <span className="site-footer__sep">·</span>
            <a href={appUrl}>Live Map</a>
          </>
        )}
      </footer>

      {!cookieConsent && (
        <div className="cookie-banner" role="dialog" aria-live="polite" aria-label="Cookie notice">
          <div className="cookie-banner__body">
            <strong>Cookies, sadly.</strong>
            <p>We only use them for the boring useful bits, like keeping logins alive and remembering site choices. No secret biscuit syndicate.</p>
          </div>
          <button className="cookie-banner__button" onClick={acceptCookies}>Accept</button>
        </div>
      )}
    </div>
  );
};
