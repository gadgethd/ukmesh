import React, { useState, useMemo } from 'react';
import {
  REGIONS, CONSTITUENT_COUNTRIES, CITY_ALIASES, COUNTRY_LABELS,
  REGION_RELATIONSHIPS, GEOGRAPHIC_GROUPS, SCOTLAND_PARENTS, getNeighbors,
  type Region,
} from './regionData.js';

type SearchResult = {
  region: Region;
  matchedCity?: string;
};

const COUNTRY_ORDER = ['gb-eng', 'gb-sct', 'gb-wls', 'gb-nir'] as const;

const COUNTRY_NOTES: Partial<Record<string, string>> = {
  'gb-sct': 'Scotland uses its own short-code convention rather than the proposed hierarchical format. Wildcard forwarding must be denied on all Scottish repeaters using region denyf * before configuring the region tree.',
  'gb-nir': 'Northern Ireland operates under the ioi (Island of Ireland) network code, shared with the wider Island of Ireland community.',
};

function countryClass(countryCode: string): string {
  const map: Record<string, string> = {
    'gb-eng': 'regions-result__country--eng',
    'gb-sct': 'regions-result__country--sct',
    'gb-wls': 'regions-result__country--wls',
    'gb-nir': 'regions-result__country--nir',
  };
  return map[countryCode] ?? '';
}

const ALL_REGIONS = [...CONSTITUENT_COUNTRIES, ...REGIONS];

function regionByCode(code: string): Region | undefined {
  return ALL_REGIONS.find(r => r.code === code);
}

// Show at most this many group chips before collapsing
const GROUP_PREVIEW = 8;

const HOP_LABELS = ['This region only', 'Immediate neighbours', 'Regional area', 'Extended area'];

function buildRegionLoad(homeCode: string, allCodes: string[], isCountryLevel: boolean): string {
  // Group codes by country
  const byCountry = new Map<string, string[]>();
  for (const code of allCodes) {
    const r = ALL_REGIONS.find(rc => rc.code === code);
    const country = r?.countryCode ?? 'gb-eng';
    if (!byCountry.has(country)) byCountry.set(country, []);
    // home region first
    if (code === homeCode) byCountry.get(country)!.unshift(code);
    else byCountry.get(country)!.push(code);
  }

  const lines: string[] = ['region load', 'gb F'];
  for (const [country, codes] of byCountry) {
    if (isCountryLevel && codes.length === 1 && codes[0] === country) {
      // country-level region — no sub-indentation needed
      lines.push(`  ${country}`);
    } else {
      lines.push(`  ${country}`);
      for (const code of codes) {
        lines.push(`    ${code}`);
      }
    }
  }
  lines.push('');
  return lines.join('\n');
}

const RelationshipsPanel: React.FC<{
  region: Region;
  onCopy: (code: string) => void;
  copied: string | null;
}> = ({ region, onCopy, copied }) => {
  const [showAllGroup, setShowAllGroup] = useState(false);
  const [neighborHops, setNeighborHops] = useState(0);
  const rel = REGION_RELATIONSHIPS[region.code];

  // Scottish/NI short codes (sco, edi, gla, ioi, …) don't start with 'gb-'
  const isShortCode = !region.code.startsWith('gb-');
  const scottishParent = isShortCode ? (SCOTLAND_PARENTS[region.code] ?? null) : undefined;

  const countrySegment = region.countryCode;
  const countryName = COUNTRY_LABELS[countrySegment];

  const historicRegion = rel?.historicParent ? regionByCode(rel.historicParent) : undefined;
  const groupName = rel?.group;
  const groupMembers = groupName ? (GEOGRAPHIC_GROUPS[groupName] ?? []) : [];
  const isCountryLevel = !isShortCode && region.code === countrySegment;

  const visibleMembers = showAllGroup ? groupMembers : groupMembers.slice(0, GROUP_PREVIEW);
  const hiddenCount = groupMembers.length - GROUP_PREVIEW;

  // Neighbor BFS
  const neighborCodes = useMemo(
    () => getNeighbors(region.code, neighborHops, REGION_RELATIONSHIPS, GEOGRAPHIC_GROUPS),
    [region.code, neighborHops],
  );

  const homeCmd = `region home ${region.code}`;

  // ── Scottish / NI short-code commands ───────────────────────────────────────
  const denyWild = 'region denyf *';

  let quickSetup: string;
  let basicLoadSetup: string;

  if (isShortCode) {
    const putLines = scottishParent
      ? [`region put ${scottishParent} *`, `region put ${region.code} ${scottishParent}`]
      : [`region put ${region.code} *`];
    quickSetup = [denyWild, ...putLines, homeCmd, 'region save'].join('\n');

    const loadIndent = scottishParent ? `  ${region.code}` : region.code;
    const loadParentLine = scottishParent ? `${scottishParent} *\n${loadIndent}` : `${region.code} *`;
    basicLoadSetup = [denyWild, 'region load', loadParentLine, '', homeCmd, 'region save'].join('\n');
  } else {
    // Standard hierarchical commands
    const hierarchyPath = isCountryLevel
      ? ['gb', region.code]
      : ['gb', countrySegment, region.code];

    const defCmd = `region def ${hierarchyPath.join(' ')}`;
    quickSetup = `${defCmd}\n${homeCmd}\nregion save`;

    const basicLoadLines = [
      'region load',
      ...hierarchyPath.map((code, i) => `${'  '.repeat(i)}${code}${i === 0 ? ' F' : ''}`),
      '',
    ];
    basicLoadSetup = `${basicLoadLines.join('\n')}${homeCmd}\nregion save`;
  }

  // Neighbour-aware commands
  const allCodes = [region.code, ...neighborCodes];
  const neighborLoadBlock = buildRegionLoad(region.code, allCodes, isCountryLevel);
  const neighborLoadSetup = isShortCode
    ? [denyWild, neighborLoadBlock, homeCmd, 'region save'].join('\n')
    : `${neighborLoadBlock}${homeCmd}\nregion save`;

  const CodeChip: React.FC<{ code: string; isSelf?: boolean }> = ({ code, isSelf }) => {
    const r = regionByCode(code);
    return (
      <button
        className={`regions-rel__chip${isSelf ? ' regions-rel__chip--self' : ''}${copied === code ? ' regions-rel__chip--copied' : ''}`}
        onClick={() => onCopy(code)}
        title={r ? `${r.name} — click to copy` : 'click to copy'}
      >
        <code>{code}</code>
        {r && <span className="regions-rel__chip-name">{r.name}</span>}
        {copied === code && <span className="regions-rel__chip-tick">✓</span>}
      </button>
    );
  };

  return (
    <div className="regions-rel">
      <div className="regions-rel__section">
        <span className="regions-rel__label">Hierarchy</span>
        <div className="regions-rel__breadcrumb">
          {isShortCode ? (
            // Scottish / NI short codes: * → [sco →] code
            <>
              <span className="regions-rel__bc-node regions-rel__bc-node--root">
                <code>*</code>
                <span>root</span>
              </span>
              {scottishParent && (
                <>
                  <span className="regions-rel__bc-sep">›</span>
                  <button className="regions-rel__bc-node" onClick={() => onCopy(scottishParent)} title={`Copy ${scottishParent}`}>
                    <code>{scottishParent}</code>
                    <span>Scotland</span>
                  </button>
                </>
              )}
              <span className="regions-rel__bc-sep">›</span>
              <button className="regions-rel__bc-node regions-rel__bc-node--self" onClick={() => onCopy(region.code)} title={`Copy ${region.code}`}>
                <code>{region.code}</code>
                <span>{region.name}</span>
                {copied === region.code && <span className="regions-rel__bc-tick">✓</span>}
              </button>
            </>
          ) : (
            // Standard gb → gb-xxx → gb-xxx-ccc hierarchy
            <>
              <button className="regions-rel__bc-node" onClick={() => onCopy('gb')} title="Copy gb">
                <code>gb</code>
                <span>United Kingdom</span>
              </button>
              <span className="regions-rel__bc-sep">›</span>
              {isCountryLevel ? (
                <button className="regions-rel__bc-node regions-rel__bc-node--self" onClick={() => onCopy(region.code)} title={`Copy ${region.code}`}>
                  <code>{region.code}</code>
                  <span>{region.name}</span>
                  {copied === region.code && <span className="regions-rel__bc-tick">✓</span>}
                </button>
              ) : (
                <>
                  <button className="regions-rel__bc-node" onClick={() => onCopy(countrySegment)} title={`Copy ${countrySegment}`}>
                    <code>{countrySegment}</code>
                    <span>{countryName}</span>
                  </button>
                  <span className="regions-rel__bc-sep">›</span>
                  <button className="regions-rel__bc-node regions-rel__bc-node--self" onClick={() => onCopy(region.code)} title={`Copy ${region.code}`}>
                    <code>{region.code}</code>
                    <span>{region.name}</span>
                    {copied === region.code && <span className="regions-rel__bc-tick">✓</span>}
                  </button>
                </>
              )}
            </>
          )}
        </div>
      </div>

      {historicRegion && (
        <div className="regions-rel__section">
          <span className="regions-rel__label">Historic county</span>
          <div className="regions-rel__historic">
            <CodeChip code={historicRegion.code} />
            <span className="regions-rel__historic-note">
              This area was part of {historicRegion.name} before 1974 local government reorganisation.
              Nodes using the historic county code can still reach devices on <code>{historicRegion.code}</code> channels.
            </span>
          </div>
        </div>
      )}

      {groupName && groupMembers.length > 0 && (
        <div className="regions-rel__section">
          <span className="regions-rel__label">{groupName} ({groupMembers.length} regions)</span>
          <div className="regions-rel__chips">
            {visibleMembers.map(code => (
              <CodeChip key={code} code={code} isSelf={code === region.code} />
            ))}
            {!showAllGroup && hiddenCount > 0 && (
              <button className="regions-rel__show-more" onClick={() => setShowAllGroup(true)}>
                +{hiddenCount} more
              </button>
            )}
          </div>
          <p className="regions-rel__group-note">
            All regions in {groupName} share this geographic area. Click any code to copy it.
          </p>
        </div>
      )}

      {!rel && !isCountryLevel && (
        <p className="regions-rel__none">
          No additional grouping relationships are defined for this region.
        </p>
      )}

      <div className="regions-rel__section">
        <span className="regions-rel__label">Repeater configuration (v1.10+)</span>
        <p className="regions-rel__cmd-intro">
          Commands to configure a MeshCore repeater for this region. Parent regions are created automatically.
        </p>

        <div className="regions-rel__neighbour-slider">
          <label className="regions-rel__neighbour-label" htmlFor={`nb-slider-${region.code}`}>
            <span>Neighbour range</span>
            <span className="regions-rel__neighbour-desc">
              {HOP_LABELS[neighborHops]}
              {neighborHops > 0 && neighborCodes.length > 0 && (
                <> &mdash; <strong>{neighborCodes.length}</strong> additional region{neighborCodes.length !== 1 ? 's' : ''}</>
              )}
            </span>
          </label>
          <input
            id={`nb-slider-${region.code}`}
            type="range"
            min={0}
            max={3}
            step={1}
            value={neighborHops}
            onChange={e => setNeighborHops(Number(e.target.value))}
            className="regions-rel__neighbour-range"
          />
          <div className="regions-rel__neighbour-ticks">
            {HOP_LABELS.map((label, i) => (
              <span key={i} className={i === neighborHops ? 'regions-rel__neighbour-tick--active' : ''}>
                {label}
              </span>
            ))}
          </div>
        </div>

        {neighborHops === 0 ? (
          <>
            <div className="regions-rel__cmd-group">
              <div className="regions-rel__cmd-header">
                <span>Using <code>region def</code> <em>remote-friendly</em></span>
                <button
                  className={`regions-rel__cmd-copy${copied === quickSetup ? ' regions-rel__cmd-copy--copied' : ''}`}
                  onClick={() => onCopy(quickSetup)}
                >
                  {copied === quickSetup ? 'Copied!' : 'Copy'}
                </button>
              </div>
              <pre className="regions-rel__cmd-block">{quickSetup}</pre>
            </div>

            <div className="regions-rel__cmd-group">
              <div className="regions-rel__cmd-header">
                <span>Using <code>region load</code> <em>serial / interactive</em></span>
                <button
                  className={`regions-rel__cmd-copy${copied === basicLoadSetup ? ' regions-rel__cmd-copy--copied' : ''}`}
                  onClick={() => onCopy(basicLoadSetup)}
                >
                  {copied === basicLoadSetup ? 'Copied!' : 'Copy'}
                </button>
              </div>
              <pre className="regions-rel__cmd-block">{basicLoadSetup}</pre>
            </div>
          </>
        ) : (
          <div className="regions-rel__cmd-group">
            <div className="regions-rel__cmd-header">
              <span>
                Using <code>region load</code>{' '}
                <em>{neighborCodes.length} region{neighborCodes.length !== 1 ? 's' : ''} included</em>
              </span>
              <button
                className={`regions-rel__cmd-copy${copied === neighborLoadSetup ? ' regions-rel__cmd-copy--copied' : ''}`}
                onClick={() => onCopy(neighborLoadSetup)}
              >
                {copied === neighborLoadSetup ? 'Copied!' : 'Copy'}
              </button>
            </div>
            <pre className="regions-rel__cmd-block">{neighborLoadSetup}</pre>
          </div>
        )}

      </div>
    </div>
  );
};

export const UKRegionsPage: React.FC = () => {
  const [query, setQuery] = useState('');
  const [copied, setCopied] = useState<string | null>(null);
  const [expandedCode, setExpandedCode] = useState<string | null>(null);
  const [openCountry, setOpenCountry] = useState<string | null>(null);

  const results = useMemo((): SearchResult[] => {
    const q = query.trim().toLowerCase();
    if (q.length < 2) return [];

    const seen = new Set<string>();
    const matched: SearchResult[] = [];

    for (const alias of CITY_ALIASES) {
      if (alias.city.toLowerCase().includes(q)) {
        if (!seen.has(alias.regionCode)) {
          seen.add(alias.regionCode);
          const region = ALL_REGIONS.find(r => r.code === alias.regionCode);
          if (region) matched.push({ region, matchedCity: alias.city });
        }
      }
    }

    for (const region of ALL_REGIONS) {
      if (!seen.has(region.code) && (
        region.name.toLowerCase().includes(q) ||
        region.code.includes(q)
      )) {
        seen.add(region.code);
        matched.push({ region });
      }
    }

    return matched.slice(0, 15);
  }, [query]);

  const byCountry = useMemo(() => {
    const groups: Partial<Record<string, Region[]>> = {};
    for (const r of REGIONS) {
      (groups[r.countryCode] ??= []).push(r);
    }
    return groups;
  }, []);

  const copyCode = async (code: string) => {
    await navigator.clipboard.writeText(code);
    setCopied(code);
    setTimeout(() => setCopied(null), 2000);
  };

  const toggleExpanded = (code: string) => {
    setExpandedCode(prev => (prev === code ? null : code));
  };

  const toggleCountry = (cc: string) => {
    setOpenCountry(prev => (prev === cc ? null : cc));
  };

  return (
    <>
      <section className="site-home">
        <div className="site-content">
          <div className="site-home__intro">
            <h1 className="site-home__title">UK MeshCore Proposed Regions</h1>
            <p className="site-home__body">
              Region codes give every MeshCore node, repeater, channel, and map a consistent,
              hierarchical location identifier across the UK and British Isles. Based on
              ISO&nbsp;3166-2:GB with Chapman code alignment. Format:&nbsp;
              <code className="regions-inline-code">gb-xxx-ccc</code>
            </p>
          </div>
        </div>
      </section>

      <section className="site-section">
        <div className="site-content">
          <div className="regions-search">
            <label className="regions-search__label" htmlFor="region-search">
              Find your region code
            </label>
            <input
              id="region-search"
              type="text"
              className="regions-search__input"
              placeholder="Type a city, town, or area name..."
              value={query}
              onChange={e => { setQuery(e.target.value); setExpandedCode(null); }}
              autoComplete="off"
              autoFocus
            />

            {query.trim().length >= 2 && (
              <div className="regions-results" aria-live="polite">
                {results.length === 0 ? (
                  <p className="regions-no-results">
                    No match for <strong>{query.trim()}</strong> — try a nearby larger town or the region name.
                  </p>
                ) : (
                  results.map(({ region, matchedCity }) => {
                    const isExpanded = expandedCode === region.code;
                    return (
                      <div
                        key={region.code}
                        className={`regions-result${isExpanded ? ' regions-result--expanded' : ''}`}
                      >
                        <div className="regions-result__row">
                          <button
                            className="regions-result__main"
                            onClick={() => toggleExpanded(region.code)}
                            aria-expanded={isExpanded}
                          >
                            <code className="regions-result__code">{region.code}</code>
                            <div className="regions-result__info">
                              <span className="regions-result__name">{region.name}</span>
                              {matchedCity && matchedCity.toLowerCase() !== region.name.toLowerCase() && (
                                <span className="regions-result__via">via {matchedCity}</span>
                              )}
                            </div>
                            <span className={`regions-result__country ${countryClass(region.countryCode)}`}>
                              {COUNTRY_LABELS[region.countryCode]}
                            </span>
                          </button>
                          <div className="regions-result__actions">
                            <button
                              className={`regions-result__copy${copied === region.code ? ' regions-result__copy--copied' : ''}`}
                              onClick={e => { e.stopPropagation(); copyCode(region.code); }}
                              aria-label={`Copy ${region.code}`}
                            >
                              {copied === region.code ? 'Copied!' : 'Copy'}
                            </button>
                            <button
                              className={`regions-result__toggle${isExpanded ? ' regions-result__toggle--open' : ''}`}
                              onClick={() => toggleExpanded(region.code)}
                              aria-label={isExpanded ? 'Hide relationships' : 'Show relationships'}
                            >
                              {isExpanded ? '▲' : '▼'}
                            </button>
                          </div>
                        </div>
                        {isExpanded && (
                          <RelationshipsPanel
                            region={region}
                            onCopy={copyCode}
                            copied={copied}
                          />
                        )}
                      </div>
                    );
                  })
                )}
              </div>
            )}

            {query.trim().length === 0 && (
              <p className="regions-search__hint">
                Search covers England, Scotland, Wales, and Northern Ireland — cities, towns, boroughs, and region names. Select a result to see its hierarchy and relationships.
              </p>
            )}
          </div>
        </div>
      </section>

      <section className="site-section site-section--dark">
        <div className="site-content">
          <div className="site-section__head">
            <h2>Specification — v1.1</h2>
            <p>
              All codes lowercase-with-dashes. Direct mapping to ISO&nbsp;3166-2:GB (e.g.{' '}
              <code className="regions-inline-code">gb-eng-bst</code> ↔ <code className="regions-inline-code">GB-BST</code>).
            </p>
          </div>

          <div className="regions-spec__format">
            <h3>Code structure</h3>
            <div className="regions-spec__format-grid">
              <div className="regions-spec__format-row">
                <code>gb</code>
                <span>ISO 3166-1 alpha-2 for United Kingdom (always lowercase)</span>
              </div>
              <div className="regions-spec__format-row">
                <code>xxx</code>
                <span>3-letter constituent country — <code>eng</code> / <code>sct</code> / <code>wls</code> / <code>nir</code></span>
              </div>
              <div className="regions-spec__format-row">
                <code>ccc</code>
                <span>3-letter area code from ISO 3166-2:GB / Chapman (lowercase)</span>
              </div>
            </div>
            <div className="regions-spec__examples">
              <span>Examples:</span>
              {[
                ['Bristol', 'gb-eng-bst'],
                ['Edinburgh', 'gb-sct-edh'],
                ['Cardiff', 'gb-wls-crf'],
                ['Belfast', 'gb-nir-bfs'],
              ].map(([label, code]) => (
                <span key={code} className="regions-spec__example-item">
                  {label}: <code>{code}</code>
                </span>
              ))}
            </div>
          </div>

          <div className="regions-spec__rules">
            <h3>Rules</h3>
            <ol>
              <li>Always use the full <code className="regions-inline-code">gb-xxx-ccc</code> format for region-specific settings.</li>
              <li>Parsing is case-insensitive, but storage and transmission must be lowercase-with-dashes.</li>
              <li>Historic data may use parent/fallback codes (e.g. historic Bristol under <code className="regions-inline-code">gb-eng-gls</code>).</li>
              <li>Updates must stay within official ISO 3166-2:GB + Chapman compatibility.</li>
            </ol>
          </div>

          <div className="regions-spec__countries">
            <h3>All regions</h3>
            {COUNTRY_ORDER.map(cc => (
              <div key={cc} className="regions-spec__country">
                <button
                  className="regions-spec__country-toggle"
                  onClick={() => toggleCountry(cc)}
                  aria-expanded={openCountry === cc}
                >
                  <span>{COUNTRY_LABELS[cc]}</span>
                  <span className="regions-spec__country-meta">
                    {byCountry[cc]?.length ?? 0} regions
                    <span className="regions-spec__chevron">{openCountry === cc ? '▲' : '▼'}</span>
                  </span>
                </button>
                {openCountry === cc && (
                  <>
                    {COUNTRY_NOTES[cc] && (
                      <div className="regions-spec__country-note">
                        <p>{COUNTRY_NOTES[cc]}</p>
                      </div>
                    )}
                    <table className="regions-spec__table">
                      <thead>
                        <tr>
                          <th>Code</th>
                          <th>Region</th>
                        </tr>
                      </thead>
                      <tbody>
                        {byCountry[cc]?.map(r => (
                          <tr key={r.code}>
                            <td>
                              <button
                                className="regions-spec__code-btn"
                                onClick={() => copyCode(r.code)}
                                title="Click to copy"
                              >
                                <code>{r.code}</code>
                                {copied === r.code && <span className="regions-spec__copied">✓</span>}
                              </button>
                            </td>
                            <td>{r.name}</td>
                          </tr>
                        ))}
                      </tbody>
                    </table>
                  </>
                )}
              </div>
            ))}
          </div>
        </div>
      </section>
    </>
  );
};
