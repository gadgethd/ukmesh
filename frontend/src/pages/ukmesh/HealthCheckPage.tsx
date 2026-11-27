import '../site-content.css';
import { Link, useSearchParams } from 'react-router';
import { getCurrentSite } from '../../config/site.js';
import { useRuntimeFeatures } from '../../config/runtimeFeatures.js';
import { useNodeLookup } from '../../hooks/useNodeNameSearch.js';
import { isPublicRepeater } from '../../utils/exploreLinks.js';
import { validRegionName } from '../../utils/regionUrlState.js';
import { ToolFrame } from './ToolFrame.js';

export default function HealthCheckPage() {
  const site = getCurrentSite();
  const { privacyGeneration } = useRuntimeFeatures();
  const [params] = useSearchParams();
  const keys = params.getAll('node');
  const key = keys.length === 1 && /^[a-f0-9]{64}$/i.test(keys[0]) ? keys[0].toLowerCase() : '';
  const regions = params.getAll('region');
  const region = regions.length === 1 && validRegionName(regions[0]) ? regions[0] : '';
  const lookup = useNodeLookup(key, { network: site.networkFilter, observer: site.observerId, privacyGeneration });
  const repeater = lookup.nodes.find(node => node.node_id.toLowerCase() === key && isPublicRepeater(node));

  const context = <>
    {repeater && <p>Selected repeater <strong>{repeater.name || 'Unnamed repeater'}</strong></p>}
    {region && <p>Requested region <strong>{region}</strong></p>}
    {key && lookup.status === 'loading' && <p role="status">Looking up the selected repeater…</p>}
    {key && lookup.status !== 'loading' && !repeater && <p role="status">Repeater details are unavailable. You can still run a check.</p>}
  </>;

  if (site.embeddedTools) return <>
    <h1 className="ui-visually-hidden">Health Check</h1>
    {(key || region) && <div className="site-content site-tool-context">{context}</div>}
    <ToolFrame title="UKMesh Health Check" src={site.embeddedTools.healthCheckUrl} />
  </>;

  return (
    <section className="site-section site-content">
      <div className="site-home__grid">
        <div className="site-home__intro">
          <h1 className="site-home__title">Health Check</h1>
          <p className="site-home__body">Send a test message from your MeshCore radio to see which observers hear it.</p>
          <div className="site-home__actions">
            <a className="site-btn site-btn--primary" href={site.healthCheckUrl} referrerPolicy="no-referrer">Open Health Check</a>
            <Link className="site-btn site-btn--ghost" to="/repeater">Browse repeaters</Link>
          </div>
          {context}
        </div>
        <section className="site-home__panel">
          <h2>How to run a check</h2>
          <ol className="site-check-steps">
            <li>Choose your observers and generate a code.</li>
            <li>Send the code from your radio on the channel shown.</li>
            <li>Watch for receipts and share the result if you need help.</li>
          </ol>
        </section>
      </div>
    </section>
  );
}
