import { getCurrentSite } from '../../config/site.js';
import { ToolFrame } from './ToolFrame.js';

export default function LiveMapPage() {
  const site = getCurrentSite();
  return site.embeddedTools ? <>
    <h1 className="ui-visually-hidden">Live Map</h1>
    <ToolFrame title="CoreScope Live Map" src={site.embeddedTools.mapUrl} />
  </> : <section className="site-section site-content">
    <h1>Live Map</h1>
    <a className="site-btn site-btn--primary" href={site.appUrl}>Open Live Map</a>
  </section>;
}
