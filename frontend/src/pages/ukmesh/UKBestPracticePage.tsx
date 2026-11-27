import { StaticDocument } from '../StaticDocument.js';
import url from '../content/docs.html?url';

export function UKBestPracticePage() {
  return <StaticDocument url={url} title="UK Mesh documentation" />;
}
