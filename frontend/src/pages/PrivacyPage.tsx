import { StaticDocument } from './StaticDocument.js';
import url from './content/privacy.html?url';

export function PrivacyPage() {
  return <StaticDocument url={url} title="Privacy" />;
}
