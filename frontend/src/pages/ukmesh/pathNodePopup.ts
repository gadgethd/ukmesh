export type PathNodePopupInput = {
  displayName: string;
  publicKey: string;
  isObserver: boolean;
};

export function buildPathNodePopupContent(input: PathNodePopupInput): HTMLElement {
  const root = document.createElement('div');
  root.className = 'path-node-popup';

  const title = document.createElement('strong');
  title.className = 'path-node-popup__title';
  title.append(document.createTextNode(input.displayName));
  if (input.isObserver) {
    const badge = document.createElement('span');
    badge.className = 'path-node-popup__observer';
    badge.textContent = ' [observer]';
    title.append(badge);
  }

  const label = document.createElement('span');
  label.className = 'path-node-popup__label';
  label.textContent = 'Public key';

  const key = document.createElement('span');
  key.className = 'path-node-popup__key';
  key.textContent = input.publicKey;

  root.append(title, document.createElement('br'), label, document.createElement('br'), key);
  return root;
}
