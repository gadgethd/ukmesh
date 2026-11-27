import { useState } from 'react';
import { useNavigate } from 'react-router';
import { Combobox } from '../../components/ui/Combobox.js';
import { useNodeLookup } from '../../hooks/useNodeNameSearch.js';
import { isPublicRepeater } from '../../utils/exploreLinks.js';
import type { PublicScope } from '../../utils/scope.js';

export default function HomeRepeaterSearch({ scope }: { scope: PublicScope }) {
  const navigate = useNavigate();
  const [query, setQuery] = useState('');
  const [open, setOpen] = useState(false);
  const [attempt, setAttempt] = useState(0);
  const { status, nodes } = useNodeLookup(query, scope, attempt);
  return <Combobox label="Find a repeater" className="site-home__search" autoFocus
    value={query} onValueChange={value => { setQuery(value); setOpen(Boolean(value.trim())); }}
    isOpen={open && Boolean(query.trim())} onOpenChange={setOpen}
    options={nodes.filter(isPublicRepeater).map(node => ({
      id: node.node_id, label: node.name || node.node_id.slice(0, 12), description: node.iata,
    }))}
    onSelectionChange={node => navigate(`/repeater?${new URLSearchParams({ q: query.trim(), node })}`)}
    placeholder="Name, observer code or key prefix"
    emptyContent={status === 'loading' ? 'Searching repeaters…' : status === 'invalid' ? 'Enter up to 160 characters.'
      : status === 'error' ? 'Repeater search is unavailable.' : 'No matching repeaters.'}
    footer={status === 'error' ? <button type="button" className="site-btn site-btn--ghost"
      onClick={() => {
        if (scope.privacyGeneration < 1) window.location.reload();
        else setAttempt(value => value + 1);
      }}>Retry search</button> : undefined}
  />;
}
