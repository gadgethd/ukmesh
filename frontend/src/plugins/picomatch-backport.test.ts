import assert from 'node:assert/strict';
import { realpathSync } from 'node:fs';
import { createRequire } from 'node:module';
import test from 'node:test';

const require = createRequire(import.meta.url);
const picomatch = require('picomatch') as {
  (pattern: string, options?: { windows?: boolean; bash?: boolean; nocase?: boolean }): (input: string) => boolean;
  makeRe(pattern: string): RegExp;
};

test('frontend build tools resolve the same glob matcher', () => {
  const matcher = realpathSync(require.resolve('picomatch'));
  for (const tool of ['vite', 'tinyglobby', 'fdir']) {
    const requireFromTool = createRequire(require.resolve(tool));
    assert.equal(realpathSync(requireFromTool.resolve('picomatch')), matcher, tool);
  }
});

test('repetition across one wildcard keeps ordinary matches without a repeated regex group', () => {
  const pattern = '+(unit*end)';
  assert.doesNotMatch(picomatch.makeRe(pattern).source, /\)[*+]/);

  for (const options of [{}, { windows: true }, { bash: true }, { nocase: true }]) {
    const repeated = picomatch(pattern, options);
    assert.ok(repeated('unitend'));
    assert.ok(repeated('unitfirstendunitlastend'));
    assert.equal(repeated('unitfirst'), false);

    const alternatives = picomatch('*(unit*|x)', options);
    assert.ok(alternatives('xxunit-example'));
    assert.equal(alternatives('xyunit-example'), false);
  }
});
