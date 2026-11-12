import assert from 'node:assert/strict';
import { test } from 'node:test';
import { runInNewContext } from 'node:vm';
import { deduplicateLegalComments } from './legal-comments.js';

test('each distinct notice remains once per chunk, including line comments', () => {
  const code = '/*! @license A */\nconst a = 1;\n/*! @license A */\n'
    + '//! @preserve B\nconst b = 2;\n//! @preserve B\n'
    + '/* @license C */\nexport {a, b};';
  const result = deduplicateLegalComments(code, 'notices.js')!;
  for (const notice of ['/*! @license A */', '//! @preserve B', '/* @license C */']) {
    assert.equal(result.split(notice).length - 1, 1);
  }
  // A later build/chunk must retain its own notice too.
  assert.equal(deduplicateLegalComments(result, 'other.js'), null);
});

test('notice-like strings and templates survive with unicode offsets', () => {
  const code = 'const emoji="😀";/*! @license A */'
    + 'const str="/*! @license A */";const tpl=`/*! @license A */`;'
    + '/*! @license A */export {emoji,str,tpl};';
  const result = deduplicateLegalComments(code, 'strings.js')!;
  assert.ok(result.includes('const str="/*! @license A */"'));
  assert.ok(result.includes('const tpl=`/*! @license A */`'));
  assert.ok(result.includes('"😀"'));
  assert.equal(result.split('/*! @license A */').length - 1, 3);
});

test('unique notices and invalid JavaScript are handled without silent loss', () => {
  assert.equal(deduplicateLegalComments('/*! @license A */export const a=1;', 'unique.js'), null);
  assert.throws(() => deduplicateLegalComments('/*! @license A */export {', 'broken.js'));
});

test('removing duplicate comments preserves automatic semicolon insertion', () => {
  const inline = '/*! @license A */function f(){return /*! @license A */ 42}f()';
  assert.equal(runInNewContext(deduplicateLegalComments(inline, 'inline.js')!), 42);
  const multiline = '/*! @license A\n */function f(){return /*! @license A\n */ 42}f()';
  assert.equal(runInNewContext(deduplicateLegalComments(multiline, 'multiline.js')!), undefined);
});
