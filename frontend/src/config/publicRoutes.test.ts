import assert from 'node:assert/strict';
import test from 'node:test';
import { PUBLIC_CONTENT_ROUTES, PUBLIC_ROUTES } from './publicRoutes.js';

test('public route manifest has unique paths and complete content metadata', () => {
  const paths = PUBLIC_ROUTES.map((route) => route.path);
  assert.equal(new Set(paths).size, paths.length);
  assert.ok(PUBLIC_CONTENT_ROUTES.length > 0);
  for (const route of PUBLIC_CONTENT_ROUTES) {
    assert.match(route.path, /^\/(?:[a-z-]+)?$/);
    assert.ok(route.title.length > 10);
    assert.ok(route.description.length > 20);
  }
});

test('redirects never enter the sitemap', () => {
  for (const route of PUBLIC_ROUTES) {
    if (route.redirectTo) assert.equal(route.sitemap, false);
  }
});
