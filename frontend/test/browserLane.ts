import test from 'node:test';
import type { LaunchOptions } from '@playwright/test';

/** One optional browser lane; historical selectors remain supported for operator scripts. */
export function browserLane(legacyFlag: string, legacyOrigin: string, defaultOrigin: string) {
  return {
    browserTest: process.env['UKMESH_BROWSER_TESTS'] === '1' || process.env[legacyFlag] === '1' ? test : test.skip,
    origin: process.env['UKMESH_BROWSER_BASE_URL'] ?? process.env[legacyOrigin] ?? defaultOrigin,
  };
}

export function browserLaunchOptions(): LaunchOptions {
  return { executablePath: process.env['PLAYWRIGHT_CHROMIUM_EXECUTABLE_PATH'], headless: true,
    args: ['--use-gl=angle', '--use-angle=swiftshader', '--enable-unsafe-swiftshader'] };
}
