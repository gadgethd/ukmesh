import { readFile } from 'node:fs/promises';
import { gzipSync } from 'node:zlib';
import path from 'node:path';

const directory = path.resolve('dist');
const target = JSON.parse(await readFile(path.join(directory, 'build-target.json'), 'utf8')).target;
if (target !== 'public-website') {
  console.log(`Public route budgets do not apply to ${target}; artifact budgets remain required.`);
} else {
  const manifest = JSON.parse(await readFile(path.join(directory, '.vite/manifest.json'), 'utf8'));
  const config = JSON.parse(await readFile(new URL('./route-budgets.json', import.meta.url), 'utf8'));
  let failed = false;
  for (const [route, budget] of Object.entries(config.routes)) {
    const visited = new Set();
    const scripts = new Set();
    const styles = new Set();
    const visit = (key) => {
      if (visited.has(key)) return;
      visited.add(key);
      const entry = manifest[key];
      if (!entry) throw new Error(`Missing manifest entry ${key} for ${route}`);
      if (entry.file.endsWith('.js')) scripts.add(entry.file);
      for (const css of entry.css ?? []) styles.add(css);
      for (const dependency of entry.imports ?? []) visit(dependency);
    };
    for (const entry of ['index.html', 'src/components/app/PublicWebsite.tsx', 'src/pages/ukmesh/UKLayout.tsx', budget.module]) visit(entry);
    const bytes = async (files) => (await Promise.all([...files].map(async (file) => gzipSync(await readFile(path.join(directory, file))).length))).reduce((a, b) => a + b, 0);
    const scriptBytes = await bytes(scripts);
    const cssBytes = await bytes(styles);
    const forbidden = [...scripts].filter((file) => (budget.forbiddenChunks ?? []).some((name) => file.includes(name)));
    const exceeds = scriptBytes > budget.scriptGzipKiB * 1024 || cssBytes > budget.cssGzipKiB * 1024 || forbidden.length > 0;
    console.log(`${exceeds ? 'FAIL' : 'PASS'} ${route}: ${(scriptBytes / 1024).toFixed(1)} KiB script / ${(cssBytes / 1024).toFixed(1)} KiB CSS gzip (limits ${budget.scriptGzipKiB}/${budget.cssGzipKiB})`);
    if (forbidden.length) console.error(`Forbidden cold-route dependencies: ${forbidden.join(', ')}`);
    failed ||= exceeds;
  }
  if (failed) process.exitCode = 1;
}
