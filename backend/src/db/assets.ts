import fs from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

export function resolveDbAssetPath(...parts: string[]): string {
  const __dirname = path.dirname(fileURLToPath(import.meta.url));
  const distPath = path.join(__dirname, ...parts);
  if (fs.existsSync(distPath)) return distPath;

  const srcPath = path.join(__dirname, '..', '..', 'src', 'db', ...parts);
  if (fs.existsSync(srcPath)) return srcPath;

  return distPath;
}
