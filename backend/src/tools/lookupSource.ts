import { createHash } from 'node:crypto';
import { createReadStream } from 'node:fs';
import { readdir, realpath } from 'node:fs/promises';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

export type LookupSourceIdentity = { source_root: string; revision: string | null; sha256: string; files: number };

/** One byte-based identity for current sources and ignored baseline archives. */
export async function lookupSourceIdentity(directory: string, revision: string | null = null): Promise<LookupSourceIdentity> {
  if (revision !== null && !/^[a-f0-9]{7,40}$/.test(revision)) throw new Error('lookup source reference revision is invalid');
  const root = await realpath(directory);
  const files: string[] = [];
  async function walk(relative: string): Promise<void> {
    for (const entry of await readdir(path.join(root, relative), { withFileTypes: true })) {
      const name = relative ? relative + '/' + entry.name : entry.name;
      if (entry.isSymbolicLink()) throw new Error('lookup source contains a symbolic link: ' + name);
      if (entry.isDirectory()) await walk(name);
      else if (entry.isFile()) files.push(name);
      else throw new Error('lookup source contains a non-file entry: ' + name);
    }
  }
  await walk('');
  if (!files.length) throw new Error('lookup source tree is empty');
  files.sort((a, b) => Buffer.compare(Buffer.from(a), Buffer.from(b)));
  const checksum = createHash('sha256');
  for (const name of files) {
    const content = createHash('sha256');
    for await (const chunk of createReadStream(path.join(root, name))) content.update(chunk);
    checksum.update(name); checksum.update('\0'); checksum.update(content.digest());
  }
  return { source_root: root, revision, sha256: checksum.digest('hex'), files: files.length };
}

if (process.argv[1] && fileURLToPath(import.meta.url) === path.resolve(process.argv[1])) {
  const directory = process.argv[2];
  (directory ? lookupSourceIdentity(directory, process.argv[3] ?? null) : Promise.reject(new Error('source root is required')))
    .then(value => console.log(JSON.stringify(value)), error => { console.error(error instanceof Error ? error.message : 'source identity failed'); process.exitCode = 1; });
}
