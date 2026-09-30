import { encryptPacketShareSecret } from '../owner/packetShareCrypto.js';

const chunks: Buffer[] = [];
process.stdin.on('data', (chunk: Buffer | string) => {
  chunks.push(Buffer.isBuffer(chunk) ? chunk : Buffer.from(chunk));
});
process.stdin.on('end', () => {
  try {
    const secret = Buffer.concat(chunks).toString('utf8').replace(/\r?\n$/, '');
    process.stdout.write(`${encryptPacketShareSecret(secret)}\n`);
  } catch (error) {
    console.error(error instanceof Error ? error.message : error);
    process.exitCode = 1;
  }
});
