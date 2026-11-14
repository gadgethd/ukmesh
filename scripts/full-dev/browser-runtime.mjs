import os from 'node:os';
import path from 'node:path';

export function browserLaunchOptions() {
  const libraries = path.join(os.homedir(), '.local/lib/ukmesh-tools/browser-libs');
  return {headless: true, executablePath: process.env.PLAYWRIGHT_CHROMIUM_EXECUTABLE_PATH,
    args: ['--no-sandbox', '--use-gl=angle', '--use-angle=swiftshader', '--enable-unsafe-swiftshader'],
    env: {PATH: process.env.PATH, HOME: os.homedir(), LANG: 'C.UTF-8',
      LD_LIBRARY_PATH: [path.join(libraries, 'usr/lib/x86_64-linux-gnu'), path.join(libraries, 'lib/x86_64-linux-gnu')].join(':')}};
}
