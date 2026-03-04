/// <reference types="vite/client" />

interface ImportMetaEnv {
  readonly VITE_APP_HOSTNAME: string;
  readonly VITE_SITE:         string; // 'teesside' | 'ukmesh'
  readonly VITE_NETWORK:      string; // 'teesside' | 'ukmesh'
}

interface ImportMeta {
  readonly env: ImportMetaEnv;
}
