/// <reference types="vite/client" />

interface ImportMetaEnv {
  readonly VITE_APP_HOSTNAME: string;
  readonly VITE_SITE:         string; // 'ukmesh' | 'dev'
  readonly VITE_NETWORK:      string; // 'ukmesh' | 'test'
  readonly VITE_OBSERVER_ID: string;
  readonly VITE_SITE_DISPLAY_NAME: string;
  readonly VITE_SITE_FOOTER_NAME: string;
  readonly VITE_SITE_APP_URL: string;
  readonly VITE_SITE_HOME_URL: string;
  readonly VITE_RF_COVERAGE_ENABLED: string | undefined;
  readonly VITE_PACKET_ARCS_ENABLED: string | undefined;
  readonly VITE_HEATMAP_ENABLED: string | undefined;
}

interface ImportMeta {
  readonly env: ImportMetaEnv;
}
