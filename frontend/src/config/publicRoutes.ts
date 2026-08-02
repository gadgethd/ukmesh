export type PublicRouteComponent =
  | 'home'
  | 'feed'
  | 'repeater'
  | 'companion'
  | 'install'
  | 'docs'
  | 'health'
  | 'owner'
  | 'open-source'
  | 'stats'
  | 'spam'
  | 'topology';

export type PublicRouteDefinition = {
  path: string;
  component?: PublicRouteComponent;
  redirectTo?: string;
  title?: string;
  description?: string;
  sitemap: boolean;
};

export const PUBLIC_ROUTES: readonly PublicRouteDefinition[] = [
  {
    path: '/',
    component: 'home',
    title: 'UK Mesh Network — MeshCore LoRa Coverage & Live Map',
    description: 'Real-time analytics for the UK MeshCore LoRa mesh network. Live packet feed, repeater coverage maps, network statistics, and install guides.',
    sitemap: true,
  },
  {
    path: '/install',
    component: 'install',
    title: 'Install MeshCore — UK Mesh Network',
    description: 'Step-by-step guide to flash MeshCore firmware on a LoRa device and join the UK mesh network. No soldering, no special tools — just a browser and a USB cable.',
    sitemap: true,
  },
  {
    path: '/stats',
    component: 'stats',
    title: 'Network Statistics — UK Mesh Network',
    description: 'Live statistics for the UK MeshCore network: active nodes, packet counts, repeater uptime, and coverage trends.',
    sitemap: true,
  },
  {
    path: '/feed',
    component: 'feed',
    title: 'Live Packet Feed — UK Mesh Network',
    description: 'Real-time decoded LoRa packet stream from UK MeshCore observers. Watch adverts, messages, and traceroutes as they arrive.',
    sitemap: true,
  },
  {
    path: '/repeater',
    component: 'repeater',
    title: 'Repeater Search — UK Mesh Network',
    description: 'Search and browse MeshCore repeater nodes across the UK network. View coverage, uptime, and connection details.',
    sitemap: true,
  },
  {
    path: '/companion',
    component: 'companion',
    title: 'MeshCore Companion Activity — UK Mesh Network',
    description: 'Explore recent privacy-safe companion activity and learn how companion clients participate in the UK MeshCore network.',
    sitemap: true,
  },
  {
    path: '/docs',
    component: 'docs',
    title: 'Docs — UK Mesh Network',
    description: 'Radio settings, repeater placement, network etiquette, and troubleshooting guidance for UK MeshCore operators.',
    sitemap: true,
  },
  {
    path: '/open-source',
    component: 'open-source',
    title: 'Open Source — UK Mesh Network',
    description: 'Libraries and open-source technologies powering the UK Mesh analytics platform.',
    sitemap: true,
  },
  {
    path: '/spam',
    component: 'spam',
    title: 'Spam Watch — UK Mesh Network',
    description: 'Privacy-safe detection of suspected repeated-message clusters on the UK MeshCore network.',
    sitemap: true,
  },
  {
    path: '/topology',
    component: 'topology',
    title: 'Repeater Topology — UK Mesh Network',
    description: 'Explore recent viable repeater relationships, highly connected relay hubs, and observed MeshCore network topology across the UK.',
    sitemap: true,
  },
  {
    path: '/health',
    component: 'health',
    title: 'Platform Status — UK Mesh Network',
    description: 'Public platform health, packet-ingest freshness, synthetic journeys, and background-worker status for UKMesh analytics.',
    sitemap: true,
  },
  {
    path: '/login',
    component: 'owner',
    title: 'Repeater Owner Portal — UK Mesh Network',
    description: 'Log in to manage your MeshCore repeater node on the UK Mesh network.',
    sitemap: false,
  },
  { path: '/regions', redirectTo: '/', sitemap: false },
  { path: '/about', redirectTo: '/', sitemap: false },
  { path: '/mqtt', redirectTo: '/install', sitemap: false },
  { path: '/status', redirectTo: '/health', sitemap: false },
] as const;

export const PUBLIC_CONTENT_ROUTES = PUBLIC_ROUTES.filter(
  (route): route is PublicRouteDefinition & {
    component: PublicRouteComponent;
    title: string;
    description: string;
  } => Boolean(route.component && route.title && route.description),
);
