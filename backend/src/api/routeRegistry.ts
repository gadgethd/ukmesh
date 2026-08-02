import type { Router } from 'express';

type ExpressLayer = {
  route?: {
    path?: string | string[];
    methods?: Record<string, boolean>;
  };
  handle?: {
    stack?: ExpressLayer[];
  };
};

export function listRegisteredRoutes(router: Router): string[] {
  const routes: string[] = [];

  const visit = (layers: ExpressLayer[] | undefined): void => {
    for (const layer of layers ?? []) {
      const route = layer.route;
      if (route?.path && route.methods) {
        const paths = Array.isArray(route.path) ? route.path : [route.path];
        const methods = Object.entries(route.methods)
          .filter(([, enabled]) => enabled)
          .map(([method]) => method.toUpperCase());
        for (const path of paths) {
          for (const method of methods) routes.push(`${method} ${path}`);
        }
      }
      visit(layer.handle?.stack);
    }
  };

  visit((router as Router & { stack?: ExpressLayer[] }).stack);
  return routes;
}

export function assertUniqueRouteRegistry(router: Router): void {
  const seen = new Set<string>();
  const duplicates = new Set<string>();
  for (const route of listRegisteredRoutes(router)) {
    if (seen.has(route)) duplicates.add(route);
    seen.add(route);
  }
  if (duplicates.size > 0) {
    throw new Error(
      `duplicate API method/path registration: ${[...duplicates].sort().join(', ')}`,
    );
  }
}
