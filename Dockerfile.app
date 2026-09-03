# App frontend — React SPA served via Nginx, proxies /api and /ws to backend
FROM node:26-alpine@sha256:2d984a15c9b54fd0aeb608b8e0d0d83529eb34d2966db27a1fb4f1edc3d298a3 AS frontend-builder
WORKDIR /build/frontend
COPY frontend/package.json frontend/package-lock.json ./
RUN npm ci
COPY frontend/ ./
ARG VITE_APP_HOSTNAME
ARG VITE_BUILD_TARGET=app
ARG VITE_NETWORK=ukmesh
ARG VITE_SITE=ukmesh
ARG VITE_OBSERVER_ID=
ARG VITE_SITE_DISPLAY_NAME=
ARG VITE_SITE_FOOTER_NAME=
ARG VITE_SITE_APP_URL=
ARG VITE_SITE_HOME_URL=
ARG VITE_RF_COVERAGE_ENABLED=false
ENV VITE_APP_HOSTNAME=$VITE_APP_HOSTNAME
ENV VITE_BUILD_TARGET=$VITE_BUILD_TARGET
ENV VITE_NETWORK=$VITE_NETWORK
ENV VITE_SITE=$VITE_SITE
ENV VITE_OBSERVER_ID=$VITE_OBSERVER_ID
ENV VITE_SITE_DISPLAY_NAME=$VITE_SITE_DISPLAY_NAME
ENV VITE_SITE_FOOTER_NAME=$VITE_SITE_FOOTER_NAME
ENV VITE_SITE_APP_URL=$VITE_SITE_APP_URL
ENV VITE_SITE_HOME_URL=$VITE_SITE_HOME_URL
ENV VITE_RF_COVERAGE_ENABLED=$VITE_RF_COVERAGE_ENABLED
RUN npm run build

FROM nginx:alpine@sha256:db35bfc6b2951e7f8a72db5db120288c127ffaeeb4a6d4b95a26fead017d5913
ARG SOURCE_REVISION=unknown
LABEL org.opencontainers.image.revision="${SOURCE_REVISION}" \
      org.opencontainers.image.source="https://github.com/gadgethd/ukmesh"
COPY --from=frontend-builder /build/frontend/dist /usr/share/nginx/html
COPY nginx.app.conf /etc/nginx/conf.d/default.conf
COPY nginx.security-headers.conf /etc/nginx/snippets/security-headers.conf
# .mjs (ES module workers, e.g. maplibre-gl) must be served as JS, not the
# nginx default (application/octet-stream) — add the mapping inside mime.types.
RUN sed -i 's/^}$/    application\/javascript mjs;\n}/' /etc/nginx/mime.types
RUN chown -R nginx:nginx /var/cache/nginx /etc/nginx/conf.d \
 && chown nginx:nginx /run
USER 101:101
EXPOSE 8080
