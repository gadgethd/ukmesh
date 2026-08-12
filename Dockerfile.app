# App frontend — React SPA served via Nginx, proxies /api and /ws to backend
FROM node:26-alpine@sha256:aadf416b2cdce311a8811ba3f0608a61b77dbf997500e2eafe781b51f6a0b019 AS frontend-builder
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

FROM nginx:alpine@sha256:4a73073bd557c65b759505da037898b61f1be6cbcc3c2c3aeac22d2a470c1752
ARG SOURCE_REVISION=unknown
LABEL org.opencontainers.image.revision="${SOURCE_REVISION}" \
      org.opencontainers.image.source="https://github.com/gadgethd/ukmesh"
COPY --from=frontend-builder /build/frontend/dist /usr/share/nginx/html
COPY nginx.app.conf /etc/nginx/conf.d/default.conf
COPY nginx.security-headers.conf /etc/nginx/snippets/security-headers.conf
RUN chown -R nginx:nginx /var/cache/nginx /etc/nginx/conf.d \
 && chown nginx:nginx /run
USER 101:101
EXPOSE 8080
