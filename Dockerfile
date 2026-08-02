# Stage 1: Build frontend
FROM node:25-alpine@sha256:bdf2cca6fe3dabd014ea60163eca3f0f7015fbd5c7ee1b0e9ccb4ced6eb02ef4 AS frontend-builder
WORKDIR /build/frontend
COPY frontend/package.json frontend/package-lock.json ./
RUN npm ci
COPY frontend/ ./
ARG VITE_APP_HOSTNAME
ENV VITE_APP_HOSTNAME=$VITE_APP_HOSTNAME
RUN npm run build

# Stage 2: Build backend
FROM node:25-alpine@sha256:bdf2cca6fe3dabd014ea60163eca3f0f7015fbd5c7ee1b0e9ccb4ced6eb02ef4 AS backend-builder
WORKDIR /build/backend
COPY backend/package.json backend/package-lock.json ./
RUN npm ci --audit
COPY backend/ ./
RUN npm run build

# Stage 3: Runtime
FROM node:25-alpine@sha256:bdf2cca6fe3dabd014ea60163eca3f0f7015fbd5c7ee1b0e9ccb4ced6eb02ef4 AS runtime
ARG SOURCE_REVISION=unknown
LABEL org.opencontainers.image.revision="${SOURCE_REVISION}" \
      org.opencontainers.image.source="https://github.com/gadgethd/ukmesh"
WORKDIR /app

# Install production deps only
COPY backend/package.json backend/package-lock.json ./
RUN npm ci --omit=dev --audit \
  && npm cache clean --force \
  && rm -rf /usr/local/lib/node_modules/npm \
  && rm -f /usr/local/bin/npm /usr/local/bin/npx
RUN mkdir -p /var/lib/meshcore-alerts && chown 1000:1000 /var/lib/meshcore-alerts

# Copy compiled backend
COPY --from=backend-builder /build/backend/dist ./dist

# Copy static database assets (not emitted by tsc)
COPY --from=backend-builder /build/backend/src/db/schema ./dist/db/schema
COPY --from=backend-builder /build/backend/src/db/migrations ./dist/db/migrations
COPY --from=backend-builder /build/backend/src/db/owner-auth.sql ./dist/db/owner-auth.sql
COPY --from=backend-builder /build/backend/src/backend-site/*.html ./dist/backend-site/
COPY docs/openapi.yaml ./openapi.yaml

# Copy frontend build into static dir served by backend
COPY --from=frontend-builder /build/frontend/dist ./public

EXPOSE 3000 9091
USER 1000:1000
CMD ["node", "dist/index.js"]
