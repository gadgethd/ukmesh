# App frontend — React SPA served via Nginx, proxies /api and /ws to backend
FROM node:20-alpine AS frontend-builder
WORKDIR /build/frontend
COPY frontend/package.json ./
RUN npm install
COPY frontend/ ./
ARG VITE_APP_HOSTNAME
ARG VITE_NETWORK=teesside
ARG VITE_SITE=teesside
ARG VITE_OBSERVER_ID=
ARG VITE_SITE_DISPLAY_NAME=
ARG VITE_SITE_FOOTER_NAME=
ARG VITE_SITE_APP_URL=
ARG VITE_SITE_HOME_URL=
ENV VITE_APP_HOSTNAME=$VITE_APP_HOSTNAME
ENV VITE_NETWORK=$VITE_NETWORK
ENV VITE_SITE=$VITE_SITE
ENV VITE_OBSERVER_ID=$VITE_OBSERVER_ID
ENV VITE_SITE_DISPLAY_NAME=$VITE_SITE_DISPLAY_NAME
ENV VITE_SITE_FOOTER_NAME=$VITE_SITE_FOOTER_NAME
ENV VITE_SITE_APP_URL=$VITE_SITE_APP_URL
ENV VITE_SITE_HOME_URL=$VITE_SITE_HOME_URL
RUN npm run build

FROM nginx:alpine
COPY --from=frontend-builder /build/frontend/dist /usr/share/nginx/html
COPY nginx.app.conf /etc/nginx/conf.d/default.conf
EXPOSE 80
