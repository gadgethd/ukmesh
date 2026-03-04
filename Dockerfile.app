# App frontend — React SPA served via Nginx, proxies /api and /ws to backend
FROM node:20-alpine AS frontend-builder
WORKDIR /build/frontend
COPY frontend/package.json ./
RUN npm install
COPY frontend/ ./
ARG VITE_APP_HOSTNAME
ARG VITE_NETWORK=teesside
ENV VITE_APP_HOSTNAME=$VITE_APP_HOSTNAME
ENV VITE_NETWORK=$VITE_NETWORK
RUN npm run build

FROM nginx:alpine
COPY --from=frontend-builder /build/frontend/dist /usr/share/nginx/html
COPY nginx.app.conf /etc/nginx/conf.d/default.conf
EXPOSE 80
