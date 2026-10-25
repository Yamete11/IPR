# syntax=docker/dockerfile:1.7
# Multi-stage build for the Angular dashboard: Node builds the static bundle, an unprivileged nginx
# serves it and reverse-proxies the two GraphQL backends under the same origin (no CORS).
#
#   docker build -t smartcity/dashboard:dev .
#
# Upstreams are filled into nginx.conf.template from TOPOLOGY_UPSTREAM / INCIDENT_API_UPSTREAM by the
# image's envsubst entrypoint; the defaults match the in-cluster Service names of the Helm charts.

FROM node:22-alpine AS build
WORKDIR /workspace
# npm 11: npm 10's arborist crashes resolving vitest's optional peer deps ("reading 'edgesOut'").
RUN npm install -g npm@11 --quiet
COPY package.json package-lock.json ./
RUN --mount=type=cache,target=/root/.npm,id=npm-dashboard npm ci --no-audit --no-fund
COPY angular.json tsconfig.json tsconfig.app.json ./
COPY public ./public
COPY src ./src
RUN npx ng build --configuration production

FROM nginxinc/nginx-unprivileged:1.29-alpine
ENV TOPOLOGY_UPSTREAM=http://topology-service:8080 \
    INCIDENT_API_UPSTREAM=http://incident-api-service:8081
COPY nginx.conf.template /etc/nginx/templates/default.conf.template
COPY --from=build /workspace/dist/dashboard/browser /usr/share/nginx/html
# The base image runs as the nginx user (UID 101) and listens on 8080.
EXPOSE 8080
