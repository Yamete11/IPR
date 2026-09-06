#!/usr/bin/env bash
# Builds the six service images with Docker and loads them into the Kind cluster (no registry).
#   deploy/scripts/build-images.sh                       # all services
#   deploy/scripts/build-images.sh ingestion-service     # a subset
set -euo pipefail

CLUSTER_NAME="${CLUSTER_NAME:-smartcity}"
TAG="${TAG:-dev}"
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
# BuildKit is needed for the Gradle cache mounts in the Dockerfiles (Docker Desktop's default).
export DOCKER_BUILDKIT=1

SERVICES=("$@")
if [ ${#SERVICES[@]} -eq 0 ]; then
  SERVICES=(topology-service ingestion-service anomaly-detector-service
            ai-incident-responder-service incident-api-service sensor-simulator)
fi

for svc in "${SERVICES[@]}"; do
  image="smartcity/${svc}:${TAG}"
  printf '\033[36m==> docker build %s\033[0m\n' "$image"
  docker build -t "$image" "$ROOT/$svc"
  printf '\033[36m==> kind load %s\033[0m\n' "$image"
  kind load docker-image "$image" --name "$CLUSTER_NAME"
done
