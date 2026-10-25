#!/usr/bin/env bash
# Brings the whole Smart City system up on a local Kind cluster: Strimzi Kafka, KEDA, the two
# PostgreSQL instances and all six services, with credentials from .env turned into Secrets.
#
#   deploy/scripts/up.sh                 # everything from scratch
#   SKIP_IMAGES=1 deploy/scripts/up.sh   # redeploy charts without rebuilding images
set -euo pipefail

CLUSTER_NAME="${CLUSTER_NAME:-smartcity}"
NAMESPACE="${NAMESPACE:-smartcity}"
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$ROOT"

step() { printf '\n\033[36m==> %s\033[0m\n' "$1"; }

# .env is the single source for credentials locally (compose) and here (Secrets).
if [ -f .env ]; then
  set -a; . ./.env; set +a
fi
PG_USER="${POSTGRES_USER:-smartcity}"
PG_PASSWORD="${POSTGRES_PASSWORD:-smartcity}"
PG_DB="${POSTGRES_DB:-smartcity}"
API_KEY="${ANTHROPIC_API_KEY:-}"
CLAUDE_MOCK="${AI_CLAUDE_MOCK:-true}"
CLAUDE_MODEL="${AI_CLAUDE_MODEL:-claude-opus-5-5}"
if [ "$CLAUDE_MOCK" = "false" ] && [ -z "$API_KEY" ]; then
  echo "AI_CLAUDE_MOCK=false but ANTHROPIC_API_KEY is empty (set it in .env or the environment)" >&2
  exit 1
fi

if [ -z "${SKIP_CLUSTER:-}" ]; then
  step "Kind cluster '$CLUSTER_NAME'"
  if kind get clusters | grep -qx "$CLUSTER_NAME"; then
    echo "cluster already exists, reusing it"
  else
    kind create cluster --name "$CLUSTER_NAME" --config deploy/kind/kind-config.yaml
  fi
fi

step "Namespace '$NAMESPACE'"
kubectl create namespace "$NAMESPACE" --dry-run=client -o yaml | kubectl apply -f -

step "Strimzi operator"
helm upgrade --install strimzi oci://quay.io/strimzi-helm/strimzi-kafka-operator \
  --namespace "$NAMESPACE" --wait --timeout 10m

step "Kafka cluster (KRaft, single node) + topics"
kubectl apply -f deploy/strimzi/kafka.yaml
kubectl wait kafka/smartcity --namespace "$NAMESPACE" --for=condition=Ready --timeout=15m
kubectl apply -f deploy/strimzi/topics.yaml
kubectl wait kafkatopic --all --namespace "$NAMESPACE" --for=condition=Ready --timeout=5m

step "KEDA (Kafka-lag autoscaling)"
helm repo add kedacore https://kedacore.github.io/charts --force-update
helm repo update kedacore
helm upgrade --install keda kedacore/keda --namespace keda --create-namespace --wait --timeout 10m

if [ -z "${SKIP_IMAGES:-}" ]; then
  step "Service images (docker build + kind load)"
  CLUSTER_NAME="$CLUSTER_NAME" "$ROOT/deploy/scripts/build-images.sh"
fi

step "Helm release 'smartcity' (infra + services)"
helm dependency build deploy/charts/smartcity
SETS=(
  --set "smartcity-infra.postgres.user=$PG_USER"
  --set "smartcity-infra.postgres.password=$PG_PASSWORD"
  --set "smartcity-infra.config.POSTGRES_DB=$PG_DB"
  --set-string "smartcity-infra.config.AI_CLAUDE_MOCK=$CLAUDE_MOCK"
  --set "smartcity-infra.config.AI_CLAUDE_MODEL=$CLAUDE_MODEL"
)
if [ -n "$API_KEY" ]; then SETS+=(--set "smartcity-infra.anthropic.apiKey=$API_KEY"); fi
helm upgrade --install smartcity deploy/charts/smartcity --namespace "$NAMESPACE" "${SETS[@]}" --wait --timeout 20m

step "Status"
kubectl get pods --namespace "$NAMESPACE"
kubectl get kafkatopics --namespace "$NAMESPACE"
kubectl get scaledobject,hpa --namespace "$NAMESPACE"

printf '\n\033[32mUp. Next:\033[0m\n'
echo "  kubectl -n $NAMESPACE port-forward svc/incident-api-service 8081:8081   # then http://localhost:8081/graphql"
echo "  kubectl -n $NAMESPACE port-forward svc/topology-service 8080:8080       # then http://localhost:8080/graphql"
echo "  deploy/scripts/demo-hpa.sh                                              # start load, watch KEDA scale ingestion"
