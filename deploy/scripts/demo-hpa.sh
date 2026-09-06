#!/usr/bin/env bash
# Demonstrates the Kafka-lag autoscaling: starts the ~1000-sensor load and watches KEDA scale
# ingestion-service out; `stop` scales the load back to zero so it scales in again.
#   deploy/scripts/demo-hpa.sh            # start load, watch for 3 minutes (WATCH=600 for longer)
#   deploy/scripts/demo-hpa.sh stop       # stop the load
set -euo pipefail
NAMESPACE="${NAMESPACE:-smartcity}"
WATCH="${WATCH:-180}"

if [ "${1:-}" = "stop" ]; then
  kubectl scale deployment/sensor-simulator --namespace "$NAMESPACE" --replicas=0
  echo "Load stopped. ingestion-service scales back to 1 replica after the cooldown (120 s)."
  exit 0
fi

kubectl scale deployment/sensor-simulator --namespace "$NAMESPACE" --replicas=1
printf '\033[36mLoad started (~1000 sensors, 100 ms interval). Watching for %ss; Ctrl+C to stop watching.\033[0m\n' "$WATCH"

end=$(( $(date +%s) + WATCH ))
while [ "$(date +%s)" -lt "$end" ]; do
  printf '\n--- %s\n' "$(date +%H:%M:%S)"
  kubectl get hpa --namespace "$NAMESPACE"
  kubectl get deployment/ingestion-service --namespace "$NAMESPACE"
  # Consumer-group lag straight from the broker (the number KEDA scales on).
  kubectl exec --namespace "$NAMESPACE" smartcity-dual-role-0 -c kafka -- \
    bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 --describe --group ingestion-service 2>/dev/null \
    | grep -E 'TOPIC|city-telemetry' || true
  sleep 15
done
printf '\n\033[32mDone watching. Stop the load with: deploy/scripts/demo-hpa.sh stop\033[0m\n'
