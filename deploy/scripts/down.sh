#!/usr/bin/env bash
# Deletes the Kind cluster and everything in it (images, volumes, releases).
set -euo pipefail
kind delete cluster --name "${CLUSTER_NAME:-smartcity}"
