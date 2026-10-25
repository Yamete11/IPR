<#
.SYNOPSIS
  Demonstrates the Kafka-lag autoscaling: starts the ~1000-sensor load and watches KEDA scale
  ingestion-service out; -Stop scales the load back to zero so it scales in again.

.EXAMPLE
  .\deploy\scripts\demo-hpa.ps1                 # start load, watch for 3 minutes
  .\deploy\scripts\demo-hpa.ps1 -Watch 600      # watch longer
  .\deploy\scripts\demo-hpa.ps1 -Stop           # stop the load
#>
[CmdletBinding()]
param(
    [string]$Namespace = "smartcity",
    [int]$Watch = 180,
    [switch]$Stop
)
# Native commands (docker, kind, kubectl) write progress to stderr, which Windows PowerShell 5.1 turns
# into terminating errors under "Stop" once output is redirected; failures are caught via $LASTEXITCODE.
$ErrorActionPreference = "Continue"

if ($Stop) {
    kubectl scale deployment/sensor-simulator --namespace $Namespace --replicas=0
    Write-Host "Load stopped. ingestion-service scales back to 1 replica after the cooldown (120 s)."
    exit 0
}

kubectl scale deployment/sensor-simulator --namespace $Namespace --replicas=1
Write-Host "Load started (~1000 sensors, 100 ms interval). Watching for $Watch s; Ctrl+C to stop watching." -ForegroundColor Cyan

$deadline = (Get-Date).AddSeconds($Watch)
while ((Get-Date) -lt $deadline) {
    Write-Host ""
    Write-Host ("--- " + (Get-Date -Format "HH:mm:ss")) -ForegroundColor DarkGray
    kubectl get hpa --namespace $Namespace
    kubectl get deployment/ingestion-service --namespace $Namespace
    # Consumer-group lag straight from the broker (the number KEDA scales on).
    kubectl exec --namespace $Namespace smartcity-dual-role-0 -c kafka -- `
        bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 --describe --group ingestion-service 2>$null |
        Select-String -Pattern "TOPIC|city-telemetry" | ForEach-Object { $_.Line }
    Start-Sleep -Seconds 15
}
Write-Host ""
Write-Host "Done watching. Stop the load with: .\deploy\scripts\demo-hpa.ps1 -Stop" -ForegroundColor Green
