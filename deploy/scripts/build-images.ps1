<#
.SYNOPSIS
  Builds the six service images plus the dashboard with Docker and loads them into the Kind cluster (no registry).
#>
[CmdletBinding()]
param(
    [string]$ClusterName = "smartcity",
    [string]$Tag = "dev",
    [string[]]$Services = @(
        "topology-service", "ingestion-service", "anomaly-detector-service",
        "ai-incident-responder-service", "incident-api-service", "sensor-simulator", "dashboard")
)

$ErrorActionPreference = "Stop"
$Root = (Resolve-Path (Join-Path $PSScriptRoot "..\..")).Path
# BuildKit is needed for the Gradle cache mounts in the Dockerfiles (Docker Desktop's default).
$env:DOCKER_BUILDKIT = "1"

foreach ($svc in $Services) {
    $image = "smartcity/${svc}:$Tag"
    Write-Host "==> docker build $image" -ForegroundColor Cyan
    docker build -t $image (Join-Path $Root $svc)
    if ($LASTEXITCODE -ne 0) { throw "docker build failed for $svc" }
    Write-Host "==> kind load $image" -ForegroundColor Cyan
    kind load docker-image $image --name $ClusterName
    if ($LASTEXITCODE -ne 0) { throw "kind load failed for $svc" }
}
