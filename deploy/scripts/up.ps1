<#
.SYNOPSIS
  Brings the whole Smart City system up on a local Kind cluster: Strimzi Kafka, KEDA, the two
  PostgreSQL instances and all six services, with credentials from .env turned into Secrets.

.EXAMPLE
  .\deploy\scripts\up.ps1                # everything from scratch
  .\deploy\scripts\up.ps1 -SkipImages    # redeploy charts without rebuilding images
#>
[CmdletBinding()]
param(
    [string]$ClusterName = "smartcity",
    [string]$Namespace = "smartcity",
    [switch]$SkipCluster,
    [switch]$SkipImages
)

$ErrorActionPreference = "Stop"
$Root = (Resolve-Path (Join-Path $PSScriptRoot "..\..")).Path
Set-Location $Root

function Step([string]$Label, [scriptblock]$Body) {
    Write-Host ""
    Write-Host "==> $Label" -ForegroundColor Cyan
    $global:LASTEXITCODE = 0
    & $Body
    if ($LASTEXITCODE -ne 0) { throw "Step failed: $Label (exit $LASTEXITCODE)" }
}

# .env is the single source for credentials locally (compose) and here (Secrets).
$DotEnv = @{}
$EnvFile = Join-Path $Root ".env"
if (Test-Path $EnvFile) {
    foreach ($line in Get-Content $EnvFile) {
        if ($line -match '^\s*([A-Za-z_][A-Za-z0-9_]*)\s*=\s*(.*?)\s*$') { $DotEnv[$Matches[1]] = $Matches[2] }
    }
}
function Setting([string]$Name, [string]$Default) {
    if ($DotEnv.ContainsKey($Name) -and $DotEnv[$Name] -ne "") { return $DotEnv[$Name] }
    $fromEnv = [Environment]::GetEnvironmentVariable($Name)
    if ($fromEnv) { return $fromEnv }
    return $Default
}
$PgUser      = Setting "POSTGRES_USER" "smartcity"
$PgPassword  = Setting "POSTGRES_PASSWORD" "smartcity"
$PgDb        = Setting "POSTGRES_DB" "smartcity"
$ApiKey      = Setting "ANTHROPIC_API_KEY" ""
$ClaudeMock  = Setting "AI_CLAUDE_MOCK" "true"
$ClaudeModel = Setting "AI_CLAUDE_MODEL" "claude-opus-5-5"
if ($ClaudeMock -eq "false" -and -not $ApiKey) {
    throw "AI_CLAUDE_MOCK=false but ANTHROPIC_API_KEY is empty (set it in .env or the environment)"
}

if (-not $SkipCluster) {
    Step "Kind cluster '$ClusterName'" {
        $existing = @(kind get clusters)
        if ($existing -contains $ClusterName) {
            Write-Host "cluster already exists, reusing it"
        } else {
            kind create cluster --name $ClusterName --config deploy/kind/kind-config.yaml
        }
    }
}

Step "Namespace '$Namespace'" {
    kubectl create namespace $Namespace --dry-run=client -o yaml | kubectl apply -f -
}

Step "Strimzi operator" {
    helm upgrade --install strimzi oci://quay.io/strimzi-helm/strimzi-kafka-operator `
        --namespace $Namespace --wait --timeout 10m
}

Step "Kafka cluster (KRaft, single node) + topics" {
    kubectl apply -f deploy/strimzi/kafka.yaml
    kubectl wait kafka/smartcity --namespace $Namespace --for=condition=Ready --timeout=15m
    kubectl apply -f deploy/strimzi/topics.yaml
    kubectl wait kafkatopic --all --namespace $Namespace --for=condition=Ready --timeout=5m
}

Step "KEDA (Kafka-lag autoscaling)" {
    helm repo add kedacore https://kedacore.github.io/charts --force-update
    helm repo update kedacore
    helm upgrade --install keda kedacore/keda --namespace keda --create-namespace --wait --timeout 10m
}

if (-not $SkipImages) {
    Step "Service images (docker build + kind load)" {
        & (Join-Path $PSScriptRoot "build-images.ps1") -ClusterName $ClusterName
    }
}

Step "Helm release 'smartcity' (infra + services)" {
    helm dependency build deploy/charts/smartcity
    $sets = @(
        "--set", "smartcity-infra.postgres.user=$PgUser",
        "--set", "smartcity-infra.postgres.password=$PgPassword",
        "--set", "smartcity-infra.config.POSTGRES_DB=$PgDb",
        "--set-string", "smartcity-infra.config.AI_CLAUDE_MOCK=$ClaudeMock",
        "--set", "smartcity-infra.config.AI_CLAUDE_MODEL=$ClaudeModel"
    )
    if ($ApiKey) { $sets += @("--set", "smartcity-infra.anthropic.apiKey=$ApiKey") }
    helm upgrade --install smartcity deploy/charts/smartcity --namespace $Namespace @sets --wait --timeout 20m
}

Step "Status" {
    kubectl get pods --namespace $Namespace
    kubectl get kafkatopics --namespace $Namespace
    kubectl get scaledobject,hpa --namespace $Namespace
}

Write-Host ""
Write-Host "Up. Next:" -ForegroundColor Green
Write-Host "  kubectl -n $Namespace port-forward svc/incident-api-service 8081:8081   # then http://localhost:8081/graphql"
Write-Host "  kubectl -n $Namespace port-forward svc/topology-service 8080:8080       # then http://localhost:8080/graphql"
Write-Host "  .\deploy\scripts\demo-hpa.ps1                                           # start load, watch KEDA scale ingestion"
