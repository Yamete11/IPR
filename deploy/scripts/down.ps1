<#
.SYNOPSIS
  Deletes the Kind cluster and everything in it (images, volumes, releases).
#>
[CmdletBinding()]
param([string]$ClusterName = "smartcity")
# Native commands (docker, kind, kubectl) write progress to stderr, which Windows PowerShell 5.1 turns
# into terminating errors under "Stop" once output is redirected; failures are caught via $LASTEXITCODE.
$ErrorActionPreference = "Continue"
kind delete cluster --name $ClusterName
exit $LASTEXITCODE
