<#
.SYNOPSIS
  Deletes the Kind cluster and everything in it (images, volumes, releases).
#>
[CmdletBinding()]
param([string]$ClusterName = "smartcity")
$ErrorActionPreference = "Stop"
kind delete cluster --name $ClusterName
