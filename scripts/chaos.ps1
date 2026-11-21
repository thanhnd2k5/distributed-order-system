param(
    [Parameter(Mandatory = $true, Position = 0)]
    [ValidateSet('offline', 'online', 'stop', 'start', 'cut-replication', 'restore-replication', 'status')]
    [string]$Action,

    [Parameter(Position = 1)]
    [string]$Target,

    [switch]$NetworkOnly
)

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'
[Console]::OutputEncoding = [System.Text.UTF8Encoding]::new($false)

$repoRoot = Split-Path -Parent $PSScriptRoot
$infraDir = Join-Path $repoRoot 'infra'

$servicesCompose = Join-Path $infraDir 'compose.services.yaml'
$kafkaCompose = Join-Path $infraDir 'compose.kafka.yaml'
$centralCompose = Join-Path $infraDir 'compose.central.yaml'
$baseCompose = Join-Path $infraDir 'compose.base.yaml'
$pos02Compose = Join-Path $infraDir 'compose.pos02.yaml'
$onlineCompose = Join-Path $infraDir 'compose.online.yaml'
$inventoryCompose = Join-Path $infraDir 'compose.inventory.yaml'
$kitchenCompose = Join-Path $infraDir 'compose.kitchen.yaml'
$testCompose = Join-Path $infraDir 'compose.test.yaml'

$targetMap = @{
    'kafka-1'             = @{ Compose = $kafkaCompose; Service = 'kafka-1' }
    'kafka1'              = @{ Compose = $kafkaCompose; Service = 'kafka-1' }
    'kafka-2'             = @{ Compose = $kafkaCompose; Service = 'kafka-2' }
    'kafka2'              = @{ Compose = $kafkaCompose; Service = 'kafka-2' }
    'kafka-3'             = @{ Compose = $kafkaCompose; Service = 'kafka-3' }
    'kafka3'              = @{ Compose = $kafkaCompose; Service = 'kafka-3' }
    'inventory-1'         = @{ Compose = $servicesCompose; Service = 'inventory-1' }
    'inventory-2'         = @{ Compose = $servicesCompose; Service = 'inventory-2' }
    'kitchen'             = @{ Compose = $servicesCompose; Service = 'kitchen' }
    'pos01'               = @{ Compose = $servicesCompose; Service = 'pos01' }
    'pos02'               = @{ Compose = $servicesCompose; Service = 'pos02' }
    'online'              = @{ Compose = $servicesCompose; Service = 'online' }
    'order-query-service' = @{ Compose = $servicesCompose; Service = 'order-query-service' }
    'query'               = @{ Compose = $servicesCompose; Service = 'order-query-service' }
    'pos01-db'            = @{ Compose = $baseCompose; Service = 'pos01-db' }
    'pos02-db'            = @{ Compose = $pos02Compose; Service = 'pos02-db' }
    'online-db'           = @{ Compose = $onlineCompose; Service = 'online-db' }
    'central-db'          = @{ Compose = $centralCompose; Service = 'central-db' }
    'inventory-db'        = @{ Compose = $inventoryCompose; Service = 'inventory-db' }
    'kitchen-db'          = @{ Compose = $kitchenCompose; Service = 'kitchen-db' }
    'test-db'             = @{ Compose = $testCompose; Service = 'test-db' }
}

$branchMap = @{
    'pos01'  = @{ Service = 'pos01'; DbCompose = $baseCompose; DbService = 'pos01-db'; Sub = 'pos01_sub' }
    'pos02'  = @{ Service = 'pos02'; DbCompose = $pos02Compose; DbService = 'pos02-db'; Sub = 'pos02_sub' }
    'online' = @{ Service = 'online'; DbCompose = $onlineCompose; DbService = 'online-db'; Sub = 'online_sub' }
}

function Require-Target {
    if ([string]::IsNullOrWhiteSpace($Target)) {
        throw "Action '$Action' requires a <target> argument."
    }
    return $Target.Trim().ToLowerInvariant()
}

function Invoke-Docker {
    param([string[]]$DockerArgs)
    $prev = $ErrorActionPreference
    $ErrorActionPreference = 'Continue'
    try {
        & docker @DockerArgs
    }
    finally {
        $ErrorActionPreference = $prev
    }
}

function Get-ContainerId {
    param([string]$ComposeFile, [string]$ServiceName)
    $id = (Invoke-Docker @('compose', '-f', $ComposeFile, 'ps', '-q', $ServiceName) | Out-String).Trim()
    if ([string]::IsNullOrWhiteSpace($id)) {
        throw "Container for service '$ServiceName' in '$ComposeFile' is not running. Start it first."
    }
    return $id
}

function Test-ContainerOnNetwork {
    param([string]$NetworkName, [string]$ContainerId)
    $containers = Invoke-Docker @('network', 'inspect', $NetworkName, '-f', '{{range $id, $v := .Containers}}{{println $id}}{{end}}')
    if ($LASTEXITCODE -ne 0) {
        throw "Docker network '$NetworkName' does not exist."
    }
    foreach ($line in @($containers)) {
        $trimmed = $line.Trim()
        if ($trimmed -eq $ContainerId -or ($trimmed.Length -ge 12 -and $ContainerId.Length -ge 12 -and $trimmed.Substring(0, 12) -eq $ContainerId.Substring(0, 12))) {
            return $true
        }
    }
    return $false
}

function Invoke-CentralSql {
    param([string]$Sql)
    $prev = $ErrorActionPreference
    $ErrorActionPreference = 'Continue'
    try {
        $Sql | & docker compose -f $centralCompose exec -T central-db psql -v ON_ERROR_STOP=1 -U central_app -d central_orders_db
    }
    finally {
        $ErrorActionPreference = $prev
    }
    if ($LASTEXITCODE -ne 0) {
        throw 'psql command on central-db failed.'
    }
}

switch ($Action) {
    'offline' {
        $key = Require-Target
        if (-not $branchMap.ContainsKey($key)) {
            throw "Unknown branch '$Target'. Expected: pos01, pos02, online."
        }
        $branch = $branchMap[$key]
        $containerId = Get-ContainerId $servicesCompose $branch.Service
        if (Test-ContainerOnNetwork 'kafka-net' $containerId) {
            Invoke-Docker @('network', 'disconnect', 'kafka-net', $containerId) | Out-Host
            if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }
            Write-Output ("[OFFLINE] Disconnected {0} ({1}) from kafka-net (DB and HTTP API remain reachable)." -f $branch.Service, $containerId)
        } else {
            Write-Output ("[OFFLINE] {0} ({1}) is already disconnected from kafka-net." -f $branch.Service, $containerId)
        }
    }

    'online' {
        $key = Require-Target
        if (-not $branchMap.ContainsKey($key)) {
            throw "Unknown branch '$Target'. Expected: pos01, pos02, online."
        }
        $branch = $branchMap[$key]
        $containerId = Get-ContainerId $servicesCompose $branch.Service
        if (-not (Test-ContainerOnNetwork 'kafka-net' $containerId)) {
            Invoke-Docker @('network', 'connect', 'kafka-net', $containerId) | Out-Host
            if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }
            Write-Output ("[ONLINE] Reconnected {0} ({1}) to kafka-net." -f $branch.Service, $containerId)
        } else {
            Write-Output ("[ONLINE] {0} ({1}) is already connected to kafka-net." -f $branch.Service, $containerId)
        }
    }

    'stop' {
        $key = Require-Target
        if (-not $targetMap.ContainsKey($key)) {
            throw "Unknown target '$Target'. Allowed: $($targetMap.Keys -join ', ')"
        }
        $entry = $targetMap[$key]
        Invoke-Docker @('compose', '-f', $entry.Compose, 'stop', $entry.Service) | Out-Host
        if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }
        Write-Output ("[STOPPED] {0}" -f $entry.Service)
    }

    'start' {
        $key = Require-Target
        if (-not $targetMap.ContainsKey($key)) {
            throw "Unknown target '$Target'. Allowed: $($targetMap.Keys -join ', ')"
        }
        $entry = $targetMap[$key]
        Invoke-Docker @('compose', '-f', $entry.Compose, 'up', '-d', '--wait', $entry.Service) | Out-Host
        if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }
        Write-Output ("[STARTED] {0}" -f $entry.Service)
    }

    'cut-replication' {
        $key = Require-Target
        if (-not $branchMap.ContainsKey($key)) {
            throw "Unknown branch '$Target'. Expected: pos01, pos02, online."
        }
        $branch = $branchMap[$key]
        if ($NetworkOnly) {
            $containerId = Get-ContainerId $branch.DbCompose $branch.DbService
            if (Test-ContainerOnNetwork 'order-replication-net' $containerId) {
                Invoke-Docker @('network', 'disconnect', 'order-replication-net', $containerId) | Out-Host
                if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }
                Write-Output ("[CUT-REPLICATION] Disconnected {0} from order-replication-net." -f $branch.DbService)
            } else {
                Write-Output ("[CUT-REPLICATION] {0} is already disconnected from order-replication-net." -f $branch.DbService)
            }
        } else {
            Invoke-CentralSql "ALTER SUBSCRIPTION $($branch.Sub) DISABLE;" | Out-Host
            Write-Output ("[CUT-REPLICATION] Disabled subscription {0} on central-db." -f $branch.Sub)
        }
    }

    'restore-replication' {
        $key = Require-Target
        if (-not $branchMap.ContainsKey($key)) {
            throw "Unknown branch '$Target'. Expected: pos01, pos02, online."
        }
        $branch = $branchMap[$key]
        if ($NetworkOnly) {
            $containerId = Get-ContainerId $branch.DbCompose $branch.DbService
            if (-not (Test-ContainerOnNetwork 'order-replication-net' $containerId)) {
                Invoke-Docker @('network', 'connect', '--alias', $branch.DbService, 'order-replication-net', $containerId) | Out-Host
                if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }
                Write-Output ("[RESTORE-REPLICATION] Reconnected {0} to order-replication-net." -f $branch.DbService)
            } else {
                Write-Output ("[RESTORE-REPLICATION] {0} is already connected to order-replication-net." -f $branch.DbService)
            }
        } else {
            Invoke-CentralSql "ALTER SUBSCRIPTION $($branch.Sub) ENABLE;" | Out-Host
            Write-Output ("[RESTORE-REPLICATION] Enabled subscription {0} on central-db." -f $branch.Sub)
        }
    }

    'status' {
        Write-Output '=== Docker containers ==='
        Invoke-Docker @('ps', '--format', 'table {{.Names}}\t{{.Status}}\t{{.Ports}}') | Out-Host
        Write-Output '=== Containers on kafka-net ==='
        Invoke-Docker @('network', 'inspect', 'kafka-net', '-f', '{{range $id, $c := .Containers}}{{.Name}} {{end}}') | Out-Host
        Write-Output ''
        $centralId = (Invoke-Docker @('compose', '-f', $centralCompose, 'ps', '-q', 'central-db') | Out-String).Trim()
        if (-not [string]::IsNullOrWhiteSpace($centralId)) {
            Write-Output '=== Central DB subscriptions ==='
            Invoke-CentralSql "SELECT subname, subenabled FROM pg_subscription ORDER BY subname;" | Out-Host
        }
    }
}
