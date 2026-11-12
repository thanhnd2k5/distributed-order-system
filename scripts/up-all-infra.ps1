Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'
[Console]::OutputEncoding = [System.Text.UTF8Encoding]::new($false)

$repoRoot = Split-Path -Parent $PSScriptRoot
$infraDir = Join-Path $repoRoot 'infra'

Write-Output 'Ensuring order-replication-net Docker network exists...'
& docker network inspect order-replication-net *> $null
if ($LASTEXITCODE -ne 0) {
    & docker network create order-replication-net | Out-Host
}

$stacks = @(
    @{ File = 'compose.base.yaml'; Service = 'pos01-db' },
    @{ File = 'compose.pos02.yaml'; Service = 'pos02-db' },
    @{ File = 'compose.online.yaml'; Service = 'online-db' },
    @{ File = 'compose.central.yaml'; Service = 'central-db' },
    @{ File = 'compose.inventory.yaml'; Service = 'inventory-db' },
    @{ File = 'compose.kitchen.yaml'; Service = 'kitchen-db' }
)

foreach ($stack in $stacks) {
    $path = Join-Path $infraDir $stack.File
    Write-Output ("Starting {0}..." -f $stack.Service)
    & docker compose -f $path up -d --wait $stack.Service | Out-Host
    if ($LASTEXITCODE -ne 0) {
        exit $LASTEXITCODE
    }
}

$kafkaCompose = Join-Path $infraDir 'compose.kafka.yaml'
Write-Output 'Starting 3-broker Kafka cluster...'
& docker compose -f $kafkaCompose up -d --wait | Out-Host
if ($LASTEXITCODE -ne 0) {
    exit $LASTEXITCODE
}

Write-Output 'Ensuring Kafka topics (order-events, status-events)...'
& docker compose -f $kafkaCompose run --rm kafka-init | Out-Host
if ($LASTEXITCODE -ne 0) {
    exit $LASTEXITCODE
}

Write-Output 'All infrastructure containers are UP and healthy.'
