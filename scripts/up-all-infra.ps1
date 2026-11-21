param(
    [switch]$SkipTestDb
)

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'
[Console]::OutputEncoding = [System.Text.UTF8Encoding]::new($false)

$repoRoot = Split-Path -Parent $PSScriptRoot
$infraDir = Join-Path $repoRoot 'infra'

function Ensure-DockerNetwork {
    param([string]$NetworkName)
    $prev = $ErrorActionPreference
    $ErrorActionPreference = 'Continue'
    try {
        & docker network inspect $NetworkName 2>&1 | Out-Null
        if ($LASTEXITCODE -ne 0) {
            & docker network create $NetworkName | Out-Host
            if ($LASTEXITCODE -ne 0) {
                exit $LASTEXITCODE
            }
        }
    }
    finally {
        $ErrorActionPreference = $prev
    }
}

$networks = @(
    'order-replication-net',
    'kafka-net',
    'pos01-net',
    'pos02-net',
    'online-net',
    'inventory-net',
    'kitchen-net',
    'central-net',
    'api-net',
    'test-net'
)

Write-Output 'Ensuring isolated Docker networks exist...'
foreach ($net in $networks) {
    Ensure-DockerNetwork $net
}

$stacks = @(
    @{ File = 'compose.base.yaml'; Service = 'pos01-db' },
    @{ File = 'compose.pos02.yaml'; Service = 'pos02-db' },
    @{ File = 'compose.online.yaml'; Service = 'online-db' },
    @{ File = 'compose.central.yaml'; Service = 'central-db' },
    @{ File = 'compose.inventory.yaml'; Service = 'inventory-db' },
    @{ File = 'compose.kitchen.yaml'; Service = 'kitchen-db' }
)

if (-not $SkipTestDb) {
    $stacks += @{ File = 'compose.test.yaml'; Service = 'test-db' }
}

$prev = $ErrorActionPreference
$ErrorActionPreference = 'Continue'
try {
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

    Write-Output 'Ensuring v1.5 Kafka topics (POS01, POS02, ONLINE + internal topics)...'
    & docker compose -f $kafkaCompose run --rm kafka-init | Out-Host
    if ($LASTEXITCODE -ne 0) {
        exit $LASTEXITCODE
    }
}
finally {
    $ErrorActionPreference = $prev
}

Write-Output 'All infrastructure containers and v1.5 Kafka topics are UP and healthy.'
