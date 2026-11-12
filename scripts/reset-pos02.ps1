param(
    [ValidatePattern('^[A-Za-z_][A-Za-z0-9_]*$')]
    [string]$Schema = 'pos02'
)

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'
[Console]::OutputEncoding = [System.Text.UTF8Encoding]::new($false)

$orderPorts = @(8082, 18082)
$listeners = [System.Net.NetworkInformation.IPGlobalProperties]::GetIPGlobalProperties().GetActiveTcpListeners()
$busyPorts = @($listeners | Where-Object { $_.Port -in $orderPorts } | Select-Object -ExpandProperty Port -Unique | Sort-Object)
if ($busyPorts.Count -gt 0) {
    Write-Error ("Refusing reset: POS02 order service port(s) still listening: {0}. Stop POS02 order-service instances before resetting." -f ($busyPorts -join ', '))
    exit 1
}

& docker network inspect order-replication-net *> $null
if ($LASTEXITCODE -ne 0) {
    & docker network create order-replication-net | Out-Host
}

$repoRoot = Split-Path -Parent $PSScriptRoot
$composeFile = Join-Path $repoRoot 'infra/compose.pos02.yaml'

function Invoke-DockerCompose {
    param([string[]]$ComposeArgs)
    $prev = $ErrorActionPreference
    $ErrorActionPreference = 'Continue'
    try {
        & docker compose -f $composeFile @ComposeArgs
    }
    finally {
        $ErrorActionPreference = $prev
    }
}

Invoke-DockerCompose @('up', '-d', '--wait', 'pos02-db') | Out-Host
if ($LASTEXITCODE -ne 0) {
    exit $LASTEXITCODE
}

$sql = @"
\set ON_ERROR_STOP on
DO `$`$
BEGIN
    IF to_regclass('$Schema.orders') IS NULL THEN
        RAISE EXCEPTION 'Schema "%" is not initialized with order tables yet. Run order-service once to apply Flyway migrations.', '$Schema';
    END IF;
END
`$`$;
BEGIN;
SET LOCAL search_path TO $Schema;
DELETE FROM outbox_events;
DELETE FROM order_items;
DELETE FROM orders;
DELETE FROM processed_events;
COMMIT;
SELECT
    (SELECT count(*) FROM $Schema.orders) AS orders,
    (SELECT count(*) FROM $Schema.order_items) AS order_items,
    (SELECT count(*) FROM $Schema.processed_events) AS processed_events,
    (SELECT count(*) FROM $Schema.outbox_events) AS outbox_events;
"@

Write-Output ("Resetting order schema '{0}' on pos02-db..." -f $Schema)
$prev = $ErrorActionPreference
$ErrorActionPreference = 'Continue'
try {
    $sql | & docker compose -f $composeFile exec -T pos02-db psql -v ON_ERROR_STOP=1 -U order_app -d pos02_orders
}
finally {
    $ErrorActionPreference = $prev
}
exit $LASTEXITCODE
