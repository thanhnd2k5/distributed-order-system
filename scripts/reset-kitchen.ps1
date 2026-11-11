param(
    [ValidatePattern('^[A-Za-z_][A-Za-z0-9_]*$')]
    [string]$Schema = 'kitchen'
)

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'
[Console]::OutputEncoding = [System.Text.UTF8Encoding]::new($false)

$kitchenPorts = @(8085, 18085)
$listeners = [System.Net.NetworkInformation.IPGlobalProperties]::GetIPGlobalProperties().GetActiveTcpListeners()
$busyPorts = @($listeners | Where-Object { $_.Port -in $kitchenPorts } | Select-Object -ExpandProperty Port -Unique | Sort-Object)
if ($busyPorts.Count -gt 0) {
    Write-Error ("Refusing reset: kitchen service port(s) still listening: {0}. Stop kitchen-service instances before resetting." -f ($busyPorts -join ', '))
    exit 1
}

$repoRoot = Split-Path -Parent $PSScriptRoot
$composeFile = Join-Path $repoRoot 'infra/compose.kitchen.yaml'

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

Invoke-DockerCompose @('up', '-d', '--wait', 'kitchen-db') | Out-Host
if ($LASTEXITCODE -ne 0) {
    exit $LASTEXITCODE
}

$sql = @"
\set ON_ERROR_STOP on
DO `$`$
BEGIN
    IF to_regclass('$Schema.kitchen_tickets') IS NULL THEN
        RAISE EXCEPTION 'Schema "%" is not initialized with kitchen tables yet. Run kitchen-service once to apply Flyway migrations.', '$Schema';
    END IF;
END
`$`$;
BEGIN;
SET LOCAL search_path TO $Schema;
DELETE FROM outbox_events;
DELETE FROM kitchen_ticket_items;
DELETE FROM kitchen_tickets;
DELETE FROM processed_events;
COMMIT;
SELECT
    (SELECT count(*) FROM $Schema.kitchen_tickets) AS tickets,
    (SELECT count(*) FROM $Schema.kitchen_ticket_items) AS ticket_items,
    (SELECT count(*) FROM $Schema.processed_events) AS processed_events,
    (SELECT count(*) FROM $Schema.outbox_events) AS outbox_events;
"@

Write-Output ("Resetting kitchen schema '{0}'..." -f $Schema)
$prev = $ErrorActionPreference
$ErrorActionPreference = 'Continue'
try {
    $sql | & docker compose -f $composeFile exec -T kitchen-db psql -v ON_ERROR_STOP=1 -U kitchen_app -d kitchen_db
}
finally {
    $ErrorActionPreference = $prev
}
exit $LASTEXITCODE
