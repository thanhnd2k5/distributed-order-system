param(
    [Parameter(Mandatory = $true)]
    [ValidateSet('NORMAL', 'LAST_PORTION', 'MISSING_RICE', 'OFFLINE_LIMITED')]
    [string]$Preset,

    [ValidatePattern('^[A-Za-z_][A-Za-z0-9_]*$')]
    [string]$Schema = 'inventory'
)

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'
[Console]::OutputEncoding = [System.Text.UTF8Encoding]::new($false)

$inventoryPorts = @(8084, 18084)
$listeners = [System.Net.NetworkInformation.IPGlobalProperties]::GetIPGlobalProperties().GetActiveTcpListeners()
$busyPorts = @($listeners | Where-Object { $_.Port -in $inventoryPorts } | Select-Object -ExpandProperty Port -Unique | Sort-Object)
if ($busyPorts.Count -gt 0) {
    Write-Error ("Refusing reset: inventory service port(s) still listening: {0}. Stop inventory-service instances before resetting." -f ($busyPorts -join ', '))
    exit 1
}

$presets = @{
    NORMAL          = @{ GAO = 1000L; THIT_GA = 1500L }
    LAST_PORTION    = @{ GAO = 1000L; THIT_GA = 150L }
    MISSING_RICE    = @{ GAO = 50L; THIT_GA = 1500L }
    OFFLINE_LIMITED = @{ GAO = 1000L; THIT_GA = 150L }
}

$target = $presets[$Preset]
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

$repoRoot = Split-Path -Parent $PSScriptRoot
$composeFile = Join-Path $repoRoot 'infra/compose.inventory.yaml'

Invoke-DockerCompose @('up', '-d', '--wait', 'inventory-db') | Out-Host
if ($LASTEXITCODE -ne 0) {
    exit $LASTEXITCODE
}

$sql = @"
\set ON_ERROR_STOP on
DO `$`$
BEGIN
    IF to_regclass('$Schema.ingredients') IS NULL THEN
        RAISE EXCEPTION 'Schema "%" is not initialized with inventory tables yet. Run inventory-service once to apply Flyway migrations.', '$Schema';
    END IF;
END
`$`$;
BEGIN;
SET LOCAL search_path TO $Schema;
DELETE FROM outbox_events;
DELETE FROM reservation_items;
DELETE FROM inventory_order_decisions;
DELETE FROM processed_events;
UPDATE ingredients SET available_quantity = $($target.GAO), updated_at = now() WHERE ingredient_id = 'GAO';
UPDATE ingredients SET available_quantity = $($target.THIT_GA), updated_at = now() WHERE ingredient_id = 'THIT_GA';
COMMIT;
SELECT ingredient_id, name, unit, available_quantity, updated_at FROM $Schema.ingredients ORDER BY ingredient_id;
SELECT
    (SELECT count(*) FROM $Schema.inventory_order_decisions) AS decisions,
    (SELECT count(*) FROM $Schema.reservation_items) AS reservations,
    (SELECT count(*) FROM $Schema.processed_events) AS processed_events,
    (SELECT count(*) FROM $Schema.outbox_events) AS outbox_events;
"@

Write-Output ("Resetting schema '{0}' to preset {1} (GAO={2}, THIT_GA={3})..." -f $Schema, $Preset, $target.GAO, $target.THIT_GA)
$prev = $ErrorActionPreference
$ErrorActionPreference = 'Continue'
try {
    $sql | & docker compose -f $composeFile exec -T inventory-db psql -v ON_ERROR_STOP=1 -U inventory_app -d inventory_db
}
finally {
    $ErrorActionPreference = $prev
}
exit $LASTEXITCODE
