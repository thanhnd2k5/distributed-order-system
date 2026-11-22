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
    NORMAL          = @{ GAO = 1000; THIT_GA = 1500; BANH_PHO = 1000; THIT_BO = 1000; HANH_LA = 500 }
    LAST_PORTION    = @{ GAO = 1000; THIT_GA = 150; BANH_PHO = 1000; THIT_BO = 1000; HANH_LA = 500 }
    MISSING_RICE    = @{ GAO = 50; THIT_GA = 1500; BANH_PHO = 1000; THIT_BO = 1000; HANH_LA = 500 }
    OFFLINE_LIMITED = @{ GAO = 1000; THIT_GA = 150; BANH_PHO = 1000; THIT_BO = 1000; HANH_LA = 500 }
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
    IF to_regclass('$Schema.stock') IS NULL AND to_regclass('$Schema.ingredients') IS NULL THEN
        RAISE EXCEPTION 'Schema "%" is not initialized with inventory tables yet. Run inventory-service once to apply Flyway migrations.', '$Schema';
    END IF;
END
`$`$;
BEGIN;
SET LOCAL search_path TO $Schema;
DELETE FROM outbox_events;
DELETE FROM processed_events;
DO `$`$
BEGIN
    IF to_regclass('$Schema.stock') IS NOT NULL THEN
        DELETE FROM inventory_reservations;
        DELETE FROM inventory_orders;
        IF to_regclass('$Schema.alerts') IS NOT NULL THEN DELETE FROM alerts; END IF;
        UPDATE stock SET available = $($target.GAO), reserved = 0, consumed = 0, wasted = 0, needs_check = FALSE, updated_at = now() WHERE ingredient_id = 'GAO';
        UPDATE stock SET available = $($target.THIT_GA), reserved = 0, consumed = 0, wasted = 0, needs_check = FALSE, updated_at = now() WHERE ingredient_id = 'THIT_GA';
        UPDATE stock SET available = $($target.BANH_PHO), reserved = 0, consumed = 0, wasted = 0, needs_check = FALSE, updated_at = now() WHERE ingredient_id = 'BANH_PHO';
        UPDATE stock SET available = $($target.THIT_BO), reserved = 0, consumed = 0, wasted = 0, needs_check = FALSE, updated_at = now() WHERE ingredient_id = 'THIT_BO';
        UPDATE stock SET available = $($target.HANH_LA), reserved = 0, consumed = 0, wasted = 0, needs_check = FALSE, updated_at = now() WHERE ingredient_id = 'HANH_LA';
        IF to_regclass('$Schema.menu_version') IS NOT NULL THEN
            UPDATE menu_version SET epoch = 1, seq = 1 WHERE id = TRUE;
        END IF;
        IF to_regclass('$Schema.dish_status') IS NOT NULL THEN
            UPDATE dish_status SET status = 'AVAILABLE', epoch = 1, seq = 1;
        END IF;
    ELSE
        DELETE FROM reservation_items;
        DELETE FROM inventory_order_decisions;
        UPDATE ingredients SET available_quantity = $($target.GAO), updated_at = now() WHERE ingredient_id = 'GAO';
        UPDATE ingredients SET available_quantity = $($target.THIT_GA), updated_at = now() WHERE ingredient_id = 'THIT_GA';
    END IF;
END
`$`$;
COMMIT;
SELECT
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
