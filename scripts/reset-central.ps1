Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'
[Console]::OutputEncoding = [System.Text.UTF8Encoding]::new($false)

$queryPorts = @(8080, 18080)
$listeners = [System.Net.NetworkInformation.IPGlobalProperties]::GetIPGlobalProperties().GetActiveTcpListeners()
$busyPorts = @($listeners | Where-Object { $_.Port -in $queryPorts } | Select-Object -ExpandProperty Port -Unique | Sort-Object)
if ($busyPorts.Count -gt 0) {
    Write-Error ("Refusing reset: order-query-service port(s) still listening: {0}. Stop order-query-service instances before resetting." -f ($busyPorts -join ', '))
    exit 1
}

& docker network inspect order-replication-net *> $null
if ($LASTEXITCODE -ne 0) {
    & docker network create order-replication-net | Out-Host
}

$repoRoot = Split-Path -Parent $PSScriptRoot
$composeFile = Join-Path $repoRoot 'infra/compose.central.yaml'

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

Invoke-DockerCompose @('up', '-d', '--wait', 'central-db') | Out-Host
if ($LASTEXITCODE -ne 0) {
    exit $LASTEXITCODE
}

# Drop subscriptions first so apply workers cannot race DELETE and leave the
# replica divergent. setup-replication.ps1 recreates them with a fresh copy.
$sql = @"
\set ON_ERROR_STOP on
DO `$`$
BEGIN
    IF to_regclass('pos01.orders') IS NULL OR to_regclass('pos02.orders') IS NULL OR to_regclass('online.orders') IS NULL THEN
        RAISE EXCEPTION 'Central replica schemas are not initialized yet. Run order-query-service once to apply Flyway migrations.';
    END IF;
END
`$`$;
DO `$`$
DECLARE
    sub text;
BEGIN
    -- DROP (not slot_name=NONE) so PostgreSQL also drops the replication slot on the publisher.
    FOREACH sub IN ARRAY ARRAY['pos01_sub', 'pos02_sub', 'online_sub'] LOOP
        IF EXISTS (SELECT 1 FROM pg_subscription WHERE subname = sub) THEN
            EXECUTE format('DROP SUBSCRIPTION %I', sub);
            RAISE NOTICE 'Dropped subscription %', sub;
        END IF;
    END LOOP;
END
`$`$;
BEGIN;
DELETE FROM pos01.order_items;
DELETE FROM pos01.orders;
DELETE FROM pos02.order_items;
DELETE FROM pos02.orders;
DELETE FROM online.order_items;
DELETE FROM online.orders;
COMMIT;
SELECT
    (SELECT count(*) FROM pos01.orders) AS pos01_orders,
    (SELECT count(*) FROM pos02.orders) AS pos02_orders,
    (SELECT count(*) FROM online.orders) AS online_orders,
    (SELECT count(*) FROM reporting.all_orders) AS all_orders;
"@

Write-Output 'Resetting central-db: dropping subscriptions, then clearing fragment tables...'
$prev = $ErrorActionPreference
$ErrorActionPreference = 'Continue'
try {
    $sql | & docker compose -f $composeFile exec -T central-db psql -v ON_ERROR_STOP=1 -U central_app -d central_orders_db
}
finally {
    $ErrorActionPreference = $prev
}
if ($LASTEXITCODE -ne 0) {
    exit $LASTEXITCODE
}

Write-Output ''
Write-Output 'Central fragments cleared and subscriptions removed.'
Write-Output 'If source DBs still hold rows, run .\scripts\setup-replication.ps1 to recreate subscriptions and re-copy.'
Write-Output 'If you also want an empty replica, reset POS01/POS02/ONLINE first, then run setup-replication.ps1.'
exit 0
