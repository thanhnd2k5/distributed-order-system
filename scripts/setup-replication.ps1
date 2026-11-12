param(
    [switch]$EnableExisting
)

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'
[Console]::OutputEncoding = [System.Text.UTF8Encoding]::new($false)

$repoRoot = Split-Path -Parent $PSScriptRoot
$baseCompose = Join-Path $repoRoot 'infra/compose.base.yaml'
$pos02Compose = Join-Path $repoRoot 'infra/compose.pos02.yaml'
$onlineCompose = Join-Path $repoRoot 'infra/compose.online.yaml'
$centralCompose = Join-Path $repoRoot 'infra/compose.central.yaml'

function Invoke-Compose {
    param([string]$ComposeFile, [string[]]$ComposeArgs)
    $prev = $ErrorActionPreference
    $ErrorActionPreference = 'Continue'
    try {
        & docker compose -f $ComposeFile @ComposeArgs
    }
    finally {
        $ErrorActionPreference = $prev
    }
}

function Invoke-Psql {
    param(
        [string]$ComposeFile,
        [string]$Service,
        [string]$User,
        [string]$Db,
        [string]$Sql
    )
    $prev = $ErrorActionPreference
    $ErrorActionPreference = 'Continue'
    try {
        $Sql | & docker compose -f $ComposeFile exec -T $Service psql -v ON_ERROR_STOP=1 -U $User -d $Db
    }
    finally {
        $ErrorActionPreference = $prev
    }
    if ($LASTEXITCODE -ne 0) {
        throw "psql failed on service $Service ($Db)"
    }
}

Write-Output '1. Ensuring Docker replication network (order-replication-net)...'
& docker network inspect order-replication-net *> $null
if ($LASTEXITCODE -ne 0) {
    & docker network create order-replication-net | Out-Host
}

Write-Output '2. Ensuring all publisher DBs and central-db are up and healthy...'
Invoke-Compose $baseCompose @('up', '-d', '--wait', 'pos01-db') | Out-Host
Invoke-Compose $pos02Compose @('up', '-d', '--wait', 'pos02-db') | Out-Host
Invoke-Compose $onlineCompose @('up', '-d', '--wait', 'online-db') | Out-Host
Invoke-Compose $centralCompose @('up', '-d', '--wait', 'central-db') | Out-Host

$publishers = @(
    @{ Name = 'POS01'; Compose = $baseCompose; Service = 'pos01-db'; Db = 'pos01_orders'; Schema = 'pos01'; Pub = 'pos01_pub'; Sub = 'pos01_sub' },
    @{ Name = 'POS02'; Compose = $pos02Compose; Service = 'pos02-db'; Db = 'pos02_orders'; Schema = 'pos02'; Pub = 'pos02_pub'; Sub = 'pos02_sub' },
    @{ Name = 'ONLINE'; Compose = $onlineCompose; Service = 'online-db'; Db = 'online_orders'; Schema = 'online'; Pub = 'online_pub'; Sub = 'online_sub' }
)

Write-Output '3. Checking wal_level, pg_hba replication rules, tables, and publications...'
foreach ($pub in $publishers) {
    $wal = (& docker compose -f $pub.Compose exec -T $pub.Service psql -U order_app -d $pub.Db -tAc "SHOW wal_level;").Trim()
    if ($wal -ne 'logical') {
        throw "$($pub.Service) wal_level is '$wal', expected 'logical'."
    }

    $hasRepRule = (& docker compose -f $pub.Compose exec -T $pub.Service psql -U order_app -d $pub.Db -tAc "SELECT count(*) FROM pg_hba_file_rules WHERE type = 'host' AND database = '{replication}' AND address = 'all';").Trim()
    if ([int]$hasRepRule -eq 0) {
        $hbaFile = (& docker compose -f $pub.Compose exec -T $pub.Service psql -U order_app -d $pub.Db -tAc "SHOW hba_file;").Trim()
        Write-Output "Adding remote replication rule to $hbaFile on $($pub.Service)..."
        & docker compose -f $pub.Compose exec -T $pub.Service sh -c "echo 'host replication all all scram-sha-256' >> '$hbaFile'"
        Invoke-Psql $pub.Compose $pub.Service 'order_app' $pub.Db "SELECT pg_reload_conf();" | Out-Null
    }

    $tableCheck = (& docker compose -f $pub.Compose exec -T $pub.Service psql -U order_app -d $pub.Db -tAc "SELECT to_regclass('$($pub.Schema).orders') IS NOT NULL AND to_regclass('$($pub.Schema).order_items') IS NOT NULL;").Trim()
    if ($tableCheck -ne 't') {
        throw "Tables in $($pub.Service) schema '$($pub.Schema)' are not migrated yet. Start order-service for $($pub.Name) once first."
    }

    $pubSql = @"
DO `$`$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_publication WHERE pubname = '$($pub.Pub)') THEN
        CREATE PUBLICATION $($pub.Pub) FOR TABLE $($pub.Schema).orders, $($pub.Schema).order_items;
    END IF;
END
`$`$;
"@
    Invoke-Psql $pub.Compose $pub.Service 'order_app' $pub.Db $pubSql | Out-Null
    Write-Output "  [OK] $($pub.Name): wal_level=logical, pg_hba replication ready, publication $($pub.Pub) ready."
}

Write-Output '4. Checking central-db schemas and creating subscriptions...'
$centralCheck = (& docker compose -f $centralCompose exec -T central-db psql -U central_app -d central_orders_db -tAc "SELECT to_regclass('pos01.orders') IS NOT NULL AND to_regclass('pos02.orders') IS NOT NULL AND to_regclass('online.orders') IS NOT NULL AND to_regclass('reporting.all_orders') IS NOT NULL;").Trim()
if ($centralCheck -ne 't') {
    throw 'Central DB schemas/views are not migrated yet. Start order-query-service once first.'
}

foreach ($pub in $publishers) {
    $subExists = (& docker compose -f $centralCompose exec -T central-db psql -U central_app -d central_orders_db -tAc "SELECT count(*) FROM pg_subscription WHERE subname = '$($pub.Sub)';").Trim()
    if ([int]$subExists -eq 0) {
        $connStr = "host=$($pub.Service) port=5432 dbname=$($pub.Db) user=order_app password=order_demo"
        $subSql = "CREATE SUBSCRIPTION $($pub.Sub) CONNECTION '$connStr' PUBLICATION $($pub.Pub);"
        Invoke-Psql $centralCompose 'central-db' 'central_app' 'central_orders_db' $subSql | Out-Null
        Write-Output "  [CREATED] Subscription $($pub.Sub) -> $($pub.Service)/$($pub.Pub)"
    } else {
        $enabled = (& docker compose -f $centralCompose exec -T central-db psql -U central_app -d central_orders_db -tAc "SELECT subenabled FROM pg_subscription WHERE subname = '$($pub.Sub)';").Trim()
        if ($EnableExisting -and $enabled -ne 't') {
            Invoke-Psql $centralCompose 'central-db' 'central_app' 'central_orders_db' "ALTER SUBSCRIPTION $($pub.Sub) ENABLE;" | Out-Null
            Write-Output "  [EXISTS] Subscription $($pub.Sub) was disabled; enabled via -EnableExisting."
        } elseif ($enabled -eq 't') {
            Write-Output "  [EXISTS] Subscription $($pub.Sub) already enabled (left unchanged)."
        } else {
            Write-Output "  [EXISTS] Subscription $($pub.Sub) is DISABLED (left unchanged; pass -EnableExisting to turn on)."
        }
    }
}

Write-Output '5. Waiting for initial sync of all 6 replicated tables...'
$deadline = (Get-Date).AddSeconds(30)
while ($true) {
    $readyCount = (& docker compose -f $centralCompose exec -T central-db psql -U central_app -d central_orders_db -tAc "SELECT count(*) FROM pg_subscription_rel WHERE srsubstate IN ('r', 's');").Trim()
    if ([int]$readyCount -ge 6) {
        break
    }
    if ((Get-Date) -gt $deadline) {
        Invoke-Psql $centralCompose 'central-db' 'central_app' 'central_orders_db' "SELECT s.subname, r.srrelid::regclass, r.srsubstate FROM pg_subscription_rel r JOIN pg_subscription s ON s.oid = r.srsubid;"
        throw "Timed out waiting for all 6 subscription relations to reach ready state (got $readyCount)."
    }
    Start-Sleep -Milliseconds 500
}

Write-Output '=== Central Subscription Status ==='
Invoke-Psql $centralCompose 'central-db' 'central_app' 'central_orders_db' "SELECT subname, subenabled FROM pg_subscription ORDER BY subname;"
Invoke-Psql $centralCompose 'central-db' 'central_app' 'central_orders_db' "SELECT subname, pid, relid, received_lsn, latest_end_lsn, latest_end_time FROM pg_stat_subscription WHERE relid IS NULL ORDER BY subname;"
Invoke-Psql $centralCompose 'central-db' 'central_app' 'central_orders_db' "SELECT s.subname, r.srrelid::regclass AS table_name, r.srsubstate AS state FROM pg_subscription_rel r JOIN pg_subscription s ON s.oid = r.srsubid ORDER BY s.subname, table_name;"
