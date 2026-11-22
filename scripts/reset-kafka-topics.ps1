Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'
[Console]::OutputEncoding = [System.Text.UTF8Encoding]::new($false)

$servicePorts = @(8081, 18081, 8082, 18082, 8083, 18083, 8084, 18084, 8085, 18085)
$listeners = [System.Net.NetworkInformation.IPGlobalProperties]::GetIPGlobalProperties().GetActiveTcpListeners()
$busyPorts = @($listeners | Where-Object { $_.Port -in $servicePorts } | Select-Object -ExpandProperty Port -Unique | Sort-Object)
if ($busyPorts.Count -gt 0) {
    Write-Error ("Refusing Kafka topic reset: service port(s) still listening: {0}. Stop order-service, inventory-service, and kitchen-service instances first." -f ($busyPorts -join ', '))
    exit 1
}

$repoRoot = Split-Path -Parent $PSScriptRoot
$composeFile = Join-Path $repoRoot 'infra/compose.kafka.yaml'
$bootstrap = 'kafka-1:9092,kafka-2:9092,kafka-3:9092'

$topicSpecs = @(
    @{ Name = 'order-events.POS01'; Partitions = 2 },
    @{ Name = 'order-events.POS02'; Partitions = 2 },
    @{ Name = 'order-events.ONLINE'; Partitions = 2 },
    @{ Name = 'cancel-events.POS01'; Partitions = 2 },
    @{ Name = 'cancel-events.POS02'; Partitions = 2 },
    @{ Name = 'cancel-events.ONLINE'; Partitions = 2 },
    @{ Name = 'status-events.POS01'; Partitions = 1 },
    @{ Name = 'status-events.POS02'; Partitions = 1 },
    @{ Name = 'status-events.ONLINE'; Partitions = 1 },
    @{ Name = 'kitchen-orders'; Partitions = 2 },
    @{ Name = 'stock-commands'; Partitions = 2 },
    @{ Name = 'menu-events'; Partitions = 1 },
    @{ Name = 'dlq'; Partitions = 1 }
)
$legacyTopics = @('order-events', 'status-events')
$allTopicsToDelete = @($topicSpecs | ForEach-Object { $_.Name }) + $legacyTopics

function Invoke-KafkaExec {
    param([string[]]$KafkaArgs)
    $prev = $ErrorActionPreference
    $ErrorActionPreference = 'Continue'
    try {
        & docker compose -f $composeFile exec -T kafka-1 @KafkaArgs
    }
    finally {
        $ErrorActionPreference = $prev
    }
}

Write-Output 'Ensuring 3-broker Kafka cluster is up and healthy...'
$prev = $ErrorActionPreference
$ErrorActionPreference = 'Continue'
try {
    & docker compose -f $composeFile up -d --wait
}
finally {
    $ErrorActionPreference = $prev
}
if ($LASTEXITCODE -ne 0) {
    exit $LASTEXITCODE
}

$groups = Invoke-KafkaExec @('/opt/kafka/bin/kafka-consumer-groups.sh', '--bootstrap-server', $bootstrap, '--list')
if ($LASTEXITCODE -ne 0) {
    exit $LASTEXITCODE
}
$groupsToDelete = @($groups | Where-Object {
    $_ -match '^(inventory|kitchen|order-status|order-menu)(-|$)'
})

foreach ($group in $groupsToDelete) {
    Write-Output ("Deleting consumer group '{0}' (waiting for any stale members to expire if recently terminated)..." -f $group)
    $groupDeadline = (Get-Date).AddSeconds(55)
    while ($true) {
        $delOut = Invoke-KafkaExec @('/opt/kafka/bin/kafka-consumer-groups.sh', '--bootstrap-server', $bootstrap, '--delete', '--group', $group) 2>&1 | ForEach-Object { $_.ToString() }
        $delText = $delOut -join "`n"
        if ($delText -match 'Deletion of requested consumer groups .* was successful' -or $delText -match 'GroupIdNotFoundException') {
            $delOut
            break
        }
        if ((Get-Date) -gt $groupDeadline) {
            $delOut
            Write-Error ("Timed out waiting to delete consumer group '{0}'." -f $group)
            exit 1
        }
        Start-Sleep -Seconds 2
    }
}

$existingTopics = Invoke-KafkaExec @('/opt/kafka/bin/kafka-topics.sh', '--bootstrap-server', $bootstrap, '--list')
if ($LASTEXITCODE -ne 0) {
    exit $LASTEXITCODE
}

foreach ($topic in $allTopicsToDelete) {
    if ($existingTopics -contains $topic) {
        Write-Output ("Deleting topic '{0}'..." -f $topic)
        Invoke-KafkaExec @('/opt/kafka/bin/kafka-topics.sh', '--bootstrap-server', $bootstrap, '--delete', '--if-exists', '--topic', $topic) | Out-Host
        if ($LASTEXITCODE -ne 0) {
            exit $LASTEXITCODE
        }
    }
}

$deadline = (Get-Date).AddSeconds(30)
while ($true) {
    $currentTopics = Invoke-KafkaExec @('/opt/kafka/bin/kafka-topics.sh', '--bootstrap-server', $bootstrap, '--list')
    if ($LASTEXITCODE -ne 0) {
        exit $LASTEXITCODE
    }
    $remaining = @($allTopicsToDelete | Where-Object { $currentTopics -contains $_ })
    if ($remaining.Count -eq 0) {
        break
    }
    if ((Get-Date) -gt $deadline) {
        Write-Error ("Timed out waiting for Kafka topic deletion: {0}" -f ($remaining -join ', '))
        exit 1
    }
    Start-Sleep -Milliseconds 500
}

foreach ($spec in $topicSpecs) {
    $topic = $spec.Name
    $partitions = $spec.Partitions
    Write-Output ("Creating topic '{0}' (partitions={1}, rf=3, min.insync.replicas=2, retention.ms=604800000)..." -f $topic, $partitions)
    Invoke-KafkaExec @(
        '/opt/kafka/bin/kafka-topics.sh',
        '--bootstrap-server', $bootstrap,
        '--create',
        '--topic', $topic,
        '--partitions', [string]$partitions,
        '--replication-factor', '3',
        '--config', 'min.insync.replicas=2',
        '--config', 'retention.ms=604800000'
    ) | Out-Host
    if ($LASTEXITCODE -ne 0) {
        exit $LASTEXITCODE
    }
}

$isrDeadline = (Get-Date).AddSeconds(30)
while ($true) {
    $allReady = $true
    foreach ($spec in $topicSpecs) {
        $topic = $spec.Name
        $expectedPartitions = $spec.Partitions
        $desc = Invoke-KafkaExec @('/opt/kafka/bin/kafka-topics.sh', '--bootstrap-server', $bootstrap, '--describe', '--topic', $topic)
        if ($LASTEXITCODE -ne 0) {
            exit $LASTEXITCODE
        }
        $partitionLines = @($desc | Where-Object { $_ -match 'Partition:\s+\d+' })
        if ($partitionLines.Count -ne $expectedPartitions) {
            $allReady = $false
            break
        }
        foreach ($line in $partitionLines) {
            if ($line -notmatch 'Leader:\s+\d+' -or $line -notmatch 'Isr:\s+([0-9,]+)') {
                $allReady = $false
                break
            }
            $isrNodes = $Matches[1].Split(',')
            if ($isrNodes.Count -lt 3) {
                $allReady = $false
                break
            }
        }
        if (-not $allReady) {
            break
        }
    }
    if ($allReady) {
        break
    }
    if ((Get-Date) -gt $isrDeadline) {
        Write-Error 'Timed out waiting for full ISR (3 brokers) on newly created topics.'
        exit 1
    }
    Start-Sleep -Milliseconds 500
}

foreach ($spec in $topicSpecs) {
    Write-Output ""
    Write-Output ("=== Describe topic: {0} ===" -f $spec.Name)
    Invoke-KafkaExec @('/opt/kafka/bin/kafka-topics.sh', '--bootstrap-server', $bootstrap, '--describe', '--topic', $spec.Name)
    if ($LASTEXITCODE -ne 0) {
        exit $LASTEXITCODE
    }
}
