param(
    [Parameter(Mandatory = $true)]
    [AllowEmptyString()]
    [string]$Key,

    [ValidateRange(1, 1000)]
    [int]$Partitions = 3
)

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

# Matches org.apache.kafka.common.utils.Utils.murmur2(byte[]) and
# BuiltInPartitioner.partitionForKey(byte[], int) in kafka-clients 4.2.2.
[byte[]]$data = [System.Text.Encoding]::UTF8.GetBytes($Key)
[int]$length = $data.Length
[uint64]$mask = 4294967295      # 0xFFFFFFFF
[uint64]$seed = 2538058380      # 0x9747b28c
[uint64]$m = 1540483477         # 0x5bd1e995
[uint64]$posMask = 2147483647   # 0x7FFFFFFF
[uint64]$h = ($seed -bxor [uint64]$length) -band $mask

[int]$length4 = [int][Math]::Floor($length / 4)
for ($i = 0; $i -lt $length4; $i++) {
    [int]$i4 = $i * 4
    [uint64]$k = (
        ([uint64]$data[$i4]) -bor
        (([uint64]$data[$i4 + 1]) -shl 8) -bor
        (([uint64]$data[$i4 + 2]) -shl 16) -bor
        (([uint64]$data[$i4 + 3]) -shl 24)
    ) -band $mask

    $k = ($k * $m) -band $mask
    $k = ($k -bxor ($k -shr 24)) -band $mask
    $k = ($k * $m) -band $mask

    $h = ($h * $m) -band $mask
    $h = ($h -bxor $k) -band $mask
}

[int]$tailStart = $length4 * 4
[int]$remaining = $length - $tailStart
if ($remaining -ge 3) {
    $h = ($h -bxor (([uint64]$data[$tailStart + 2]) -shl 16)) -band $mask
}
if ($remaining -ge 2) {
    $h = ($h -bxor (([uint64]$data[$tailStart + 1]) -shl 8)) -band $mask
}
if ($remaining -ge 1) {
    $h = ($h -bxor ([uint64]$data[$tailStart])) -band $mask
    $h = ($h * $m) -band $mask
}

$h = ($h -bxor ($h -shr 13)) -band $mask
$h = ($h * $m) -band $mask
$h = ($h -bxor ($h -shr 15)) -band $mask

[int]$positive = [int]($h -band $posMask)
$positive % $Partitions
