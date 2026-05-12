param(
    [string]$LocalPath = "data/local_raw/game_event_log",
    [string]$HdfsPath = "/game_balance/ods/game_event_log",
    [string]$Date = "",
    [switch]$Overwrite,
    [switch]$DryRun
)

$ErrorActionPreference = "Stop"

function Invoke-Hdfs {
    param([string[]]$HdfsArgs)

    $display = "hdfs dfs " + ($HdfsArgs -join " ")
    Write-Host $display
    if (-not $DryRun) {
        & hdfs dfs @HdfsArgs
    }
}

if ((-not $DryRun) -and (-not (Get-Command hdfs -ErrorAction SilentlyContinue))) {
    throw "hdfs command was not found. Please install Hadoop or run this script in a Hadoop environment."
}

$localRoot = Resolve-Path -LiteralPath $LocalPath
$partitions = @()

if ($Date -ne "") {
    $target = Join-Path $localRoot "dt=$Date"
    if (-not (Test-Path -LiteralPath $target)) {
        throw "Local partition does not exist: $target"
    }
    $partitions = @(Get-Item -LiteralPath $target)
} else {
    $partitions = Get-ChildItem -LiteralPath $localRoot -Directory -Filter "dt=*"
}

if ($partitions.Count -eq 0) {
    throw "No dt=* partitions found under $localRoot"
}

foreach ($partition in $partitions) {
    $eventsFile = Join-Path $partition.FullName "events.jsonl"
    if (-not (Test-Path -LiteralPath $eventsFile)) {
        Write-Warning "Skip partition without events.jsonl: $($partition.FullName)"
        continue
    }

    $remotePartition = "$HdfsPath/$($partition.Name)"
    Invoke-Hdfs @("-mkdir", "-p", $remotePartition)

    if ($Overwrite) {
        Invoke-Hdfs @("-put", "-f", $eventsFile, "$remotePartition/")
    } else {
        Invoke-Hdfs @("-put", $eventsFile, "$remotePartition/")
    }
}

Write-Host "Upload completed."
