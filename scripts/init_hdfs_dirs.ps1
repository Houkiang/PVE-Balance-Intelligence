param(
    [string]$RootPath = "/game_balance",
    [switch]$DryRun
)

$ErrorActionPreference = "Stop"

if ((-not $DryRun) -and (-not (Get-Command hdfs -ErrorAction SilentlyContinue))) {
    throw "hdfs command was not found. Please install Hadoop or run this script in a Hadoop environment."
}

$paths = @(
    "$RootPath/ods/game_event_log",
    "$RootPath/dwd/game_event_detail",
    "$RootPath/dwd/player_event_detail",
    "$RootPath/dwd/player_wave_stat_detail",
    "$RootPath/dwd/card_pick_detail",
    "$RootPath/dwd/position_tick_detail",
    "$RootPath/dwd/enemy_spawn_detail",
    "$RootPath/dws/match_summary",
    "$RootPath/dws/player_match_summary",
    "$RootPath/dws/player_card_set",
    "$RootPath/dws/map_grid_summary",
    "$RootPath/ads/hero_balance_report",
    "$RootPath/ads/map_grid_heatmap_report",
    "$RootPath/ads/card_strength_report",
    "$RootPath/ads/build_combination_report"
)

foreach ($path in $paths) {
    Write-Host "hdfs dfs -mkdir -p $path"
    if (-not $DryRun) {
        & hdfs dfs -mkdir -p $path
    }
}

Write-Host "HDFS directory initialization completed."
