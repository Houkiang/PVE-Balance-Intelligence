#!/usr/bin/env bash
set -euo pipefail

ROOT_PATH="/game_balance"
DRY_RUN=0

usage() {
  cat <<'EOF'
Usage: scripts/init_hdfs_dirs.sh [options]

Options:
  --root-path PATH   HDFS project root. Default: /game_balance
  --dry-run          Print hdfs commands without executing them.
  -h, --help         Show this help.
EOF
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --root-path)
      ROOT_PATH="$2"
      shift 2
      ;;
    --dry-run)
      DRY_RUN=1
      shift
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "Unknown argument: $1" >&2
      usage
      exit 1
      ;;
  esac
done

if [[ "$DRY_RUN" -eq 0 ]] && ! command -v hdfs >/dev/null 2>&1; then
  echo "hdfs command was not found. Please install Hadoop or run this script in a Hadoop environment." >&2
  exit 1
fi

run_hdfs() {
  echo "hdfs dfs $*"
  if [[ "$DRY_RUN" -eq 0 ]]; then
    hdfs dfs "$@"
  fi
}

PATHS=(
  "$ROOT_PATH/ods/game_event_log"
  "$ROOT_PATH/dwd/game_event_detail"
  "$ROOT_PATH/dwd/player_event_detail"
  "$ROOT_PATH/dwd/player_wave_stat_detail"
  "$ROOT_PATH/dwd/card_pick_detail"
  "$ROOT_PATH/dwd/position_tick_detail"
  "$ROOT_PATH/dwd/enemy_spawn_detail"
  "$ROOT_PATH/dws/match_summary"
  "$ROOT_PATH/dws/player_match_summary"
  "$ROOT_PATH/dws/player_card_set"
  "$ROOT_PATH/dws/map_grid_summary"
  "$ROOT_PATH/ads/hero_balance_report"
  "$ROOT_PATH/ads/map_grid_heatmap_report"
  "$ROOT_PATH/ads/card_strength_report"
  "$ROOT_PATH/ads/build_combination_report"
)

for path in "${PATHS[@]}"; do
  run_hdfs -mkdir -p "$path"
done

echo "HDFS directory initialization completed."
