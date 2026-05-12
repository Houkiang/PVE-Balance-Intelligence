#!/usr/bin/env bash
set -euo pipefail

LOCAL_PATH="data/local_raw/game_event_log"
HDFS_PATH="/game_balance/ods/game_event_log"
DATE=""
OVERWRITE=0
DRY_RUN=0

usage() {
  cat <<'EOF'
Usage: scripts/upload_to_hdfs.sh [options]

Options:
  --local-path PATH   Local root containing dt=YYYY-MM-DD partitions.
  --hdfs-path PATH    HDFS destination root.
  --date YYYY-MM-DD   Upload only one date partition.
  --overwrite         Replace existing events.jsonl on HDFS.
  --dry-run           Print hdfs commands without executing them.
  -h, --help          Show this help.
EOF
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --local-path)
      LOCAL_PATH="$2"
      shift 2
      ;;
    --hdfs-path)
      HDFS_PATH="$2"
      shift 2
      ;;
    --date)
      DATE="$2"
      shift 2
      ;;
    --overwrite)
      OVERWRITE=1
      shift
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

if [[ ! -d "$LOCAL_PATH" ]]; then
  echo "Local path does not exist: $LOCAL_PATH" >&2
  exit 1
fi

if [[ -n "$DATE" ]]; then
  PARTITIONS=("$LOCAL_PATH/dt=$DATE")
else
  mapfile -t PARTITIONS < <(find "$LOCAL_PATH" -maxdepth 1 -type d -name 'dt=*' | sort)
fi

if [[ "${#PARTITIONS[@]}" -eq 0 ]]; then
  echo "No dt=* partitions found under $LOCAL_PATH" >&2
  exit 1
fi

for partition in "${PARTITIONS[@]}"; do
  if [[ ! -f "$partition/events.jsonl" ]]; then
    echo "Skip partition without events.jsonl: $partition" >&2
    continue
  fi

  partition_name="$(basename "$partition")"
  remote_partition="$HDFS_PATH/$partition_name"
  run_hdfs -mkdir -p "$remote_partition"

  if [[ "$OVERWRITE" -eq 1 ]]; then
    run_hdfs -put -f "$partition/events.jsonl" "$remote_partition/"
  else
    run_hdfs -put "$partition/events.jsonl" "$remote_partition/"
  fi
done

echo "Upload completed."
