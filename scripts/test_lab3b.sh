#!/usr/bin/env bash
set -uo pipefail

usage() {
  cat <<'USAGE'
Usage:
  scripts/test_lab3b.sh [all|lab3a|lab3b|smoke|conf|split]

Modes:
  all     Run Lab3A, then every Lab3B raftstore test. This is the default.
  lab3a   Run only Lab3A raft tests.
  lab3b   Run only every Lab3B raftstore test.
  smoke   Run Lab3A plus the quickest Lab3B sanity checks.
  conf    Run Lab3A plus Lab3B conf-change/leader-transfer tests.
  split   Run Lab3A plus Lab3B split tests.

Environment:
  RUNS=20           Repeat the selected suite this many times.
  FAIL_FAST=0       Set to 1 to stop at the first failing case.
  TIMEOUT=20m       go test timeout for each case.
  LOG_LEVEL=fatal   TinyKV log level. Use debug when investigating.
  TEST_LOG_DIR=...  Directory for per-case logs. Defaults to /tmp.
  SKIP_3A=0         Set to 1 to skip Lab3A for all/conf/split/smoke.

Examples:
  scripts/test_lab3b.sh
  scripts/test_lab3b.sh smoke
  FAIL_FAST=1 scripts/test_lab3b.sh lab3b
  RUNS=3 TIMEOUT=30m scripts/test_lab3b.sh all
  RUNS=1 scripts/test_lab3b.sh smoke
USAGE
}

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT" || exit 1

MODE="${1:-all}"
if [[ "$MODE" == "-h" || "$MODE" == "--help" ]]; then
  usage
  exit 0
fi

if [[ $# -gt 1 ]]; then
  echo "error: expected at most one mode argument" >&2
  usage >&2
  exit 2
fi

RUNS="${RUNS:-20}"
FAIL_FAST="${FAIL_FAST:-0}"
TIMEOUT="${TIMEOUT:-20m}"
LOG_LEVEL="${LOG_LEVEL:-fatal}"
SKIP_3A="${SKIP_3A:-0}"

if ! [[ "$RUNS" =~ ^[0-9]+$ ]] || [[ "$RUNS" -lt 1 ]]; then
  echo "error: RUNS must be a positive integer" >&2
  exit 2
fi

if [[ "$FAIL_FAST" != "0" && "$FAIL_FAST" != "1" ]]; then
  echo "error: FAIL_FAST must be 0 or 1" >&2
  exit 2
fi

if [[ "$SKIP_3A" != "0" && "$SKIP_3A" != "1" ]]; then
  echo "error: SKIP_3A must be 0 or 1" >&2
  exit 2
fi

if ! command -v go >/dev/null 2>&1; then
  for go_bin_dir in /usr/local/go/bin /usr/lib/go/bin /snap/bin; do
    if [[ -x "${go_bin_dir}/go" ]]; then
      PATH="${go_bin_dir}:${PATH}"
      break
    fi
  done
fi

if ! command -v go >/dev/null 2>&1; then
  echo "error: go is not available in PATH" >&2
  exit 127
fi

LAB3B_CONF_TESTS=(
  TestTransferLeader3B
  TestBasicConfChange3B
  TestConfChangeRemoveLeader3B
  TestConfChangeRecover3B
  TestConfChangeRecoverManyClients3B
  TestConfChangeUnreliable3B
  TestConfChangeUnreliableRecover3B
  TestConfChangeSnapshotUnreliableRecover3B
  TestConfChangeSnapshotUnreliableRecoverConcurrentPartition3B
)

LAB3B_SPLIT_TESTS=(
  TestOneSplit3B
  TestSplitRecover3B
  TestSplitRecoverManyClients3B
  TestSplitUnreliable3B
  TestSplitUnreliableRecover3B
  TestSplitConfChangeSnapshotUnreliableRecover3B
  TestSplitConfChangeSnapshotUnreliableRecoverConcurrentPartition3B
)

LAB3B_SMOKE_TESTS=(
  TestTransferLeader3B
  TestBasicConfChange3B
  TestOneSplit3B
)

run_lab3a=0
lab3b_tests=()

case "$MODE" in
  all)
    run_lab3a=1
    lab3b_tests=("${LAB3B_CONF_TESTS[@]}" "${LAB3B_SPLIT_TESTS[@]}")
    ;;
  lab3a)
    run_lab3a=1
    ;;
  lab3b)
    lab3b_tests=("${LAB3B_CONF_TESTS[@]}" "${LAB3B_SPLIT_TESTS[@]}")
    ;;
  smoke)
    run_lab3a=1
    lab3b_tests=("${LAB3B_SMOKE_TESTS[@]}")
    ;;
  conf)
    run_lab3a=1
    lab3b_tests=("${LAB3B_CONF_TESTS[@]}")
    ;;
  split)
    run_lab3a=1
    lab3b_tests=("${LAB3B_SPLIT_TESTS[@]}")
    ;;
  *)
    echo "error: unknown mode '$MODE'" >&2
    usage >&2
    exit 2
    ;;
esac

if [[ "$SKIP_3A" == "1" ]]; then
  run_lab3a=0
fi

timestamp="$(date '+%Y%m%d-%H%M%S')"
log_dir="${TEST_LOG_DIR:-/tmp/tinykv-lab3-${timestamp}}"
mkdir -p "$log_dir" || exit 1
summary_file="$log_dir/summary.tsv"
printf "status\telapsed\tpackage\tpattern\tlabel\n" > "$summary_file"

passed_count=0
failed_count=0
failed_cases=()

cleanup_raftstore_tmp() {
  rm -rf /tmp/*test-raftstore*
}

trap cleanup_raftstore_tmp EXIT

safe_log_name() {
  printf "%s" "$1" | tr -c 'A-Za-z0-9_.-' '_'
}

run_case() {
  local round="$1"
  local label="$2"
  local pkg="$3"
  local pattern="$4"
  local safe_name
  local log_file
  local start
  local end
  local elapsed
  local status

  safe_name="$(safe_log_name "$label")"
  log_file="$log_dir/round-${round}-${safe_name}.log"

  printf "\n==> round %s/%s: %s\n" "$round" "$RUNS" "$label"
  printf "    LOG_LEVEL=%s go test -v --count=1 --parallel=1 -p=1 -timeout %s %s -run '%s'\n" \
    "$LOG_LEVEL" "$TIMEOUT" "$pkg" "$pattern"

  start="$(date +%s)"
  if LOG_LEVEL="$LOG_LEVEL" TZ=Asia/Shanghai GO111MODULE=on \
    go test -v --count=1 --parallel=1 -p=1 -timeout "$TIMEOUT" "$pkg" -run "$pattern" 2>&1 | tee "$log_file"; then
    status="PASS"
    passed_count=$((passed_count + 1))
  else
    status="FAIL"
    failed_count=$((failed_count + 1))
    failed_cases+=("round ${round}: ${label}")
  fi
  end="$(date +%s)"
  elapsed="$((end - start))s"

  printf "%s\t%s\t%s\t%s\t%s\n" "$status" "$elapsed" "$pkg" "$pattern" "$label" >> "$summary_file"
  printf "<== %s: %s (%s)\n" "$status" "$label" "$elapsed"

  if [[ "$status" == "FAIL" && "$FAIL_FAST" == "1" ]]; then
    return 99
  fi

  return 0
}

printf "TinyKV Lab3 test runner\n"
printf "mode: %s\n" "$MODE"
printf "runs: %s, fail_fast: %s, timeout: %s, log_level: %s\n" "$RUNS" "$FAIL_FAST" "$TIMEOUT" "$LOG_LEVEL"
printf "logs: %s\n" "$log_dir"

stop_now=0
for ((round = 1; round <= RUNS; round++)); do
  if [[ "$run_lab3a" == "1" ]]; then
    run_case "$round" "Lab3A raft package" "./raft" "3A"
    if [[ $? -eq 99 ]]; then
      stop_now=1
      break
    fi
  fi

  for test_name in "${lab3b_tests[@]}"; do
    cleanup_raftstore_tmp
    run_case "$round" "Lab3B ${test_name}" "./kv/test_raftstore" "^${test_name}$"
    if [[ $? -eq 99 ]]; then
      stop_now=1
      break
    fi
  done

  if [[ "$stop_now" == "1" ]]; then
    break
  fi
done

cleanup_raftstore_tmp

printf "\nSummary: %s passed, %s failed\n" "$passed_count" "$failed_count"
printf "summary file: %s\n" "$summary_file"

if [[ "$failed_count" -gt 0 ]]; then
  printf "\nFailed cases:\n"
  for failed_case in "${failed_cases[@]}"; do
    printf "  - %s\n" "$failed_case"
  done
  exit 1
fi

exit 0
